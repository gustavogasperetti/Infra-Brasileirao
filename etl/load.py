"""
etl/load.py
===========
Camada Load — Publicação da One Big Table no Google Sheets.

Entrada  : data/gold/brasileirao_obt.csv  (ou um DataFrame já em memória)
Destino  : Planilha pública do Google Sheets (aba "partidas")

Autenticação (Service Account):
  1. GOOGLE_CREDENTIALS  → variável de ambiente com o CONTEÚDO JSON do
     credentials.json (é assim que o GitHub Actions injeta o Secret).
  2. Fallback local: arquivo apontado por GOOGLE_APPLICATION_CREDENTIALS
     ou ./credentials.json na raiz (apenas desenvolvimento — o arquivo é
     ignorado pelo git).

Estratégia de escrita:
  - overwrite (padrão): limpa a aba e regrava tudo — idempotente, a
    planilha sempre espelha exatamente a OBT gerada pela pipeline.
  - append (upsert incremental): compara a OBT com o conteúdo atual da
    aba usando a chave natural (ano_campeonato, Mandante, Visitante,
    Fase). Linhas modificadas (ex.: jogo futuro que ganhou placar,
    adiamento, correção) são ATUALIZADAS in-place; partidas inéditas
    são acrescentadas ao final; o restante não é tocado.
  - Upload em chunks para respeitar os limites de payload da API.

Uso:
  python etl/load.py

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 2.0.0
"""

import json
import logging
import os
import time
from collections import defaultdict, deque
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from config import SPREADSHEET_ID, WORKSHEET_PARTIDAS

# ---------------------------------------------------------------------------
# Configuração de Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caminhos e Constantes
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
OBT_CSV = BASE_DIR / "data" / "gold" / "brasileirao_obt.csv"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CHUNK_ROWS = 5_000  # linhas por requisição de escrita

# Chave natural de uma partida — estável mesmo com adiamentos (a rodada/
# fase não muda; o id_partida muda, pois é regenerado pela ordem das datas)
KEY_COLS = ["ano_campeonato", "Mandante", "Visitante", "Fase"]

# Acima deste nº de linhas modificadas, regravar tudo é mais eficiente
MAX_UPDATES_UPSERT = 2_000


# ===================================================================
# Autenticação
# ===================================================================

def obter_credenciais() -> Credentials:
    """
    Constrói as credenciais da Service Account.

    Prioridade:
      1. GOOGLE_CREDENTIALS (conteúdo JSON — Secrets do GitHub Actions)
      2. GOOGLE_APPLICATION_CREDENTIALS (caminho para o arquivo)
      3. ./credentials.json (fallback para desenvolvimento local)
    """
    raw = os.getenv("GOOGLE_CREDENTIALS", "").strip()
    if raw:
        logger.info("🔐 Autenticando via variável de ambiente GOOGLE_CREDENTIALS")
        try:
            info = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                "GOOGLE_CREDENTIALS não contém um JSON válido. "
                "O Secret deve receber o conteúdo integral do credentials.json."
            ) from e
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    caminho = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(BASE_DIR / "credentials.json"),
    )
    if Path(caminho).exists():
        logger.info(f"🔐 Autenticando via arquivo local: {caminho}")
        return Credentials.from_service_account_file(caminho, scopes=SCOPES)

    raise RuntimeError(
        "Nenhuma credencial encontrada. Defina a variável de ambiente "
        "GOOGLE_CREDENTIALS (conteúdo do credentials.json) ou disponibilize "
        "o arquivo credentials.json na raiz do projeto."
    )


def conectar_worksheet(creds: Credentials) -> gspread.Worksheet:
    """Abre a planilha pelo ID e retorna a aba de destino (cria se não existir)."""
    if not SPREADSHEET_ID:
        raise RuntimeError(
            "SPREADSHEET_ID não definido. Configure a variável de ambiente "
            "(Secret no GitHub Actions ou .env local)."
        )

    gc = gspread.authorize(creds)
    planilha = gc.open_by_key(SPREADSHEET_ID)
    logger.info(f"📄 Planilha conectada: '{planilha.title}'")

    try:
        ws = planilha.worksheet(WORKSHEET_PARTIDAS)
    except gspread.WorksheetNotFound:
        logger.info(f"   Aba '{WORKSHEET_PARTIDAS}' não existe — criando...")
        ws = planilha.add_worksheet(title=WORKSHEET_PARTIDAS, rows=1, cols=1)

    return ws


# ===================================================================
# Preparação dos dados
# ===================================================================

def preparar_valores(df: pd.DataFrame) -> list[list]:
    """
    Converte o DataFrame em uma matriz de valores serializáveis para a API:
      - datas   → string ISO (YYYY-MM-DD)
      - NA/NaN  → célula vazia
      - bool    → TRUE/FALSE
      - números → tipos nativos do Python
    """
    out = df.copy()

    for col in out.select_dtypes(include=["datetime64[ns]"]).columns:
        out[col] = out[col].dt.strftime("%Y-%m-%d")

    for col in out.select_dtypes(include=["bool"]).columns:
        out[col] = out[col].map({True: "TRUE", False: "FALSE"})

    # Nullable Int64 / float / string → objeto nativo, NA vira ""
    out = out.astype(object).where(out.notna(), "")

    return out.values.tolist()


# ===================================================================
# Diff OBT × Planilha (upsert)
# ===================================================================

def _normalizar_linha(linha: list, n_cols: int) -> list[str]:
    """
    Normaliza uma linha para comparação: tudo vira string sem espaços nas
    pontas, com padding de células vazias até n_cols (o Sheets omite
    células vazias no fim da linha).
    """
    linha = list(linha) + [""] * (n_cols - len(linha))
    return [str(c).strip() for c in linha[:n_cols]]


def calcular_diff(
    dados_sheet: list[list],
    valores_obt: list[list],
    header: list[str],
) -> tuple[list[tuple[int, list]], list[list], int, int]:
    """
    Compara o conteúdo atual da planilha com a OBT usando a chave natural
    (KEY_COLS). Partidas com a mesma chave que aparecem mais de uma vez
    (ex.: finais de ida e volta antigas) são pareadas na ordem em que
    ocorrem.

    Parâmetros:
      dados_sheet : linhas de dados da planilha (SEM o cabeçalho)
      valores_obt : linhas da OBT já serializadas (preparar_valores)
      header      : lista de colunas da OBT

    Retorna:
      atualizacoes : [(nº da linha no Sheets, valores novos), ...]
      novas        : linhas da OBT sem correspondência na planilha
      inalteradas  : contagem de linhas idênticas
      orfas        : linhas da planilha sem correspondência na OBT
    """
    idxs_chave = [header.index(c) for c in KEY_COLS]
    n_cols = len(header)

    def chave(linha_norm: list[str]) -> tuple:
        return tuple(linha_norm[i] for i in idxs_chave)

    # Indexa a planilha: chave → fila de (nº da linha, valores normalizados)
    existentes: dict[tuple, deque] = defaultdict(deque)
    for n, linha in enumerate(dados_sheet, start=2):  # linha 1 = cabeçalho
        norm = _normalizar_linha(linha, n_cols)
        existentes[chave(norm)].append((n, norm))

    atualizacoes: list[tuple[int, list]] = []
    novas: list[list] = []
    inalteradas = 0

    for row in valores_obt:
        norm = _normalizar_linha(row, n_cols)
        fila = existentes.get(chave(norm))
        if fila:
            n_linha, atual = fila.popleft()
            if atual != norm:
                atualizacoes.append((n_linha, row))
            else:
                inalteradas += 1
        else:
            novas.append(row)

    orfas = sum(len(fila) for fila in existentes.values())
    return atualizacoes, novas, inalteradas, orfas


# ===================================================================
# Escrita no Google Sheets
# ===================================================================

def _regravar_tudo(ws: gspread.Worksheet, header: list[str], valores: list[list]) -> None:
    """Limpa a aba e regrava cabeçalho + todos os dados em chunks."""
    logger.info(f"🗑️  Limpando a aba '{ws.title}' ...")
    ws.clear()

    # Redimensiona a grade para o tamanho exato dos dados (+1 do header)
    ws.resize(rows=len(valores) + 1, cols=len(header))

    logger.info(f"⬆️  Enviando {len(valores)} linhas em chunks de {CHUNK_ROWS} ...")
    ws.update(values=[header], range_name="A1")

    for inicio in range(0, len(valores), CHUNK_ROWS):
        chunk = valores[inicio: inicio + CHUNK_ROWS]
        primeira_linha = inicio + 2  # +1 do header, +1 pois Sheets é 1-indexed
        ws.update(
            values=chunk,
            range_name=f"A{primeira_linha}",
            value_input_option="RAW",
        )
        logger.info(f"   ... linhas {inicio + 1} a {inicio + len(chunk)} enviadas")


def _upsert(ws: gspread.Worksheet, header: list[str], valores: list[list]) -> None:
    """
    Sincroniza a aba com a OBT sem regravar tudo:
      - linhas modificadas (placar novo, adiamento, correção) → update in-place
      - partidas inéditas → append ao final
      - linhas idênticas → não são tocadas
    """
    dados = ws.get_all_values()

    # Aba vazia → carga completa
    if len(dados) <= 1:
        logger.info("   Aba vazia — realizando carga completa")
        _regravar_tudo(ws, header, valores)
        return

    # Cabeçalho divergente → o esquema mudou, upsert não é confiável
    sheet_header = [str(h).strip() for h in dados[0]]
    if sheet_header != header:
        logger.warning(
            "   ⚠ Cabeçalho da planilha difere da OBT (esquema mudou) — "
            "regravando tudo."
        )
        _regravar_tudo(ws, header, valores)
        return

    atualizacoes, novas, inalteradas, orfas = calcular_diff(dados[1:], valores, header)

    logger.info(
        f"   Diff: {inalteradas} inalteradas | {len(atualizacoes)} modificadas | "
        f"{len(novas)} novas | {orfas} órfãs na planilha"
    )

    if orfas:
        logger.warning(
            f"   ⚠ {orfas} linha(s) na planilha sem correspondência na OBT "
            "(não foram tocadas). Rode com LOAD_MODO=overwrite para limpar."
        )

    # Mudança em massa (ex.: ids deslocados por reordenação) → regravar
    # tudo custa menos requisições do que milhares de updates pontuais
    if len(atualizacoes) > MAX_UPDATES_UPSERT:
        logger.info(
            f"   {len(atualizacoes)} linhas modificadas (> {MAX_UPDATES_UPSERT}) — "
            "regravando tudo por eficiência."
        )
        _regravar_tudo(ws, header, valores)
        return

    if not atualizacoes and not novas:
        logger.info("✅ Planilha já está em dia — nada a fazer.")
        return

    if atualizacoes:
        logger.info(f"🔁 Atualizando {len(atualizacoes)} linha(s) modificada(s) ...")
        payload = [
            {"range": f"A{n_linha}", "values": [row]}
            for n_linha, row in atualizacoes
        ]
        # values.batchUpdate aceita várias faixas por chamada
        for inicio in range(0, len(payload), 500):
            ws.batch_update(payload[inicio: inicio + 500], value_input_option="RAW")
            logger.info(f"   ... {min(inicio + 500, len(payload))}/{len(payload)} updates enviados")

    if novas:
        logger.info(f"➕ Acrescentando {len(novas)} linha(s) nova(s) ...")
        for inicio in range(0, len(novas), CHUNK_ROWS):
            ws.append_rows(novas[inicio: inicio + CHUNK_ROWS], value_input_option="RAW")
            logger.info(f"   ... linhas {inicio + 1} a {min(inicio + CHUNK_ROWS, len(novas))} enviadas")


def carregar_para_sheets(df: pd.DataFrame, modo: str = "overwrite") -> None:
    """
    Publica o DataFrame final no Google Sheets.

    Parâmetros:
      df   : DataFrame processado (One Big Table)
      modo : 'overwrite' — limpa e regrava tudo (idempotente), ou
             'append'    — upsert incremental: atualiza in-place as linhas
                           que mudaram (jogo que ganhou placar, adiamento,
                           correção) e acrescenta apenas partidas inéditas,
                           casando OBT × planilha pela chave natural
                           (ano_campeonato, Mandante, Visitante, Fase).
    """
    if modo not in {"overwrite", "append"}:
        raise ValueError(f"Modo inválido: '{modo}'. Use 'overwrite' ou 'append'.")

    t0 = time.time()

    creds = obter_credenciais()
    ws = conectar_worksheet(creds)

    header = df.columns.tolist()
    valores = preparar_valores(df)
    logger.info(f"⚙️  Modo de escrita: {modo}")

    if modo == "overwrite":
        _regravar_tudo(ws, header, valores)
    else:
        _upsert(ws, header, valores)

    elapsed = time.time() - t0
    logger.info(f"✅ Carga concluída em {elapsed:.1f}s — modo '{modo}', aba '{ws.title}'")


# ===================================================================
# Pipeline Principal
# ===================================================================

def main() -> None:
    logger.info("=" * 60)
    logger.info("LOAD — One Big Table → Google Sheets")
    logger.info("=" * 60)

    if not OBT_CSV.exists():
        raise FileNotFoundError(
            f"❌ OBT não encontrada: {OBT_CSV}\n"
            f"Execute primeiro a camada Gold (python etl/gold.py)."
        )

    logger.info(f"📂 Carregando {OBT_CSV.name} ...")
    df = pd.read_csv(OBT_CSV, parse_dates=["Data"], encoding="utf-8-sig")

    # read_csv promove inteiros com nulos para float (1 → 1.0);
    # restaura Int64 nullable para não subir "1.0" na planilha
    for col in df.select_dtypes(include=["float"]).columns:
        if (df[col].dropna() % 1 == 0).all():
            df[col] = df[col].astype("Int64")

    logger.info(f"   {len(df)} partidas, {len(df.columns)} colunas")

    modo = os.getenv("LOAD_MODO", "overwrite").strip().lower()
    carregar_para_sheets(df, modo=modo)

    logger.info("Load finalizado.")


if __name__ == "__main__":
    main()
