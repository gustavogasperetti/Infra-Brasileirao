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
  - append: acrescenta linhas ao final (sem cabeçalho), para cargas
    incrementais.
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
# Escrita no Google Sheets
# ===================================================================

def carregar_para_sheets(df: pd.DataFrame, modo: str = "overwrite") -> None:
    """
    Publica o DataFrame final no Google Sheets.

    Parâmetros:
      df   : DataFrame processado (One Big Table)
      modo : 'overwrite' (limpa e regrava tudo — padrão) ou
             'append' (acrescenta linhas ao final, sem cabeçalho)
    """
    if modo not in {"overwrite", "append"}:
        raise ValueError(f"Modo inválido: '{modo}'. Use 'overwrite' ou 'append'.")

    t0 = time.time()

    creds = obter_credenciais()
    ws = conectar_worksheet(creds)

    header = df.columns.tolist()
    valores = preparar_valores(df)

    if modo == "overwrite":
        logger.info(f"🗑️  Modo overwrite — limpando a aba '{ws.title}' ...")
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

    else:  # append
        logger.info(f"⬆️  Modo append — acrescentando {len(valores)} linhas ...")
        for inicio in range(0, len(valores), CHUNK_ROWS):
            chunk = valores[inicio: inicio + CHUNK_ROWS]
            ws.append_rows(chunk, value_input_option="RAW")
            logger.info(f"   ... linhas {inicio + 1} a {inicio + len(chunk)} enviadas")

    elapsed = time.time() - t0
    logger.info(f"✅ Carga concluída em {elapsed:.1f}s — {len(valores)} linhas na aba '{ws.title}'")


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
