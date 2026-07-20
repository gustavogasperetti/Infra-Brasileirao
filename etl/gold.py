"""
etl/gold.py
===========
Camada Gold — One Big Table (OBT) do Brasileirão Histórico.

Entrada  : data/silver/jogos_{ano}.csv  (dados limpos — camada Silver)
Saída    : data/gold/brasileirao_obt.csv  (tabela única desnormalizada)

Filosofia da camada:
  A Gold entrega apenas a "fotografia" de cada partida — uma linha por
  jogo, com todas as informações legíveis por extenso (nomes de clubes e
  estados, sem IDs ou tabelas dimensão). Cálculos dinâmicos e temporais
  (confronto direto/H2H, forma dos últimos 5 jogos, sequências, fadiga,
  acumulados) NÃO são pré-calculados aqui: são responsabilidade da
  biblioteca Python que consumirá esta tabela.

Enriquecimentos por partida (não temporais):
  1. Métricas diretas do jogo (total_gols, saldo, pontos com regra
     histórica: 2 pts/vitória antes de 1995, 3 pts a partir de 1995)
  2. Classificação de fases (tipo_fase, is_mata_mata)
  3. Estado (UF) de mandante e visitante, por extenso na própria linha
  4. Fator clássico (is_classico_estadual — times do mesmo estado)

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 3.0.0
"""

import logging
from pathlib import Path

import pandas as pd

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
# Caminhos
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"
OBT_CSV = GOLD_DIR / "brasileirao_obt.csv"

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Fases classificadas como mata-mata
FASES_MATA_MATA = {"F", "SF", "QF", "1/8", "3/4", "POff"}

# Fases classificatórias (fase única / repescagem)
FASES_CLASSIFICATORIA = {"1F", "2F", "3F", "4F"}

# Fases que são rodadas de pontos corridos: R1, R2, ..., R46 e 1R
FASE_RODADA_PREFIX = "R"

# Mapeamento time → UF (estado) para todos os 167 times históricos
ESTADO_TIME: dict[str, str] = {
    "AA Colatina": "ES", "ABC": "RN", "ASA": "AL", "Alecrim": "RN",
    "America-RJ": "RJ", "Americano": "RJ", "América Mineiro": "MG",
    "América-RN": "RN", "América-SP": "SP", "Anapolina": "GO",
    "Anápolis": "GO", "Athletico Paranaense": "PR",
    "Atlético Goianiense": "GO", "Atlético Mineiro": "MG",
    "Auto Esporte-PI": "PI", "Avaí": "SC", "Bahia": "BA",
    "Bandeirante-DF": "DF", "Bangu": "RJ", "Baré": "RR",
    "Botafogo": "RJ", "Botafogo-PB": "PB", "Botafogo-SP": "SP",
    "Brasil de Pelotas": "RS", "Brasiliense": "DF", "Brasília": "DF",
    "CEUB": "DF", "CR Guará": "DF", "CRB": "AL", "CSA": "AL",
    "Caldense": "MG", "Camaçari": "BA", "Campinense": "PB",
    "Campo Grande-RJ": "RJ", "Cascavel EC": "PR", "Catuense": "BA",
    "Caxias": "RS", "Ceará": "CE", "Central": "PE",
    "Chapecoense": "SC", "Colorado": "PR", "Comercial": "SP",
    "Comercial-MS": "MS", "Confiança": "SE", "Corinthians": "SP",
    "Corinthians-AL": "AL", "Coritiba": "PR", "Corumbaense": "MS",
    "Criciúma": "SC", "Cruzeiro": "MG", "Cuiabá": "MT",
    "Desportiva Capixaba": "ES", "Desportiva Ferroviária": "ES",
    "Dom Bosco": "MT", "Dom Pedro": "MA", "Fast Clube": "AM",
    "Ferroviária": "SP", "Ferroviário": "CE", "Figueirense": "SC",
    "Flamengo": "RJ", "Flamengo-PI": "PI", "Fluminense": "RJ",
    "Fluminense de Feira": "BA", "Fortaleza": "CE", "Francana": "SP",
    "Friburguense": "RJ", "Galícia": "BA", "Gama": "DF",
    "Genus": "RO", "Goiás": "GO", "Goiânia": "GO", "Goytacaz": "RJ",
    "Grêmio": "RS", "Grêmio Barueri": "SP", "Grêmio Maringá": "PR",
    "Guarani": "SP", "Guarany de Sobral": "CE", "Inter SM": "RS",
    "Inter de Limeira": "SP", "Internacional": "RS", "Ipatinga": "MG",
    "Itabaiana": "SE", "Itabuna": "BA", "Ituano": "SP",
    "Itumbiara": "GO", "Joinville": "SC", "Juazeiro": "BA",
    "Juventude": "RS", "Juventus-SP": "SP", "Leônico": "SP",
    "Londrina": "PR", "Madureira": "RJ", "Malutrom": "PA",
    "Maranhão": "MA", "Marcílio Dias": "SC", "Matonense": "SP",
    "Mirassol": "SP", "Mixto": "MT", "Mogi Mirim": "SP",
    "Moto Club": "MA", "Nacional-AM": "AM", "Nacional-SP": "SP",
    "Noroeste": "SP", "Novo Hamburgo": "RS", "Náutico": "PE",
    "Olaria": "RJ", "Olímpia": "SP", "Operário Ferroviário": "PR",
    "Operário-MS": "MS", "Operário-MT": "MT", "Palmeiras": "SP",
    "Paraná": "PR", "Paulista": "SP", "Paysandu": "PA",
    "Piauí": "PI", "Pinheiros-PR": "PR", "Ponte Preta": "SP",
    "Porto-PE": "PE", "Portuguesa": "SP", "Portuguesa Santista": "SP",
    "Potiguar": "RN", "Red Bull Bragantino": "SP", "Remo": "PA",
    "Rio Branco SC": "SC", "Rio Branco-AC": "AC", "Rio Branco-ES": "ES",
    "Rio Branco-PR": "PR", "Rio Branco-SP": "SP", "Rio Negro-AM": "AM",
    "River-PI": "PI", "Sampaio Corrêa": "MA", "Santa Cruz": "PE",
    "Santo André": "SP", "Santos": "SP", "Sergipe": "SE",
    "Serra": "ES", "Sobradinho": "DF", "Sport": "PE",
    "Sport Belém": "PA", "São Bento": "SP", "São Caetano": "SP",
    "São Cristóvão": "RJ", "São José": "SP", "São Paulo": "SP",
    "São Paulo-RS": "RS", "São Raimundo-AM": "AM", "Taguatinga": "DF",
    "Tiradentes-PI": "PI", "Tocantinópolis": "TO", "Treze": "PB",
    "Tuna Luso": "PA", "Uberaba": "MG", "Uberlândia": "MG",
    "Ubiratan-MS": "MS", "União Bandeirante": "RO",
    "União Barbarense": "SP", "União Rondonópolis": "MT",
    "União São João": "SP", "Vasco": "RJ", "Vila Nova": "GO",
    "Villa Nova-MG": "MG", "Vitória": "BA", "Vitória-ES": "ES",
    "Volta Redonda": "RJ", "XV de Jaú": "SP", "XV de Piracicaba": "SP",
    "Ypiranga-AP": "AP",
}


# ===================================================================
# GRUPO 1 — Métricas Diretas do Jogo
# ===================================================================

def calcular_metricas_diretas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas derivadas diretamente da partida:
      - total_gols, saldo_gols, pontos (respeitando regra histórica)
    """
    logger.info("  [Grupo 1] Calculando métricas diretas...")

    # Total de gols na partida
    df["total_gols"] = df["gols_mandante"] + df["gols_visitante"]

    # Saldo de gols
    df["saldo_gols_mandante"] = df["gols_mandante"] - df["gols_visitante"]
    df["saldo_gols_visitante"] = df["gols_visitante"] - df["gols_mandante"]

    # Pontos (regra histórica: 2 pts antes de 1995, 3 pts a partir de 1995)
    pontos_vitoria = df["ano_campeonato"].apply(lambda y: 3 if y >= 1995 else 2)

    df["pontos_mandante"] = 0
    df["pontos_visitante"] = 0

    mask_v_mand = df["resultado_mandante"] == "V"
    mask_e = df["resultado_mandante"] == "E"
    mask_d_mand = df["resultado_mandante"] == "D"

    df.loc[mask_v_mand, "pontos_mandante"] = pontos_vitoria[mask_v_mand]
    df.loc[mask_e, "pontos_mandante"] = 1
    df.loc[mask_d_mand, "pontos_visitante"] = pontos_vitoria[mask_d_mand]
    df.loc[mask_e, "pontos_visitante"] = 1

    # Converter para Int64 (nullable) para jogos WO/ANU/IC
    mask_nulo = df["resultado_mandante"].isna()
    for col in ["total_gols", "saldo_gols_mandante", "saldo_gols_visitante",
                "pontos_mandante", "pontos_visitante"]:
        df[col] = df[col].astype("Int64")
        df.loc[mask_nulo, col] = pd.NA

    return df


# ===================================================================
# GRUPO 2 — Classificação de Fases
# ===================================================================

def classificar_fases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classifica cada fase original em:
      - tipo_fase: categoria padronizada
      - is_mata_mata: flag booleana
    """
    logger.info("  [Grupo 2] Classificando fases...")

    def _mapear_fase(fase: str) -> tuple[str, bool]:
        if pd.isna(fase):
            return ("Desconhecida", False)

        fase = str(fase).strip()

        # Rodadas de pontos corridos: R1, R2, ..., R46
        if fase.startswith(FASE_RODADA_PREFIX) and fase[1:].isdigit():
            return ("Pontos Corridos", False)

        # 1R (formato antigo de rodada)
        if fase == "1R":
            return ("Pontos Corridos", False)

        # Mata-mata direto
        if fase in FASES_MATA_MATA:
            tipo_map = {
                "F": "Final",
                "SF": "Semifinal",
                "QF": "Quartas de Final",
                "1/8": "Oitavas de Final",
                "3/4": "Disputa 3º Lugar",
                "POff": "Playoff",
            }
            return (tipo_map[fase], True)

        # Fases classificatórias
        if fase in FASES_CLASSIFICATORIA:
            return ("Fase Classificatória", False)

        # Letras simples (A–V) → Fase de Grupos
        if len(fase) == 1 and fase.isalpha():
            return ("Fase de Grupos", False)

        # Compostos de grupos: "A e B", "A/B", "C/D"
        if fase in {"A e B", "A/B", "C/D"}:
            return ("Fase de Grupos", False)

        # Fases numéricas: 1, 2, 3, 4, 5, 6
        if fase.isdigit():
            return ("Fase de Grupos", False)

        return ("Outra", False)

    classificacoes = df["Fase"].apply(_mapear_fase)
    df["tipo_fase"] = classificacoes.apply(lambda x: x[0])
    df["is_mata_mata"] = classificacoes.apply(lambda x: x[1])

    logger.info(f"    tipo_fase:\n{df['tipo_fase'].value_counts().to_string()}")
    logger.info(f"    is_mata_mata: {df['is_mata_mata'].sum()} jogos")

    return df


# ===================================================================
# GRUPO 3 — Estados e Fator Clássico (Derby Flag)
# ===================================================================

def adicionar_estados_e_derby(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona o estado (UF) de cada clube diretamente na linha da partida
    (desnormalizado — sem tabela dimensão) e marca partidas entre times
    do mesmo estado como clássico estadual.
    """
    logger.info("  [Grupo 3] Adicionando estados (UF) e fator clássico...")

    df["estado_mandante"] = df["Mandante"].map(ESTADO_TIME)
    df["estado_visitante"] = df["Visitante"].map(ESTADO_TIME)

    sem_estado = sorted(
        set(df.loc[df["estado_mandante"].isna(), "Mandante"])
        | set(df.loc[df["estado_visitante"].isna(), "Visitante"])
    )
    if sem_estado:
        logger.warning(f"    ⚠ Times sem estado mapeado: {sem_estado}")

    df["is_classico_estadual"] = (
        df["estado_mandante"] == df["estado_visitante"]
    ) & df["estado_mandante"].notna()

    n_classicos = df["is_classico_estadual"].sum()
    logger.info(
        f"    {n_classicos} jogos marcados como clássico estadual "
        f"({n_classicos / len(df) * 100:.1f}%)"
    )

    return df


# ===================================================================
# Pipeline Principal — One Big Table
# ===================================================================

def carregar_silver() -> pd.DataFrame:
    """Carrega e consolida todos os CSVs Silver em um único DataFrame."""
    arquivos = sorted(SILVER_DIR.glob("jogos_*.csv"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo Silver encontrado em {SILVER_DIR}")

    dfs = []
    for f in arquivos:
        df = pd.read_csv(f, parse_dates=["Data"])
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    df.sort_values(["Data", "ano_campeonato"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(f"  Silver carregado: {df.shape[0]} jogos, {df.shape[1]} colunas")
    return df


def definir_colunas_finais() -> list[str]:
    """Define a ordem final das colunas da One Big Table."""
    return [
        # Identificador da linha (sequencial, apenas para rastreio)
        "id_partida",
        # Fotografia da partida
        "ano_campeonato",
        "Data",
        "Mandante",
        "Visitante",
        "estado_mandante",
        "estado_visitante",
        "gols_mandante",
        "gols_visitante",
        "resultado_mandante",
        "resultado_visitante",
        "placar_status",
        "Fase",
        "tipo_fase",
        "is_mata_mata",
        "is_classico_estadual",
        # Métricas diretas do jogo
        "total_gols",
        "saldo_gols_mandante",
        "saldo_gols_visitante",
        "pontos_mandante",
        "pontos_visitante",
    ]


def construir_obt() -> pd.DataFrame:
    """
    Constrói a One Big Table consolidada a partir da camada Silver.
    Retorna o DataFrame final (uma linha por partida, tudo por extenso).
    """
    df = carregar_silver()

    df = calcular_metricas_diretas(df)
    df = classificar_fases(df)
    df = adicionar_estados_e_derby(df)

    # ID sequencial da partida (ordem cronológica)
    df["id_partida"] = range(1, len(df) + 1)

    colunas = [c for c in definir_colunas_finais() if c in df.columns]
    return df[colunas]


def salvar_obt(df: pd.DataFrame) -> Path:
    """Salva a One Big Table na camada Gold."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OBT_CSV, index=False, encoding="utf-8-sig")
    logger.info(f"  ✅ OBT salva: {OBT_CSV}")
    return OBT_CSV


def main() -> None:
    logger.info("=" * 60)
    logger.info("CAMADA GOLD — One Big Table (Brasileirão Histórico)")
    logger.info("=" * 60)

    df = construir_obt()
    salvar_obt(df)

    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO FINAL — CAMADA GOLD (OBT)")
    logger.info("=" * 60)
    logger.info(f"Total de partidas : {len(df)}")
    logger.info(f"Total de colunas  : {len(df.columns)}")
    logger.info(f"Colunas: {list(df.columns)}")
    logger.info("=" * 60)
    logger.info("Camada Gold finalizada.")


if __name__ == "__main__":
    main()
