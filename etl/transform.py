"""
etl/transform.py
=================
Camada Silver — Limpeza e padronização dos dados brutos do Brasileirão.

Entrada  : data/bronze/jogos_{ano}.csv  (dados brutos — camada Bronze)
Saída    : data/silver/jogos_{ano}.csv  (dados limpos — camada Silver)

Transformações aplicadas:
  1. Padronização de nomes de times (DE_PARA_TIMES)
  2. Split do Placar_Bruto → gols_mandante + gols_visitante
  3. Conversão de tipos (Data → datetime, gols → Int64)
  4. Remoção de espaços em branco nas colunas texto
  5. Adição da coluna 'ano' (edição do campeonato)
  6. Adição da coluna 'resultado_mandante' (V / D / E / None)
  7. Tratamento de placares especiais (WO, ANU, IC)

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 1.0.0
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
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

# ---------------------------------------------------------------------------
# De-Para de Times — Padronização de nomes
# ---------------------------------------------------------------------------
DE_PARA_TIMES = {
    "AA Colatina": "AA Colatina",
    "ABC": "ABC",
    "ASA": "ASA",
    "Alecrim": "Alecrim",
    "America-RJ": "America-RJ",
    "Americano": "Americano",
    "América Mineiro": "América Mineiro",
    "América-RN": "América-RN",
    "América-SP": "América-SP",
    "Anapolina": "Anapolina",
    "Anápolis": "Anápolis",
    "Athletico Paranaense": "Athletico Paranaense",
    "Atlético Goianiense": "Atlético Goianiense",
    "Atlético Mineiro": "Atlético Mineiro",
    "Atlético Paranaense": "Athletico Paranaense",
    "Auto Esporte-PI": "Auto Esporte-PI",
    "Avaí": "Avaí",
    "Bahia": "Bahia",
    "Bandeirante-DF": "Bandeirante-DF",
    "Bangu": "Bangu",
    "Baré": "Baré",
    "Botafogo": "Botafogo",
    "Botafogo-PB": "Botafogo-PB",
    "Botafogo-SP": "Botafogo-SP",
    "Bragantino": "Red Bull Bragantino",
    "Brasiliense": "Brasiliense",
    "Brasília": "Brasília",
    "CEUB": "CEUB",
    "CR Guará": "CR Guará",
    "CRB": "CRB",
    "CSA": "CSA",
    "Caldense": "Caldense",
    "Camaçari": "Camaçari",
    "Campinense": "Campinense",
    "Campo Grande-RJ": "Campo Grande-RJ",
    "Cascavel EC": "Cascavel EC",
    "Catuense": "Catuense",
    "Caxias": "Caxias",
    "Ceará": "Ceará",
    "Central": "Central",
    "Chapecoense": "Chapecoense",
    "Colorado": "Colorado",
    "Comercial": "Comercial",
    "Comercial-MS": "Comercial-MS",
    "Confiança": "Confiança",
    "Corinthians": "Corinthians",
    "Corinthians-AL": "Corinthians-AL",
    "Coritiba": "Coritiba",
    "Corumbaense": "Corumbaense",
    "Criciúma": "Criciúma",
    "Cruzeiro": "Cruzeiro",
    "Cuiabá": "Cuiabá",
    "Desportiva Capixaba": "Desportiva Capixaba",
    "Desportiva Ferroviária": "Desportiva Ferroviária",
    "Dom Bosco": "Dom Bosco",
    "Dom Pedro": "Dom Pedro",
    "Fast Clube": "Fast Clube",
    "Ferroviária": "Ferroviária",
    "Ferroviário": "Ferroviário",
    "Figueirense": "Figueirense",
    "Flamengo": "Flamengo",
    "Flamengo-PI": "Flamengo-PI",
    "Fluminense": "Fluminense",
    "Fluminense de Feira": "Fluminense de Feira",
    "Fortaleza": "Fortaleza",
    "Francana": "Francana",
    "Friburguense": "Friburguense",
    "GE Brasil": "Brasil de Pelotas",
    "Galícia": "Galícia",
    "Gama": "Gama",
    "Genus": "Genus",
    "Goiás": "Goiás",
    "Goiânia": "Goiânia",
    "Goytacaz": "Goytacaz",
    "Grêmio": "Grêmio",
    "Grêmio Barueri": "Grêmio Barueri",
    "Grêmio Maringá": "Grêmio Maringá",
    "Guarani": "Guarani",
    "Guarany de Sobral": "Guarany de Sobral",
    "Inter SM": "Inter SM",
    "Inter de Limeira": "Inter de Limeira",
    "Internacional": "Internacional",
    "Ipatinga": "Ipatinga",
    "Itabaiana": "Itabaiana",
    "Itabuna": "Itabuna",
    "Ituano": "Ituano",
    "Itumbiara": "Itumbiara",
    "Joinville": "Joinville",
    "Juazeiro": "Juazeiro",
    "Juventude": "Juventude",
    "Juventus-SP": "Juventus-SP",
    "Leônico": "Leônico",
    "Londrina": "Londrina",
    "Madureira": "Madureira",
    "Malutrom": "Malutrom",
    "Maranhão": "Maranhão",
    "Marcílio Dias": "Marcílio Dias",
    "Matonense": "Matonense",
    "Mirassol": "Mirassol",
    "Mixto": "Mixto",
    "Mogi Mirim": "Mogi Mirim",
    "Moto Club": "Moto Club",
    "Nacional-AM": "Nacional-AM",
    "Nacional-SP": "Nacional-SP",
    "Noroeste": "Noroeste",
    "Novo Hamburgo": "Novo Hamburgo",
    "Náutico": "Náutico",
    "Olaria": "Olaria",
    "Olímpia": "Olímpia",
    "Operário Ferroviário": "Operário Ferroviário",
    "Operário-MS": "Operário-MS",
    "Operário-MT": "Operário-MT",
    "Palmeiras": "Palmeiras",
    "Paraná": "Paraná",
    "Paulista": "Paulista",
    "Paysandu": "Paysandu",
    "Piauí": "Piauí",
    "Pinheiros-PR": "Pinheiros-PR",
    "Ponte Preta": "Ponte Preta",
    "Porto-PE": "Porto-PE",
    "Portuguesa": "Portuguesa",
    "Portuguesa Santista": "Portuguesa Santista",
    "Potiguar": "Potiguar",
    "Red Bull Bragantino": "Red Bull Bragantino",
    "Remo": "Remo",
    "Rio Branco SC": "Rio Branco SC",
    "Rio Branco-AC": "Rio Branco-AC",
    "Rio Branco-ES": "Rio Branco-ES",
    "Rio Branco-PR": "Rio Branco-PR",
    "Rio Branco-SP": "Rio Branco-SP",
    "Rio Negro-AM": "Rio Negro-AM",
    "River-PI": "River-PI",
    "Sampaio Corrêa": "Sampaio Corrêa",
    "Santa Cruz": "Santa Cruz",
    "Santo André": "Santo André",
    "Santos": "Santos",
    "Sergipe": "Sergipe",
    "Serra": "Serra",
    "Sobradinho": "Sobradinho",
    "Sport": "Sport",
    "Sport Belém": "Sport Belém",
    "São Bento": "São Bento",
    "São Caetano": "São Caetano",
    "São Cristóvão": "São Cristóvão",
    "São José": "São José",
    "São Paulo": "São Paulo",
    "São Paulo-RS": "São Paulo-RS",
    "São Raimundo-AM": "São Raimundo-AM",
    "Taguatinga": "Taguatinga",
    "Tiradentes-PI": "Tiradentes-PI",
    "Tocantinópolis": "Tocantinópolis",
    "Treze": "Treze",
    "Tuna Luso": "Tuna Luso",
    "Uberaba": "Uberaba",
    "Uberlândia": "Uberlândia",
    "Ubiratan-MS": "Ubiratan-MS",
    "União Bandeirante": "União Bandeirante",
    "União Barbarense": "União Barbarense",
    "União Rondonópolis": "União Rondonópolis",
    "União São João": "União São João",
    "Vasco": "Vasco",
    "Vila Nova": "Vila Nova",
    "Villa Nova-MG": "Villa Nova-MG",
    "Vitória": "Vitória",
    "Vitória-ES": "Vitória-ES",
    "Volta Redonda": "Volta Redonda",
    "XV de Jaú": "XV de Jaú",
    "XV de Piracicaba": "XV de Piracicaba",
    "Ypiranga-AP": "Ypiranga-AP",
}

# Placares especiais que não seguem o formato "X-Y"
PLACARES_ESPECIAIS = {"WO", "ANU", "IC"}


# ---------------------------------------------------------------------------
# Funções de transformação
# ---------------------------------------------------------------------------

def padronizar_nomes_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o dicionário DE_PARA_TIMES nas colunas Mandante e Visitante.
    Times não mapeados são mantidos com o nome original e logados como aviso.
    """
    for coluna in ["Mandante", "Visitante"]:
        originais = df[coluna].unique()
        nao_mapeados = [t for t in originais if t not in DE_PARA_TIMES and pd.notna(t)]
        if nao_mapeados:
            logger.warning(
                f"  ⚠️  Times sem mapeamento na coluna '{coluna}': {nao_mapeados}"
            )

        df[coluna] = df[coluna].map(DE_PARA_TIMES).fillna(df[coluna])

    return df


def split_placar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a coluna Placar_Bruto em duas colunas numéricas:
      - gols_mandante (antes do '-')
      - gols_visitante (depois do '-')

    Placares especiais (WO, ANU, IC) geram valores nulos nos gols,
    e são preservados em uma nova coluna 'placar_status'.
    """
    # Identifica linhas com placar especial
    mask_especial = df["Placar_Bruto"].isin(PLACARES_ESPECIAIS)

    # Cria coluna de status do placar
    df["placar_status"] = "NORMAL"
    df.loc[mask_especial, "placar_status"] = df.loc[mask_especial, "Placar_Bruto"]

    # Split nos placares normais
    placar_split = df.loc[~mask_especial, "Placar_Bruto"].str.split("-", n=1, expand=True)

    df["gols_mandante"] = pd.NA
    df["gols_visitante"] = pd.NA

    if not placar_split.empty:
        df.loc[~mask_especial, "gols_mandante"] = pd.to_numeric(
            placar_split[0].str.strip(), errors="coerce"
        )
        df.loc[~mask_especial, "gols_visitante"] = pd.to_numeric(
            placar_split[1].str.strip(), errors="coerce"
        )

    # Converte para Int64 (inteiro nullable do pandas)
    df["gols_mandante"] = df["gols_mandante"].astype("Int64")
    df["gols_visitante"] = df["gols_visitante"].astype("Int64")

    return df


def calcular_resultado_mandante(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria as colunas 'resultado_mandante' e 'resultado_visitante' com base nos gols:
      - 'V' → Vitória
      - 'D' → Derrota
      - 'E' → Empate
      - None → Placar indisponível (WO, ANU, IC)
    """
    df["resultado_mandante"] = None
    df["resultado_visitante"] = None

    # Máscara de jogos com placar válido
    mask_valido = df["gols_mandante"].notna() & df["gols_visitante"].notna()

    df.loc[mask_valido & (df["gols_mandante"] > df["gols_visitante"]), "resultado_mandante"] = "V"
    df.loc[mask_valido & (df["gols_mandante"] > df["gols_visitante"]), "resultado_visitante"] = "D"

    df.loc[mask_valido & (df["gols_mandante"] < df["gols_visitante"]), "resultado_mandante"] = "D"
    df.loc[mask_valido & (df["gols_mandante"] < df["gols_visitante"]), "resultado_visitante"] = "V"

    df.loc[mask_valido & (df["gols_mandante"] == df["gols_visitante"]), "resultado_mandante"] = "E"
    df.loc[mask_valido & (df["gols_mandante"] == df["gols_visitante"]), "resultado_visitante"] = "E"

    return df


def padronizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas para os tipos adequados:
      - Data        → datetime64
      - Mandante    → string
      - Visitante   → string
      - Fase        → string (category)
    """
    # Converte Data para datetime
    df["Data"] = pd.to_datetime(df["Data"], format="%Y-%m-%d", errors="coerce")

    # Converte colunas texto para tipo string do pandas
    for col in ["Mandante", "Visitante", "Fase", "placar_status"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def limpar_espacos(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços em branco nas extremidades de colunas texto."""
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].str.strip()
    return df


# ---------------------------------------------------------------------------
# Pipeline principal de transformação
# ---------------------------------------------------------------------------

def transformar_bronze_para_silver(filepath: Path, ano: int) -> pd.DataFrame | None:
    """
    Executa o pipeline completo de transformação Bronze → Silver
    para um único arquivo CSV.
    """
    logger.info(f"  📂 Lendo: {filepath.name}")

    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig")
    except Exception as e:
        logger.error(f"  ❌ Erro ao ler {filepath.name}: {e}")
        return None

    if df.empty:
        logger.warning(f"  ⚠️  Arquivo vazio: {filepath.name}")
        return None

    logger.info(f"  Shape original: {df.shape}")

    # 1. Remoção de espaços em branco
    df = limpar_espacos(df)

    # 2. Padronização de nomes de times
    df = padronizar_nomes_times(df)

    # 3. Split do placar
    df = split_placar(df)

    # 4. Resultado do mandante (V/D/E)
    df = calcular_resultado_mandante(df)

    # 5. Adiciona coluna do ano da edição
    df["ano_campeonato"] = ano

    # 6. Padronização de tipos
    df = padronizar_tipos(df)

    # 7. Reordena colunas para melhor legibilidade
    colunas_finais = [
        "ano_campeonato",
        "Data",
        "Mandante",
        "Visitante",
        "gols_mandante",
        "gols_visitante",
        "resultado_mandante",
        "resultado_visitante",
        "placar_status",
        "Fase",
    ]
    # Garante que só use colunas que existem
    colunas_finais = [c for c in colunas_finais if c in df.columns]
    df = df[colunas_finais]

    logger.info(f"  Shape final: {df.shape}")
    logger.info(f"  Colunas: {list(df.columns)}")
    logger.info(f"  Tipos:\n{df.dtypes.to_string()}")

    return df


def salvar_silver(df: pd.DataFrame, ano: int) -> Path:
    """Salva o DataFrame transformado na camada Silver."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    output_path = SILVER_DIR / f"jogos_{ano}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"  ✅ Salvo em: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("TRANSFORMAÇÃO SILVER — Brasileirão Histórico")
    logger.info("=" * 60)

    # Descobre todos os CSVs Bronze disponíveis
    arquivos_bronze = sorted(BRONZE_DIR.glob("jogos_*.csv"))

    if not arquivos_bronze:
        logger.error(f"Nenhum arquivo encontrado em {BRONZE_DIR}")
        return

    logger.info(f"Arquivos Bronze encontrados: {len(arquivos_bronze)}")

    total_jogos = 0
    anos_ok: list[int] = []
    anos_falha: list[int] = []

    for filepath in arquivos_bronze:
        # Extrai o ano do nome do arquivo (jogos_YYYY.csv)
        try:
            ano = int(filepath.stem.split("_")[1])
        except (IndexError, ValueError):
            logger.warning(f"  ⚠️  Nome de arquivo inesperado: {filepath.name}")
            continue

        logger.info("")
        logger.info(f"{'─' * 50}")
        logger.info(f"Processando: {filepath.name} (ano {ano})")
        logger.info(f"{'─' * 50}")

        df_silver = transformar_bronze_para_silver(filepath, ano)

        if df_silver is not None and not df_silver.empty:
            salvar_silver(df_silver, ano)
            total_jogos += len(df_silver)
            anos_ok.append(ano)
        else:
            anos_falha.append(ano)

    # ── Resumo ─────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO FINAL — CAMADA SILVER")
    logger.info("=" * 60)
    logger.info(f"Anos transformados com sucesso : {len(anos_ok)}  → {anos_ok}")
    logger.info(f"Anos com falha                 : {len(anos_falha)} → {anos_falha}")
    logger.info(f"Total de jogos processados     : {total_jogos}")
    logger.info(f"Arquivos salvos em             : {SILVER_DIR}")
    logger.info("=" * 60)
    logger.info("Transformação Silver finalizada.")


if __name__ == "__main__":
    main()
