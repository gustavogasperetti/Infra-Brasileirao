"""
etl/load.py
===========
Camada Load — Ingestão dos CSVs Gold no PostgreSQL via SQLAlchemy.

Entrada  : data/gold/fato_partidas_ouro.csv
           data/gold/dim_times.csv
Destino  : PostgreSQL (tabelas ``dim_times`` e ``fato_partidas_ouro``)

Estratégia:
  - Idempotente: DROP + CREATE + INSERT a cada execução.
  - Usa pandas.to_sql() com chunksize para performance.
  - Valida contagem pós-carga.

Uso:
  python -m etl.load

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 1.0.0
"""

import logging
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Reutiliza engine e Base do módulo da API
# ---------------------------------------------------------------------------
from api.database import engine, Base

# Importa os modelos para que o Base.metadata conheça as tabelas
import api.models  # noqa: F401

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
GOLD_DIR = BASE_DIR / "data" / "gold"

FATO_CSV = GOLD_DIR / "fato_partidas_ouro.csv"
DIM_CSV  = GOLD_DIR / "dim_times.csv"


# ===================================================================
# Funções de Carga
# ===================================================================

def verificar_csvs() -> None:
    """Verifica se os CSVs Gold existem antes de tentar a carga."""
    for path in [FATO_CSV, DIM_CSV]:
        if not path.exists():
            raise FileNotFoundError(
                f"❌ CSV não encontrado: {path}\n"
                f"Execute primeiro o pipeline Gold (python -m etl.gold)."
            )


def carregar_dim_times() -> pd.DataFrame:
    """Carrega e prepara o CSV da dimensão de times."""
    logger.info("📂 Carregando dim_times.csv ...")
    df = pd.read_csv(DIM_CSV)
    logger.info(f"   {len(df)} times carregados do CSV")
    return df


def carregar_fato_partidas() -> pd.DataFrame:
    """Carrega e prepara o CSV da tabela fato."""
    logger.info("📂 Carregando fato_partidas_ouro.csv ...")
    df = pd.read_csv(FATO_CSV, parse_dates=["Data"])

    # Converter Data para date (sem hora) para compatibilidade com o ORM
    df["Data"] = pd.to_datetime(df["Data"]).dt.date

    logger.info(f"   {len(df)} partidas carregadas do CSV ({len(df.columns)} colunas)")
    return df


def recriar_tabelas() -> None:
    """
    DROP + CREATE de todas as tabelas mapeadas no Base.metadata.
    Isso garante idempotência — cada execução recria do zero.
    """
    logger.info("🗑️  Removendo tabelas existentes (DROP ALL) ...")
    Base.metadata.drop_all(bind=engine)

    logger.info("🏗️  Criando tabelas (CREATE ALL) ...")
    Base.metadata.create_all(bind=engine)

    logger.info("   ✅ Tabelas recriadas com sucesso")


def inserir_dataframe(df: pd.DataFrame, tabela: str, chunksize: int = 500) -> None:
    """
    Insere um DataFrame no PostgreSQL usando pandas.to_sql().
    Usa ``if_exists='append'`` pois as tabelas já foram criadas pelo ORM.
    """
    t0 = time.time()
    logger.info(f"⬆️  Inserindo {len(df)} registros na tabela '{tabela}' ...")

    df.to_sql(
        name=tabela,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=chunksize,
        method="multi",
    )

    elapsed = time.time() - t0
    logger.info(f"   ✅ '{tabela}' — {len(df)} registros inseridos em {elapsed:.1f}s")


def validar_carga() -> None:
    """Valida a integridade da carga com queries de contagem."""
    logger.info("🔍 Validando carga ...")

    with engine.connect() as conn:
        n_times = conn.execute(text("SELECT COUNT(*) FROM dim_times")).scalar()
        n_partidas = conn.execute(text("SELECT COUNT(*) FROM fato_partidas_ouro")).scalar()

        # Verificar integridade referencial (FKs sem match)
        fk_orfas = conn.execute(text("""
            SELECT COUNT(*)
            FROM fato_partidas_ouro f
            LEFT JOIN dim_times d1 ON f.id_mandante = d1.id_time
            LEFT JOIN dim_times d2 ON f.id_visitante = d2.id_time
            WHERE d1.id_time IS NULL OR d2.id_time IS NULL
        """)).scalar()

    logger.info(f"   dim_times         : {n_times} registros")
    logger.info(f"   fato_partidas_ouro: {n_partidas} registros")

    if fk_orfas and fk_orfas > 0:
        logger.warning(f"   ⚠ {fk_orfas} partidas com FK órfã (time sem match na dimensão)")
    else:
        logger.info("   ✅ Integridade referencial OK — 0 FKs órfãs")


# ===================================================================
# Pipeline Principal
# ===================================================================

def main() -> None:
    logger.info("=" * 60)
    logger.info("LOAD — Ingestão Gold → PostgreSQL")
    logger.info("=" * 60)

    t_total = time.time()

    # 1. Verificar CSVs
    verificar_csvs()

    # 2. Carregar DataFrames
    df_dim = carregar_dim_times()
    df_fato = carregar_fato_partidas()

    # 3. Recriar tabelas (idempotente)
    recriar_tabelas()

    # 4. Inserir dados (dimensão primeiro por causa das FKs)
    inserir_dataframe(df_dim, "dim_times")
    inserir_dataframe(df_fato, "fato_partidas_ouro")

    # 5. Validar
    validar_carga()

    # 6. Resumo
    elapsed = time.time() - t_total
    logger.info("")
    logger.info("=" * 60)
    logger.info("LOAD CONCLUÍDO")
    logger.info(f"Tempo total: {elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
