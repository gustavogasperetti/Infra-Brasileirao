"""
api/database.py
===============
Configuração do SQLAlchemy para a API FastAPI.

- Engine e SessionLocal a partir das variáveis de ambiente (.env).
- Base declarativa compartilhada para os modelos ORM.
- Dependency ``get_db()`` no padrão generator do FastAPI.

Autor  : Brasileirão Analytics
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------------------------------------------------------------------
# Carregar .env (raiz do projeto)
# ---------------------------------------------------------------------------
_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_ENV_PATH)

DB_USER = os.getenv("DB_USER", "brasileirao")
DB_PASS = os.getenv("DB_PASS", "brasileirao2026")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "brasileirao_db")

DATABASE_URL = (
    f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---------------------------------------------------------------------------
# Engine e Session
# ---------------------------------------------------------------------------
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # verifica conexão antes de usar
    pool_size=10,
    max_overflow=20,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ---------------------------------------------------------------------------
# Base declarativa
# ---------------------------------------------------------------------------
Base = declarative_base()


# ---------------------------------------------------------------------------
# Dependency (FastAPI)
# ---------------------------------------------------------------------------
def get_db():
    """Gera uma sessão do banco e garante o fechamento após o request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
