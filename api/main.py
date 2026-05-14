"""
api/main.py
===========
Ponto de entrada da aplicação FastAPI — Brasileirão Analytics.

Responsabilidades:
  - Criar a instância ``app`` com metadata (Swagger automático em /docs)
  - Configurar CORS middleware para acesso cross-origin
  - Registrar o router de ``routes.py``
  - Evento on_startup para garantir que as tabelas existam
  - Endpoint raiz ``/`` como health check

Uso:
  uvicorn api.main:app --reload --port 8000

Autor  : Brasileirão Analytics
Versão : 1.0.0
"""

import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.database import engine, Base
from api.routes import router

# Importar models para registrar as tabelas no Base.metadata
import api.models  # noqa: F401

# ---------------------------------------------------------------------------
# Carregar .env
# ---------------------------------------------------------------------------
from pathlib import Path

_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_ENV_PATH)


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Cria as tabelas no banco (se ainda não existirem) ao iniciar."""
    Base.metadata.create_all(bind=engine)
    yield


# ---------------------------------------------------------------------------
# Instância FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Brasileirão Analytics API",
    description=(
        "API RESTful que serve dados históricos do Campeonato Brasileiro "
        "de Futebol (1971–2025). Dados processados via Arquitetura Medalhão "
        "(Bronze → Prata → Ouro) com 66 features analíticas por partida."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# CORS Middleware
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # libera tudo (TCC / dev)
    allow_credentials=True,
    allow_methods=["GET"],       # API somente leitura
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Registrar rotas
# ---------------------------------------------------------------------------
app.include_router(router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Endpoint raiz — Health Check
# ---------------------------------------------------------------------------
@app.get("/", tags=["Health"])
async def root():
    """Health check da API."""
    return {
        "status": "online",
        "projeto": "Brasileirão Analytics",
        "versao": "1.0.0",
        "docs": "/docs",
    }
