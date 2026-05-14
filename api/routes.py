"""
api/routes.py
=============
Endpoints GET da API Brasileirão Analytics.

Endpoints:
  GET /times              — Lista todos os times (filtro por estado)
  GET /times/{id}         — Busca um time por ID
  GET /partidas           — Lista partidas com paginação e filtros
  GET /partidas/{id}      — Busca uma partida por ID (66 colunas)
  GET /confronto          — Histórico H2H entre dois times
  GET /estatisticas/time/{id} — Estatísticas agregadas de um time
  GET /campeonatos        — Lista anos com contagem de jogos

Autor  : Brasileirão Analytics
Versão : 1.0.0
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from api.database import get_db
from api.models import DimTimes, FatoPartidasOuro

router = APIRouter(tags=["Brasileirão"])

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
MAX_PER_PAGE = 500
DEFAULT_PER_PAGE = 50


# ===================================================================
# Helpers
# ===================================================================

def _serializar_partida(p: FatoPartidasOuro) -> dict:
    """Converte um objeto ORM FatoPartidasOuro em dicionário serializável."""
    d = {}
    for col in p.__table__.columns:
        val = getattr(p, col.name)
        # date → str ISO
        if hasattr(val, "isoformat"):
            val = val.isoformat()
        d[col.name] = val
    return d


def _serializar_time(t: DimTimes) -> dict:
    """Converte um objeto ORM DimTimes em dicionário serializável."""
    return {
        "id_time": t.id_time,
        "nome_time": t.nome_time,
        "estado": t.estado,
    }


# ===================================================================
# GET /times — Lista todos os times
# ===================================================================

@router.get("/times", summary="Listar times")
def listar_times(
    estado: Optional[str] = Query(
        None, max_length=2, description="Filtrar por UF (ex: SP, RJ, MG)"
    ),
    db: Session = Depends(get_db),
):
    """
    Retorna todos os times da dimensão.
    Filtro opcional por estado (UF).
    """
    query = db.query(DimTimes).order_by(DimTimes.nome_time)

    if estado:
        query = query.filter(DimTimes.estado == estado.upper())

    times = query.all()
    return {
        "total": len(times),
        "times": [_serializar_time(t) for t in times],
    }


# ===================================================================
# GET /times/{id} — Busca time por ID
# ===================================================================

@router.get("/times/{id_time}", summary="Buscar time por ID")
def buscar_time(id_time: int, db: Session = Depends(get_db)):
    """Retorna os dados de um time específico pelo ID."""
    time = db.query(DimTimes).filter(DimTimes.id_time == id_time).first()
    if not time:
        raise HTTPException(status_code=404, detail=f"Time com id={id_time} não encontrado")
    return _serializar_time(time)


# ===================================================================
# GET /partidas — Lista partidas com paginação e filtros
# ===================================================================

@router.get("/partidas", summary="Listar partidas")
def listar_partidas(
    ano: Optional[int] = Query(None, ge=1971, le=2025, description="Ano do campeonato"),
    mandante: Optional[str] = Query(None, description="Nome do time mandante"),
    visitante: Optional[str] = Query(None, description="Nome do time visitante"),
    time: Optional[str] = Query(
        None,
        description="Nome do time (busca como mandante OU visitante)",
    ),
    fase: Optional[str] = Query(None, description="Tipo de fase (ex: Pontos Corridos, Final)"),
    mata_mata: Optional[bool] = Query(None, description="Filtrar apenas jogos de mata-mata"),
    page: int = Query(1, ge=1, description="Página (1-indexed)"),
    per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE, description="Itens por página"),
    db: Session = Depends(get_db),
):
    """
    Retorna partidas com paginação e filtros opcionais.

    - **ano**: Filtra por edição do campeonato (1971–2025)
    - **mandante/visitante**: Filtra por nome exato do time
    - **time**: Filtra partidas onde o time aparece como mandante OU visitante
    - **fase**: Filtra por tipo_fase
    - **mata_mata**: Filtra apenas jogos eliminatórios (true/false)
    """
    query = db.query(FatoPartidasOuro).order_by(
        FatoPartidasOuro.Data, FatoPartidasOuro.id_partida
    )

    if ano:
        query = query.filter(FatoPartidasOuro.ano_campeonato == ano)
    if mandante:
        query = query.filter(FatoPartidasOuro.Mandante == mandante)
    if visitante:
        query = query.filter(FatoPartidasOuro.Visitante == visitante)
    if time:
        query = query.filter(
            (FatoPartidasOuro.Mandante == time) | (FatoPartidasOuro.Visitante == time)
        )
    if fase:
        query = query.filter(FatoPartidasOuro.tipo_fase == fase)
    if mata_mata is not None:
        query = query.filter(FatoPartidasOuro.is_mata_mata == mata_mata)

    # Contagem total (antes da paginação)
    total = query.count()

    # Paginação
    offset = (page - 1) * per_page
    partidas = query.offset(offset).limit(per_page).all()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "partidas": [_serializar_partida(p) for p in partidas],
    }


# ===================================================================
# GET /partidas/{id} — Busca partida por ID
# ===================================================================

@router.get("/partidas/{id_partida}", summary="Buscar partida por ID")
def buscar_partida(id_partida: int, db: Session = Depends(get_db)):
    """Retorna todos os dados (66 colunas) de uma partida específica."""
    partida = (
        db.query(FatoPartidasOuro)
        .filter(FatoPartidasOuro.id_partida == id_partida)
        .first()
    )
    if not partida:
        raise HTTPException(
            status_code=404,
            detail=f"Partida com id={id_partida} não encontrada",
        )
    return _serializar_partida(partida)


# ===================================================================
# GET /confronto — Histórico H2H entre dois times
# ===================================================================

@router.get("/confronto", summary="Confronto direto (H2H)")
def confronto_direto(
    time_a: str = Query(..., description="Nome do primeiro time"),
    time_b: str = Query(..., description="Nome do segundo time"),
    db: Session = Depends(get_db),
):
    """
    Retorna o histórico completo de confrontos entre dois times.
    A ordem dos times não importa — ambas as combinações de mando são retornadas.
    """
    partidas = (
        db.query(FatoPartidasOuro)
        .filter(
            (
                (FatoPartidasOuro.Mandante == time_a)
                & (FatoPartidasOuro.Visitante == time_b)
            )
            | (
                (FatoPartidasOuro.Mandante == time_b)
                & (FatoPartidasOuro.Visitante == time_a)
            )
        )
        .order_by(FatoPartidasOuro.Data)
        .all()
    )

    if not partidas:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum confronto encontrado entre '{time_a}' e '{time_b}'",
        )

    # Calcular estatísticas agregadas
    total = len(partidas)
    vitorias_a = 0
    vitorias_b = 0
    empates = 0
    gols_a = 0
    gols_b = 0

    for p in partidas:
        if p.Mandante == time_a:
            ga = p.gols_mandante or 0
            gb = p.gols_visitante or 0
        else:
            ga = p.gols_visitante or 0
            gb = p.gols_mandante or 0

        gols_a += ga
        gols_b += gb

        if p.resultado_mandante == "V":
            if p.Mandante == time_a:
                vitorias_a += 1
            else:
                vitorias_b += 1
        elif p.resultado_mandante == "D":
            if p.Mandante == time_a:
                vitorias_b += 1
            else:
                vitorias_a += 1
        elif p.resultado_mandante == "E":
            empates += 1

    return {
        "time_a": time_a,
        "time_b": time_b,
        "resumo": {
            "total_jogos": total,
            f"vitorias_{time_a}": vitorias_a,
            f"vitorias_{time_b}": vitorias_b,
            "empates": empates,
            f"gols_{time_a}": gols_a,
            f"gols_{time_b}": gols_b,
        },
        "partidas": [_serializar_partida(p) for p in partidas],
    }


# ===================================================================
# GET /estatisticas/time/{id} — Estatísticas de um time por ano
# ===================================================================

@router.get("/estatisticas/time/{id_time}", summary="Estatísticas de um time")
def estatisticas_time(
    id_time: int,
    ano: Optional[int] = Query(None, ge=1971, le=2025, description="Ano do campeonato"),
    db: Session = Depends(get_db),
):
    """
    Retorna estatísticas agregadas de um time em um campeonato específico.
    Se ``ano`` não for informado, retorna estatísticas de toda a história.
    """
    # Verificar se o time existe
    time_obj = db.query(DimTimes).filter(DimTimes.id_time == id_time).first()
    if not time_obj:
        raise HTTPException(status_code=404, detail=f"Time com id={id_time} não encontrado")

    nome = time_obj.nome_time

    # Buscar partidas como mandante
    q_mand = db.query(FatoPartidasOuro).filter(FatoPartidasOuro.Mandante == nome)
    q_visit = db.query(FatoPartidasOuro).filter(FatoPartidasOuro.Visitante == nome)

    if ano:
        q_mand = q_mand.filter(FatoPartidasOuro.ano_campeonato == ano)
        q_visit = q_visit.filter(FatoPartidasOuro.ano_campeonato == ano)

    jogos_mand = q_mand.all()
    jogos_visit = q_visit.all()

    total_jogos = len(jogos_mand) + len(jogos_visit)

    if total_jogos == 0:
        return {
            "time": _serializar_time(time_obj),
            "ano": ano,
            "mensagem": "Nenhuma partida encontrada para os filtros informados",
        }

    # Agregar
    vitorias = empates = derrotas = gols_marcados = gols_sofridos = 0

    for p in jogos_mand:
        gols_marcados += p.gols_mandante or 0
        gols_sofridos += p.gols_visitante or 0
        if p.resultado_mandante == "V":
            vitorias += 1
        elif p.resultado_mandante == "E":
            empates += 1
        elif p.resultado_mandante == "D":
            derrotas += 1

    for p in jogos_visit:
        gols_marcados += p.gols_visitante or 0
        gols_sofridos += p.gols_mandante or 0
        if p.resultado_visitante == "V":
            vitorias += 1
        elif p.resultado_visitante == "E":
            empates += 1
        elif p.resultado_visitante == "D":
            derrotas += 1

    aproveitamento = 0.0
    if total_jogos > 0:
        # Pontos considerando regra geral de 3 pts/vitória para simplificar
        pontos = (vitorias * 3) + empates
        max_pontos = total_jogos * 3
        aproveitamento = round((pontos / max_pontos) * 100, 1) if max_pontos > 0 else 0.0

    return {
        "time": _serializar_time(time_obj),
        "ano": ano if ano else "todos",
        "estatisticas": {
            "total_jogos": total_jogos,
            "jogos_mandante": len(jogos_mand),
            "jogos_visitante": len(jogos_visit),
            "vitorias": vitorias,
            "empates": empates,
            "derrotas": derrotas,
            "gols_marcados": gols_marcados,
            "gols_sofridos": gols_sofridos,
            "saldo_gols": gols_marcados - gols_sofridos,
            "aproveitamento_pct": aproveitamento,
        },
    }


# ===================================================================
# GET /campeonatos — Lista anos disponíveis
# ===================================================================

@router.get("/campeonatos", summary="Listar campeonatos")
def listar_campeonatos(db: Session = Depends(get_db)):
    """
    Retorna todos os anos de campeonato disponíveis na base,
    com contagem de jogos por edição.
    """
    resultados = (
        db.query(
            FatoPartidasOuro.ano_campeonato,
            func.count(FatoPartidasOuro.id_partida).label("total_jogos"),
        )
        .group_by(FatoPartidasOuro.ano_campeonato)
        .order_by(FatoPartidasOuro.ano_campeonato)
        .all()
    )

    campeonatos = [
        {"ano": r.ano_campeonato, "total_jogos": r.total_jogos}
        for r in resultados
    ]

    return {
        "total_edicoes": len(campeonatos),
        "campeonatos": campeonatos,
    }
