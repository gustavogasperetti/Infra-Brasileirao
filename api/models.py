"""
api/models.py
=============
Modelos ORM (SQLAlchemy Declarative) que espelham as tabelas do PostgreSQL.

Tabelas:
  - ``dim_times``          — Dimensão com 167 times (PK, nome, estado/UF).
  - ``fato_partidas_ouro`` — Fato com 21.453 jogos e 66 colunas.

Autor  : Brasileirão Analytics
"""

from sqlalchemy import (
    Column, Integer, Float, String, Date, Boolean, ForeignKey, SmallInteger,
)
from sqlalchemy.orm import relationship

from api.database import Base


# ===================================================================
# Dimensão — Times
# ===================================================================

class DimTimes(Base):
    """Tabela dimensão de times."""
    __tablename__ = "dim_times"

    id_time    = Column(Integer, primary_key=True, index=True)
    nome_time  = Column(String(100), unique=True, nullable=False, index=True)
    estado     = Column(String(2), nullable=True)

    def __repr__(self) -> str:
        return f"<DimTimes(id={self.id_time}, nome='{self.nome_time}', uf='{self.estado}')>"


# ===================================================================
# Fato — Partidas (Camada Ouro)
# ===================================================================

class FatoPartidasOuro(Base):
    """Tabela fato com todas as 66 features da camada Gold."""
    __tablename__ = "fato_partidas_ouro"

    # --- Identificadores e FKs ---
    id_partida   = Column(Integer, primary_key=True, index=True)
    id_mandante  = Column(Integer, ForeignKey("dim_times.id_time"), nullable=True)
    id_visitante = Column(Integer, ForeignKey("dim_times.id_time"), nullable=True)

    # --- Dados Base da Partida ---
    ano_campeonato      = Column(SmallInteger, nullable=False, index=True)
    Data                = Column(Date, nullable=True, index=True)
    Mandante            = Column(String(100), nullable=False, index=True)
    Visitante           = Column(String(100), nullable=False, index=True)
    gols_mandante       = Column(Integer, nullable=True)
    gols_visitante      = Column(Integer, nullable=True)
    resultado_mandante  = Column(String(1), nullable=True)   # V, E, D ou None
    resultado_visitante = Column(String(1), nullable=True)
    placar_status       = Column(String(10), nullable=True)  # NORMAL, WO, ANU, IC
    Fase                = Column(String(20), nullable=True)

    # --- Classificação de Fase ---
    tipo_fase   = Column(String(30), nullable=True)
    is_mata_mata = Column(Boolean, nullable=True)

    # --- Métricas Diretas ---
    total_gols            = Column(Integer, nullable=True)
    saldo_gols_mandante   = Column(Integer, nullable=True)
    saldo_gols_visitante  = Column(Integer, nullable=True)
    pontos_mandante       = Column(Integer, nullable=True)
    pontos_visitante      = Column(Integer, nullable=True)

    # --- Rolling Mandante ---
    mandante_media_gols_marcados_5j   = Column(Float, nullable=True)
    mandante_media_gols_sofridos_5j   = Column(Float, nullable=True)
    mandante_pontos_ultimos_5j        = Column(Float, nullable=True)
    mandante_aproveitamento_5j        = Column(Float, nullable=True)
    mandante_sequencia_vitorias       = Column(Integer, nullable=True)
    mandante_sequencia_invicta        = Column(Integer, nullable=True)
    mandante_sequencia_derrotas       = Column(Integer, nullable=True)
    mandante_jogos_no_campeonato      = Column(Integer, nullable=True)
    mandante_pontos_acumulados        = Column(Float, nullable=True)
    mandante_gols_marcados_acumulados = Column(Float, nullable=True)
    mandante_gols_sofridos_acumulados = Column(Float, nullable=True)
    mandante_saldo_gols_acumulado     = Column(Float, nullable=True)
    mandante_vitorias_acumuladas      = Column(Float, nullable=True)
    mandante_empates_acumulados       = Column(Float, nullable=True)
    mandante_derrotas_acumuladas      = Column(Float, nullable=True)

    # --- Clean Sheets Mandante ---
    mandante_clean_sheets_5j    = Column(Float, nullable=True)
    mandante_falhou_marcar_5j   = Column(Float, nullable=True)
    mandante_clean_sheets_camp  = Column(Float, nullable=True)
    mandante_falhou_marcar_camp = Column(Float, nullable=True)

    # --- Rolling Visitante ---
    visitante_media_gols_marcados_5j   = Column(Float, nullable=True)
    visitante_media_gols_sofridos_5j   = Column(Float, nullable=True)
    visitante_pontos_ultimos_5j        = Column(Float, nullable=True)
    visitante_aproveitamento_5j        = Column(Float, nullable=True)
    visitante_sequencia_vitorias       = Column(Integer, nullable=True)
    visitante_sequencia_invicta        = Column(Integer, nullable=True)
    visitante_sequencia_derrotas       = Column(Integer, nullable=True)
    visitante_jogos_no_campeonato      = Column(Integer, nullable=True)
    visitante_pontos_acumulados        = Column(Float, nullable=True)
    visitante_gols_marcados_acumulados = Column(Float, nullable=True)
    visitante_gols_sofridos_acumulados = Column(Float, nullable=True)
    visitante_saldo_gols_acumulado     = Column(Float, nullable=True)
    visitante_vitorias_acumuladas      = Column(Float, nullable=True)
    visitante_empates_acumulados       = Column(Float, nullable=True)
    visitante_derrotas_acumuladas      = Column(Float, nullable=True)

    # --- Clean Sheets Visitante ---
    visitante_clean_sheets_5j    = Column(Float, nullable=True)
    visitante_falhou_marcar_5j   = Column(Float, nullable=True)
    visitante_clean_sheets_camp  = Column(Float, nullable=True)
    visitante_falhou_marcar_camp = Column(Float, nullable=True)

    # --- H2H ---
    h2h_jogos              = Column(Integer, nullable=True)
    h2h_vitorias_mandante  = Column(Integer, nullable=True)
    h2h_vitorias_visitante = Column(Integer, nullable=True)
    h2h_empates            = Column(Integer, nullable=True)

    # --- Fadiga ---
    mandante_dias_descanso  = Column(Integer, nullable=True)
    visitante_dias_descanso = Column(Integer, nullable=True)
    diferenca_descanso      = Column(Integer, nullable=True)

    # --- Derby Flag ---
    is_classico_estadual = Column(Boolean, nullable=True)

    # --- Relationships ---
    time_mandante  = relationship("DimTimes", foreign_keys=[id_mandante])
    time_visitante = relationship("DimTimes", foreign_keys=[id_visitante])

    def __repr__(self) -> str:
        return (
            f"<FatoPartida(id={self.id_partida}, "
            f"{self.Mandante} vs {self.Visitante}, "
            f"ano={self.ano_campeonato})>"
        )
