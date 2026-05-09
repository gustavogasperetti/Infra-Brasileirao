"""
etl/gold.py
===========
Camada Gold — Feature Engineering avançado para o Brasileirão Histórico.

Entrada  : data/silver/jogos_{ano}.csv  (dados limpos — camada Silver)
Saída    : data/gold/fato_partidas_ouro.csv  (tabela fato enriquecida)
           data/gold/dim_times.csv            (tabela dimensão de times)

Grupos de features:
  1. Métricas diretas do jogo (total_gols, saldo, pontos)
  2. Classificação de fases (tipo_fase, is_mata_mata)
  3. Métricas rolling por time (média móvel 5j, streaks, acumulados)
  4. Confronto direto histórico (H2H)
  5. Tabela dimensão (dim_times)
  6. Métricas de Fadiga (dias de descanso — cross-year)
  7. Solidez Defensiva e Ofensiva (clean sheets)
  8. Fator Clássico (derby flag — is_classico_estadual)

Regra de negócio:
  - Pontuação: 2 pts/vitória antes de 1995, 3 pts/vitória a partir de 1995.
  - Métricas rolling resetam a cada edição do campeonato.
  - H2H é cross-year (histórico completo).
  - Fadiga é cross-year (dias de descanso entre campeonatos).

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 2.0.0
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np

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

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
JANELA_ROLLING = 5  # Janela de jogos para médias móveis

# Fases classificadas como mata-mata
FASES_MATA_MATA = {"F", "SF", "QF", "1/8", "3/4", "POff"}

# Fases classificatórias (fase única / repescagem)
FASES_CLASSIFICATORIA = {"1F", "2F", "3F", "4F"}

# Fases que são rodadas de pontos corridos
# R1, R2, ..., R46 e 1R
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

    # Log de distribuição
    logger.info(f"    tipo_fase:\n{df['tipo_fase'].value_counts().to_string()}")
    logger.info(f"    is_mata_mata: {df['is_mata_mata'].sum()} jogos")

    return df


# ===================================================================
# GRUPO 3 — Métricas Rolling por Time
# ===================================================================

def _criar_visao_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    'Unpivot' — cria visão centrada no time.
    Cada partida gera 2 linhas: uma do ponto de vista do mandante,
    outra do ponto de vista do visitante.
    """
    mand = pd.DataFrame({
        "match_idx": df.index,
        "perspectiva": "mandante",
        "ano_campeonato": df["ano_campeonato"].values,
        "Data": df["Data"].values,
        "time": df["Mandante"].values,
        "gols_marcados": df["gols_mandante"].values,
        "gols_sofridos": df["gols_visitante"].values,
        "resultado": df["resultado_mandante"].values,
        "pontos": df["pontos_mandante"].values,
    })

    visit = pd.DataFrame({
        "match_idx": df.index,
        "perspectiva": "visitante",
        "ano_campeonato": df["ano_campeonato"].values,
        "Data": df["Data"].values,
        "time": df["Visitante"].values,
        "gols_marcados": df["gols_visitante"].values,
        "gols_sofridos": df["gols_mandante"].values,
        "resultado": df["resultado_visitante"].values,
        "pontos": df["pontos_visitante"].values,
    })

    tv = pd.concat([mand, visit], ignore_index=True)
    tv.sort_values(["ano_campeonato", "time", "Data", "match_idx"], inplace=True)
    tv.reset_index(drop=True, inplace=True)
    return tv


def _calcular_sequencias(tv: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula streaks (sequência de vitórias, invicta, derrotas)
    de forma iterativa, registrando o estado ANTES de cada jogo.
    """
    seq_vit = np.zeros(len(tv), dtype=int)
    seq_inv = np.zeros(len(tv), dtype=int)
    seq_der = np.zeros(len(tv), dtype=int)

    prev_key = (None, None)
    cv, ci, cd = 0, 0, 0

    for i, row in enumerate(tv.itertuples()):
        key = (row.ano_campeonato, row.time)

        # Reset ao trocar de grupo (time ou campeonato)
        if key != prev_key:
            cv, ci, cd = 0, 0, 0
            prev_key = key

        # Registra estado ANTES do jogo
        seq_vit[i] = cv
        seq_inv[i] = ci
        seq_der[i] = cd

        # Atualiza com o resultado do jogo
        res = row.resultado
        if res == "V":
            cv += 1; ci += 1; cd = 0
        elif res == "E":
            cv = 0; ci += 1; cd = 0
        elif res == "D":
            cv = 0; ci = 0; cd += 1
        else:  # None (WO/ANU/IC)
            cv = 0; ci = 0; cd = 0

    tv["sequencia_vitorias"] = seq_vit
    tv["sequencia_invicta"] = seq_inv
    tv["sequencia_derrotas"] = seq_der
    return tv


def calcular_metricas_rolling(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas rolling e acumuladas por time dentro de cada campeonato.
    Todas refletem o estado ANTES do jogo atual (shift de 1 posição).
    """
    logger.info("  [Grupo 3] Calculando métricas rolling por time...")

    # 1. Criar visão por time
    tv = _criar_visao_time(df)
    grouped = tv.groupby(["ano_campeonato", "time"], sort=False)

    # 2. Médias móveis (janela de 5 jogos, shift=1 para "antes do jogo")
    W = JANELA_ROLLING
    tv["media_gols_marcados_5j"] = grouped["gols_marcados"].transform(
        lambda x: x.rolling(W, min_periods=1).mean().shift(1)
    ).round(2)

    tv["media_gols_sofridos_5j"] = grouped["gols_sofridos"].transform(
        lambda x: x.rolling(W, min_periods=1).mean().shift(1)
    ).round(2)

    tv["pontos_ultimos_5j"] = grouped["pontos"].transform(
        lambda x: x.rolling(W, min_periods=1).sum().shift(1)
    )

    # Aproveitamento nos últimos 5 jogos (%)
    jogos_janela = grouped["pontos"].transform(
        lambda x: x.rolling(W, min_periods=1).count().shift(1)
    )
    pontos_vit = tv["ano_campeonato"].apply(lambda y: 3 if y >= 1995 else 2)
    max_pts = jogos_janela * pontos_vit
    tv["aproveitamento_5j"] = ((tv["pontos_ultimos_5j"] / max_pts) * 100).round(1)

    # 3. Acumulados no campeonato (shift=1)
    tv["jogos_no_campeonato"] = grouped.cumcount()  # 0-indexed = jogos ANTES deste

    for col_orig, col_dest in [
        ("pontos", "pontos_acumulados"),
        ("gols_marcados", "gols_marcados_acumulados"),
        ("gols_sofridos", "gols_sofridos_acumulados"),
    ]:
        tv[col_dest] = grouped[col_orig].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )

    tv["saldo_gols_acumulado"] = (
        tv["gols_marcados_acumulados"] - tv["gols_sofridos_acumulados"]
    )

    # Vitórias/Empates/Derrotas acumulados
    tv["_is_v"] = (tv["resultado"] == "V").astype(int)
    tv["_is_e"] = (tv["resultado"] == "E").astype(int)
    tv["_is_d"] = (tv["resultado"] == "D").astype(int)

    tv["vitorias_acumuladas"] = grouped["_is_v"].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )
    tv["empates_acumulados"] = grouped["_is_e"].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )
    tv["derrotas_acumuladas"] = grouped["_is_d"].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )

    # 4. Sequências (streaks)
    tv = _calcular_sequencias(tv)

    # 5. Clean Sheets e Falha em Marcar (Grupo 7)
    tv["_clean_sheet"] = (tv["gols_sofridos"] == 0).astype(int)
    tv["_falhou_marcar"] = (tv["gols_marcados"] == 0).astype(int)

    # Rolling 5 jogos (shift=1 → antes do jogo)
    tv["clean_sheets_5j"] = grouped["_clean_sheet"].transform(
        lambda x: x.rolling(W, min_periods=1).sum().shift(1)
    )
    tv["falhou_marcar_5j"] = grouped["_falhou_marcar"].transform(
        lambda x: x.rolling(W, min_periods=1).sum().shift(1)
    )

    # Acumulado no campeonato (shift=1)
    tv["clean_sheets_camp"] = grouped["_clean_sheet"].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )
    tv["falhou_marcar_camp"] = grouped["_falhou_marcar"].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )

    # 6. Merge de volta ao DataFrame original (match-level)
    metricas = [
        "media_gols_marcados_5j", "media_gols_sofridos_5j",
        "pontos_ultimos_5j", "aproveitamento_5j",
        "sequencia_vitorias", "sequencia_invicta", "sequencia_derrotas",
        "jogos_no_campeonato", "pontos_acumulados",
        "gols_marcados_acumulados", "gols_sofridos_acumulados",
        "saldo_gols_acumulado",
        "vitorias_acumuladas", "empates_acumulados", "derrotas_acumuladas",
        "clean_sheets_5j", "falhou_marcar_5j",
        "clean_sheets_camp", "falhou_marcar_camp",
    ]

    # Mandante
    mand_tv = tv[tv["perspectiva"] == "mandante"][["match_idx"] + metricas].copy()
    mand_tv = mand_tv.set_index("match_idx")
    mand_tv.columns = ["mandante_" + c for c in metricas]

    # Visitante
    visit_tv = tv[tv["perspectiva"] == "visitante"][["match_idx"] + metricas].copy()
    visit_tv = visit_tv.set_index("match_idx")
    visit_tv.columns = ["visitante_" + c for c in metricas]

    df = df.join(mand_tv).join(visit_tv)

    logger.info(f"    {len(metricas)} métricas × 2 perspectivas = {len(metricas)*2} colunas adicionadas")
    return df


# ===================================================================
# GRUPO 4 — Confronto Direto (H2H)
# ===================================================================

def calcular_h2h(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula estatísticas históricas de confronto direto entre mandante
    e visitante, considerando TODOS os jogos anteriores (cross-year).
    """
    logger.info("  [Grupo 4] Calculando confronto direto (H2H)...")

    # Ordena cronologicamente
    df = df.sort_values(["Data", "ano_campeonato"]).reset_index(drop=True)

    n = len(df)
    h2h_jogos = np.zeros(n, dtype=int)
    h2h_vit_m = np.zeros(n, dtype=int)
    h2h_vit_v = np.zeros(n, dtype=int)
    h2h_emp = np.zeros(n, dtype=int)

    # Dicionário: (time_a_sorted, time_b_sorted) -> {time_a_wins, time_b_wins, draws, total}
    historico: dict[tuple, dict] = {}

    for i, row in enumerate(df.itertuples()):
        mand = row.Mandante
        visit = row.Visitante

        # Chave canônica (ordem alfabética)
        key = tuple(sorted([mand, visit]))

        if key not in historico:
            historico[key] = {}

        pair = historico[key]
        total = pair.get("total", 0)
        vm = pair.get(f"v_{mand}", 0)
        vv = pair.get(f"v_{visit}", 0)
        em = pair.get("emp", 0)

        # Estado ANTES do jogo
        h2h_jogos[i] = total
        h2h_vit_m[i] = vm
        h2h_vit_v[i] = vv
        h2h_emp[i] = em

        # Atualiza com resultado deste jogo
        res = row.resultado_mandante
        if res == "V":
            pair[f"v_{mand}"] = vm + 1
        elif res == "D":
            pair[f"v_{visit}"] = vv + 1
        elif res == "E":
            pair["emp"] = em + 1
        # WO/ANU/IC: não atualiza

        pair["total"] = total + 1

    df["h2h_jogos"] = h2h_jogos
    df["h2h_vitorias_mandante"] = h2h_vit_m
    df["h2h_vitorias_visitante"] = h2h_vit_v
    df["h2h_empates"] = h2h_emp

    logger.info(f"    {len(historico)} pares únicos de confronto mapeados")
    return df


# ===================================================================
# GRUPO 5 — Tabela Dimensão (dim_times)
# ===================================================================

def gerar_dim_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera a tabela dimensão de times com ID único e estado (UF).
    """
    logger.info("  [Grupo 5] Gerando dim_times...")

    todos_times = sorted(
        set(df["Mandante"].unique()) | set(df["Visitante"].unique())
    )

    dim = pd.DataFrame({
        "id_time": range(1, len(todos_times) + 1),
        "nome_time": todos_times,
    })

    # Adicionar estado (UF) via dicionário
    dim["estado"] = dim["nome_time"].map(ESTADO_TIME)

    sem_estado = dim[dim["estado"].isna()]["nome_time"].tolist()
    if sem_estado:
        logger.warning(f"    ⚠ Times sem estado mapeado: {sem_estado}")

    logger.info(f"    {len(dim)} times na dimensão, {dim['estado'].notna().sum()} com estado mapeado")
    return dim


def adicionar_fk_times(df: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    """Adiciona id_mandante e id_visitante como FKs referenciando dim_times."""
    mapa = dict(zip(dim["nome_time"], dim["id_time"]))
    df["id_mandante"] = df["Mandante"].map(mapa).astype("Int64")
    df["id_visitante"] = df["Visitante"].map(mapa).astype("Int64")
    return df


# ===================================================================
# GRUPO 6 — Métricas de Fadiga (Days of Rest)
# ===================================================================

def calcular_fadiga(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula dias de descanso de cada time de forma cross-year.
    Usa visão unificada (mandante + visitante) ordenada por data.
    """
    logger.info("  [Grupo 6] Calculando métricas de fadiga (dias de descanso)...")

    # Visão unificada: cada partida gera 2 registros (mandante + visitante)
    mand = pd.DataFrame({
        "match_idx": df.index,
        "perspectiva": "mandante",
        "Data": df["Data"].values,
        "time": df["Mandante"].values,
    })
    visit = pd.DataFrame({
        "match_idx": df.index,
        "perspectiva": "visitante",
        "Data": df["Data"].values,
        "time": df["Visitante"].values,
    })
    tv = pd.concat([mand, visit], ignore_index=True)
    tv.sort_values(["time", "Data", "match_idx"], inplace=True)
    tv.reset_index(drop=True, inplace=True)

    # Dias desde o último jogo (cross-year)
    tv["Data"] = pd.to_datetime(tv["Data"])
    tv["dias_descanso"] = tv.groupby("time")["Data"].diff().dt.days

    # Converter para Int64 (nullable) — primeiro jogo = NaN
    tv["dias_descanso"] = tv["dias_descanso"].astype("Int64")

    # Merge de volta: mandante
    mand_desc = tv[tv["perspectiva"] == "mandante"][["match_idx", "dias_descanso"]].copy()
    mand_desc = mand_desc.set_index("match_idx")
    mand_desc.columns = ["mandante_dias_descanso"]

    # Merge de volta: visitante
    visit_desc = tv[tv["perspectiva"] == "visitante"][["match_idx", "dias_descanso"]].copy()
    visit_desc = visit_desc.set_index("match_idx")
    visit_desc.columns = ["visitante_dias_descanso"]

    df = df.join(mand_desc).join(visit_desc)

    # Diferença de descanso (positivo = vantagem mandante)
    df["diferenca_descanso"] = df["mandante_dias_descanso"] - df["visitante_dias_descanso"]

    logger.info(f"    Média de descanso mandante: {df['mandante_dias_descanso'].mean():.1f} dias")
    logger.info(f"    Média de descanso visitante: {df['visitante_dias_descanso'].mean():.1f} dias")
    return df


# ===================================================================
# GRUPO 8 — Fator Clássico (Derby Flag)
# ===================================================================

def calcular_derby_flag(df: pd.DataFrame, dim_times: pd.DataFrame) -> pd.DataFrame:
    """
    Marca partidas entre times do mesmo estado como clássico estadual.
    Utiliza a coluna 'estado' da dim_times.
    """
    logger.info("  [Grupo 8] Calculando fator clássico (derby flag)...")

    mapa_estado = dict(zip(dim_times["nome_time"], dim_times["estado"]))

    df["estado_mandante"] = df["Mandante"].map(mapa_estado)
    df["estado_visitante"] = df["Visitante"].map(mapa_estado)

    df["is_classico_estadual"] = (
        df["estado_mandante"] == df["estado_visitante"]
    ) & df["estado_mandante"].notna()

    n_classicos = df["is_classico_estadual"].sum()
    logger.info(f"    {n_classicos} jogos marcados como clássico estadual ({n_classicos/len(df)*100:.1f}%)")

    # Remover colunas auxiliares (estado já está na dim_times)
    df.drop(columns=["estado_mandante", "estado_visitante"], inplace=True)

    return df


# ===================================================================
# Pipeline Principal
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
    """Define a ordem final das colunas da fato_partidas_ouro."""
    return [
        # Identificadores
        "id_partida",
        "id_mandante",
        "id_visitante",
        # Dados core da partida
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
        # Classificação de fase
        "tipo_fase",
        "is_mata_mata",
        # Métricas diretas
        "total_gols",
        "saldo_gols_mandante",
        "saldo_gols_visitante",
        "pontos_mandante",
        "pontos_visitante",
        # Rolling mandante
        "mandante_media_gols_marcados_5j",
        "mandante_media_gols_sofridos_5j",
        "mandante_pontos_ultimos_5j",
        "mandante_aproveitamento_5j",
        "mandante_sequencia_vitorias",
        "mandante_sequencia_invicta",
        "mandante_sequencia_derrotas",
        "mandante_jogos_no_campeonato",
        "mandante_pontos_acumulados",
        "mandante_gols_marcados_acumulados",
        "mandante_gols_sofridos_acumulados",
        "mandante_saldo_gols_acumulado",
        "mandante_vitorias_acumuladas",
        "mandante_empates_acumulados",
        "mandante_derrotas_acumuladas",
        # Clean sheets mandante
        "mandante_clean_sheets_5j",
        "mandante_falhou_marcar_5j",
        "mandante_clean_sheets_camp",
        "mandante_falhou_marcar_camp",
        # Rolling visitante
        "visitante_media_gols_marcados_5j",
        "visitante_media_gols_sofridos_5j",
        "visitante_pontos_ultimos_5j",
        "visitante_aproveitamento_5j",
        "visitante_sequencia_vitorias",
        "visitante_sequencia_invicta",
        "visitante_sequencia_derrotas",
        "visitante_jogos_no_campeonato",
        "visitante_pontos_acumulados",
        "visitante_gols_marcados_acumulados",
        "visitante_gols_sofridos_acumulados",
        "visitante_saldo_gols_acumulado",
        "visitante_vitorias_acumuladas",
        "visitante_empates_acumulados",
        "visitante_derrotas_acumuladas",
        # Clean sheets visitante
        "visitante_clean_sheets_5j",
        "visitante_falhou_marcar_5j",
        "visitante_clean_sheets_camp",
        "visitante_falhou_marcar_camp",
        # H2H
        "h2h_jogos",
        "h2h_vitorias_mandante",
        "h2h_vitorias_visitante",
        "h2h_empates",
        # Fadiga
        "mandante_dias_descanso",
        "visitante_dias_descanso",
        "diferenca_descanso",
        # Derby
        "is_classico_estadual",
    ]


def main() -> None:
    logger.info("=" * 60)
    logger.info("FEATURE ENGINEERING GOLD — Brasileirão Histórico")
    logger.info("=" * 60)

    # ── Carregar Silver ────────────────────────────────────────────
    df = carregar_silver()

    # ── Grupo 1: Métricas diretas ──────────────────────────────────
    df = calcular_metricas_diretas(df)

    # ── Grupo 2: Classificação de fases ────────────────────────────
    df = classificar_fases(df)

    # ── Grupo 3: Métricas rolling ──────────────────────────────────
    df = calcular_metricas_rolling(df)

    # ── Grupo 4: Confronto direto (H2H) ───────────────────────────
    df = calcular_h2h(df)

    # ── Grupo 5: dim_times ─────────────────────────────────────────
    dim_times = gerar_dim_times(df)
    df = adicionar_fk_times(df, dim_times)

    # ── Grupo 6: Métricas de fadiga ────────────────────────────────
    df = calcular_fadiga(df)

    # ── Grupo 8: Fator clássico (derby flag) ───────────────────────
    df = calcular_derby_flag(df, dim_times)

    # ── ID da partida ──────────────────────────────────────────────
    df["id_partida"] = range(1, len(df) + 1)

    # ── Reordenar colunas ──────────────────────────────────────────
    colunas = definir_colunas_finais()
    colunas = [c for c in colunas if c in df.columns]
    df = df[colunas]

    # ── Salvar ─────────────────────────────────────────────────────
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    fato_path = GOLD_DIR / "fato_partidas_ouro.csv"
    df.to_csv(fato_path, index=False, encoding="utf-8-sig")
    logger.info(f"  ✅ Fato salva: {fato_path}")
    logger.info(f"     Shape: {df.shape}")

    dim_path = GOLD_DIR / "dim_times.csv"
    dim_times.to_csv(dim_path, index=False, encoding="utf-8-sig")
    logger.info(f"  ✅ Dimensão salva: {dim_path}")
    logger.info(f"     Shape: {dim_times.shape}")

    # ── Resumo ─────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO FINAL — CAMADA GOLD")
    logger.info("=" * 60)
    logger.info(f"Total de partidas    : {len(df)}")
    logger.info(f"Total de colunas     : {len(df.columns)}")
    logger.info(f"Times na dimensão    : {len(dim_times)}")
    logger.info(f"Colunas: {list(df.columns)}")
    logger.info("")
    logger.info("Tipos:")
    logger.info(f"\n{df.dtypes.to_string()}")
    logger.info("=" * 60)
    logger.info("Feature Engineering Gold finalizado.")


if __name__ == "__main__":
    main()
