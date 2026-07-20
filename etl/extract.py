"""
etl/extract.py
==============
Camada Bronze — Scraping do frontend da tabela da CBF.

Fonte  : https://www.cbf.com.br/futebol-brasileiro/tabelas/campeonato-brasileiro/serie-a/{ano}
         (página renderizada no navegador — sem barreira anti-bot,
         funciona inclusive de IPs de datacenter, como os runners do
         GitHub Actions)
Alvo   : Edições >= 2012 (anos disponíveis na tabela detalhada da CBF).
         O histórico 1971–2011 foi extraído do Ogol via etl/extract_ogol.py
         e permanece versionado em data/bronze/.
Saída  : data/bronze/jogos_{ano}.csv  (um arquivo por ano)

Fluxo:
  1. Playwright abre Chromium headless na página do ano e aguarda os
     cards de jogos substituírem os skeletons de carregamento
  2. Itera o seletor de rodadas da seção "Jogos" (1..38), aguardando a
     re-renderização dos cards a cada troca
  3. O HTML dos cards é passado ao BeautifulSoup, que extrai:
       Data, Mandante, Placar_Bruto, Visitante, Fase ("R{rodada}")
     (jogos futuros ficam com placar vazio; "A Definir" → data vazia)
  4. Resultado salvo em CSV por ano

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 7.0.0
"""

import os
import re
import sys
import time
import logging
from datetime import date
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, Locator, TimeoutError as PWTimeout

from config import CBF_TABELA_URL

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
# Constantes
# ---------------------------------------------------------------------------
BRONZE_DIR = Path(__file__).resolve().parents[1] / "data" / "bronze"
DEBUG_DIR = Path(__file__).resolve().parents[1] / "debug"

PRIMEIRO_ANO_CBF = 2012   # primeira edição disponível na tabela da CBF
RODADA_DELAY = 0.3        # pausa extra entre rodadas (segundos)
RENDER_TIMEOUT = 20_000   # espera máxima pela renderização dos cards (ms)

# Seletores por fragmento de classe: o Next.js gera sufixos hasheados
# (ex.: styles_gameCardContainer__qbcs6), então casamos pelo prefixo estável
SEL_CARD = "div[class*='gameCardContainer']"
SEL_SKELETON = ".react-loading-skeleton"


# ---------------------------------------------------------------------------
# Debug
# ---------------------------------------------------------------------------

def _dump_debug(page: Page, tag: str) -> None:
    """
    Salva screenshot + HTML da página atual em ./debug para diagnóstico
    (no GitHub Actions a pasta é publicada como artifact em caso de falha).
    """
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        tag = "".join(c if c.isalnum() else "_" for c in tag)
        page.screenshot(path=str(DEBUG_DIR / f"{tag}.png"), full_page=True)
        (DEBUG_DIR / f"{tag}.html").write_text(page.content(), encoding="utf-8")
        logger.info(f"  🧪 Debug salvo em {DEBUG_DIR / tag}.png/.html")
    except Exception as e:
        logger.warning(f"  Falha ao salvar debug: {e}")


# ---------------------------------------------------------------------------
# Navegação
# ---------------------------------------------------------------------------

def abrir_pagina_ano(page: Page, ano: int) -> Locator:
    """
    Abre a página da edição e retorna o <aside> da seção "Jogos" com os
    cards já renderizados (skeletons substituídos).
    """
    url = CBF_TABELA_URL.format(ano=ano)
    logger.info(f"  Abrindo: {url}")

    page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    # Aguarda ao menos um card real de jogo aparecer
    page.wait_for_selector(SEL_CARD, state="visible", timeout=RENDER_TIMEOUT)

    aside = page.locator("aside").filter(has_text="Jogos").first
    aside.wait_for(state="visible", timeout=RENDER_TIMEOUT)
    return aside


def listar_rodadas(aside: Locator) -> list[str]:
    """Lê os valores disponíveis no seletor de rodadas (ex.: '1'..'38')."""
    valores = aside.locator("select option").evaluate_all(
        "opts => opts.map(o => o.value)"
    )
    rodadas = sorted({v for v in valores if str(v).isdigit()}, key=int)
    if not rodadas:
        raise RuntimeError("Seletor de rodadas não encontrado — layout da página mudou?")
    return rodadas


def rodada_selecionada(aside: Locator) -> str:
    """Valor atualmente selecionado no seletor de rodadas."""
    return str(aside.locator("select").first.evaluate("s => s.value"))


def selecionar_rodada(page: Page, aside: Locator, rodada: str) -> None:
    """
    Troca a rodada no seletor e aguarda os cards re-renderizarem
    (skeletons aparecem durante o carregamento e depois somem).
    """
    aside.locator("select").first.select_option(rodada)

    # Espera qualquer skeleton de carregamento desaparecer
    try:
        page.wait_for_function(
            f"document.querySelectorAll(\"aside {SEL_SKELETON}\").length === 0",
            timeout=RENDER_TIMEOUT,
        )
    except PWTimeout:
        logger.warning(f"  Rodada {rodada}: skeletons persistem — tentando parsear mesmo assim.")

    # Pequena folga para o React terminar de montar os cards
    page.wait_for_timeout(400)


# ---------------------------------------------------------------------------
# Parsing dos cards
# ---------------------------------------------------------------------------

def _info_time(div) -> tuple[str, str]:
    """
    Extrai (nome, gols) de um bloco de time dentro do card.
    O nome completo está no atributo title do <strong> (o texto visível
    é abreviado, ex.: 'Bot'); os gols ficam no <span> irmão.
    """
    strong = div.find("strong")
    nome = ""
    if strong:
        nome = (strong.get("title") or strong.get_text(strip=True) or "").strip()

    span = div.find("span")
    gols = span.get_text(strip=True) if span else ""
    return nome, gols


def parse_cards(aside_html: str, rodada: str) -> list[dict]:
    """
    Extrai os jogos dos cards renderizados de uma rodada.
    Estrutura de cada card:
      div[gameCardContainer]
        └─ div[score]  → div(mandante: strong[title] + span gols)
                         span "X"
                         div(visitante: strong[title] + span gols)
        └─ div[informations] → p com "dd/mm/aaaa - hh:mm | cidade | estádio"
    """
    soup = BeautifulSoup(aside_html, "lxml")
    jogos: list[dict] = []

    for card in soup.select(SEL_CARD):
        try:
            score = card.select_one("div[class*='score']")
            if not score:
                continue

            times = score.find_all("div", recursive=False)
            if len(times) != 2:
                continue

            mandante, gols_m = _info_time(times[0])
            visitante, gols_v = _info_time(times[1])

            if not mandante and not visitante:
                continue

            # Placar só quando ambos os gols são numéricos (jogo realizado)
            placar = f"{gols_m}-{gols_v}" if gols_m.isdigit() and gols_v.isdigit() else ""

            # Data no bloco de informações: "16/07/2026 - 19:30 ..."
            info = card.select_one("div[class*='informations'] p")
            texto_info = info.get_text(" ", strip=True) if info else ""
            m = re.search(r"(\d{2})/(\d{2})/(\d{4})", texto_info)
            data_iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""

            jogos.append({
                "Data": data_iso,
                "Mandante": mandante,
                "Placar_Bruto": placar,
                "Visitante": visitante,
                "Fase": f"R{rodada}",
            })

        except (AttributeError, TypeError) as e:
            logger.warning(f"  Card ignorado por estrutura inesperada: {e}")

    return jogos


# ---------------------------------------------------------------------------
# Extração de um ano
# ---------------------------------------------------------------------------

def extrair_ano(page: Page, ano: int) -> list[dict]:
    """Percorre todas as rodadas da edição parseando os cards de cada uma."""
    aside = abrir_pagina_ano(page, ano)
    rodadas = listar_rodadas(aside)
    logger.info(f"  {len(rodadas)} rodadas encontradas no seletor")

    matches: list[dict] = []
    atual = rodada_selecionada(aside)

    for rodada in rodadas:
        # A rodada corrente já vem renderizada; as demais precisam do select
        if rodada != atual:
            selecionar_rodada(page, aside, rodada)
            atual = rodada

        extraidos = parse_cards(aside.inner_html(), rodada)

        if not extraidos:
            logger.warning(f"  Rodada {rodada}: nenhum jogo parseado.")
            _dump_debug(page, f"rodada_vazia_{ano}_r{rodada}")
        else:
            logger.info(f"  Rodada {rodada}: {len(extraidos)} jogo(s)")
            matches.extend(extraidos)

        time.sleep(RODADA_DELAY)

    return matches


# ---------------------------------------------------------------------------
# Seleção de anos
# ---------------------------------------------------------------------------

def selecionar_anos() -> list[int]:
    """
    Filtra os anos a extrair com base na variável de ambiente ANOS_EXTRACAO:
      - "atual" (padrão)  → apenas o ano corrente
      - "todos"           → todas as edições cobertas pela CBF (>= 2012)
      - "2025,2026"       → lista explícita de anos
    """
    raw = os.getenv("ANOS_EXTRACAO", "atual").strip().lower()
    ano_atual = date.today().year

    if raw in ("", "atual", "current"):
        return [ano_atual]

    if raw in ("todos", "all"):
        return list(range(PRIMEIRO_ANO_CBF, ano_atual + 1))

    anos = []
    for parte in raw.split(","):
        parte = parte.strip()
        if parte.isdigit() and PRIMEIRO_ANO_CBF <= int(parte) <= ano_atual:
            anos.append(int(parte))
        elif parte:
            logger.warning(
                f"ANOS_EXTRACAO: ano '{parte}' fora da cobertura da CBF "
                f"({PRIMEIRO_ANO_CBF}–{ano_atual}; para o histórico anterior "
                f"use etl/extract_ogol.py)"
            )

    if not anos:
        raise ValueError(f"ANOS_EXTRACAO='{raw}' não resultou em nenhum ano válido.")

    return sorted(anos)


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

def save_to_csv(matches: list[dict], output_path: Path, ano: int) -> None:
    if not matches:
        logger.warning(f"Nenhum jogo extraído para {ano}. CSV não será criado.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(matches, columns=["Data", "Mandante", "Placar_Bruto", "Visitante", "Fase"])
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info(f"{len(df)} jogo(s) salvos em: {output_path}")


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

def main() -> None:
    anos = selecionar_anos()

    logger.info("=" * 60)
    logger.info("EXTRAÇÃO BRONZE — Brasileirão (frontend CBF)")
    logger.info(f"Anos a processar: {anos}")
    logger.info("=" * 60)

    total_jogos = 0
    anos_ok: list[int] = []
    anos_falha: list[int] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="pt-BR",
        )
        page = context.new_page()

        for ano in anos:
            logger.info("")
            logger.info(f"{'─' * 50}")
            logger.info(f"Processando ano: {ano}")
            logger.info(f"{'─' * 50}")

            try:
                matches = extrair_ano(page, ano)
            except Exception as e:
                logger.error(f"Falha ao extrair {ano}: {e}")
                _dump_debug(page, f"falha_{ano}")
                anos_falha.append(ano)
                continue

            if matches:
                save_to_csv(matches, BRONZE_DIR / f"jogos_{ano}.csv", ano)
                total_jogos += len(matches)
                anos_ok.append(ano)
            else:
                anos_falha.append(ano)

        browser.close()

    # ── Resumo ─────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO FINAL")
    logger.info("=" * 60)
    logger.info(f"Anos extraídos com sucesso : {len(anos_ok)}  → {anos_ok}")
    logger.info(f"Anos sem dados / com falha : {len(anos_falha)} → {anos_falha}")
    logger.info(f"Total de jogos extraídos   : {total_jogos}")
    logger.info(f"Arquivos salvos em         : {BRONZE_DIR}")
    logger.info("=" * 60)
    logger.info("Extração finalizada.")

    # Nenhum ano extraído = falha da pipeline (evita que o Load publique
    # a planilha sem os dados novos)
    if anos and not anos_ok:
        logger.error("Nenhum ano foi extraído com sucesso — abortando com erro.")
        sys.exit(1)


if __name__ == "__main__":
    main()
