"""
etl/extract.py
==============
Camada Bronze — Web Scraping de partidas históricas do Brasileirão.

Fonte  : https://www.ogol.com.br  (login + scraping inteiramente via Playwright)
Alvo   : Todos os anos mapeados em etl/config.py
Saída  : data/bronze/jogos_{ano}.csv  (um arquivo por ano)

Fluxo:
  1. Playwright abre Chromium headless e faz login com a 1ª conta da lista
  2. O mesmo contexto autenticado navega em todas as URLs de calendário
  3. Ao detectar mensagem de limite de visualizações, troca automaticamente
     para a próxima conta da lista e retoma do mesmo ano/página
  4. HTML de cada página é passado para BeautifulSoup para extração
  5. Resultado salvo em CSV por ano

Autor  : Brasileirão Analytics — Engenharia de Dados
Versão : 5.0.0
"""

import time
import logging
from pathlib import Path

from bs4 import BeautifulSoup
import pandas as pd
from playwright.sync_api import sync_playwright, Page, BrowserContext, TimeoutError as PWTimeout

from config import URLS_OGOL_BRASILEIRAO, OGOL_ACCOUNTS

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
PAGE_DELAY: float = 1.5        # pausa entre páginas (segundos)
YEAR_DELAY: float = 2.0        # pausa extra entre anos
MAX_EMPTY_STREAK: int = 3      # páginas vazias consecutivas para parar

BRONZE_DIR = Path(__file__).resolve().parents[1] / "data" / "bronze"
TABLE_CLASS = "zztable stats"
PAGE_PARAMS = "?fase_in=0&equipa=0&estado=&filtro=&op=calendario&page={page}"

# Textos que indicam limite de visualizações atingido
LIMIT_PHRASES = [
    "atingiu o limite de visualizações",
    "limite de visualiz",
    "view limit",
    "limite de vistas",
]

# ---------------------------------------------------------------------------
# Autenticação
# ---------------------------------------------------------------------------

def _do_login(page: Page, email: str, password: str) -> bool:
    """
    Faz login com um par (email, password) especifico.
    Retorna True se o login for bem-sucedido.
    """
    logger.info(f"  → Acessando ogol.com.br/login.php com: {email}")
    try:
        page.goto(
            "https://www.ogol.com.br/login.php",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        # Aguarda o JS carregar o modal de login automaticamente
        page.wait_for_timeout(4_000)

        # Aguarda o campo de e-mail do modal (XPath exato do formulário)
        logger.info("Aguardando modal de login ...")
        XPATH_EMAIL = "xpath=//*[@id='zz-login-form']/div[2]/label/input"
        XPATH_SENHA = "xpath=//*[@id='zz-login-form']/div[3]/label/input"

        page.wait_for_selector(XPATH_EMAIL, state="visible", timeout=20_000)
        page.fill(XPATH_EMAIL, email)
        logger.info(f"Email preenchido: {email}")

        page.wait_for_selector(XPATH_SENHA, state="visible", timeout=5_000)
        page.fill(XPATH_SENHA, password)
        logger.info("Senha preenchida.")

        # Submete o formulário
        try:
            page.wait_for_selector("button.zz-btn.image.block", state="visible", timeout=5_000)
            page.click("button.zz-btn.image.block")
        except PWTimeout:
            logger.info("Botão não encontrado, usando Enter.")
            page.press(XPATH_SENHA, "Enter")

        page.wait_for_timeout(3_000)

        content = page.content()
        logged_in = (
            "ZZ.logged\t= 1" in content
            or "ZZ.logged = 1" in content
            or "logout" in content.lower()
        )

        if logged_in:
            logger.info(f"Login realizado com sucesso! ({email})")
            return True

        logger.error(f"Login falhou para: {email}")
        return False

    except PWTimeout as e:
        logger.error(f"Timeout durante login com {email}: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado durante login com {email}: {e}")
        return False


def login_with_fallback(page: Page, account_index: int) -> int:
    """
    Tenta fazer login percorrendo a lista OGOL_ACCOUNTS a partir de account_index.
    Retorna o índice da conta que conseguiu logar, ou -1 se todas falharem.
    """
    valid_accounts = [
        (u, p) for u, p in OGOL_ACCOUNTS
        if u not in ("SEGUNDO_EMAIL_AQUI", "") and p not in ("SEGUNDA_SENHA_AQUI", "")
    ]

    if not valid_accounts:
        logger.error("Nenhuma conta configurada em OGOL_ACCOUNTS (etl/config.py).")
        return -1

    for idx in range(account_index, len(valid_accounts)):
        email, password = valid_accounts[idx]
        logger.info(f"Tentando conta [{idx + 1}/{len(valid_accounts)}]: {email}")
        if _do_login(page, email, password):
            return idx

    logger.error("Todas as contas falharam no login.")
    return -1


def logout(page: Page) -> None:
    """Faz logout da conta atual para preparar a troca de conta."""
    try:
        page.goto(
            "https://www.ogol.com.br/login.php?op=logout",
            wait_until="domcontentloaded",
            timeout=15_000,
        )
        page.wait_for_timeout(2_000)
        logger.info("  ↩️  Logout realizado.")
    except Exception as e:
        logger.warning(f"  Falha ao fazer logout (continuando mesmo assim): {e}")


# ---------------------------------------------------------------------------
# Detecção de limite
# ---------------------------------------------------------------------------

def is_view_limit_reached(html: str) -> bool:
    """
    Retorna True se o HTML contiver mensagem de limite de visualizações do Ogol.
    """
    html_lower = html.lower()
    return any(phrase in html_lower for phrase in LIMIT_PHRASES)


# ---------------------------------------------------------------------------
# Parsing de jogos
# ---------------------------------------------------------------------------

def parse_text(tag) -> str:
    if tag is None:
        return ""
    return tag.get_text(strip=True)


def parse_row(row) -> dict | None:
    """
    Extrai os dados de uma linha <tr> da tabela de jogos.
        <td class="date">       → Data
        <td class="... home">   → Mandante
        <td class="result">     → Placar_Bruto
        <td class="... away">   → Visitante
        <td class="phase">      → Fase
    """
    try:
        td_date = row.find("td", class_="date")
        data = parse_text(td_date)

        td_home = row.find("td", class_="home")
        mandante_tag = (td_home.find("a") or td_home.find("b")) if td_home else None
        mandante = parse_text(mandante_tag)

        td_result = row.find("td", class_="result")
        placar_tag = td_result.find("a") if td_result else None
        placar_bruto = parse_text(placar_tag)

        td_away = row.find("td", class_="away")
        visitante_tag = (td_away.find("a") or td_away.find("b")) if td_away else None
        visitante = parse_text(visitante_tag)

        td_phase = row.find("td", class_="phase")
        fase = parse_text(td_phase)

        if not mandante and not visitante:
            return None

        return {
            "Data": data,
            "Mandante": mandante,
            "Placar_Bruto": placar_bruto,
            "Visitante": visitante,
            "Fase": fase,
        }

    except (AttributeError, TypeError) as e:
        logger.warning(f"  Linha ignorada por estrutura inesperada: {e}")
        return None


# ---------------------------------------------------------------------------
# Scraping via Playwright — com suporte a troca de conta
# ---------------------------------------------------------------------------

class AccountLimitError(Exception):
    """Sinaliza que a conta atual atingiu o limite de visualizações."""
    pass


def scrape_year(page: Page, base_url: str, ano: int) -> list[dict]:
    """
    Itera todas as páginas do calendário de um ano usando o browser autenticado.
    Lança AccountLimitError se detectar mensagem de limite durante o scraping.
    """
    all_matches: list[dict] = []
    pg = 1
    empty_streak = 0

    while True:
        url = base_url + PAGE_PARAMS.format(page=pg)
        logger.info(f"  --- Processando página {pg} ---")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            # Aguarda tabela OU detecção de limite (o que vier primeiro)
            try:
                page.wait_for_selector(
                    "table.zztable.stats, .limit-message, [class*='limit']",
                    state="visible",
                    timeout=10_000,
                )
            except PWTimeout:
                # Nenhum dos dois apareceu — verifica o HTML atual
                pass

        except PWTimeout:
            logger.warning(f"Timeout ao carregar página {pg}. Pulando.")
            pg += 1
            time.sleep(PAGE_DELAY)
            continue

        html = page.content()

        # ── Detecção de limite de visualizações ────────────────────────────
        if is_view_limit_reached(html):
            logger.warning(
                f"Limite de visualizações atingido na página {pg} do ano {ano}!"
            )
            raise AccountLimitError(f"Limite atingido no ano {ano}, página {pg}")

        # ── Verificação da tabela ──────────────────────────────────────────
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_=TABLE_CLASS)
        if not table:
            logger.info("Tabela de jogos não encontrada — fim da paginação detectado.")
            break

        tbody = table.find("tbody")
        if not tbody:
            logger.warning(f"Página {pg}: <tbody> não encontrado. Pulando.")
            pg += 1
            time.sleep(PAGE_DELAY)
            continue

        rows = tbody.find_all("tr")
        page_matches: list[dict] = []
        for row in rows:
            match_data = parse_row(row)
            if match_data:
                page_matches.append(match_data)

        logger.info(f"Página {pg}: {len(page_matches)} jogo(s) extraído(s).")
        all_matches.extend(page_matches)

        if len(page_matches) == 0:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_STREAK:
                logger.info(f"{MAX_EMPTY_STREAK} páginas consecutivas sem jogos — fim dos dados.")
                break
        else:
            empty_streak = 0

        pg += 1
        time.sleep(PAGE_DELAY)

    return all_matches


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
    logger.info(f"Shape: {df.shape} | Colunas: {list(df.columns)}")


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

def main() -> None:
    valid_accounts = [
        (u, p) for u, p in OGOL_ACCOUNTS
        if u not in ("SEGUNDO_EMAIL_AQUI", "") and p not in ("SEGUNDA_SENHA_AQUI", "")
    ]

    logger.info("=" * 60)
    logger.info("EXTRAÇÃO BRONZE — Brasileirão Histórico (Ogol)")
    logger.info(f"Anos a processar : {len(URLS_OGOL_BRASILEIRAO)}")
    logger.info(f"Contas disponíveis: {len(valid_accounts)}")
    logger.info("=" * 60)

    total_jogos = 0
    anos_ok: list[int] = []
    anos_falha: list[int] = []

    anos_para_processar = sorted(URLS_OGOL_BRASILEIRAO.items())
    current_account_idx = 0

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

        # ── Login inicial ──────────────────────────────────────────────────
        current_account_idx = login_with_fallback(page, current_account_idx)
        if current_account_idx == -1:
            logger.error("Abortando: não foi possível autenticar em nenhuma conta.")
            browser.close()
            return

        # ── Loop por todos os anos ─────────────────────────────────────────
        i = 0
        while i < len(anos_para_processar):
            ano, base_url = anos_para_processar[i]

            logger.info("")
            logger.info(f"{'─' * 50}")
            logger.info(f"Processando ano: {ano}")
            logger.info(f"URL: {base_url}")
            logger.info(f"{'─' * 50}")

            output_path = BRONZE_DIR / f"jogos_{ano}.csv"

            try:
                matches = scrape_year(page, base_url, ano)

                if matches:
                    save_to_csv(matches, output_path, ano)
                    total_jogos += len(matches)
                    anos_ok.append(ano)
                else:
                    anos_falha.append(ano)

                i += 1
                time.sleep(YEAR_DELAY)

            except AccountLimitError:
                # ── Troca de conta ─────────────────────────────────────────
                next_idx = current_account_idx + 1

                if next_idx >= len(valid_accounts):
                    logger.error("Limite atingido e não há mais contas disponíveis.")
                    logger.error(f"Anos restantes não processados: {[a for a, _ in anos_para_processar[i:]]}")
                    break

                logger.info(f"Trocando para a conta [{next_idx + 1}/{len(valid_accounts)}]...")
                logout(page)

                current_account_idx = login_with_fallback(page, next_idx)
                if current_account_idx == -1:
                    logger.error("Todas as contas esgotadas. Encerrando extração.")
                    break

                logger.info(f"Retomando do ano {ano} com a nova conta.")
                # NÃO incrementa i — retenta o mesmo ano com a nova conta

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


if __name__ == "__main__":
    main()
