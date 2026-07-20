# 🏟️ Brasileirão Analytics - Infraestrutura e Engenharia de Dados

> **[System Context para Assistentes de IA]**
> Se você é uma IA lendo este documento, utilize-o como contexto absoluto para ajudar no desenvolvimento deste projeto. O usuário é um estudante de Ciência da Computação construindo seu TCC focado em Engenharia de Dados. Assuma o papel de um Engenheiro de Dados Sênior/Arquiteto de Software. Responda de forma técnica, utilize boas práticas de mercado, foque em performance e mantenha o escopo definido abaixo. Não sugira mudanças arquiteturais drásticas a menos que haja um erro fatal de design.

---

## 📌 Visão Geral do Repositório
Este é o repositório de **Dados (`brasileirao-infra`)** do projeto Brasileirão Analytics. Ele utiliza uma arquitetura **Polyrepo**.
A responsabilidade exclusiva deste repositório é:
1. Extrair dados históricos do futebol brasileiro (1971 - Presente).
2. Limpar e padronizar os dados usando a Arquitetura Medalhão (Bronze > Prata > Ouro).
3. Consolidar tudo em uma **One Big Table (OBT)** desnormalizada e legível.
4. Publicar a OBT em uma **planilha pública do Google Sheets**, que serve de repositório final de dados.

*Nota: A biblioteca Python (Client) que consome a planilha residirá em um repositório separado (`brasileirao-lib`). Todos os cálculos dinâmicos — confronto direto (H2H), forma dos últimos 5 jogos, sequências, fadiga, acumulados — são responsabilidade dela, não desta pipeline.*

## 🛠️ Stack Tecnológico
* **Linguagem:** Python 3.x
* **Processamento de Dados:** Pandas, NumPy
* **Repositório de Dados Final:** Google Sheets (via `gspread` + `google-auth`)
* **Web Scraping:** Playwright (headless Chromium) + BeautifulSoup4
* **Orquestração:** GitHub Actions (agendado 3x na semana)

## 🏗️ Pipeline de Dados (Medallion Architecture)
O ETL está estruturado em três camadas dentro da pasta `/data` (versionada no git — os CSVs são o ativo do projeto e permitem execuções incrementais no CI):

* **🥉 Bronze (Extract):** Dados brutos extraídos da web via Playwright. O formato original é preservado 100% como backup. Cobertura: 1971–2025, 55 edições, ~21.500 jogos. No CI, apenas o ano corrente é re-raspado (`ANOS_EXTRACAO=atual`).
* **🥈 Prata (Transform):** Limpeza, tipagem rigorosa (datetime, Int64 nullable), padronização de nomenclatura de clubes (~170 mapeamentos de-para), split de placar, cálculo de resultados. Saída: 10 colunas limpas por jogo.
* **🥇 Ouro (OBT):** Consolidação de todas as edições em uma única tabela desnormalizada (`brasileirao_obt.csv`, 21 colunas) — a "fotografia" de cada partida, com tudo por extenso (nomes de clubes, estados/UF, fases legíveis), sem IDs externos nem tabelas dimensão. Enriquecimentos por partida (não temporais): total/saldo de gols, pontos com regra histórica (2 pts antes de 1995, 3 pts depois), `tipo_fase`/`is_mata_mata` e `is_classico_estadual`.

O dicionário de dados completo da OBT está em [`data/gold/dicionario_dados_gold.md`](data/gold/dicionario_dados_gold.md).

## 📤 Carga (Load) — Google Sheets
O `etl/load.py` publica a OBT diretamente na planilha do Google Sheets (aba `partidas`):
* **Autenticação:** Service Account do Google Cloud. Em produção, o conteúdo do `credentials.json` é injetado via Secret (`GOOGLE_CREDENTIALS`); em desenvolvimento, o arquivo local `credentials.json` (ignorado pelo git) é usado como fallback.
* **Modos** (via `LOAD_MODO`): `overwrite` (padrão — limpa e regrava, idempotente) ou `append` (upsert incremental — casa OBT × planilha pela chave natural `ano + Mandante + Visitante + Fase`, atualiza in-place as linhas que mudaram, como jogos futuros que ganharam placar ou adiamentos, e acrescenta apenas partidas inéditas).
* **Performance:** upload em chunks de 5.000 linhas para respeitar os limites da API.

## ⚙️ Orquestração — GitHub Actions
O workflow [`.github/workflows/etl.yml`](.github/workflows/etl.yml) roda **3x na semana** (seg/qui/sáb às 06:00 UTC) e também aceita disparo manual (`workflow_dispatch`):
1. Extract (apenas ano corrente) → 2. Transform → 3. Gold (OBT) → 4. Load (Sheets) → 5. Commit automático dos CSVs atualizados.

**Secrets necessários** (Settings → Secrets and variables → Actions):
| Secret | Conteúdo |
|--------|----------|
| `OGOL_ACCOUNTS` | Contas do Ogol: `email1:senha1,email2:senha2` |
| `SPREADSHEET_ID` | ID da planilha de destino (trecho entre `/d/` e `/edit` na URL) |
| `GOOGLE_CREDENTIALS` | Conteúdo JSON integral do `credentials.json` da Service Account |

*A Service Account precisa de acesso de **Editor** na planilha (compartilhe a planilha com o `client_email` da conta).*

## 📂 Estrutura de Diretórios

```text
brasileirao-infra/
├── .env.example            # Modelo das variáveis de ambiente
├── .gitignore              # Ignora credenciais e venv (dados são versionados)
├── requirements.txt        # Dependências do projeto
├── README.md               # Documentação principal
│
├── .github/workflows/
│   └── etl.yml             # Pipeline agendada (3x/semana)
│
├── data/                   # Armazenamento versionado
│   ├── bronze/             # Dados brutos extraídos
│   ├── silver/             # Dados limpos e padronizados (10 cols)
│   └── gold/               # One Big Table consolidada
│       ├── brasileirao_obt.csv
│       └── dicionario_dados_gold.md
│
├── etl/                    # Scripts de Engenharia de Dados
│   ├── __init__.py
│   ├── config.py           # URLs e leitura de credenciais via env
│   ├── extract.py          # Camada Bronze (Web Scraping via Playwright)
│   ├── transform.py        # Camada Prata (Limpeza e padronização)
│   ├── gold.py             # Camada Ouro (One Big Table)
│   ├── mapear_times.py     # Utilitário: extrai times únicos da Bronze
│   └── load.py             # Publicação no Google Sheets (gspread)
│
└── relatório acompanhamento/  # Logs de progresso do projeto
```

## ▶️ Execução Local

```bash
pip install -r requirements.txt
playwright install chromium

cp .env.example .env        # preencha OGOL_ACCOUNTS e SPREADSHEET_ID
# deixe o credentials.json da Service Account na raiz (ignorado pelo git)

python etl/extract.py       # Bronze  (ANOS_EXTRACAO=atual para só o ano corrente)
python etl/transform.py     # Prata
python etl/gold.py          # Ouro (OBT)
python etl/load.py          # Publica no Google Sheets
```
