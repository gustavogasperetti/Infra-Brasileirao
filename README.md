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
* **Extração corrente:** scraping do frontend da CBF com Playwright (headless Chromium) + BeautifulSoup4 — o site da CBF não tem barreira anti-bot, então funciona nos runners do GitHub Actions
* **Extração histórica (legado):** Playwright + BeautifulSoup4 sobre o Ogol (`etl/extract_ogol.py` — o Ogol bloqueia IPs de datacenter via Cloudflare Turnstile, então só roda localmente)
* **Orquestração:** GitHub Actions (agendado 3x na semana, sem servidores)

## 🏗️ Pipeline de Dados (Medallion Architecture)
O ETL está estruturado em três camadas dentro da pasta `/data` (**versionada no git** — os CSVs são o ativo do projeto e permitem execuções incrementais no CI, que parte do histórico já commitado e re-extrai só o ano corrente):

* **🥉 Bronze (Extract):** Dados brutos no contrato `Data, Mandante, Placar_Bruto, Visitante, Fase`. Histórico 1971–2025 extraído do Ogol (55 edições, ~21.500 jogos); ano corrente raspado do frontend da tabela da CBF (`etl/extract.py`, edições ≥ 2012 — o Playwright renderiza a página, itera o seletor de rodadas e o BeautifulSoup parseia os cards de jogos; seletores casam por fragmento estável de classe, resistindo aos hashes do Next.js). Jogos futuros entram com placar vazio (o calendário completo da temporada fica disponível). Controle por `ANOS_EXTRACAO`: `atual` (padrão), `todos` ou lista `2025,2026`.
* **🥈 Prata (Transform):** Limpeza, tipagem rigorosa (datetime, Int64 nullable), padronização de nomenclatura de clubes (~170 mapeamentos de-para, cobrindo as grafias do Ogol e da CBF — ex.: `Santos FC`→`Santos`, `Coritiba SAF`→`Coritiba`), split de placar, tratamento de placares especiais (WO/ANU/IC), cálculo de resultados (V/E/D). Saída: 10 colunas limpas por jogo.
* **🥇 Ouro (OBT):** Consolidação de todas as edições em uma única tabela desnormalizada (`brasileirao_obt.csv`, 21 colunas, ~21.800 partidas) — a "fotografia" de cada partida, com tudo por extenso (nomes de clubes, estados/UF, fases legíveis), sem IDs externos nem tabelas dimensão. Enriquecimentos por partida (não temporais): total/saldo de gols, pontos com regra histórica (2 pts antes de 1995, 3 pts a partir de 1995), `tipo_fase`/`is_mata_mata` e `is_classico_estadual`.

O dicionário de dados completo da OBT está em [`data/gold/dicionario_dados_gold.md`](data/gold/dicionario_dados_gold.md).

## 📤 Carga (Load) — Google Sheets
O `etl/load.py` publica a OBT diretamente na planilha do Google Sheets (aba `partidas`):
* **Autenticação:** Service Account do Google Cloud. Em produção, o conteúdo do `credentials.json` é injetado via Secret (`GOOGLE_CREDENTIALS`); em desenvolvimento, o arquivo local `credentials.json` (ignorado pelo git) é usado como fallback.
* **Modos** (via `LOAD_MODO`):
  * `overwrite` (padrão) — limpa a aba e regrava tudo (idempotente; a planilha sempre espelha a OBT).
  * `append` — **upsert incremental**: casa OBT × planilha pela chave natural (`ano_campeonato + Mandante + Visitante + Fase`, estável mesmo com adiamentos), atualiza in-place as linhas que mudaram (jogo futuro que ganhou placar, adiamento, correção) e acrescenta apenas partidas inéditas. Fallback automático para regravação completa quando a aba está vazia, o cabeçalho diverge (esquema mudou) ou há mais de 2.000 linhas modificadas.
* **Performance:** uploads em chunks de 5.000 linhas e updates via `values.batchUpdate` para respeitar os limites da API.

## ⚙️ Orquestração — GitHub Actions
O workflow [`.github/workflows/etl.yml`](.github/workflows/etl.yml) roda **segunda, quarta e sexta às 06:00 UTC** (03:00 BRT — horário em que todos os jogos da noite anterior já terminaram) e também aceita disparo manual (`workflow_dispatch`):

1. Extract (ano corrente, frontend CBF) → 2. Transform → 3. Gold (OBT) → 4. Load (Sheets) → 5. **Commit automático** dos CSVs atualizados de volta ao repositório.

Robustez embutida:
* Falha na extração encerra com `exit 1` **antes** do Load — a planilha nunca é sobrescrita com dados desatualizados.
* Em caso de falha, screenshots/HTML do navegador (pasta `debug/`) são publicados como artifact (`debug-playwright`) para diagnóstico.
* Configuração com precedência: input manual do `workflow_dispatch` > Secret > Variable > padrão.

**Secrets necessários** (Settings → Secrets and variables → Actions):
| Secret | Conteúdo |
|--------|----------|
| `SPREADSHEET_ID` | ID da planilha de destino (trecho entre `/d/` e `/edit` na URL) |
| `GOOGLE_CREDENTIALS` | Conteúdo JSON integral do `credentials.json` da Service Account |

Opcionais (Secret ou Variable): `ANOS_EXTRACAO` (`atual`/`todos`/lista) e `LOAD_MODO` (`overwrite`/`append`).

*A Service Account precisa de acesso de **Editor** na planilha (compartilhe a planilha com o `client_email` da conta).*

> **Notas sobre o cron do GitHub:** o disparo pode atrasar alguns minutos em horários de pico, e agendamentos são pausados após ~60 dias sem atividade no repositório — o que se auto-resolve aqui, pois o próprio workflow commita dados 3x na semana.

## 📂 Estrutura de Diretórios

```text
brasileirao-infra/
├── .env.example            # Modelo das variáveis de ambiente
├── .gitignore              # Ignora credenciais, venv e debug/ (dados são versionados)
├── requirements.txt        # Dependências do projeto
├── README.md               # Documentação principal
│
├── .github/workflows/
│   └── etl.yml             # Pipeline agendada (seg/qua/sex, 06:00 UTC)
│
├── data/                   # Armazenamento versionado
│   ├── bronze/             # Dados brutos extraídos (1 CSV por edição)
│   ├── silver/             # Dados limpos e padronizados (10 cols)
│   └── gold/               # One Big Table consolidada
│       ├── brasileirao_obt.csv
│       └── dicionario_dados_gold.md
│
├── etl/                    # Scripts de Engenharia de Dados
│   ├── __init__.py
│   ├── config.py           # URLs e leitura de credenciais via env
│   ├── extract.py          # Camada Bronze (scraping do frontend da CBF)
│   ├── extract_ogol.py     # Legado: scraping histórico do Ogol (1971–2011)
│   ├── transform.py        # Camada Prata (limpeza e padronização)
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

cp .env.example .env        # preencha SPREADSHEET_ID
# deixe o credentials.json da Service Account na raiz (ignorado pelo git)

python etl/extract.py       # Bronze: frontend da CBF (ANOS_EXTRACAO=atual por padrão)
python etl/transform.py     # Prata
python etl/gold.py          # Ouro (OBT)
python etl/load.py          # Publica no Google Sheets

# Apenas para re-extrair o histórico do Ogol (requer OGOL_ACCOUNTS no .env):
# python etl/extract_ogol.py
```

## 🧭 Histórico de Decisões Arquiteturais

| Decisão | Motivo |
|---------|--------|
| **PostgreSQL + FastAPI → Google Sheets** | Eliminar servidor ativo: a planilha é o repositório final, consumida pela futura `brasileirao-lib`. Zero custo de infraestrutura. |
| **Fato/Dimensão → One Big Table** | Consumo humano e pela biblioteca sem joins/IDs; tudo por extenso na linha. |
| **Remoção de features temporais (H2H, forma 5j, streaks, fadiga)** | Cálculos dinâmicos delegados à biblioteca consumidora; a pipeline entrega apenas a fotografia de cada partida. |
| **Dados versionados no git** | Runner do CI nasce de checkout limpo — sem o histórico versionado, a OBT perderia 1971–2025 a cada execução. |
| **Fonte corrente: Ogol → CBF** | O Ogol desafia IPs de datacenter com Cloudflare Turnstile (comprovado via artifact de debug do runner); o site da CBF responde normalmente. |
| **Scraping do frontend (Playwright) na CBF** | Extração pelos cards renderizados da página oficial de tabelas; seletores por fragmento de classe resistem aos hashes do Next.js. |
| **`extract_ogol.py` preservado** | Proveniência e reprodutibilidade do histórico 1971–2011, fora da cobertura da CBF (≥ 2012). |
| **Upsert por chave natural no modo `append`** | `id_partida` é regenerado por ordenação cronológica e se desloca com adiamentos; `ano+mandante+visitante+fase` é estável. |
