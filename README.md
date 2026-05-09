# 🏟️ Brasileirão Analytics - Infraestrutura e Engenharia de Dados

> **[System Context para Assistentes de IA]**
> Se você é uma IA lendo este documento, utilize-o como contexto absoluto para ajudar no desenvolvimento deste projeto. O usuário é um estudante de Ciência da Computação construindo seu TCC focado em Engenharia de Dados. Assuma o papel de um Engenheiro de Dados Sênior/Arquiteto de Software. Responda de forma técnica, utilize boas práticas de mercado, foque em performance e mantenha o escopo definido abaixo. Não sugira mudanças arquiteturais drásticas a menos que haja um erro fatal de design.

---

## 📌 Visão Geral do Repositório
Este é o repositório de **Back-end e Dados (`brasileirao-infra`)** do projeto Brasileirão Analytics. Ele utiliza uma arquitetura **Polyrepo**. 
A responsabilidade exclusiva deste repositório é:
1. Extrair dados históricos do futebol brasileiro (1971 - Presente).
2. Processar e enriquecer os dados usando a Arquitetura Medalhão (Bronze > Prata > Ouro).
3. Ingerir os dados em um banco de dados relacional.
4. Servir esses dados através de uma API RESTful para garantir o desacoplamento.

*Nota: A biblioteca Python (Client) consumidora desta API residirá em um repositório separado (`brasileirao-lib`).*

## 🛠️ Stack Tecnológico
* **Linguagem:** Python 3.x
* **Processamento de Dados:** Pandas, NumPy
* **Banco de Dados:** PostgreSQL (via Docker)
* **ORM e Ingestão:** SQLAlchemy
* **API REST:** FastAPI (com Uvicorn)
* **Web Scraping:** Playwright (headless Chromium) + BeautifulSoup4

## 🏗️ Pipeline de Dados (Medallion Architecture)
O ETL local está estruturado em três camadas dentro da pasta transitória `/data` (ignorada no versionamento):

* **🥉 Bronze (Extract):** Dados brutos extraídos da web via Playwright. O formato original é preservado 100% como backup. Cobertura: 1971–2025, 55 edições, ~21.500 jogos.
* **🥈 Prata (Transform):** Limpeza, tipagem rigorosa (datetime, Int64 nullable), padronização de nomenclatura de clubes (170 mapeamentos de-para), split de placar, cálculo de resultados. Saída: 10 colunas limpas por jogo.
* **🥇 Ouro (Feature Engineering & Load):** Geração de 66 features analíticas organizadas em 8 grupos:
  1. Métricas diretas (gols, saldo, pontos com regra histórica 2pts/3pts)
  2. Classificação de fases (tipo_fase, is_mata_mata)
  3. Métricas rolling por time (médias móveis 5j, streaks, acumulados no campeonato)
  4. Confronto direto histórico (H2H cross-year)
  5. Tabela dimensão (dim_times com estado/UF)
  6. Métricas de fadiga (dias de descanso cross-year)
  7. Solidez defensiva/ofensiva (clean sheets e falhas de gol)
  8. Fator clássico (derby flag — clássicos estaduais)

## 🗄️ Modelagem do Banco de Dados (Star Schema)
O banco de dados PostgreSQL foi modelado para otimizar consultas analíticas (OLAP), achatando a hierarquia de datas/rodadas em uma estrutura de Fato e Dimensões.

### Regras de Negócio e Padronização Histórica
Como o campeonato possui regulamentos distintos entre 1971 e o presente, a camada Ouro aplica as seguintes abstrações:
* **Pontuação histórica:** Vitória vale 2 pontos antes de 1995 e 3 pontos a partir de 1995, respeitando a regra real da CBF.
* **`tipo_fase` e `is_mata_mata`:** Substitui a lógica de "Rodadas" contínuas, classificando os jogos para suportar formatos antigos (ex: Quadrangular Final, Repescagem, Finais).
* **Métricas rolling:** Resetam a cada edição do campeonato. Todas refletem o estado ANTES do jogo começar.
* **H2H e Fadiga:** São cross-year (histórico completo entre campeonatos).

### Estrutura (Core)
1. **`dim_times` (Dimensão):** Tabela dicionário com ID único, nome padronizado e estado (UF) dos clubes. 167 times mapeados.
2. **`fato_partidas_ouro` (Fato):** A tabela central ("One Big Table"). Contém 21.453 jogos de 1971 a 2025, referenciando os times por *Foreign Keys* e contendo todas as 66 features analíticas prontas para consumo.

## 📂 Estrutura de Diretórios

```text
brasileirao-infra/
├── .env                    # Variáveis de ambiente (DB_USER, DB_PASS, API_PORT)
├── .gitignore              # Ignora /data, /venv e __pycache__
├── docker-compose.yml      # Imagem do PostgreSQL
├── requirements.txt        # Dependências do projeto
├── README.md               # Documentação principal
│
├── data/                   # Armazenamento local (Ignorado no Git)
│   ├── bronze/             # Dados brutos extraídos
│   ├── silver/             # Dados limpos e padronizados (10 cols)
│   └── gold/               # Features enriquecidas (66 cols) + dim_times
│       ├── fato_partidas_ouro.csv
│       ├── dim_times.csv
│       └── dicionario_dados_gold.md
│
├── etl/                    # Scripts de Engenharia de Dados
│   ├── __init__.py
│   ├── config.py           # URLs, credenciais e constantes
│   ├── extract.py          # Camada Bronze (Web Scraping via Playwright)
│   ├── transform.py        # Camada Prata (Limpeza e padronização)
│   ├── gold.py             # Camada Ouro (Feature Engineering — 8 grupos)
│   ├── mapear_times.py     # Utilitário: extrai times únicos da Bronze
│   └── load.py             # Ingestão no PostgreSQL (a implementar)
│
├── relatório acompanhamento/  # Logs de progresso do projeto
│   ├── 21-04-2026.md
│   └── 09-05-2026.md
│
└── api/                    # Aplicação FastAPI (a implementar)
    ├── __init__.py
    ├── main.py             # Ponto de entrada (App, Middlewares)
    ├── database.py         # Configuração da Session e Engine do SQLAlchemy
    ├── models.py           # Classes declarativas (Base) representando as tabelas
    └── routes.py           # Endpoints de consulta (GET)
```