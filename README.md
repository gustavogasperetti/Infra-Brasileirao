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
* **Web Scraping:** (A definir: BeautifulSoup4 / Scrapy)

## 🏗️ Pipeline de Dados (Medallion Architecture)
O ETL local está estruturado em três camadas dentro da pasta transitória `/data` (ignorada no versionamento):

* **🥉 Bronze (Extract):** Dados brutos extraídos da web. O formato original é preservado 100% como backup.
* **🥈 Prata (Transform):** Limpeza, tipagem rigorosa (ex: conversão de strings para datas), tratamento de nulos e padronização de nomenclatura de clubes.
* **🥇 Ouro (Feature Engineering & Load):** Geração de métricas avançadas (médias móveis, win streaks, saldo de gols acumulado) e preparação para o modelo relacional do banco.

## 🗄️ Modelagem do Banco de Dados (Star Schema)
O banco de dados PostgreSQL foi modelado para otimizar consultas analíticas (OLAP), achatando a hierarquia de datas/rodadas em uma estrutura de Fato e Dimensões.

### Regras de Negócio e Padronização Histórica
Como o campeonato possui regulamentos distintos entre 1971 e o presente, a camada Ouro aplica as seguintes abstrações:
* **`pts_padronizados`:** Toda vitória vale 3 pontos, independentemente do ano, para permitir análises longitudinais justas.
* **`tipo_fase` e `is_mata_mata`:** Substitui a lógica de "Rodadas" contínuas, classificando os jogos para suportar formatos antigos (ex: Quadrangular Final, Repescagem, Finais).

### Estrutura (Core)
1. **`dim_times` (Dimensão):** Tabela dicionário com o ID único e informações cadastrais dos clubes.
2. **`fato_partidas_ouro` (Fato):** A tabela central ("One Big Table"). Contém todos os jogos da história, referenciando os times por *Foreign Keys* e contendo todas as features matemáticas prontas para consumo.

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
│   ├── bronze/             
│   ├── silver/             
│   └── gold/               
│
├── etl/                    # Scripts de Engenharia de Dados
│   ├── __init__.py
│   ├── config.py           # Dicionários e constantes (ex: Mapeamento de nomes de times)
│   ├── 01_extract.py       # Lógica de Web Scraping
│   ├── 02_transform.py     # Lógica de Bronze para Prata e Prata para Ouro
│   └── 03_load.py          # Ingestão do DataFrame Ouro via SQLAlchemy no Postgres
│
└── api/                    # Aplicação FastAPI
    ├── __init__.py
    ├── main.py             # Ponto de entrada (App, Middlewares)
    ├── database.py         # Configuração da Session e Engine do SQLAlchemy
    ├── models.py           # Classes declarativas (Base) representando as tabelas
    └── routes.py           # Endpoints de consulta (GET)