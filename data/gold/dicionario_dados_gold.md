# 📖 Dicionário de Dados — Camada Gold (One Big Table)

**Arquivo:** `brasileirao_obt.csv` — uma linha por partida, 1971–presente.
**Destino:** publicado no Google Sheets (aba `partidas`) pelo `etl/load.py`.

A tabela é **desnormalizada**: todos os nomes (clubes, estados, fases) estão
por extenso na própria linha, sem IDs externos ou tabelas dimensão. Cálculos
temporais (H2H, forma dos últimos 5 jogos, sequências, fadiga, acumulados)
**não** são pré-calculados — são responsabilidade da biblioteca consumidora.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_partida` | int | Identificador sequencial da linha (ordem cronológica) |
| `ano_campeonato` | int | Edição do campeonato (1971–presente) |
| `Data` | date (YYYY-MM-DD) | Data da partida |
| `Mandante` | texto | Nome padronizado do clube mandante |
| `Visitante` | texto | Nome padronizado do clube visitante |
| `estado_mandante` | texto (UF) | Estado do clube mandante |
| `estado_visitante` | texto (UF) | Estado do clube visitante |
| `gols_mandante` | int (nullable) | Gols do mandante (vazio em WO/ANU/IC) |
| `gols_visitante` | int (nullable) | Gols do visitante (vazio em WO/ANU/IC) |
| `resultado_mandante` | texto | `V` / `E` / `D` na perspectiva do mandante |
| `resultado_visitante` | texto | `V` / `E` / `D` na perspectiva do visitante |
| `placar_status` | texto | `NORMAL`, `WO` (walkover), `ANU` (anulado), `IC` (incompleto) |
| `Fase` | texto | Fase original da fonte (ex.: `R12`, `SF`, `A`) |
| `tipo_fase` | texto | Classificação legível: Pontos Corridos, Fase de Grupos, Fase Classificatória, Oitavas/Quartas/Semifinal/Final, Playoff, Disputa 3º Lugar |
| `is_mata_mata` | booleano | `TRUE` se a fase é eliminatória |
| `is_classico_estadual` | booleano | `TRUE` se mandante e visitante são do mesmo estado |
| `total_gols` | int (nullable) | `gols_mandante + gols_visitante` |
| `saldo_gols_mandante` | int (nullable) | `gols_mandante − gols_visitante` |
| `saldo_gols_visitante` | int (nullable) | `gols_visitante − gols_mandante` |
| `pontos_mandante` | int (nullable) | Pontos do mandante na partida (regra histórica) |
| `pontos_visitante` | int (nullable) | Pontos do visitante na partida (regra histórica) |

## Regras de negócio

* **Pontuação histórica:** vitória vale **2 pontos antes de 1995** e
  **3 pontos a partir de 1995**, respeitando a regra real da CBF.
* **Placares especiais:** jogos `WO`/`ANU`/`IC` mantêm a linha na tabela com
  gols, resultados e pontos vazios; o motivo fica em `placar_status`.
* **Nomes de clubes:** padronizados via de-para com ~170 mapeamentos
  (ex.: `Atlético Paranaense` → `Athletico Paranaense`).
