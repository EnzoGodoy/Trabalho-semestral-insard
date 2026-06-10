# Arquitetura — Pipeline de Indicadores Macroeconômicos

## Visão Geral

Pipeline ETL orquestrado pelo Airflow que consolida dados de duas fontes heterogêneas num Data Warehouse Postgres, produzindo indicadores macroeconômicos brasileiros (SELIC, IPCA, câmbio) prontos para análise.

---

## Fontes de Dados

| Fonte | Tipo | Formato | Auth |
|---|---|---|---|
| API Banco Central do Brasil (`api.bcb.gov.br`) | REST API | JSON semi-estruturado | Nenhuma |
| CSV de Metas de Inflação do CMN | Arquivo estático | CSV estruturado | N/A |

**Por que essas fontes?** A API do BCB é pública, confiável, sem limite de requisições e sem autenticação. O CSV de metas é a fonte complementar ideal: é estruturado (heterogêneo ao JSON), tem relevância analítica direta (comparar inflação realizada × meta) e não exige integração de rede.

---

## Fluxo do Pipeline

```
BCB API (JSON) ──┐
                 ├──► extract ──► validate ──► transform ──► load ──► Postgres DW
CSV (metas) ─────┘
                         ↑
                     init_db (DDL)
```

Os arquivos intermediários (raw JSON + parquet) são gravados em `data/tmp/` dentro do volume Docker. Essa escolha evita o limite de tamanho do XCom do Airflow e mantém rastreabilidade dos dados brutos.

---

## DAG — Tarefas e Dependências

```
init_db >> extract >> validate >> transform >> load
```

| Task | Operador | Responsabilidade |
|---|---|---|
| `init_db` | PythonOperator | Executa `create_tables.sql` via SQLAlchemy (IF NOT EXISTS — idempotente) |
| `extract` | PythonOperator | GET nas séries BCB + leitura do CSV; salva raw em `data/tmp/` |
| `validate` | PythonOperator | Validações de qualidade pré-transformação nos dados brutos |
| `transform` | PythonOperator | Normaliza para modelo dimensional; salva parquets em `data/tmp/` |
| `load` | PythonOperator | Upsert de dimensões e fato no Postgres |

**Agendamento:** toda segunda-feira às 06h00 UTC (`0 6 * * 1`). Catchup desabilitado para evitar backfill indesejado.

---

## Modelo Dimensional

```
dim_indicador ──┐
                ├──► fact_serie_temporal ◄── dim_tempo
dim_meta_inflacao (auxiliar, JOIN por ano)
```

**Grão do fato:** um valor por indicador por data.

**Por que dimensional e não relacional puro?** O workload é analítico (agregações por período, cruzamentos entre séries). O star schema minimiza JOINs nas queries de valor. `dim_meta_inflacao` é uma dimensão degenerada — referenciada por ano, não por FK, porque o grão de metas é anual enquanto o fato é diário/mensal.

---

## Validações de Qualidade

| # | Regra | Comportamento ao violar |
|---|---|---|
| 1 | **Nulos** em `data` e `valor` | Levanta `ValidationError` — aborta o pipeline |
| 2 | **Duplicatas** na chave `(data, indicador_id)` | Loga aviso e remove (keep=first) |
| 3 | **Range de valores** por série (ex: SELIC ∈ [0, 50]) | Loga aviso e remove outliers |
| 4 | **Integridade referencial** fato → dim_tempo | Levanta `ValidationError` — aborta o pipeline |

---

## Decisões de Arquitetura

**Arquivos intermediários em vez de XCom**
O Airflow armazena XCom no banco de metadados. Com 24 meses de cotações diárias (~1500 registros × 3 séries), o payload excede o limite padrão de 48 KB. Gravar em `data/tmp/` (volume compartilhado) resolve isso e mantém os dados brutos acessíveis para debug.

**Upsert (ON CONFLICT DO UPDATE) em todas as tabelas**
Torna o pipeline idempotente: re-runs ou correções não geram duplicatas. Essencial para Airflow, onde retries são comuns.

**DDL em arquivo SQL externo (`create_tables.sql`)**
Mantém separação clara entre infraestrutura (SQL) e lógica (Python). O `init_db` executa o DDL a cada run sem risco de perda de dados (IF NOT EXISTS).

**LocalExecutor em vez de CeleryExecutor**
Suficiente para o volume de dados e número de workers desta carga. Reduz drasticamente a complexidade do docker-compose (sem Redis, sem workers extras).

**Uma única instância Postgres com dois bancos**
`airflow` (metadados do Airflow) e `dw` (Data Warehouse) vivem no mesmo container. Justificativa: simplicidade operacional para ambiente de desenvolvimento/avaliação. Em produção, seriam instâncias separadas.

---

## Estrutura de Pastas

```
.
├── dags/           # DAG do Airflow
├── etl/            # Módulos Python: extract, transform, validate, load
├── sql/            # DDL, init do banco, queries analíticas
├── data/           # CSV seed; data/tmp/ para artefatos intermediários
├── tests/          # pytest — cobre transform e validate
├── docs/           # Esta documentação
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```
