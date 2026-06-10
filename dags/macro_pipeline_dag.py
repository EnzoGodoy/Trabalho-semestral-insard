"""
macro_pipeline_dag.py — DAG principal do pipeline de indicadores macroeconômicos.

Fluxo de tasks:
  init_db >> extract >> validate >> transform >> load

  - init_db   : cria schema e tabelas no DW (idempotente via IF NOT EXISTS)
  - extract   : puxa séries do BCB (JSON) + lê CSV; salva raw em data/tmp/
  - validate  : validações de qualidade nos dados brutos (pré-transformação)
  - transform : normaliza para modelo dimensional; salva parquets em data/tmp/
  - load      : carrega parquets no Postgres DW via upsert

Agendamento: toda segunda-feira às 06h00 (UTC)
"""

import os
import sys
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Garante que o módulo etl seja encontrado dentro do container
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# ─── Configuração via env ─────────────────────────────────────────────────────
TMP_DIR       = "/opt/airflow/data/tmp"
CSV_PATH      = os.environ.get("METAS_CSV_PATH", "/opt/airflow/data/metas_inflacao.csv")
DW_CONN_STR   = os.environ.get("DW_CONN_STR", "postgresql+psycopg2://dw:dw@postgres/dw")
MONTHS        = int(os.environ.get("BCB_MONTHS_HISTORY", "24"))

# ─── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-integration",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}


# ─── Callables das tasks ──────────────────────────────────────────────────────

def _init_db(**_):
    """Cria schema dw e todas as tabelas via SQL externo (idempotente)."""
    from sqlalchemy import create_engine, text
    engine = create_engine(DW_CONN_STR, future=True)
    sql_path = "/opt/airflow/sql/create_tables.sql"
    with open(sql_path, encoding="utf-8") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Schema DW inicializado.")


def _extract(**_):
    """Extrai dados das duas fontes e persiste raw em data/tmp/."""
    from etl.extract import extract_bcb, extract_csv
    extract_bcb(months=MONTHS, tmp_dir=TMP_DIR)
    extract_csv(csv_path=CSV_PATH, tmp_dir=TMP_DIR)
    logger.info("Extração concluída.")


def _validate(**_):
    """
    Validações de qualidade nos dados brutos, antes da transformação.
    Levanta exceção se alguma regra crítica for violada.
    """
    import json
    import pandas as pd
    from pathlib import Path
    from etl.validate import check_nulls, check_range, ValidationError

    tmp = Path(TMP_DIR)

    # Valida cada série BCB
    from etl.extract import SERIES_CONFIG
    for chave in SERIES_CONFIG:
        with open(tmp / f"raw_{chave}.json", encoding="utf-8") as f:
            raw = json.load(f)
        if not raw:
            raise ValidationError(f"Série {chave} retornou vazia da API do BCB.")
        df = pd.DataFrame(raw)
        # Regra 1: sem nulos nos campos chave
        check_nulls(df, ["data", "valor"], context=f"raw/{chave}")
        logger.info("Série %s: %d registros brutos validados.", chave, len(df))

    # Valida CSV de metas
    df_metas = pd.read_parquet(tmp / "raw_metas.parquet")
    check_nulls(df_metas, ["ano", "meta_inflacao"], context="raw/metas")
    logger.info("CSV metas: %d linhas validadas.", len(df_metas))


def _transform(**_):
    """Transforma dados brutos para o modelo dimensional e salva parquets."""
    from etl.transform import transform_all
    transform_all(tmp_dir=TMP_DIR)


def _load(**_):
    """Carrega parquets no DW Postgres."""
    from etl.load import load_all
    load_all(tmp_dir=TMP_DIR, conn_str=DW_CONN_STR)


# ─── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="macro_pipeline",
    description="Pipeline ETL de indicadores macroeconômicos (BCB API + CSV → Postgres DW)",
    schedule_interval="0 6 * * 1",   # toda segunda às 06h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["macroeconomia", "bcb", "etl"],
    doc_md=__doc__,
) as dag:

    t_init_db = PythonOperator(
        task_id="init_db",
        python_callable=_init_db,
        doc_md="Cria schema `dw` e tabelas via DDL externo (idempotente).",
    )

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        doc_md="Extrai séries SELIC/IPCA/Câmbio da API BCB (JSON) e CSV de metas.",
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=_validate,
        doc_md="Valida nulos, ranges e completude dos dados brutos.",
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        doc_md="Normaliza dados para dim_tempo, dim_indicador, dim_meta e fact_serie_temporal.",
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=_load,
        doc_md="Carrega dimensões e fato no Postgres via upsert (idempotente).",
    )

    # ── Dependências ──────────────────────────────────────────────────────────
    t_init_db >> t_extract >> t_validate >> t_transform >> t_load
