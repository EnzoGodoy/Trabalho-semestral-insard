"""
load.py — Carrega os artefatos transformados no Data Warehouse (Postgres).

Estratégia: INSERT ... ON CONFLICT DO UPDATE (upsert) em todas as tabelas.
Isso torna o pipeline idempotente — re-runs não geram duplicatas.
"""

import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_engine(conn_str: str | None = None) -> Engine:
    url = conn_str or os.environ["DW_CONN_STR"]
    return create_engine(url, future=True)


# ─── Loaders por tabela ───────────────────────────────────────────────────────

def load_dim_tempo(df: pd.DataFrame, engine: Engine) -> None:
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO dw.dim_tempo (data, dia, mes, ano, trimestre, semestre, nome_mes)
                    VALUES (:data, :dia, :mes, :ano, :trimestre, :semestre, :nome_mes)
                    ON CONFLICT (data) DO NOTHING
                """),
                row._asdict(),
            )
    logger.info("dim_tempo: %d registros processados", len(df))


def load_dim_indicador(df: pd.DataFrame, engine: Engine) -> None:
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO dw.dim_indicador (id, chave, codigo_bcb, nome, unidade, periodicidade, fonte)
                    VALUES (:id, :chave, :codigo_bcb, :nome, :unidade, :periodicidade, :fonte)
                    ON CONFLICT (id) DO UPDATE
                        SET nome = EXCLUDED.nome,
                            unidade = EXCLUDED.unidade
                """),
                row._asdict(),
            )
    logger.info("dim_indicador: %d registros processados", len(df))


def load_dim_meta_inflacao(df: pd.DataFrame, engine: Engine) -> None:
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            d = row._asdict()
            # bateu_meta pode ser NaT/None para anos sem inflação realizada
            d["bateu_meta"] = None if pd.isna(d.get("bateu_meta")) else bool(d["bateu_meta"])
            conn.execute(
                text("""
                    INSERT INTO dw.dim_meta_inflacao
                        (ano, meta_inflacao, banda_superior, banda_inferior, inflacao_realizada, bateu_meta)
                    VALUES
                        (:ano, :meta_inflacao, :banda_superior, :banda_inferior, :inflacao_realizada, :bateu_meta)
                    ON CONFLICT (ano) DO UPDATE
                        SET inflacao_realizada = EXCLUDED.inflacao_realizada,
                            bateu_meta         = EXCLUDED.bateu_meta
                """),
                d,
            )
    logger.info("dim_meta_inflacao: %d registros processados", len(df))


def load_fact_serie_temporal(df: pd.DataFrame, engine: Engine) -> None:
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO dw.fact_serie_temporal (data, indicador_id, valor)
                    VALUES (:data, :indicador_id, :valor)
                    ON CONFLICT (data, indicador_id) DO UPDATE
                        SET valor = EXCLUDED.valor
                """),
                row._asdict(),
            )
    logger.info("fact_serie_temporal: %d registros carregados", len(df))


# ─── Entry point ──────────────────────────────────────────────────────────────

def load_all(tmp_dir: str = "/opt/airflow/data/tmp", conn_str: str | None = None) -> None:
    """Lê parquets do tmp_dir e carrega no DW na ordem correta (dims antes do fato)."""
    tmp = Path(tmp_dir)
    engine = get_engine(conn_str)

    dim_indicador = pd.read_parquet(tmp / "dim_indicador.parquet")
    dim_tempo     = pd.read_parquet(tmp / "dim_tempo.parquet")
    dim_metas     = pd.read_parquet(tmp / "dim_meta_inflacao.parquet")
    fact_df       = pd.read_parquet(tmp / "fact_serie_temporal.parquet")

    # Dimensões primeiro (FK constraints)
    load_dim_indicador(dim_indicador, engine)
    load_dim_tempo(dim_tempo, engine)
    load_dim_meta_inflacao(dim_metas, engine)

    # Fato por último
    load_fact_serie_temporal(fact_df, engine)

    logger.info("Carga concluída: %d fatos inseridos/atualizados", len(fact_df))
