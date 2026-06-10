"""
transform.py — Normalização dos dados brutos para o modelo dimensional.

Responsabilidades:
  - Parsear datas no formato BCB (DD/MM/YYYY)
  - Converter valores string → float
  - Construir dim_tempo, dim_indicador, dim_meta_inflacao
  - Construir fact_serie_temporal
  - Salvar artefatos transformados em tmp_dir (parquet)
"""

import json
import logging
from pathlib import Path

import pandas as pd

from etl.extract import SERIES_CONFIG
from etl.validate import validate_series, validate_metas, check_referential_integrity

logger = logging.getLogger(__name__)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _parse_bcb_record(item: dict, indicador_id: int) -> dict | None:
    """Converte um registro bruto do BCB para o schema do fato. Retorna None se inválido."""
    try:
        data = pd.to_datetime(item["data"], format="%d/%m/%Y").date()
        raw_valor = item.get("valor", "")
        if raw_valor in (None, "", "-"):
            return None
        valor = float(str(raw_valor).replace(",", "."))
        return {"data": data, "indicador_id": indicador_id, "valor": valor}
    except (KeyError, ValueError, TypeError) as exc:
        logger.debug("Registro ignorado: %s — %s", item, exc)
        return None


# ─── Dimensões ────────────────────────────────────────────────────────────────

def build_dim_indicador() -> pd.DataFrame:
    """Constrói dim_indicador a partir da config estática de SERIES_CONFIG."""
    rows = [
        {
            "id": idx,
            "chave": chave,
            "codigo_bcb": cfg["codigo"],
            "nome": cfg["nome"],
            "unidade": cfg["unidade"],
            "periodicidade": cfg["periodicidade"],
            "fonte": cfg["fonte"],
        }
        for idx, (chave, cfg) in enumerate(SERIES_CONFIG.items(), start=1)
    ]
    return pd.DataFrame(rows)


def build_dim_tempo(dates: list) -> pd.DataFrame:
    """Cria dim_tempo a partir de uma lista de datas únicas."""
    s = pd.to_datetime(list(set(dates)))
    df = pd.DataFrame({"data": s}).sort_values("data").reset_index(drop=True)
    df["dia"] = df["data"].dt.day.astype("int16")
    df["mes"] = df["data"].dt.month.astype("int16")
    df["ano"] = df["data"].dt.year.astype("int16")
    df["trimestre"] = df["data"].dt.quarter.astype("int16")
    df["semestre"] = df["data"].dt.month.apply(lambda m: 1 if m <= 6 else 2).astype("int16")
    df["nome_mes"] = df["data"].dt.strftime("%B")
    df["data"] = df["data"].dt.date
    return df


# ─── Transformação principal ──────────────────────────────────────────────────

def transform_all(tmp_dir: str = "/opt/airflow/data/tmp") -> None:
    """
    Lê arquivos raw do tmp_dir, aplica validações e transformações,
    e salva os artefatos dimensionais em parquet.
    """
    tmp = Path(tmp_dir)

    # ── dim_indicador ────────────────────────────────────────────────────────
    dim_indicador = build_dim_indicador()
    chave_to_id = {row["chave"]: row["id"] for _, row in dim_indicador.iterrows()}

    # ── Séries BCB → fatos ───────────────────────────────────────────────────
    all_facts: list[pd.DataFrame] = []

    for chave in SERIES_CONFIG:
        raw_path = tmp / f"raw_{chave}.json"
        with open(raw_path, encoding="utf-8") as f:
            raw = json.load(f)

        indicador_id = chave_to_id[chave]
        records = [r for item in raw if (r := _parse_bcb_record(item, indicador_id)) is not None]

        if not records:
            raise ValueError(f"Série {chave} sem registros válidos após parsing.")

        df = pd.DataFrame(records)
        df = validate_series(df, chave)
        all_facts.append(df)
        logger.info("Série %s: %d registros válidos", chave, len(df))

    fact_df = pd.concat(all_facts, ignore_index=True)

    # ── dim_tempo ────────────────────────────────────────────────────────────
    dim_tempo = build_dim_tempo(fact_df["data"].tolist())

    # ── Integridade referencial fato → dim_tempo ─────────────────────────────
    check_referential_integrity(
        fact_df, dim_tempo, fact_key="data", dim_key="data", context="fact→dim_tempo"
    )

    # ── CSV de metas → dim_meta_inflacao ────────────────────────────────────
    raw_metas = pd.read_parquet(tmp / "raw_metas.parquet")
    dim_metas = _transform_metas(raw_metas)
    dim_metas = validate_metas(dim_metas)

    # ── Persistência ─────────────────────────────────────────────────────────
    dim_indicador.to_parquet(tmp / "dim_indicador.parquet", index=False)
    dim_tempo.to_parquet(tmp / "dim_tempo.parquet", index=False)
    dim_metas.to_parquet(tmp / "dim_meta_inflacao.parquet", index=False)
    fact_df.to_parquet(tmp / "fact_serie_temporal.parquet", index=False)

    logger.info(
        "Transform concluído: %d fatos | %d datas | %d indicadores | %d metas",
        len(fact_df), len(dim_tempo), len(dim_indicador), len(dim_metas),
    )


def _transform_metas(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e enriquece o CSV de metas de inflação."""
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.dropna(subset=["ano", "meta_inflacao"])
    df["ano"] = df["ano"].astype(int)

    for col in ["meta_inflacao", "banda_superior", "banda_inferior", "inflacao_realizada"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Coluna derivada: se a inflação realizada ficou dentro da banda
    df["bateu_meta"] = (
        df["inflacao_realizada"].notna()
        & (df["inflacao_realizada"] >= df["banda_inferior"])
        & (df["inflacao_realizada"] <= df["banda_superior"])
    )
    return df[["ano", "meta_inflacao", "banda_superior", "banda_inferior", "inflacao_realizada", "bateu_meta"]]
