"""
validate.py — Validações de qualidade de dados.

Regras implementadas:
  1. Nulos em colunas críticas (data, valor)
  2. Duplicatas na chave natural (data + indicador)
  3. Range de valores por série (outliers fora do domínio esperado)
  4. Integridade referencial: datas do fato devem existir na dim_tempo
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Levantada quando uma validação crítica falha e o pipeline deve parar."""


# ─── Ranges esperados por série ───────────────────────────────────────────────
SERIES_RANGES: dict[str, tuple[float, float]] = {
    "selic_diaria": (0.0, 50.0),
    "ipca_mensal":  (-5.0, 30.0),
    "cambio_usd":   (0.5, 20.0),
}


# ─── Regra 1: Nulos ───────────────────────────────────────────────────────────
def check_nulls(df: pd.DataFrame, columns: list[str], context: str) -> None:
    """Levanta ValidationError se qualquer coluna crítica contiver nulo."""
    for col in columns:
        n_nulls = df[col].isna().sum()
        if n_nulls > 0:
            raise ValidationError(
                f"[{context}] Coluna '{col}' contém {n_nulls} valor(es) nulo(s)."
            )
    logger.info("[%s] ✓ Regra 1 (nulos) — OK", context)


# ─── Regra 2: Duplicatas ─────────────────────────────────────────────────────
def check_duplicates(
    df: pd.DataFrame, subset: list[str], context: str
) -> pd.DataFrame:
    """
    Loga duplicatas e as remove (a primeira ocorrência é mantida).
    Não aborta o pipeline — duplicatas são toleradas com aviso.
    """
    n_dup = df.duplicated(subset=subset).sum()
    if n_dup > 0:
        logger.warning("[%s] Regra 2 (duplicatas): %d duplicatas removidas em %s", context, n_dup, subset)
        df = df.drop_duplicates(subset=subset, keep="first")
    else:
        logger.info("[%s] ✓ Regra 2 (duplicatas) — OK", context)
    return df


# ─── Regra 3: Range de valores ───────────────────────────────────────────────
def check_range(
    df: pd.DataFrame,
    col: str,
    lo: float,
    hi: float,
    context: str,
    strict: bool = False,
) -> pd.DataFrame:
    """
    Loga registros fora do range [lo, hi].
    Se strict=True, levanta ValidationError em vez de apenas logar.
    """
    mask = (df[col] < lo) | (df[col] > hi)
    n_out = mask.sum()
    if n_out > 0:
        msg = f"[{context}] Regra 3 (range): {n_out} valor(es) fora de [{lo}, {hi}] em '{col}'"
        if strict:
            raise ValidationError(msg)
        logger.warning(msg)
        # Remove outliers para não contaminar o DW
        df = df[~mask].copy()
    else:
        logger.info("[%s] ✓ Regra 3 (range '%s') — OK", context, col)
    return df


# ─── Regra 4: Integridade referencial ────────────────────────────────────────
def check_referential_integrity(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    fact_key: str,
    dim_key: str,
    context: str,
) -> None:
    """Verifica que todas as chaves do fato existem na dimensão."""
    orphans = ~fact_df[fact_key].isin(dim_df[dim_key])
    n_orphans = orphans.sum()
    if n_orphans > 0:
        raise ValidationError(
            f"[{context}] Regra 4 (integridade): {n_orphans} chave(s) '{fact_key}' "
            f"sem correspondência na dimensão."
        )
    logger.info("[%s] ✓ Regra 4 (integridade referencial) — OK", context)


# ─── Validação completa de série BCB ─────────────────────────────────────────
def validate_series(df: pd.DataFrame, chave: str) -> pd.DataFrame:
    check_nulls(df, ["data", "valor"], chave)
    df = check_duplicates(df, ["data", "indicador_id"], chave)
    if chave in SERIES_RANGES:
        lo, hi = SERIES_RANGES[chave]
        df = check_range(df, "valor", lo, hi, chave)
    return df


# ─── Validação do CSV de metas ───────────────────────────────────────────────
def validate_metas(df: pd.DataFrame) -> pd.DataFrame:
    check_nulls(df, ["ano", "meta_inflacao"], "metas_inflacao")
    df = check_duplicates(df, ["ano"], "metas_inflacao")
    df = check_range(df, "meta_inflacao", 0.0, 20.0, "metas_inflacao")
    return df
