"""
extract.py — Extração de dados de duas fontes heterogêneas:
  1. API REST do Banco Central do Brasil (JSON semi-estruturado)
  2. CSV estático de metas de inflação do CMN
"""

import logging
import os
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ─── Configuração das séries do BCB ──────────────────────────────────────────
# api.bcb.gov.br — pública, sem autenticação, retorna JSON
BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

SERIES_CONFIG: dict[str, dict] = {
    "selic_diaria": {
        "codigo": 432,
        "nome": "SELIC - Taxa Over",
        "unidade": "% a.a.",
        "periodicidade": "diaria",
        "fonte": "BCB",
    },
    "ipca_mensal": {
        "codigo": 433,
        "nome": "IPCA - Variação Mensal",
        "unidade": "%",
        "periodicidade": "mensal",
        "fonte": "BCB",
    },
    "cambio_usd": {
        "codigo": 1,
        "nome": "Taxa de Câmbio USD/BRL (venda)",
        "unidade": "R$",
        "periodicidade": "diaria",
        "fonte": "BCB",
    },
}


def _bcb_fetch(codigo: int, data_inicio: str, data_fim: str) -> list[dict]:
    """Faz GET na API do BCB e retorna lista de {data, valor}."""
    url = BCB_BASE_URL.format(codigo=codigo)
    params = {"formato": "json", "dataInicial": data_inicio, "dataFinal": data_fim}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def extract_bcb(months: int = 24, tmp_dir: str = "/opt/airflow/data/tmp") -> None:
    """
    Extrai todas as séries do BCB e salva como JSON em tmp_dir.
    Usa arquivos intermediários para evitar limite de XCom do Airflow.
    """
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    data_fim = datetime.today()
    data_inicio = data_fim - timedelta(days=months * 31)  # margem para meses completos

    for chave, config in SERIES_CONFIG.items():
        logger.info("Extraindo série BCB: %s (código %s)", chave, config["codigo"])
        records = _bcb_fetch(
            config["codigo"],
            data_inicio.strftime("%d/%m/%Y"),
            data_fim.strftime("%d/%m/%Y"),
        )
        out_path = Path(tmp_dir) / f"raw_{chave}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
        logger.info("  → %d registros salvos em %s", len(records), out_path)


def extract_csv(csv_path: str, tmp_dir: str = "/opt/airflow/data/tmp") -> None:
    """
    Lê CSV de metas de inflação e copia para tmp_dir como parquet
    (preserva tipos sem re-parsear datas como string).
    """
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(csv_path)
    logger.info("CSV metas: %d linhas extraídas de %s", len(df), csv_path)
    out_path = Path(tmp_dir) / "raw_metas.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("  → salvo em %s", out_path)
