"""
tests/test_transform.py — Testes das funções de transformação e validação.
Executar com: pytest tests/ -v
"""

import pytest
import pandas as pd
from datetime import date

# ── Importações dos módulos ETL ───────────────────────────────────────────────
from etl.transform import (
    _parse_bcb_record,
    build_dim_tempo,
    build_dim_indicador,
    _transform_metas,
)
from etl.validate import (
    check_nulls,
    check_duplicates,
    check_range,
    check_referential_integrity,
    validate_series,
    validate_metas,
    ValidationError,
)


# ─── _parse_bcb_record ────────────────────────────────────────────────────────

class TestParseBcbRecord:
    def test_valid_record(self):
        rec = _parse_bcb_record({"data": "02/01/2024", "valor": "12.25"}, indicador_id=1)
        assert rec is not None
        assert rec["valor"] == 12.25
        assert rec["data"] == date(2024, 1, 2)

    def test_valor_com_virgula(self):
        rec = _parse_bcb_record({"data": "02/01/2024", "valor": "12,25"}, indicador_id=1)
        assert rec["valor"] == pytest.approx(12.25)

    def test_valor_invalido_retorna_none(self):
        rec = _parse_bcb_record({"data": "02/01/2024", "valor": "N/D"}, indicador_id=1)
        assert rec is None

    def test_valor_vazio_retorna_none(self):
        rec = _parse_bcb_record({"data": "02/01/2024", "valor": ""}, indicador_id=1)
        assert rec is None

    def test_data_invalida_retorna_none(self):
        rec = _parse_bcb_record({"data": "32/01/2024", "valor": "10.0"}, indicador_id=1)
        assert rec is None

    def test_campo_faltando_retorna_none(self):
        rec = _parse_bcb_record({"valor": "10.0"}, indicador_id=1)
        assert rec is None


# ─── build_dim_tempo ──────────────────────────────────────────────────────────

class TestBuildDimTempo:
    def test_colunas_presentes(self):
        datas = [date(2024, 1, 15), date(2024, 7, 20)]
        df = build_dim_tempo(datas)
        for col in ["data", "dia", "mes", "ano", "trimestre", "semestre", "nome_mes"]:
            assert col in df.columns

    def test_semestre_correto(self):
        datas = [date(2024, 1, 1), date(2024, 8, 1)]
        df = build_dim_tempo(datas).set_index("mes")
        assert df.loc[1, "semestre"] == 1
        assert df.loc[8, "semestre"] == 2

    def test_trimestre_correto(self):
        datas = [date(2024, 4, 1)]
        df = build_dim_tempo(datas)
        assert df.iloc[0]["trimestre"] == 2

    def test_remove_duplicatas(self):
        datas = [date(2024, 1, 1), date(2024, 1, 1)]
        df = build_dim_tempo(datas)
        assert len(df) == 1


# ─── build_dim_indicador ──────────────────────────────────────────────────────

class TestBuildDimIndicador:
    def test_tem_tres_indicadores(self):
        df = build_dim_indicador()
        assert len(df) == 3

    def test_chaves_esperadas(self):
        df = build_dim_indicador()
        assert set(df["chave"]) == {"selic_diaria", "ipca_mensal", "cambio_usd"}


# ─── _transform_metas ─────────────────────────────────────────────────────────

class TestTransformMetas:
    def _sample(self):
        return pd.DataFrame({
            "ano": [2022, 2023, 2024],
            "meta_inflacao": [3.5, 3.25, 3.0],
            "banda_superior": [5.0, 4.75, 4.5],
            "banda_inferior": [2.0, 1.75, 1.5],
            "inflacao_realizada": [5.79, 4.62, 4.83],
        })

    def test_bateu_meta_correto(self):
        df = _transform_metas(self._sample())
        # 2022: 5.79 dentro [2.0, 5.0] → False
        # 2023: 4.62 dentro [1.75, 4.75] → True
        # 2024: 4.83 dentro [1.5, 4.5] → False
        assert df.set_index("ano").loc[2022, "bateu_meta"] == False
        assert df.set_index("ano").loc[2023, "bateu_meta"] == True
        assert df.set_index("ano").loc[2024, "bateu_meta"] == False

    def test_colunas_saida(self):
        df = _transform_metas(self._sample())
        assert "bateu_meta" in df.columns
        assert "inflacao_realizada" in df.columns

    def test_sem_inflacao_realizada(self):
        sample = self._sample()
        sample.loc[2, "inflacao_realizada"] = None
        df = _transform_metas(sample)
        # Ano sem inflação realizada: bateu_meta deve ser False (não é None que passa)
        assert df.set_index("ano").loc[2024, "bateu_meta"] == False


# ─── Validações ───────────────────────────────────────────────────────────────

class TestCheckNulls:
    def test_levanta_com_nulos(self):
        df = pd.DataFrame({"data": [None, "2024-01-01"], "valor": [1.0, 2.0]})
        with pytest.raises(ValidationError, match="nulo"):
            check_nulls(df, ["data"], "test")

    def test_ok_sem_nulos(self):
        df = pd.DataFrame({"data": ["2024-01-01"], "valor": [1.0]})
        check_nulls(df, ["data", "valor"], "test")  # não levanta


class TestCheckDuplicates:
    def test_remove_duplicatas(self):
        df = pd.DataFrame({"data": ["2024-01-01", "2024-01-01"], "ind": [1, 1]})
        result = check_duplicates(df, ["data", "ind"], "test")
        assert len(result) == 1

    def test_sem_duplicatas_ok(self):
        df = pd.DataFrame({"data": ["2024-01-01", "2024-01-02"], "ind": [1, 1]})
        result = check_duplicates(df, ["data", "ind"], "test")
        assert len(result) == 2


class TestCheckRange:
    def test_remove_outliers(self):
        df = pd.DataFrame({"valor": [10.0, 100.0, 12.0]})
        result = check_range(df, "valor", 0.0, 50.0, "test")
        assert 100.0 not in result["valor"].values

    def test_strict_levanta(self):
        df = pd.DataFrame({"valor": [-1.0]})
        with pytest.raises(ValidationError):
            check_range(df, "valor", 0.0, 50.0, "test", strict=True)


class TestCheckReferentialIntegrity:
    def test_ok_quando_todos_existem(self):
        fact = pd.DataFrame({"data": [date(2024, 1, 1)]})
        dim = pd.DataFrame({"data": [date(2024, 1, 1), date(2024, 1, 2)]})
        check_referential_integrity(fact, dim, "data", "data", "test")  # não levanta

    def test_levanta_com_orfaos(self):
        fact = pd.DataFrame({"data": [date(2024, 1, 3)]})
        dim = pd.DataFrame({"data": [date(2024, 1, 1)]})
        with pytest.raises(ValidationError, match="integridade"):
            check_referential_integrity(fact, dim, "data", "data", "test")
