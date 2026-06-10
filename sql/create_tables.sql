-- DDL do Data Warehouse — modelo dimensional simples
-- Schema: dw
-- Executado pelo task init_db da DAG antes de cada run.

CREATE SCHEMA IF NOT EXISTS dw;

-- ─── Dimensão Tempo ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    data        DATE        PRIMARY KEY,
    dia         SMALLINT    NOT NULL,
    mes         SMALLINT    NOT NULL,
    ano         SMALLINT    NOT NULL,
    trimestre   SMALLINT    NOT NULL,
    semestre    SMALLINT    NOT NULL,
    nome_mes    VARCHAR(20)
);

-- ─── Dimensão Indicador ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_indicador (
    id              SERIAL      PRIMARY KEY,
    chave           VARCHAR(50) UNIQUE NOT NULL,
    codigo_bcb      INTEGER     NOT NULL,
    nome            VARCHAR(100) NOT NULL,
    unidade         VARCHAR(20),
    periodicidade   VARCHAR(20),
    fonte           VARCHAR(50)
);

-- ─── Dimensão Meta de Inflação (fonte: CSV) ───────────────────────────────────
-- Dimension auxiliar — referencia o ano para enriquecer análises de IPCA
CREATE TABLE IF NOT EXISTS dw.dim_meta_inflacao (
    ano                 SMALLINT    PRIMARY KEY,
    meta_inflacao       NUMERIC(5,2),
    banda_superior      NUMERIC(5,2),
    banda_inferior      NUMERIC(5,2),
    inflacao_realizada  NUMERIC(5,2),
    bateu_meta          BOOLEAN
);

-- ─── Fato: Série Temporal ─────────────────────────────────────────────────────
-- Grão: um valor por indicador por data
CREATE TABLE IF NOT EXISTS dw.fact_serie_temporal (
    id              SERIAL      PRIMARY KEY,
    data            DATE        NOT NULL REFERENCES dw.dim_tempo(data),
    indicador_id    INTEGER     NOT NULL REFERENCES dw.dim_indicador(id),
    valor           NUMERIC(12,6) NOT NULL,
    UNIQUE (data, indicador_id)
);

-- Índices para suporte às queries analíticas
CREATE INDEX IF NOT EXISTS idx_fact_data        ON dw.fact_serie_temporal(data);
CREATE INDEX IF NOT EXISTS idx_fact_indicador   ON dw.fact_serie_temporal(indicador_id);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano    ON dw.dim_tempo(ano, mes);
