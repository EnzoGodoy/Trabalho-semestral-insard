-- ─── Consultas analíticas de valor ───────────────────────────────────────────
-- Executar contra o banco "dw" após carga completa do pipeline.

-- Query 1: Evolução da SELIC — média mensal nos últimos 24 meses
-- Responde: "Como a taxa básica de juros variou mês a mês?"
SELECT
    t.ano,
    t.mes,
    t.nome_mes,
    ROUND(AVG(f.valor)::numeric, 4) AS selic_media_mensal_pct
FROM dw.fact_serie_temporal f
JOIN dw.dim_tempo      t ON f.data = t.data
JOIN dw.dim_indicador  i ON f.indicador_id = i.id
WHERE i.chave = 'selic_diaria'
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;

-- ─────────────────────────────────────────────────────────────────────────────

-- Query 2: IPCA acumulado por ano vs meta do CMN
-- Responde: "O Brasil cumpriu a meta de inflação?"
SELECT
    t.ano,
    ROUND(SUM(f.valor)::numeric, 2)  AS ipca_acumulado_pct,
    m.meta_inflacao,
    m.banda_inferior,
    m.banda_superior,
    m.bateu_meta
FROM dw.fact_serie_temporal  f
JOIN dw.dim_tempo             t ON f.data = t.data
JOIN dw.dim_indicador         i ON f.indicador_id = i.id
LEFT JOIN dw.dim_meta_inflacao m ON t.ano = m.ano
WHERE i.chave = 'ipca_mensal'
GROUP BY t.ano, m.meta_inflacao, m.banda_inferior, m.banda_superior, m.bateu_meta
ORDER BY t.ano;

-- ─────────────────────────────────────────────────────────────────────────────

-- Query 3: Câmbio USD/BRL — estatísticas trimestrais
-- Responde: "Qual foi a volatilidade cambial em cada trimestre?"
SELECT
    t.ano,
    t.trimestre,
    ROUND(AVG(f.valor)::numeric, 4) AS cambio_medio,
    ROUND(MAX(f.valor)::numeric, 4) AS cambio_max,
    ROUND(MIN(f.valor)::numeric, 4) AS cambio_min,
    ROUND((MAX(f.valor) - MIN(f.valor))::numeric, 4) AS amplitude
FROM dw.fact_serie_temporal f
JOIN dw.dim_tempo      t ON f.data = t.data
JOIN dw.dim_indicador  i ON f.indicador_id = i.id
WHERE i.chave = 'cambio_usd'
GROUP BY t.ano, t.trimestre
ORDER BY t.ano, t.trimestre;
