-- Kui read_parquet ei tööta kohe, proovi vajadusel enne järgmist rida:
-- SET duckdb.force_execution = true;

SELECT *
FROM read_parquet('/data/kasutaja_rikastus.parquet')
LIMIT 5;

SELECT COUNT(*) AS rows_in_parquet
FROM read_parquet('/data/kasutaja_rikastus.parquet');

SELECT
    p['email']::TEXT AS email,
    p['loyalty_tier']::TEXT AS loyalty_tier,
    p['risk_level']::TEXT AS risk_level,
    p['snapshot_date']::DATE AS snapshot_date
FROM read_parquet('/data/kasutaja_rikastus.parquet') AS p
ORDER BY p['email']::TEXT;
