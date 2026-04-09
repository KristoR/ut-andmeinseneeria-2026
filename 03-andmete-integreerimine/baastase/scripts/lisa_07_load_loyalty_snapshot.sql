-- Loo vaade, mis loeb vajalikud väljad otse Parquet failist.

CREATE OR REPLACE VIEW staging.user_loyalty_snapshot AS
SELECT
    p['email']::TEXT AS email,
    p['loyalty_tier']::TEXT AS loyalty_tier,
    p['risk_level']::TEXT AS risk_level,
    p['snapshot_date']::DATE AS snapshot_date
FROM read_parquet('/data/kasutaja_rikastus.parquet') AS p;
