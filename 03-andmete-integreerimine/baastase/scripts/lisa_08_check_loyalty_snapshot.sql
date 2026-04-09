-- Kõigepealt kontrollime staging-vaate mahu üle.
SELECT COUNT(*) AS rows_in_user_loyalty_snapshot
FROM staging.user_loyalty_snapshot;

-- Siis vaatame, mitu kasutajat olemasoleva lõpptabeliga päriselt sobitub.
SELECT COUNT(*) AS matched_users
FROM analytics.user_profile AS u
JOIN staging.user_loyalty_snapshot AS s
    ON u.email = s.email;

-- Lõpuks vaatame joinitud tulemust rea tasemel.
SELECT
    u.user_id,
    u.full_name,
    u.email,
    u.account_status,
    s.loyalty_tier,
    s.risk_level,
    s.snapshot_date
FROM analytics.user_profile AS u
LEFT JOIN staging.user_loyalty_snapshot AS s
    ON u.email = s.email
ORDER BY u.user_id;
