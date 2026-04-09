-- LAHENDUS: Ulesanne 3 - Kasutaja aktiivsusraport
--
-- Uhendab kasutajadimensiooni, postituste meetrikad ja snapshotajaloo.
-- Naitab iga kasutaja praegust ja eelmist linna (kui aadress on muutunud).

WITH current_address AS (
    -- Praegune kehtiv aadress snapshotist
    SELECT
        uuid,
        city    AS current_city,
        country AS current_country
    FROM {{ ref('snap_users') }}
    WHERE dbt_valid_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
),

previous_address AS (
    -- Viimane aegunud aadress (kui on muudetud)
    SELECT
        uuid,
        city    AS previous_city,
        country AS previous_country
    FROM (
        SELECT
            uuid,
            city,
            country,
            ROW_NUMBER() OVER (
                PARTITION BY uuid
                ORDER BY dbt_valid_from DESC
            ) AS rn
        FROM {{ ref('snap_users') }}
        WHERE dbt_valid_to != to_timestamp('9999-12-31', 'YYYY-MM-DD')
    ) ranked
    WHERE rn = 1
),

post_metrics AS (
    -- Postituste statistika kasutaja kohta
    SELECT
        user_key,
        COUNT(*)            AS total_posts,
        ROUND(AVG(body_length)) AS avg_post_length,
        MAX(loaded_at)      AS last_post_at
    FROM {{ ref('fct_posts') }}
    GROUP BY user_key
)

SELECT
    u.user_key,
    u.full_name,
    u.email,
    ca.current_city,
    ca.current_country,
    pa.previous_city,
    pa.previous_country,
    COALESCE(pm.total_posts, 0)     AS total_posts,
    COALESCE(pm.avg_post_length, 0) AS avg_post_length,
    pm.last_post_at,
    u.registered_date
FROM {{ ref('dim_users') }} u
LEFT JOIN current_address ca    ON u.user_key = ca.uuid
LEFT JOIN previous_address pa   ON u.user_key = pa.uuid
LEFT JOIN post_metrics pm       ON u.user_key = pm.user_key
ORDER BY total_posts DESC
