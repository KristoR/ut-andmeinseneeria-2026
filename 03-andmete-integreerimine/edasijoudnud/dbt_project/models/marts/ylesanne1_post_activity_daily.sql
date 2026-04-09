-- LAHENDUS: Ulesanne 1 - Inkrementaalne postituste paevakokkuvote
--
-- See mudel koondab postituste statistika kasutaja ja paeva kaupa.
-- Inkrementaalne lahenemine: ainult uute paevade andmed lisatakse.
-- Komposiitne unique_key tagab, et sama kasutaja + kuupaev kombinatsiooni
-- korral uuendatakse olemasolevat rida.

{{ config(
    materialized='incremental',
    unique_key=['user_key', 'load_date']
) }}

SELECT
    u.uuid                      AS user_key,
    p.loaded_at::DATE           AS load_date,
    COUNT(p.post_id)            AS posts_count,
    ROUND(AVG(LENGTH(p.body)))  AS avg_body_length,
    SUM(LENGTH(p.body))         AS total_body_chars
FROM {{ ref('int_posts') }} p
LEFT JOIN {{ ref('int_users') }} u
    ON p.user_id = u.user_id

{% if is_incremental() %}
WHERE p.loaded_at::DATE > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}

GROUP BY u.uuid, p.loaded_at::DATE
