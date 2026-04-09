SELECT COUNT(*) AS rows_in_api_users
FROM staging.api_users;

SELECT COUNT(*) AS rows_in_intermediate_user_profile
FROM intermediate.user_profile_enriched;

SELECT COUNT(*) AS rows_in_user_profile
FROM analytics.user_profile;

SELECT
    user_id,
    full_name,
    email,
    account_status,
    source_system
FROM intermediate.user_profile_enriched
ORDER BY user_id;

SELECT
    user_id,
    full_name,
    email,
    account_status,
    source_system
FROM analytics.user_profile
ORDER BY user_id;
