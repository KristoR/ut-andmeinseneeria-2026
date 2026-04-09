CREATE TABLE IF NOT EXISTS staging.notification_preferences (
    email TEXT PRIMARY KEY,
    newsletter_opt_in BOOLEAN,
    preferred_channel TEXT,
    updated_at DATE,
    loaded_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE analytics.user_profile
ADD COLUMN IF NOT EXISTS newsletter_opt_in BOOLEAN;

ALTER TABLE analytics.user_profile
ADD COLUMN IF NOT EXISTS preferred_channel TEXT;

CREATE OR REPLACE VIEW intermediate.notification_preferences_normalized AS
SELECT
    email AS source_email,
    LOWER(TRIM(email)) AS email_key,
    newsletter_opt_in,
    preferred_channel,
    updated_at
FROM staging.notification_preferences;

CREATE OR REPLACE VIEW intermediate.user_profile_enriched AS
SELECT
    a.user_id,
    a.full_name,
    a.username,
    a.email_key AS email,
    a.city,
    a.company_name,
    s.account_status,
    s.source_system,
    p.newsletter_opt_in,
    p.preferred_channel
FROM intermediate.api_users_normalized AS a
LEFT JOIN intermediate.user_status_normalized AS s
    ON a.email_key = s.email_key
LEFT JOIN intermediate.notification_preferences_normalized AS p
    ON a.email_key = p.email_key;
