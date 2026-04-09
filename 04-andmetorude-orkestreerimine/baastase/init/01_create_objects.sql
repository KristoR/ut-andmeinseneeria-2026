CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS staging.products_raw (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    base_price_eur NUMERIC(10, 2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.stores_raw (
    store_id TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    city TEXT NOT NULL,
    region TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.orders_raw (
    order_id TEXT PRIMARY KEY,
    order_date DATE NOT NULL,
    store_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_eur NUMERIC(10, 2) NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.pipeline_run_log (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    step_name TEXT NOT NULL,
    logical_date DATE,
    attempt_no INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.daily_product_sales (
    sales_date DATE NOT NULL,
    store_id TEXT NOT NULL,
    store_name TEXT NOT NULL,
    region TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    order_count INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    gross_sales_eur NUMERIC(12, 2) NOT NULL,
    rebuilt_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sales_date, store_id, product_id)
);

CREATE OR REPLACE VIEW intermediate.orders_enriched AS
SELECT
    o.order_id,
    o.order_date,
    o.store_id,
    s.store_name,
    s.city AS store_city,
    s.region,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.unit_price_eur,
    ROUND(o.quantity * o.unit_price_eur, 2) AS total_amount_eur,
    o.source_updated_at,
    o.loaded_at
FROM staging.orders_raw AS o
LEFT JOIN staging.products_raw AS p
    ON LOWER(TRIM(o.product_id)) = LOWER(TRIM(p.product_id))
LEFT JOIN staging.stores_raw AS s
    ON LOWER(TRIM(o.store_id)) = LOWER(TRIM(s.store_id));
