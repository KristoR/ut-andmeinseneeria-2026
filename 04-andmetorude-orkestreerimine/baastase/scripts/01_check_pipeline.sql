SELECT COUNT(*) AS products_rows
FROM staging.products_raw;

SELECT COUNT(*) AS stores_rows
FROM staging.stores_raw;

SELECT order_date, COUNT(*) AS orders_rows
FROM staging.orders_raw
GROUP BY order_date
ORDER BY order_date;

SELECT
    sales_date,
    store_name,
    product_name,
    total_quantity,
    gross_sales_eur
FROM analytics.daily_product_sales
ORDER BY sales_date, store_name, product_name;

SELECT
    run_id,
    step_name,
    logical_date,
    attempt_no,
    status,
    rows_loaded,
    message,
    started_at,
    finished_at
FROM staging.pipeline_run_log
ORDER BY id DESC
LIMIT 25;
