-- Materialized views: 3-tier pattern via Null engine staging layer.
-- Kafka > [MV: transforms] > staging.stg_* (Null) > [MV: SELECT *] > dwh.* + dwh.*_hist
--
-- Order matters: staging>current and staging>hist MVs must exist BEFORE
-- the Kafka>staging MVs, so downstream targets are ready when data arrives.

-- ============================================================
-- Tier 1: Staging > Current (SELECT * fan-out, no transforms)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.customers_to_current
TO dwh.dim_customers AS
SELECT * FROM staging.stg_customers;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.products_to_current
TO dwh.dim_products AS
SELECT * FROM staging.stg_products;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.orders_to_current
TO dwh.fact_orders AS
SELECT * FROM staging.stg_orders;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.order_items_to_current
TO dwh.fact_order_items AS
SELECT * FROM staging.stg_order_items;

-- ============================================================
-- Tier 2: Staging > History (SELECT * fan-out, no transforms)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.customers_to_hist
TO dwh.dim_customers_hist AS
SELECT * FROM staging.stg_customers;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.products_to_hist
TO dwh.dim_products_hist AS
SELECT * FROM staging.stg_products;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.orders_to_hist
TO dwh.fact_orders_hist AS
SELECT * FROM staging.stg_orders;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv.order_items_to_hist
TO dwh.fact_order_items_hist AS
SELECT * FROM staging.stg_order_items;

-- ============================================================
-- Tier 3: Kafka > Staging (transformation logic, single place)
-- Created LAST so downstream MVs above are ready to receive data.
-- ============================================================

-- Customers
CREATE MATERIALIZED VIEW IF NOT EXISTS mv.customers_to_staging
TO staging.stg_customers AS
SELECT
    customer_id,
    customer_name,
    ifNull(is_active, 1) AS is_active,
    customer_address,
    if(updated_at IS NOT NULL, fromUnixTimestamp64Milli(updated_at), NULL) AS updated_at,
    updated_by,
    if(created_at IS NOT NULL, fromUnixTimestamp64Milli(created_at), NULL) AS created_at,
    created_by,
    ifNull(cdc_deleted, 0) AS cdc_deleted,
    ifNull(cdc_op, '') AS cdc_op,
    ifNull(cdc_source_ts_ms, 0) AS cdc_source_ts_ms
FROM raw.customers_kafka;

-- Products
CREATE MATERIALIZED VIEW IF NOT EXISTS mv.products_to_staging
TO staging.stg_products AS
SELECT
    product_id,
    product_name,
    barcode,
    unity_price,
    ifNull(is_active, 1) AS is_active,
    if(updated_at IS NOT NULL, fromUnixTimestamp64Milli(updated_at), NULL) AS updated_at,
    updated_by,
    if(created_at IS NOT NULL, fromUnixTimestamp64Milli(created_at), NULL) AS created_at,
    created_by,
    ifNull(cdc_deleted, 0) AS cdc_deleted,
    ifNull(cdc_op, '') AS cdc_op,
    ifNull(cdc_source_ts_ms, 0) AS cdc_source_ts_ms
FROM raw.products_kafka;

-- Orders (dates come as epoch days from Debezium; guard against negative values)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv.orders_to_staging
TO staging.stg_orders AS
SELECT
    order_id,
    if(order_date IS NOT NULL AND order_date >= 0, toDate(order_date), NULL) AS order_date,
    if(delivery_date IS NOT NULL AND delivery_date >= 0, toDate(delivery_date), NULL) AS delivery_date,
    customer_id,
    status,
    if(updated_at IS NOT NULL, fromUnixTimestamp64Milli(updated_at), NULL) AS updated_at,
    updated_by,
    if(created_at IS NOT NULL, fromUnixTimestamp64Milli(created_at), NULL) AS created_at,
    created_by,
    ifNull(cdc_deleted, 0) AS cdc_deleted,
    ifNull(cdc_op, '') AS cdc_op,
    ifNull(cdc_source_ts_ms, 0) AS cdc_source_ts_ms
FROM raw.orders_kafka;

-- Order Items
CREATE MATERIALIZED VIEW IF NOT EXISTS mv.order_items_to_staging
TO staging.stg_order_items AS
SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    if(updated_at IS NOT NULL, fromUnixTimestamp64Milli(updated_at), NULL) AS updated_at,
    updated_by,
    if(created_at IS NOT NULL, fromUnixTimestamp64Milli(created_at), NULL) AS created_at,
    created_by,
    ifNull(cdc_deleted, 0) AS cdc_deleted,
    ifNull(cdc_op, '') AS cdc_op,
    ifNull(cdc_source_ts_ms, 0) AS cdc_source_ts_ms
FROM raw.order_items_kafka;
