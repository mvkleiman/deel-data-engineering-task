-- SCD Type 2 views over history tables.
-- Provide classic valid_from / valid_to / is_current columns
-- for point-in-time ("as-of") queries on dimension history like
-- show me all the customers that were active in 2025 and still active today:
-- SELECT DISTINCT customer_id, customer_name FROM dwh.v_customers_history WHERE valid_from >= '2025-01-01' AND valid_to < '2026-01-01' and is_active and is_current

-- valid_from / valid_to are derived from cdc_source_ts_ms (Debezium event timestamp)
-- rather than _inserted_at, because CDC events can arrive in ClickHouse out of temporal order.

-- Window computations are wrapped in a subquery so the optimizer cannot inline
-- valid_to into the is_current expression. Without this barrier, queries that
-- filter on both is_current and valid_from/valid_to hit
-- TOO_MANY_QUERY_PLAN_OPTIMIZATIONS (error 572).

CREATE OR REPLACE VIEW dwh.v_customers_history AS
SELECT
    *,
    valid_to = toDateTime64('9999-12-31 23:59:59.999', 3) AS is_current
FROM (
    SELECT
        customer_id,
        customer_name,
        is_active,
        customer_address,
        updated_at,
        updated_by,
        created_at,
        created_by,
        cdc_op,
        cdc_deleted,
        cdc_source_ts_ms,
        fromUnixTimestamp64Milli(cdc_source_ts_ms) AS valid_from,
        leadInFrame(fromUnixTimestamp64Milli(cdc_source_ts_ms), 1, toDateTime64('9999-12-31 23:59:59.999', 3))
            OVER (PARTITION BY customer_id ORDER BY cdc_source_ts_ms, _inserted_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS valid_to
    FROM dwh.dim_customers_hist
);

CREATE OR REPLACE VIEW dwh.v_products_history AS
SELECT
    *,
    valid_to = toDateTime64('9999-12-31 23:59:59.999', 3) AS is_current
FROM (
    SELECT
        product_id,
        product_name,
        barcode,
        unity_price,
        is_active,
        updated_at,
        updated_by,
        created_at,
        created_by,
        cdc_op,
        cdc_deleted,
        cdc_source_ts_ms,
        fromUnixTimestamp64Milli(cdc_source_ts_ms) AS valid_from,
        leadInFrame(fromUnixTimestamp64Milli(cdc_source_ts_ms), 1, toDateTime64('9999-12-31 23:59:59.999', 3))
            OVER (PARTITION BY product_id ORDER BY cdc_source_ts_ms, _inserted_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS valid_to
    FROM dwh.dim_products_hist
);

CREATE OR REPLACE VIEW dwh.v_orders_history AS
SELECT
    *,
    valid_to = toDateTime64('9999-12-31 23:59:59.999', 3) AS is_current
FROM (
    SELECT
        order_id,
        order_date,
        delivery_date,
        customer_id,
        status,
        updated_at,
        updated_by,
        created_at,
        created_by,
        cdc_op,
        cdc_deleted,
        cdc_source_ts_ms,
        fromUnixTimestamp64Milli(cdc_source_ts_ms) AS valid_from,
        leadInFrame(fromUnixTimestamp64Milli(cdc_source_ts_ms), 1, toDateTime64('9999-12-31 23:59:59.999', 3))
            OVER (PARTITION BY order_id ORDER BY cdc_source_ts_ms, _inserted_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS valid_to
    FROM dwh.fact_orders_hist
);

CREATE OR REPLACE VIEW dwh.v_order_items_history AS
SELECT
    *,
    valid_to = toDateTime64('9999-12-31 23:59:59.999', 3) AS is_current
FROM (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        updated_at,
        updated_by,
        created_at,
        created_by,
        cdc_op,
        cdc_deleted,
        cdc_source_ts_ms,
        fromUnixTimestamp64Milli(cdc_source_ts_ms) AS valid_from,
        leadInFrame(fromUnixTimestamp64Milli(cdc_source_ts_ms), 1, toDateTime64('9999-12-31 23:59:59.999', 3))
            OVER (PARTITION BY order_item_id ORDER BY cdc_source_ts_ms, _inserted_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS valid_to
    FROM dwh.fact_order_items_hist
);
