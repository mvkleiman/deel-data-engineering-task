-- Current-state tables using ReplacingMergeTree.
-- Version by cdc_source_ts_ms, soft-delete via cdc_deleted flag.
-- Query with FINAL to get deduplicated current state.

CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_id      Int64,
    customer_name    String,
    is_active        UInt8 DEFAULT 1,
    customer_address Nullable(String),
    updated_at       Nullable(DateTime64(3)),
    updated_by       Nullable(Int64),
    created_at       Nullable(DateTime64(3)),
    created_by       Nullable(Int64),
    cdc_deleted      UInt8 DEFAULT 0,
    cdc_op           String DEFAULT '',
    cdc_source_ts_ms Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree(cdc_source_ts_ms, cdc_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_id   Int64,
    product_name String,
    barcode      String DEFAULT '',
    unity_price  Float64 DEFAULT 0,
    is_active    UInt8 DEFAULT 1,
    updated_at   Nullable(DateTime64(3)),
    updated_by   Nullable(Int64),
    created_at   Nullable(DateTime64(3)),
    created_by   Nullable(Int64),
    cdc_deleted      UInt8 DEFAULT 0,
    cdc_op           String DEFAULT '',
    cdc_source_ts_ms Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree(cdc_source_ts_ms, cdc_deleted)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id      Int64,
    order_date    Nullable(Date),
    delivery_date Nullable(Date),
    customer_id   Nullable(Int64),
    status        Nullable(String),
    updated_at    Nullable(DateTime64(3)),
    updated_by    Nullable(Int64),
    created_at    Nullable(DateTime64(3)),
    created_by    Nullable(Int64),
    cdc_deleted      UInt8 DEFAULT 0,
    cdc_op           String DEFAULT '',
    cdc_source_ts_ms Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree(cdc_source_ts_ms, cdc_deleted)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    order_item_id Int64,
    order_id      Nullable(Int64),
    product_id    Nullable(Int64),
    quantity      Nullable(Int32),
    updated_at    Nullable(DateTime64(3)),
    updated_by    Nullable(Int64),
    created_at    Nullable(DateTime64(3)),
    created_by    Nullable(Int64),
    cdc_deleted      UInt8 DEFAULT 0,
    cdc_op           String DEFAULT '',
    cdc_source_ts_ms Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree(cdc_source_ts_ms, cdc_deleted)
ORDER BY order_item_id;
