-- Null engine staging tables: transformation fan-out layer.
-- Data is transformed once (Kafka > staging) then fans out via downstream MVs
-- to both current-state and history tables. Null engine stores nothing (zero disk cost).

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id      Int64,
    customer_name    String,
    is_active        UInt8,
    customer_address Nullable(String),
    updated_at       Nullable(DateTime64(3)),
    updated_by       Nullable(Int64),
    created_at       Nullable(DateTime64(3)),
    created_by       Nullable(Int64),
    cdc_deleted      UInt8,
    cdc_op           String,
    cdc_source_ts_ms Int64
) ENGINE = Null;

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id   Int64,
    product_name String,
    barcode      String,
    unity_price  Float64,
    is_active    UInt8,
    updated_at   Nullable(DateTime64(3)),
    updated_by   Nullable(Int64),
    created_at   Nullable(DateTime64(3)),
    created_by   Nullable(Int64),
    cdc_deleted      UInt8,
    cdc_op           String,
    cdc_source_ts_ms Int64
) ENGINE = Null;

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id      Int64,
    order_date    Nullable(Date),
    delivery_date Nullable(Date),
    customer_id   Nullable(Int64),
    status        Nullable(String),
    updated_at    Nullable(DateTime64(3)),
    updated_by    Nullable(Int64),
    created_at    Nullable(DateTime64(3)),
    created_by    Nullable(Int64),
    cdc_deleted      UInt8,
    cdc_op           String,
    cdc_source_ts_ms Int64
) ENGINE = Null;

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_item_id Int64,
    order_id      Nullable(Int64),
    product_id    Nullable(Int64),
    quantity      Nullable(Int32),
    updated_at    Nullable(DateTime64(3)),
    updated_by    Nullable(Int64),
    created_at    Nullable(DateTime64(3)),
    created_by    Nullable(Int64),
    cdc_deleted      UInt8,
    cdc_op           String,
    cdc_source_ts_ms Int64
) ENGINE = Null;
