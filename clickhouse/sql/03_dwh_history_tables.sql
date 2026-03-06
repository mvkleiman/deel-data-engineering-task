-- Append-only history tables (MergeTree).
-- Every CDC event is recorded for full audit trail and as-of queries.

CREATE TABLE IF NOT EXISTS dwh.dim_customers_hist (
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
    cdc_source_ts_ms Int64 DEFAULT 0,
    _inserted_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PRIMARY KEY (customer_id)
ORDER BY (customer_id, cdc_source_ts_ms, _inserted_at);

CREATE TABLE IF NOT EXISTS dwh.dim_products_hist (
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
    cdc_source_ts_ms Int64 DEFAULT 0,
    _inserted_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PRIMARY KEY (product_id)
ORDER BY (product_id, cdc_source_ts_ms, _inserted_at);

CREATE TABLE IF NOT EXISTS dwh.fact_orders_hist (
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
    cdc_source_ts_ms Int64 DEFAULT 0,
    _inserted_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PRIMARY KEY (order_id)
ORDER BY (order_id, cdc_source_ts_ms, _inserted_at);

CREATE TABLE IF NOT EXISTS dwh.fact_order_items_hist (
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
    cdc_source_ts_ms Int64 DEFAULT 0,
    _inserted_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PRIMARY KEY (order_item_id)
ORDER BY (order_item_id, cdc_source_ts_ms, _inserted_at);
