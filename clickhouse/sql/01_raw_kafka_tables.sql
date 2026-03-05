-- Kafka engine tables: ephemeral consumers that read from Redpanda topics.
-- Data is consumed exactly once by materialized views attached to these tables.

CREATE TABLE IF NOT EXISTS raw.customers_kafka (
    customer_id      Int64,
    customer_name    String,
    is_active        Nullable(Bool),
    customer_address Nullable(String),
    updated_at       Nullable(Int64),
    updated_by       Nullable(Int64),
    created_at       Nullable(Int64),
    created_by       Nullable(Int64),
    cdc_deleted      Nullable(Bool),
    cdc_op           Nullable(String),
    cdc_source_ts_ms Nullable(Int64)
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'deel.operations.customers',
    kafka_group_name = 'clickhouse_customers_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS raw.products_kafka (
    product_id   Int64,
    product_name String,
    barcode      String,
    unity_price  Float64,
    is_active    Nullable(Bool),
    updated_at   Nullable(Int64),
    updated_by   Nullable(Int64),
    created_at   Nullable(Int64),
    created_by   Nullable(Int64),
    cdc_deleted      Nullable(Bool),
    cdc_op           Nullable(String),
    cdc_source_ts_ms Nullable(Int64)
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'deel.operations.products',
    kafka_group_name = 'clickhouse_products_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS raw.orders_kafka (
    order_id      Int64,
    order_date    Nullable(Int32),
    delivery_date Nullable(Int32),
    customer_id   Nullable(Int64),
    status        Nullable(String),
    updated_at    Nullable(Int64),
    updated_by    Nullable(Int64),
    created_at    Nullable(Int64),
    created_by    Nullable(Int64),
    cdc_deleted      Nullable(Bool),
    cdc_op           Nullable(String),
    cdc_source_ts_ms Nullable(Int64)
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'deel.operations.orders',
    kafka_group_name = 'clickhouse_orders_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS raw.order_items_kafka (
    order_item_id Int64,
    order_id      Nullable(Int64),
    product_id    Nullable(Int64),
    quantity      Nullable(Int32),
    updated_at    Nullable(Int64),
    updated_by    Nullable(Int64),
    created_at    Nullable(Int64),
    created_by    Nullable(Int64),
    cdc_deleted      Nullable(Bool),
    cdc_op           Nullable(String),
    cdc_source_ts_ms Nullable(Int64)
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'deel.operations.order_items',
    kafka_group_name = 'clickhouse_order_items_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;
