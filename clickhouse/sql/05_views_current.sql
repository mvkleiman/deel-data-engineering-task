-- Convenience views for querying current state.
-- FINAL is for deduplication, filtering out soft-deleted rows.

CREATE OR REPLACE VIEW dwh.v_customers_current AS
SELECT *
FROM dwh.dim_customers FINAL
WHERE cdc_deleted = 0;

CREATE OR REPLACE VIEW dwh.v_products_current AS
SELECT *
FROM dwh.dim_products FINAL
WHERE cdc_deleted = 0;

CREATE OR REPLACE VIEW dwh.v_orders_current AS
SELECT *
FROM dwh.fact_orders FINAL
WHERE cdc_deleted = 0;

CREATE OR REPLACE VIEW dwh.v_order_items_current AS
SELECT *
FROM dwh.fact_order_items FINAL
WHERE cdc_deleted = 0;
