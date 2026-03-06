-- Refreshable materialized views for business query aggregates.
-- Re-computed every 5 seconds from current-state tables with FINAL.
-- Backend API queries these pre-computed tables with sub-millisecond response.
--
-- Status filtering uses dwh.dim_order_status as single source of truth:
--   "open"    = statuses where is_terminal = 0 (PENDING, PROCESSING, REPROCESSING)
--   "pending" = only PENDING status (subset of open)

-- 1. Orders by delivery_date and status (all statuses, filtered at query time)
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.agg_orders_by_date_status
REFRESH EVERY 5 SECOND
ENGINE = MergeTree()
ORDER BY (delivery_date, status)
AS
SELECT
    assumeNotNull(delivery_date) AS delivery_date,
    assumeNotNull(status) AS status,
    count() AS order_count
FROM dwh.fact_orders FINAL
WHERE cdc_deleted = 0
  AND delivery_date IS NOT NULL
  AND status IS NOT NULL
GROUP BY delivery_date, status;

-- 2. Top delivery dates with most open orders
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.agg_top_delivery_dates
REFRESH EVERY 5 SECOND DEPENDS ON dwh.agg_orders_by_date_status
ENGINE = MergeTree()
ORDER BY delivery_date
AS
SELECT
    assumeNotNull(delivery_date) AS delivery_date,
    count() AS order_count,
    uniq(customer_id) AS unique_customers
FROM dwh.fact_orders FINAL
WHERE cdc_deleted = 0
  AND status IN (SELECT status FROM dwh.dim_order_status FINAL WHERE is_terminal = 0)
  AND delivery_date IS NOT NULL
GROUP BY delivery_date;

-- 3. Open items by product (all open statuses, filtered at query time)
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.agg_pending_items_by_product
REFRESH EVERY 5 SECOND DEPENDS ON dwh.agg_top_delivery_dates
ENGINE = MergeTree()
ORDER BY (product_id, status)
AS
SELECT
    assumeNotNull(oi.product_id) AS product_id,
    ifNull(p.product_name, '') AS product_name,
    assumeNotNull(o.status) AS status,
    sum(oi.quantity) AS pending_quantity
FROM dwh.v_order_items_current AS oi
INNER JOIN dwh.v_orders_current AS o ON oi.order_id = o.order_id
LEFT JOIN dwh.v_products_current AS p ON oi.product_id = p.product_id
WHERE o.status IN (SELECT status FROM dwh.dim_order_status FINAL WHERE is_terminal = 0)
  AND oi.product_id IS NOT NULL
GROUP BY oi.product_id, p.product_name, o.status;

-- 4. Top customers with open orders (grouped by status for flexible filtering)
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.agg_top_customers_open
REFRESH EVERY 5 SECOND DEPENDS ON dwh.agg_pending_items_by_product
ENGINE = MergeTree()
ORDER BY (customer_id, status)
AS
SELECT
    assumeNotNull(o.customer_id) AS customer_id,
    ifNull(c.customer_name, '') AS customer_name,
    assumeNotNull(o.status) AS status,
    count() AS order_count
FROM dwh.v_orders_current AS o
LEFT JOIN dwh.v_customers_current AS c ON o.customer_id = c.customer_id
WHERE o.status IN (SELECT status FROM dwh.dim_order_status FINAL WHERE is_terminal = 0)
  AND o.customer_id IS NOT NULL
GROUP BY o.customer_id, c.customer_name, o.status;
