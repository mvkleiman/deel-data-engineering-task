import os

import clickhouse_connect
from fastapi import FastAPI, Query

app = FastAPI(
    title="ACME Delivery Analytics API",
    description="Real-time analytics powered by CDC streaming pipeline (Postgres > Debezium > Redpanda > ClickHouse)",
    version="1.0.0",
)

_client = None

def get_client():
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
    return _client


def _query(sql, params=None):
    client = get_client()
    result = client.query(sql, parameters=params)
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]


@app.get("/health")
def health_check():
    try:
        get_client().query("SELECT 1")
        return {"status": "healthy", "clickhouse": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/analytics/orders")
def get_open_orders(status: str = Query("open", description="Filter: 'open' or 'all'")):
    # Open orders grouped by delivery date and status
    if status == "open":
        rows = _query('''
            SELECT
                delivery_date,
                status,
                order_count
            FROM dwh.agg_open_orders_by_date_status
            ORDER BY delivery_date, status
        ''')
    else:
        rows = _query('''
            SELECT
                delivery_date,
                status,
                count() AS order_count
            FROM dwh.fact_orders FINAL
            WHERE cdc_deleted = 0 AND delivery_date IS NOT NULL
            GROUP BY delivery_date, status
            ORDER BY delivery_date, status
        ''')
    return {"data": rows, "total": len(rows)}


@app.get("/analytics/orders/top")
def get_top_delivery_dates(limit: int = Query(3, ge=1, description="Number of top dates")):
    # Top N delivery dates with the most open orders
    rows = _query('''
        SELECT
            delivery_date,
            order_count,
            unique_customers
        FROM dwh.agg_top_delivery_dates
        ORDER BY order_count DESC
        LIMIT {limit: UInt32}
    ''', {"limit": limit})
    return {"data": rows, "limit": limit}


@app.get("/analytics/orders/product")
def get_pending_items_by_product():
    # Pending order items grouped by product_id
    rows = _query('''
        SELECT
            product_id,
            product_name,
            pending_quantity
        FROM dwh.agg_pending_items_by_product
        ORDER BY pending_quantity DESC
    ''')
    return {"data": rows, "total": len(rows)}


@app.get("/analytics/orders/customers/")
def get_top_customers_pending(limit: int = Query(3, ge=1, description="Number of top customers")):
    # Top N customers with the most pending orders
    rows = _query('''
        SELECT
            customer_id,
            customer_name,
            pending_order_count
        FROM dwh.agg_top_customers_pending
        ORDER BY pending_order_count DESC
        LIMIT {limit: UInt32}
    ''', {"limit": limit})
    return {"data": rows, "limit": limit}
