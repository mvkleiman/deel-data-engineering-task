import os

import clickhouse_connect
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

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
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)},
        )


@app.get("/analytics/orders")
def get_open_orders(status: str = Query("open", description="Filter: 'open' or 'all'")):
    status_filter = (
        "WHERE status IN (SELECT status FROM dwh.dim_order_status FINAL WHERE is_terminal = 0)"
        if status == "open" else ""
    )
    rows = _query(f'''
        SELECT
            delivery_date,
            status,
            sum(order_count) AS order_count
        FROM dwh.agg_orders_by_date_status
        {status_filter}
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
def get_pending_items_by_product(
    status: str = Query("pending", description="Filter: 'open' or 'pending'"),
):
    status_filter = "AND status = 'PENDING'" if status == "pending" else ""
    rows = _query(f'''
        SELECT
            product_id,
            product_name,
            sum(pending_quantity) AS pending_quantity
        FROM dwh.agg_pending_items_by_product
        WHERE 1=1 {status_filter}
        GROUP BY product_id, product_name
        ORDER BY pending_quantity DESC
    ''')
    return {"data": rows, "total": len(rows), "status": status}


@app.get("/analytics/orders/customers/")
def get_top_customers(
    status: str = Query("pending", description="Filter: 'open' or 'pending'"),
    limit: int = Query(3, ge=1, description="Number of top customers"),
):
    status_filter = "AND status = 'PENDING'" if status == "pending" else ""
    rows = _query(f'''
        SELECT
            customer_id,
            customer_name,
            sum(order_count) AS order_count
        FROM dwh.agg_top_customers_open
        WHERE 1=1 {status_filter}
        GROUP BY customer_id, customer_name
        ORDER BY order_count DESC
        LIMIT {{limit: UInt32}}
    ''', {"limit": limit})
    return {"data": rows, "limit": limit, "status": status}
