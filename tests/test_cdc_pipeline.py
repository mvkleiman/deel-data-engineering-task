"""Integration tests for CDC pipeline: Postgres > Debezium > Redpanda > ClickHouse."""

import requests


def test_redpanda_topics_exist():
    """Verify Debezium created the expected 4 topics in Redpanda."""
    resp = requests.get("http://localhost:18082/topics")
    assert resp.status_code == 200
    topics = resp.json()
    expected = [
        "deel.operations.customers",
        "deel.operations.products",
        "deel.operations.orders",
        "deel.operations.order_items",
    ]
    for topic in expected:
        assert topic in topics, f"Topic {topic} not found in Redpanda"


def test_connector_running():
    """Verify Debezium connector is in RUNNING state."""
    resp = requests.get("http://localhost:8083/connectors/postgres-source/status")
    assert resp.status_code == 200
    status = resp.json()
    assert status["connector"]["state"] == "RUNNING"
    for task in status["tasks"]:
        assert task["state"] == "RUNNING"


def test_clickhouse_customers_have_data(ch_client):
    """Verify customers data flows from Postgres through CDC to ClickHouse."""
    result = ch_client.query("SELECT count() FROM dwh.dim_customers FINAL WHERE cdc_deleted = 0")
    count = result.result_rows[0][0]
    assert count > 0, "No customers found in ClickHouse"


def test_clickhouse_products_have_data(ch_client):
    """Verify products data flows through CDC pipeline."""
    result = ch_client.query("SELECT count() FROM dwh.dim_products FINAL WHERE cdc_deleted = 0")
    count = result.result_rows[0][0]
    assert count > 0, "No products found in ClickHouse"


def test_clickhouse_orders_have_data(ch_client):
    """Verify orders data flows through CDC pipeline."""
    result = ch_client.query("SELECT count() FROM dwh.fact_orders FINAL WHERE cdc_deleted = 0")
    count = result.result_rows[0][0]
    assert count > 0, "No orders found in ClickHouse"


def test_clickhouse_order_items_have_data(ch_client):
    """Verify order items data flows through CDC pipeline."""
    result = ch_client.query("SELECT count() FROM dwh.fact_order_items FINAL WHERE cdc_deleted = 0")
    count = result.result_rows[0][0]
    assert count > 0, "No order items found in ClickHouse"


def test_history_tables_have_data(ch_client):
    """Verify append-only history tables capture CDC events."""
    for table in ["dim_customers_hist", "dim_products_hist", "fact_orders_hist", "fact_order_items_hist"]:
        result = ch_client.query(f"SELECT count() FROM dwh.{table}")
        count = result.result_rows[0][0]
        assert count > 0, f"No data in dwh.{table}"


def test_history_has_more_rows_than_current(ch_client):
    """History tables should have >= rows than current (due to updates)."""
    result_current = ch_client.query("SELECT count() FROM dwh.dim_customers FINAL")
    result_hist = ch_client.query("SELECT count() FROM dwh.dim_customers_hist")
    assert result_hist.result_rows[0][0] >= result_current.result_rows[0][0]


def test_quantity_column_renamed(ch_client):
    """Verify the quanity > quantity rename via Debezium SMT works."""
    result = ch_client.query("SELECT quantity FROM dwh.fact_order_items FINAL LIMIT 1")
    assert len(result.column_names) == 1
    assert result.column_names[0] == "quantity"


def test_scd2_has_current_row(ch_client):
    """Every entity must have at least one is_current=1 row in SCD-2 views."""
    views = {
        "v_customers_history": "customer_id",
        "v_products_history": "product_id",
        "v_orders_history": "order_id",
        "v_order_items_history": "order_item_id",
    }
    for view, entity_id in views.items():
        result = ch_client.query(
            f"SELECT {entity_id} FROM dwh.{view} "
            f"GROUP BY {entity_id} "
            f"HAVING sum(is_current) = 0"
        )
        assert len(result.result_rows) == 0, (
            f"dwh.{view}: entities with no current row: {result.result_rows}"
        )


def test_scd2_valid_from_lte_valid_to(ch_client):
    """No SCD-2 row should have valid_from > valid_to."""
    views = [
        "v_customers_history",
        "v_products_history",
        "v_orders_history",
        "v_order_items_history",
    ]
    for view in views:
        result = ch_client.query(
            f"SELECT count() FROM dwh.{view} WHERE valid_from > valid_to"
        )
        count = result.result_rows[0][0]
        assert count == 0, f"dwh.{view}: {count} rows with valid_from > valid_to"
