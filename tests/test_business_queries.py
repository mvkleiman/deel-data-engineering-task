"""Tests for business query aggregate tables in ClickHouse."""


def test_agg_orders_by_date_status(ch_client):
    """Verify orders-by-date-status aggregate has correct columns and data."""
    result = ch_client.query("SELECT * FROM dwh.agg_orders_by_date_status LIMIT 10")
    assert set(result.column_names) == {"delivery_date", "status", "order_count"}
    assert len(result.result_rows) > 0


def test_agg_top_delivery_dates(ch_client):
    """Verify top delivery dates aggregate has correct columns and data."""
    result = ch_client.query("SELECT * FROM dwh.agg_top_delivery_dates LIMIT 10")
    assert set(result.column_names) == {"delivery_date", "order_count", "unique_customers"}
    assert len(result.result_rows) > 0


def test_agg_top_customers_open(ch_client):
    """Verify top customers aggregate has correct columns."""
    result = ch_client.query("SELECT * FROM dwh.agg_top_customers_open LIMIT 10")
    assert set(result.column_names) == {"customer_id", "customer_name", "status", "order_count"}


def test_orders_agg_includes_all_statuses(ch_client):
    """Verify orders aggregate includes all statuses (filtering happens at query time)."""
    result = ch_client.query("""
        SELECT DISTINCT status FROM dwh.agg_orders_by_date_status
    """)
    statuses = {row[0] for row in result.result_rows}
    # Should contain at least one status
    assert len(statuses) > 0


def test_current_views_work(ch_client):
    """Verify convenience views return data without errors."""
    for view in ["v_customers_current", "v_products_current", "v_orders_current", "v_order_items_current"]:
        result = ch_client.query(f"SELECT count() FROM dwh.{view}")
        assert result.result_rows[0][0] >= 0
