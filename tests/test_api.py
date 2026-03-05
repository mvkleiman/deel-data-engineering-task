"""Tests for FastAPI analytics endpoints."""

import requests


def test_health(api_base_url):
    resp = requests.get(f"{api_base_url}/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"


def test_open_orders(api_base_url):
    resp = requests.get(f"{api_base_url}/analytics/orders", params={"status": "open"})
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert "total" in data
    if data["total"] > 0:
        row = data["data"][0]
        assert "delivery_date" in row
        assert "status" in row
        assert "order_count" in row


def test_top_delivery_dates(api_base_url):
    resp = requests.get(f"{api_base_url}/analytics/orders/top", params={"limit": 3})
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert data["limit"] == 3
    assert len(data["data"]) <= 3
    if data["data"]:
        row = data["data"][0]
        assert "delivery_date" in row
        assert "order_count" in row


def test_pending_items_by_product(api_base_url):
    resp = requests.get(f"{api_base_url}/analytics/orders/product")
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert "total" in data
    if data["total"] > 0:
        row = data["data"][0]
        assert "product_id" in row
        assert "pending_quantity" in row


def test_top_customers_pending(api_base_url):
    resp = requests.get(f"{api_base_url}/analytics/orders/customers/", params={"limit": 3})
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert data["limit"] == 3
    assert len(data["data"]) <= 3
    if data["data"]:
        row = data["data"][0]
        assert "customer_id" in row
        assert "pending_order_count" in row
