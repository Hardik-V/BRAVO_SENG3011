"""
Unit Tests   Retrieval Microservice
Tests the /retrieve endpoints in isolation, covering health checks,
parameter validation, date logic, and successful data retrieval.
"""
import os
import requests

BASE_URL = os.getenv(
    "API_BASE_URL", "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev")
API_KEY = os.environ.get("API_KEY", "")
HEADERS = {"x-api-key": API_KEY}


# ── Health ────────────────────────────────────────────────────────────────────

def test_retrieve_health_returns_200():
    """Health endpoint responds with HTTP 200."""
    res = requests.get(f"{BASE_URL}/retrieve/health")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"


def test_retrieve_health_returns_healthy_status():
    """Health endpoint body contains status=healthy."""
    res = requests.get(f"{BASE_URL}/retrieve/health")
    assert res.json().get(
        "status") == "healthy", f"Unexpected body: {res.text}"


# ── Valid Requests ─────────────────────────────────────────────────────────────

def test_retrieve_valid_request_returns_200_or_404():
    """Valid AAPL query returns 200 (data found) or 404 (no data in S3 yet)   never 500."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code in [
        200, 404], f"Expected 200/404, got {res.status_code}: {res.text}"


def test_retrieve_200_response_contains_events_key():
    """When data is found (200), response body must contain an 'events' key."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    if res.status_code == 200:
        body = res.json()
        assert "events" in body, f"'events' key missing from response: {body}"


def test_retrieve_200_response_contains_data_source():
    """When data is found (200), response body must contain 'data_source' = Yahoo Finance."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    if res.status_code == 200:
        body = res.json()
        assert body.get(
            "data_source") == "Yahoo Finance", f"Unexpected data_source: {body}"


# ── Parameter Validation ───────────────────────────────────────────────────────

def test_retrieve_missing_ticker_returns_400():
    """Request missing 'ticker' must return 400."""
    params = {"from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_retrieve_missing_from_returns_400():
    """Request missing 'from' date must return 400."""
    params = {"ticker": "AAPL", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_retrieve_missing_to_returns_400():
    """Request missing 'to' date must return 400."""
    params = {"ticker": "AAPL", "from": "2024-01-01"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_retrieve_no_params_returns_400():
    """Request with no query parameters at all must return 400."""
    res = requests.get(f"{BASE_URL}/retrieve/financial", headers=HEADERS)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


# ── Date Logic Validation ──────────────────────────────────────────────────────

def test_retrieve_invalid_date_format_returns_400():
    """Date in DD-MM-YYYY format (not ISO 8601) must return 400."""
    params = {"ticker": "AAPL", "from": "01-01-2024", "to": "10-01-2024"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400 for bad date format, got {res.status_code}"


def test_retrieve_from_after_to_returns_400():
    """'from' date after 'to' date must return 400."""
    params = {"ticker": "AAPL", "from": "2024-06-01", "to": "2024-01-01"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400 for inverted date range, got {res.status_code}"


def test_retrieve_same_from_and_to_date_handled():
    """Same 'from' and 'to' date should not crash the service (no 500)."""
    params = {"ticker": "AAPL", "from": "2024-01-05", "to": "2024-01-05"}
    res = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=HEADERS, params=params)
    assert res.status_code != 500, f"Service crashed with 500: {res.text}"


# ── Auth & Routing ─────────────────────────────────────────────────────────────

def test_retrieve_no_api_key_returns_403():
    """Request without API key must be rejected (403)."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial", params=params)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"


def test_retrieve_invalid_route_returns_403_or_404():
    """Calling a non-existent route must return 403 or 404."""
    res = requests.get(
        f"{BASE_URL}/retrieve/invalid_endpoint_xyz", headers=HEADERS)
    assert res.status_code in [
        403, 404], f"Expected 403/404, got {res.status_code}"
