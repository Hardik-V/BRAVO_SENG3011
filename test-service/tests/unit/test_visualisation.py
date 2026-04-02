"""
Unit Tests   Visualisation Microservice
Tests the /visualise endpoints in isolation, covering health checks,
parameter validation, format options, and response structure.
"""
import os
import requests

BASE_URL = os.getenv(
    "API_BASE_URL", "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev")
API_KEY = os.environ.get("API_KEY", "")
HEADERS = {"x-api-key": API_KEY}


# ── Health ────────────────────────────────────────────────────────────────────

def test_visualise_health_returns_200():
    """Health endpoint responds with HTTP 200."""
    res = requests.get(f"{BASE_URL}/visualise/health")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"


def test_visualise_health_returns_healthy_status():
    """Health endpoint body contains status=healthy."""
    res = requests.get(f"{BASE_URL}/visualise/health")
    assert res.json().get(
        "status") == "healthy", f"Unexpected body: {res.text}"


# ── Valid Requests ─────────────────────────────────────────────────────────────

def test_visualise_valid_json_format_returns_200_or_404():
    """Valid request with format=json returns 200 (data exists) or 404 (no data)."""
    params = {"ticker": "AAPL", "from": "2024-01-01",
              "to": "2024-01-10", "format": "json"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    assert res.status_code in [
        200, 404], f"Expected 200/404, got {res.status_code}: {res.text}"


def test_visualise_200_response_contains_ticker():
    """When data is found (200), response must contain 'ticker' field."""
    params = {"ticker": "AAPL", "from": "2024-01-01",
              "to": "2024-01-10", "format": "json"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    if res.status_code == 200:
        body = res.json()
        assert "ticker" in body, f"'ticker' key missing from response: {body}"


def test_visualise_200_response_contains_chart_data():
    """When data is found (200), response must contain 'chart_data' field."""
    params = {"ticker": "AAPL", "from": "2024-01-01",
              "to": "2024-01-10", "format": "json"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    if res.status_code == 200:
        body = res.json()
        assert "chart_data" in body, f"'chart_data' key missing from response: {body}"


def test_visualise_msft_valid_request():
    """Valid MSFT request does not crash the service (no 500)."""
    params = {"ticker": "MSFT", "from": "2024-01-01",
              "to": "2024-01-10", "format": "json"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    assert res.status_code != 500, f"Service crashed with 500: {res.text}"


# ── Parameter Validation ───────────────────────────────────────────────────────

def test_visualise_no_params_returns_400():
    """Request with no query parameters at all must return 400."""
    res = requests.get(f"{BASE_URL}/visualise/financial", headers=HEADERS)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_visualise_missing_ticker_returns_400():
    """Request missing 'ticker' must return 400."""
    params = {"from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_visualise_missing_from_date_returns_400():
    """Request missing 'from' date must return 400."""
    params = {"ticker": "AAPL", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_visualise_missing_to_date_returns_400():
    """Request missing 'to' date must return 400."""
    params = {"ticker": "AAPL", "from": "2024-01-01"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_visualise_ticker_only_returns_400():
    """Only providing ticker (missing both date params) must return 400."""
    params = {"ticker": "AAPL"}
    res = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=HEADERS, params=params)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


# ── Auth & Routing ─────────────────────────────────────────────────────────────

def test_visualise_no_api_key_returns_403():
    """Request without API key must be rejected (403)."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/visualise/financial", params=params)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"


def test_visualise_invalid_route_returns_403_or_404():
    """Calling a non-existent route must return 403 or 404."""
    res = requests.get(
        f"{BASE_URL}/visualise/invalid_endpoint_xyz", headers=HEADERS)
    assert res.status_code in [
        403, 404], f"Expected 403/404, got {res.status_code}"
