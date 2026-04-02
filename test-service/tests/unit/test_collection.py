"""
Unit Tests   Collection Microservice
Tests the /collect endpoints in isolation, covering health checks,
valid requests, input validation, and edge cases.
"""
import os
import requests

BASE_URL = os.getenv(
    "API_BASE_URL", "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev")
API_KEY = os.environ.get("API_KEY", "")
HEADERS = {"x-api-key": API_KEY, "Content-Type": "application/json"}


# ── Health ────────────────────────────────────────────────────────────────────

def test_collect_health_returns_200():
    """Health endpoint responds with HTTP 200."""
    res = requests.get(f"{BASE_URL}/collect/health")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"


def test_collect_health_returns_healthy_status():
    """Health endpoint body contains status=healthy."""
    res = requests.get(f"{BASE_URL}/collect/health")
    assert res.json().get(
        "status") == "healthy", f"Unexpected body: {res.text}"


# ── Valid Requests ─────────────────────────────────────────────────────────────

def test_collect_valid_request_aapl():
    """Valid AAPL payload is accepted (200 or 201)."""
    payload = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code in [
        200, 201], f"Expected 200/201, got {res.status_code}: {res.text}"


def test_collect_valid_request_msft():
    """Valid MSFT payload is accepted (200 or 201)."""
    payload = {"ticker": "MSFT", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code in [
        200, 201], f"Expected 200/201, got {res.status_code}: {res.text}"


def test_collect_valid_request_googl():
    """Valid GOOGL payload is accepted (200 or 201)."""
    payload = {"ticker": "GOOGL", "from": "2024-03-01", "to": "2024-03-15"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code in [
        200, 201], f"Expected 200/201, got {res.status_code}: {res.text}"


# ── Input Validation ───────────────────────────────────────────────────────────

def test_collect_missing_ticker_returns_400():
    """Request missing 'ticker' field must return 400."""
    payload = {"from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_collect_missing_from_date_returns_400():
    """Request missing 'from' date field must return 400."""
    payload = {"ticker": "AAPL", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_collect_missing_to_date_returns_400():
    """Request missing 'to' date field must return 400."""
    payload = {"ticker": "AAPL", "from": "2024-01-01"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_collect_empty_body_returns_400():
    """Empty request body must return 400."""
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json={})
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


def test_collect_malformed_json_returns_400():
    """Malformed JSON string (not valid JSON) must return 400."""
    bad = "{ticker: 'AAPL'}"
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, data=bad)
    assert res.status_code == 400, f"Expected 400, got {res.status_code}"


# ── Edge Cases ─────────────────────────────────────────────────────────────────

def test_collect_fake_ticker_returns_400_or_404():
    """Fake ticker symbol should be rejected gracefully (400 or 404)."""
    payload = {"ticker": "FAKE_TICKER_XYZ999",
               "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code in [
        400, 404], f"Expected 400/404, got {res.status_code}"


def test_collect_no_api_key_returns_403():
    """Request without API key must be rejected (403)."""
    payload = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial", json=payload)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"


def test_collect_invalid_api_key_returns_403():
    """Request with wrong API key must be rejected (403)."""
    bad_headers = {"x-api-key": "WRONG-KEY-999",
                   "Content-Type": "application/json"}
    payload = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=bad_headers, json=payload)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"


def test_collect_date_range_too_large_handled():
    """Very large date range should not crash the service (no 500)."""
    payload = {"ticker": "AAPL", "from": "2020-01-01", "to": "2024-12-31"}
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=payload)
    assert res.status_code != 500, f"Service crashed with 500: {res.text}"
