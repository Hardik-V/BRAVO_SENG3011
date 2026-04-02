"""
Integration Tests   Cross-Service Flows
Tests data flowing between microservices. Each test exercises at least
two services in sequence, verifying that they work together correctly.
Run manually via GitHub Actions (workflow_dispatch).
"""
import os
import time
import requests

BASE_URL = os.getenv(
    "API_BASE_URL", "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev")
API_KEY = os.environ.get("API_KEY", "")
HEADERS = {"x-api-key": API_KEY, "Content-Type": "application/json"}
GET_HDR = {"x-api-key": API_KEY}

SLEEP_S = 3  # seconds to allow Lambda/S3 async processing


# ── All Services Available ─────────────────────────────────────────────────────

def test_all_three_health_endpoints_are_up():
    """Integration sanity: all 3 microservices must be healthy before cross-service tests run."""
    for service in ["collect", "retrieve", "visualise"]:
        res = requests.get(f"{BASE_URL}/{service}/health")
        assert res.status_code == 200, f"{service} health check failed: {res.status_code}"
        assert res.json().get(
            "status") == "healthy", f"{service} not healthy: {res.text}"


# ── Collect → Retrieve ─────────────────────────────────────────────────────────

def test_collect_then_retrieve_aapl():
    """Collect AAPL data, then retrieve it   retrieve must not crash."""
    body = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=body)
    assert col.status_code in [
        200, 201], f"Collect failed: {col.status_code}   {col.text}"

    time.sleep(SLEEP_S)

    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR, params=body)
    assert ret.status_code in [
        200, 404], f"Retrieve crashed: {ret.status_code}   {ret.text}"


def test_collect_then_retrieve_response_structure():
    """After collecting AAPL, a 200 retrieve response must include 'events' and 'data_source'."""
    body = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    requests.post(f"{BASE_URL}/collect/financial", headers=HEADERS, json=body)
    time.sleep(SLEEP_S)

    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR, params=body)
    if ret.status_code == 200:
        data = ret.json()
        assert "events" in data, f"'events' missing: {data}"
        assert "data_source" in data, f"'data_source' missing: {data}"
        assert data["data_source"] == "Yahoo Finance"


def test_collect_then_retrieve_msft():
    """Collect MSFT data, then retrieve it   retrieve must not crash."""
    body = {"ticker": "MSFT", "from": "2024-02-01", "to": "2024-02-10"}
    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=body)
    assert col.status_code in [200, 201], f"Collect failed: {col.status_code}"

    time.sleep(SLEEP_S)

    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR, params=body)
    assert ret.status_code in [
        200, 404], f"Retrieve crashed: {ret.status_code}"


def test_invalid_collect_does_not_pollute_retrieve():
    """A failed collect (fake ticker) must not cause retrieve to return corrupt data."""
    bad_body = {"ticker": "FAKE_XYZ999",
                "from": "2024-01-01", "to": "2024-01-10"}
    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=bad_body)
    # Collect should reject it
    assert col.status_code in [
        400, 404], f"Expected collect to reject fake ticker, got {col.status_code}"

    time.sleep(SLEEP_S)

    # Retrieve should return 404 (nothing stored), not 200 or 500
    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR, params=bad_body)
    assert ret.status_code in [
        400, 404], f"Expected retrieve 400/404 for fake ticker, got {ret.status_code}"


# ── Collect → Visualise ────────────────────────────────────────────────────────

def test_collect_then_visualise_googl():
    """Collect GOOGL data, then visualise it   visualise must not crash."""
    body = {"ticker": "GOOGL", "from": "2024-03-01", "to": "2024-03-15"}
    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=HEADERS, json=body)
    assert col.status_code in [200, 201], f"Collect failed: {col.status_code}"

    time.sleep(SLEEP_S)

    params = {**body, "format": "json"}
    vis = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=GET_HDR, params=params)
    assert vis.status_code in [
        200, 404], f"Visualise crashed: {vis.status_code}   {vis.text}"


def test_collect_then_visualise_response_keys():
    """After collecting AAPL, a 200 visualise response must include 'ticker' and 'chart_data'."""
    body = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    requests.post(f"{BASE_URL}/collect/financial", headers=HEADERS, json=body)
    time.sleep(SLEEP_S)

    params = {**body, "format": "json"}
    vis = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=GET_HDR, params=params)
    if vis.status_code == 200:
        chart = vis.json()
        assert "ticker" in chart, f"'ticker' missing from visualise response: {chart}"
        assert "chart_data" in chart, f"'chart_data' missing from visualise response: {chart}"


# ── Auth Propagation ───────────────────────────────────────────────────────────

def test_missing_api_key_blocked_at_collect():
    """No API key must be blocked at collect   not forwarded downstream."""
    body = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.post(f"{BASE_URL}/collect/financial", json=body)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"


def test_missing_api_key_blocked_at_retrieve():
    """No API key must be blocked at retrieve."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/retrieve/financial", params=params)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"


def test_missing_api_key_blocked_at_visualise():
    """No API key must be blocked at visualise."""
    params = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    res = requests.get(f"{BASE_URL}/visualise/financial", params=params)
    assert res.status_code == 403, f"Expected 403, got {res.status_code}"
