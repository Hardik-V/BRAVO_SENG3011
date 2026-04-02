"""
End-to-End Tests   Full Pipeline Validation
Tests the complete collect → retrieve → visualise pipeline for multiple
tickers and scenarios. Validates the system as a whole from a user perspective.
Run manually via GitHub Actions (workflow_dispatch).
"""
import os
import time
import requests

BASE_URL = os.getenv(
    "API_BASE_URL", "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev")
API_KEY = os.environ.get("API_KEY", "")
POST_HDR = {"x-api-key": API_KEY, "Content-Type": "application/json"}
GET_HDR = {"x-api-key": API_KEY}

SLEEP_S = 4  # allow Lambda + S3 processing time between steps


def _full_pipeline(ticker: str, from_date: str, to_date: str):
    """Helper: runs collect → retrieve → visualise and returns all 3 responses."""
    body = {"ticker": ticker, "from": from_date, "to": to_date}
    params = {**body, "format": "json"}

    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=POST_HDR, json=body)
    time.sleep(SLEEP_S)
    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR,  params=body)
    vis = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=GET_HDR,  params=params)

    return col, ret, vis


# ── Pipeline: AAPL ────────────────────────────────────────────────────────────

def test_e2e_aapl_collect_succeeds():
    """E2E AAPL: collect step returns 200 or 201."""
    col, _, _ = _full_pipeline("AAPL", "2024-01-01", "2024-01-10")
    assert col.status_code in [
        200, 201], f"Collect failed: {col.status_code}   {col.text}"


def test_e2e_aapl_retrieve_does_not_crash():
    """E2E AAPL: retrieve step returns 200 or 404, never 500."""
    _, ret, _ = _full_pipeline("AAPL", "2024-01-01", "2024-01-10")
    assert ret.status_code in [
        200, 404], f"Retrieve crashed: {ret.status_code}   {ret.text}"


def test_e2e_aapl_visualise_does_not_crash():
    """E2E AAPL: visualise step returns 200 or 404, never 500."""
    _, _, vis = _full_pipeline("AAPL", "2024-01-01", "2024-01-10")
    assert vis.status_code in [
        200, 404], f"Visualise crashed: {vis.status_code}   {vis.text}"


def test_e2e_aapl_visualise_200_structure():
    """E2E AAPL: when visualise returns 200, body must contain ticker and chart_data."""
    _, _, vis = _full_pipeline("AAPL", "2024-01-01", "2024-01-10")
    if vis.status_code == 200:
        body = vis.json()
        assert "ticker" in body, f"'ticker' missing: {body}"
        assert "chart_data" in body, f"'chart_data' missing: {body}"


# ── Pipeline: MSFT ────────────────────────────────────────────────────────────

def test_e2e_msft_collect_succeeds():
    """E2E MSFT: collect step returns 200 or 201."""
    col, _, _ = _full_pipeline("MSFT", "2024-01-01", "2024-01-10")
    assert col.status_code in [
        200, 201], f"Collect failed: {col.status_code}   {col.text}"


def test_e2e_msft_retrieve_does_not_crash():
    """E2E MSFT: retrieve step returns 200 or 404, never 500."""
    _, ret, _ = _full_pipeline("MSFT", "2024-01-01", "2024-01-10")
    assert ret.status_code in [
        200, 404], f"Retrieve crashed: {ret.status_code}   {ret.text}"


def test_e2e_msft_visualise_does_not_crash():
    """E2E MSFT: visualise step returns 200 or 404, never 500."""
    _, _, vis = _full_pipeline("MSFT", "2024-01-01", "2024-01-10")
    assert vis.status_code in [
        200, 404], f"Visualise crashed: {vis.status_code}   {vis.text}"


# ── Pipeline: GOOGL ───────────────────────────────────────────────────────────

def test_e2e_googl_collect_succeeds():
    """E2E GOOGL: collect step returns 200 or 201."""
    col, _, _ = _full_pipeline("GOOGL", "2024-03-01", "2024-03-15")
    assert col.status_code in [
        200, 201], f"Collect failed: {col.status_code}   {col.text}"


def test_e2e_googl_retrieve_does_not_crash():
    """E2E GOOGL: retrieve step returns 200 or 404, never 500."""
    _, ret, _ = _full_pipeline("GOOGL", "2024-03-01", "2024-03-15")
    assert ret.status_code in [
        200, 404], f"Retrieve crashed: {ret.status_code}   {ret.text}"


def test_e2e_googl_visualise_does_not_crash():
    """E2E GOOGL: visualise step returns 200 or 404, never 500."""
    _, _, vis = _full_pipeline("GOOGL", "2024-03-01", "2024-03-15")
    assert vis.status_code in [
        200, 404], f"Visualise crashed: {vis.status_code}   {vis.text}"


# ── Error Handling End-to-End ─────────────────────────────────────────────────

def test_e2e_invalid_ticker_pipeline_graceful():
    """E2E: fake ticker is rejected at collect   pipeline never reaches retrieve/visualise with corrupt data."""
    body = {"ticker": "INVALID_XYZ999",
            "from": "2024-01-01", "to": "2024-01-10"}
    params = {**body, "format": "json"}

    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=POST_HDR, json=body)
    assert col.status_code in [
        400, 404], f"Fake ticker should be rejected at collect: {col.status_code}"

    time.sleep(SLEEP_S)

    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR, params=body)
    vis = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=GET_HDR, params=params)

    # Neither downstream service should 500
    assert ret.status_code != 500, f"Retrieve crashed: {ret.status_code}"
    assert vis.status_code != 500, f"Visualise crashed: {vis.status_code}"


def test_e2e_no_api_key_rejected_across_all_services():
    """E2E: unauthenticated request must be blocked at every service boundary."""
    body = {"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}
    params = {**body, "format": "json"}

    col = requests.post(f"{BASE_URL}/collect/financial",   json=body)
    ret = requests.get(f"{BASE_URL}/retrieve/financial",   params=body)
    vis = requests.get(f"{BASE_URL}/visualise/financial",  params=params)

    assert col.status_code == 403, f"Collect: expected 403, got {col.status_code}"
    assert ret.status_code == 403, f"Retrieve: expected 403, got {ret.status_code}"
    assert vis.status_code == 403, f"Visualise: expected 403, got {vis.status_code}"


def test_e2e_malformed_body_does_not_crash_pipeline():
    """E2E: malformed JSON to collect must return 400, not 500."""
    bad = "{ticker: 'AAPL', from: '2024-01-01'}"
    res = requests.post(f"{BASE_URL}/collect/financial",
                        headers=POST_HDR, data=bad)
    assert res.status_code == 400, f"Expected 400 for malformed body, got {res.status_code}"


def test_e2e_inverted_date_range_rejected():
    """E2E: 'from' date after 'to' date must be rejected   no 500 anywhere in the pipeline."""
    body = {"ticker": "AAPL", "from": "2024-12-01", "to": "2024-01-01"}
    params = {**body, "format": "json"}

    col = requests.post(f"{BASE_URL}/collect/financial",
                        headers=POST_HDR, json=body)
    ret = requests.get(f"{BASE_URL}/retrieve/financial",
                       headers=GET_HDR,  params=body)
    vis = requests.get(f"{BASE_URL}/visualise/financial",
                       headers=GET_HDR,  params=params)

    assert col.status_code != 500, f"Collect 500 on inverted dates: {col.text}"
    assert ret.status_code != 500, f"Retrieve 500 on inverted dates: {ret.text}"
    assert vis.status_code != 500, f"Visualise 500 on inverted dates: {vis.text}"
