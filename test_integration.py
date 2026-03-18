"""
Integration tests for the Bravo Financial Event API.
These tests hit the live AWS endpoints and require a real API key.

Run with:
    API_KEY=your-key pytest test_integration.py -v --timeout=60

Note: These tests are slow (10-30s each) due to live API calls.
They are excluded from the CI pipeline and should be run manually.
"""

import requests
import os
import time
from dotenv import load_dotenv
load_dotenv()

BASE_URL = os.environ.get("API_BASE_URL", "https://...")
API_KEY = os.environ.get("API_KEY", "")
HEADERS = {"x-api-key": API_KEY}

TICKER = "AAPL"
FROM_DATE = "2024-01-01"
TO_DATE = "2024-01-10"


# ─── Health Checks ────────────────────────────────

def test_collection_health():
    """Collection service should be live and healthy."""
    response = requests.get(f"{BASE_URL}/collect/health",
                            timeout=30)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["service"] == "bravo-collection"
    assert body["version"] == "1.0.0"


def test_retrieval_health():
    """Retrieval service should be live and healthy."""
    response = requests.get(f"{BASE_URL}/retrieve/health",
                            timeout=30)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["service"] == "bravo-retrieval"
    assert body["version"] == "1.0.0"


def test_visualisation_health():
    """Visualisation service should be live and healthy."""
    response = requests.get(f"{BASE_URL}/visualise/health",
                            timeout=30)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["service"] == "bravo-visualisation"
    assert body["version"] == "1.0.0"


# ─── Authentication ─────────────────────────────────────────

def test_collect_without_api_key_returns_403():
    """Requests without API key should be rejected by API Gateway."""
    response = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        timeout=30
    )
    assert response.status_code == 403


def test_retrieve_without_api_key_returns_403():
    """Requests without API key should be rejected by API Gateway."""
    response = requests.get(
        f"{BASE_URL}/retrieve/financial",
        params={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        timeout=30
    )
    assert response.status_code == 403


# ─── Collection ───────────────────────────────────────────────

def test_collect_valid_request():
    """Collection should fetch from Yahoo Finance and store in S3."""
    response = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 201
    body = response.json()
    assert "id" in body
    assert TICKER in body["id"]


def test_collect_missing_ticker():
    """Collection should return 400 if ticker is missing."""
    response = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 400


def test_collect_invalid_ticker():
    """Collection should return 400 if ticker has no data."""
    response = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": "INVALIDTICKER999", "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 400


def test_collect_missing_dates():
    """Collection should return 400 if dates are missing."""
    response = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 400


# ─── Retrieval ────────────────────────────────────────────────────

def test_retrieve_after_collect():
    """Retrieval should return ADAGE 3.0 data after collection."""
    # Collect first
    collect_response = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert collect_response.status_code == 201

    # Small buffer between calls
    time.sleep(5)

    # Then retrieve
    retrieve_response = requests.get(
        f"{BASE_URL}/retrieve/financial",
        params={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert retrieve_response.status_code == 200
    body = retrieve_response.json()
    assert body["data_source"] == "Yahoo Finance"
    assert "events" in body
    assert len(body["events"]) > 0

    # Validate ADAGE 3.0 structure
    event = body["events"][0]
    assert "event_time_object" in event
    assert "event_attributes" in event
    attrs = event["event_attributes"]
    assert "open" in attrs
    assert "high" in attrs
    assert "low" in attrs
    assert "close" in attrs
    assert "volume" in attrs


def test_retrieve_sub_range():
    """Retrieval should return filtered events for a sub-range."""
    # Collect wide range
    requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER, "from": "2024-01-01", "to": "2024-01-31"},
        headers=HEADERS,
        timeout=30
    )

    time.sleep(2)

    # Retrieve narrow range
    response = requests.get(
        f"{BASE_URL}/retrieve/financial",
        params={"ticker": TICKER, "from": "2024-01-05", "to": "2024-01-10"},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["events"]) > 0

    for event in body["events"]:
        date = event["event_time_object"]["timestamp"][:10]
        assert "2024-01-05" <= date <= "2024-01-10"


def test_retrieve_not_found():
    """Retrieval should return 404 for uncollected ticker."""
    response = requests.get(
        f"{BASE_URL}/retrieve/financial",
        params={
            "ticker": "ZZZNOTREAL",
            "from": FROM_DATE,
            "to": TO_DATE
        },
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 404


def test_retrieve_invalid_date_format():
    """Retrieval should return 400 for invalid date format."""
    response = requests.get(
        f"{BASE_URL}/retrieve/financial",
        params={"ticker": TICKER, "from": "01-01-2024", "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 400


# ─── Visualisation ───────────────────────────────────────────────

def test_visualise_png():
    """Visualisation should return a base64 PNG after collection."""
    # Collect first
    requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )

    time.sleep(2)

    response = requests.get(
        f"{BASE_URL}/visualise/financial",
        params={
            "ticker": TICKER,
            "from": FROM_DATE,
            "to": TO_DATE,
            "format": "png"
        },
        headers=HEADERS,
        timeout=60  # visualisation takes longer
    )
    assert response.status_code == 200
    if response.headers.get("Content-Type") == "application/json":
        body = response.json()
        assert "image_base64" in body
    else:
        assert isinstance(response.text, str)
        assert len(response.text) > 100
        import base64
        base64.b64decode(response.text)


def test_visualise_json():
    """Visualisation should return chart JSON when format=json."""
    response = requests.get(
        f"{BASE_URL}/visualise/financial",
        params={
            "ticker": TICKER,
            "from": FROM_DATE,
            "to": TO_DATE,
            "format": "json"
        },
        headers=HEADERS,
        timeout=60
    )
    assert response.status_code == 200
    body = response.json()
    assert body["chart_type"] == "time_series"
    assert body["ticker"] == TICKER
    assert "chart_data" in body


def test_visualise_not_found():
    """Visualisation should return 404 for uncollected ticker."""
    response = requests.get(
        f"{BASE_URL}/visualise/financial",
        params={
            "ticker": "ZZZNOTREAL",
            "from": FROM_DATE,
            "to": TO_DATE
        },
        headers=HEADERS,
        timeout=60
    )
    assert response.status_code == 404


def test_visualise_missing_params():
    """Visualisation should return 400 if params are missing."""
    response = requests.get(
        f"{BASE_URL}/visualise/financial",
        params={"ticker": TICKER},
        headers=HEADERS,
        timeout=30
    )
    assert response.status_code == 400


# ─── End to End ────────────────────────────────────────────────────

def test_full_pipeline():
    """
    Full end to end test:
    Collect → Retrieve → Visualise
    """
    # Step 1 - Collect
    collect = requests.post(
        f"{BASE_URL}/collect/financial",
        json={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert collect.status_code == 201

    time.sleep(2)

    # Step 2 - Retrieve
    retrieve = requests.get(
        f"{BASE_URL}/retrieve/financial",
        params={"ticker": TICKER, "from": FROM_DATE, "to": TO_DATE},
        headers=HEADERS,
        timeout=30
    )
    assert retrieve.status_code == 200
    assert len(retrieve.json()["events"]) > 0

    # Step 3 - Visualise
    visualise = requests.get(
        f"{BASE_URL}/visualise/financial",
        params={
            "ticker": TICKER,
            "from": FROM_DATE,
            "to": TO_DATE,
            "format": "png"
        },
        headers=HEADERS,
        timeout=60
    )
    assert visualise.status_code == 200
    assert len(visualise.text) > 0
