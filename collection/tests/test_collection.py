import json
import importlib.util
import os
import sys
from unittest.mock import patch, MagicMock


collection_dir = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.abspath(collection_dir))

os.environ["AWS_BUCKET_NAME"] = "bravo-adage-event-store"
os.environ["ENVIRONMENT"] = "dev"

spec = importlib.util.spec_from_file_location(
    "collection_handler",
    os.path.join(os.path.dirname(__file__), '..', 'handler.py')
)
module = importlib.util.module_from_spec(spec)
sys.modules["collection_handler"] = module
spec.loader.exec_module(module)
handler = module.handler


# Test Case 1: Valid request with correct API key and parameters
@patch('collection_handler.fetch_and_standardize_finance')
@patch('boto3.client')
def test_handler_success(mock_boto_client, mock_fetch):
    """Verifies that a valid POST request returns 201 and an S3 ID."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_fetch.return_value = {"events": [{"event_type": "financial_market_reading"}]}

    mock_event = {
        "path": "/collect/financial",
        "httpMethod": "POST",
        "headers": {"X-API-Key": "ecosystem-secret-123"},
        "body": json.dumps({
            "ticker": "AAPL",
            "from": "2024-01-01",
            "to": "2024-01-10"
        })
    }

    response = handler(mock_event, None)
    assert response['statusCode'] == 201
    assert "id" in json.loads(response['body'])


# Test Case 2: Validation check for missing required body fields
@patch('boto3.client')
def test_handler_missing_params(mock_boto_client):
    """Verifies that missing ticker/dates returns 400 Bad Request."""
    mock_event = {
        "path": "/collect/financial",
        "httpMethod": "POST",
        "headers": {"X-API-Key": "ecosystem-secret-123"},
        "body": json.dumps({"from": "2024-01-01", "to": "2024-01-10"})
    }

    response = handler(mock_event, None)
    assert response['statusCode'] == 400
    assert "invalid parameters" in json.loads(response['body'])['message']


# Test Case 3: Validation check for syntax errors in the JSON body
@patch('boto3.client')
def test_handler_malformed_json(mock_boto_client):
    """Verifies that malformed JSON strings return 400 Bad Request."""
    mock_event = {
        "path": "/collect/financial",
        "httpMethod": "POST",
        "headers": {"X-API-Key": "ecosystem-secret-123"},
        "body": "{ticker: 'AAPL'}"  # Invalid JSON syntax
    }

    response = handler(mock_event, None)
    assert response['statusCode'] == 400
    assert "invalid JSON body" in json.loads(response['body'])['message']


# Test Case 4: Logic check for when yfinance finds no market data
@patch('boto3.client')
@patch('collection.fetch_and_standardize_finance')
def test_handler_no_data(mock_boto_client, mock_fetch):
    """Verifies 400 response when the ticker exists but has no data."""
    mock_fetch.return_value = None

    mock_event = {
        "path": "/collect/financial",
        "httpMethod": "POST",
        "headers": {"X-API-Key": "ecosystem-secret-123"},
        "body": json.dumps({
            "ticker": "FAKE",
            "from": "2024-01-01",
            "to": "10"
        })
    }

    response = handler(mock_event, None)
    assert response['statusCode'] == 400
    assert "no data found" in json.loads(response['body'])['message']


# Test Case 5: Basic service availability check
def test_handler_health():
    """Verifies that the /health endpoint returns 200 Healthy."""
    mock_event = {"path": "/collect/health", "httpMethod": "GET"}
    response = handler(mock_event, None)

    assert response['statusCode'] == 200
    assert json.loads(response['body'])['status'] == "healthy"

# Unit tests for collection.py logic (fetch_and_standardize_finance and generate_s3_key)
from collection import fetch_and_standardize_finance, generate_s3_key # noqa
# helpers


def make_mock_df(rows=2):
    """Return a mock DataFrame-like object for yf.download."""
    mock_df = MagicMock()
    mock_df.empty = False
    mock_df.iterrows.return_value = iter([
        (MagicMock(isoformat=lambda: "2024-01-02T00:00:00"),
         {"Open": 185.0, "High": 187.0, "Low": 184.0, "Close": 186.0, "Volume": 50000000}),
        (MagicMock(isoformat=lambda: "2024-01-03T00:00:00"),
         {"Open": 186.5, "High": 188.0, "Low": 185.5, "Close": 187.0, "Volume": 52000000}),
    ][:rows])
    return mock_df

# fetch_and_standardize_finance


@patch('collection.yf.download')
def test_fetch_returns_correct_structure(mock_download):
    """Returned dict has all required top-level ADAGE keys."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    assert result is not None
    assert result["data_source"] == "Yahoo Finance"
    assert result["dataset_type"] == "Financial Records"
    assert "events" in result
    assert "dataset_time_object" in result


@patch('collection.yf.download')
def test_fetch_event_count_matches_rows(mock_download):
    """One event is created per row in the DataFrame."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    assert len(result["events"]) == 2


@patch('collection.yf.download')
def test_fetch_event_attributes_correct(mock_download):
    """Each event has the right OHLCV values and types."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    attrs = result["events"][0]["event_attributes"]
    assert attrs["ticker"] == "AAPL"
    assert isinstance(attrs["open"], float)
    assert isinstance(attrs["high"], float)
    assert isinstance(attrs["low"], float)
    assert isinstance(attrs["close"], float)
    assert isinstance(attrs["volume"], int)
    assert attrs["open"] == 185.0
    assert attrs["volume"] == 50000000


@patch('collection.yf.download')
def test_fetch_event_time_object_format(mock_download):
    """Timestamp ends with Z and duration is 86400 seconds (1 day)."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    time_obj = result["events"][0]["event_time_object"]
    assert time_obj["timestamp"].endswith("Z")
    assert time_obj["duration"] == 86400
    assert time_obj["unit"] == "seconds"
    assert time_obj["timezone"] == "UTC"


@patch('collection.yf.download')
def test_fetch_dataset_id_is_pending(mock_download):
    """dataset_id starts as PENDING before S3 assigns a real ID."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    assert result["dataset_id"] == "PENDING"


# generate_s3_key

def test_s3_key_format():
    """Key follows the expected pattern: stage/financial/TICKER_from_to.json"""
    key = generate_s3_key("AAPL", "2024-01-01", "2024-01-10")
    assert key == "dev/financial/AAPL_2024-01-01_2024-01-10.json"


def test_s3_key_ticker_in_filename():
    """Ticker symbol appears in the filename portion of the key."""
    key = generate_s3_key("MSFT", "2024-01-01", "2024-01-31")
    assert "MSFT" in key


def test_s3_key_dates_in_filename():
    """Both date bounds appear in the key."""
    key = generate_s3_key("AAPL", "2024-03-01", "2024-03-31")
    assert "2024-03-01" in key
    assert "2024-03-31" in key


def test_s3_key_ends_with_json():
    """Key always ends with .json extension."""
    key = generate_s3_key("AAPL", "2024-01-01", "2024-01-10")
    assert key.endswith(".json")
