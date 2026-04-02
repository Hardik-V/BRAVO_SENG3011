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
