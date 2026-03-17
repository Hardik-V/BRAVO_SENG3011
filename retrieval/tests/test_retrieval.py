import json
import importlib.util
import os
import sys
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

retrieval_dir = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.abspath(retrieval_dir))

os.environ["AWS_BUCKET_NAME"] = "bravo-adage-event-store"
os.environ["ENVIRONMENT"] = "dev"

spec = importlib.util.spec_from_file_location(
    "retrieval_handler",
    os.path.join(os.path.dirname(__file__), '..', 'handler.py')
)
module = importlib.util.module_from_spec(spec)
sys.modules["retrieval_handler"] = module  # ← register it
spec.loader.exec_module(module)
handler = module.handler
get_s3_client = module.get_s3_client


def test_health_check():
    event = {"path": "/retrieve/health", "httpMethod": "GET"}
    response = handler(event, None)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "healthy"
    assert body["service"] == "bravo-retrieval"


def test_missing_parameters():
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2025-01-01"
            # missing "to"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


def test_invalid_date_format():
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "01-01-2025",
            "to": "2025-01-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


def test_from_date_after_to_date():
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2025-02-01",
            "to": "2025-01-01"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


def test_no_query_parameters():
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": None
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


@patch("retrieval_handler.get_s3_client")
def test_successful_retrieval(mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({
            "data_source": "yahoo_finance",
            "dataset_type": "Daily stock data",
            "events": []
        }).encode("utf-8"))
    }
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2025-01-01",
            "to": "2025-01-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["data_source"] == "yahoo_finance"


@patch("retrieval_handler.get_s3_client")
def test_data_not_found(mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
        "GetObject"
    )
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2025-01-01",
            "to": "2025-01-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 404


@patch("retrieval_handler.get_s3_client")
def test_ticker_uppercased(mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({
            "data_source": "yahoo_finance",
            "events": []
        }).encode("utf-8"))
    }
    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "aapl",  # lowercase
            "from": "2025-01-01",
            "to": "2025-01-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200


def test_route_not_found():
    event = {"path": "/invalid", "httpMethod": "GET"}
    response = handler(event, None)
    assert response["statusCode"] == 404
