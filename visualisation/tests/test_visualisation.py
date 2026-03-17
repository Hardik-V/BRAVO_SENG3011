import sys
import os
import json
import importlib.util
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

visualisation_dir = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.abspath(visualisation_dir))

os.environ["AWS_BUCKET_NAME"] = "bravo-adage-event-store"
os.environ["ENVIRONMENT"] = "dev"

spec = importlib.util.spec_from_file_location(
    "visualisation_handler",
    os.path.join(os.path.dirname(__file__), '..', 'handler.py')
)
module = importlib.util.module_from_spec(spec)
sys.modules["visualisation_handler"] = module
spec.loader.exec_module(module)
handler = module.handler


def test_visualise_health():
    event = {"path": "/visualise/health", "httpMethod": "GET"}
    response = handler(event, None)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "healthy"
    assert body["service"] == "bravo-visualisation"
    assert body["version"] == "1.0.0"


def test_visualise_missing_params():
    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "ecosystem-secret-123"},
        "queryStringParameters": {
            "ticker": "AAPL"
            # missing from and to
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


def test_visualise_no_query_params():
    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "ecosystem-secret-123"},
        "queryStringParameters": None
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


def test_visualise_invalid_date_format():
    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "ecosystem-secret-123"},
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "01-01-2024",
            "to": "2024-01-10"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


def test_visualise_from_after_to():
    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "ecosystem-secret-123"},
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-02-01",
            "to": "2024-01-01"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 400


@patch("visualisation_handler.get_s3_client")
@patch("visualisation_handler.create_graph")
def test_visualise_success_png(mock_create_graph, mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({
            "data_source": "Yahoo Finance",
            "event": {
                "event_attributes": {
                    "ticker": "AAPL",
                    "open": 182.0,
                    "high": 183.0,
                    "low": 180.0,
                    "close": 182.5,
                    "volume": 42000000
                }
            }
        }).encode("utf-8"))
    }
    mock_create_graph.return_value = b"fakepngbytes"

    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "ecosystem-secret-123"},
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-01-01",
            "to": "2024-01-10",
            "format": "png"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200


@patch("visualisation_handler.get_s3_client")
def test_visualise_data_not_found(mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
        "GetObject"
    )

    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "ecosystem-secret-123"},
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-01-01",
            "to": "2024-01-10"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 404


def test_visualise_route_not_found():
    event = {"path": "/invalid", "httpMethod": "GET"}
    response = handler(event, None)
    assert response["statusCode"] == 404
