import sys
import os
import json
import importlib.util
from unittest.mock import patch, MagicMock

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


@patch("visualisation_handler.get_financial_data")
@patch("visualisation_handler.create_graph")
def test_visualise_success(mock_create_graph, mock_get_financial_data):
    mock_get_financial_data.return_value = {
        "data_source": "Yahoo Finance",
        "events": [
            {
                "event_attributes": {
                    "ticker": "AAPL",
                    "open": 182.0,
                    "high": 183.0,
                    "low": 180.0,
                    "close": 182.5,
                    "volume": 42000000
                }
            }
        ]
    }
    mock_create_graph.return_value = "/tmp/chart.png"

    # mock file open
    with patch("builtins.open", patch("builtins.open", 
               return_value=MagicMock(
                   __enter__=lambda s, *a: s,
                   __exit__=MagicMock(return_value=False),
                   read=lambda: b"fakepngbytes"
               ))):
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
        assert response["statusCode"] == 200


@patch("visualisation_handler.get_financial_data")
def test_visualise_no_data(mock_get_financial_data):
    mock_get_financial_data.return_value = None

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


@patch("visualisation_handler.get_financial_data")
def test_visualise_no_events(mock_get_financial_data):
    mock_get_financial_data.return_value = {"data_source": "Yahoo Finance"}

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
