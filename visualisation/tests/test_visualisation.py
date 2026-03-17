import sys
import os
import base64
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from visualisation.handler import handler

def test_visualise_health():
    event = {
        "path": "/visualise/health",
        "httpMethod": "GET"
    }

    response = handler(event, None)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "healthy"


def test_visualise_wrong_api_key():
    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
        "headers": {"x-api-key": "wrong-key"},
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-01-01",
            "to": "2024-01-10"
        }
    }

    response = handler(event, None)

    assert response["statusCode"] == 401


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