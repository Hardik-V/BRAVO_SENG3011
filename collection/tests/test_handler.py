import json
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from handler import handler

@patch('boto3.client')
def test_handler_success(mock_boto_client):
    """Mocks AWS and tests a successful 201 response."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    
    mock_event = {
        "path": "/collect/financial",
        "httpMethod": "POST",
        "headers": {"X-API-Key": "ecosystem-secret-123"},
        "body": json.dumps({"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"})
    }

    response = handler(mock_event, None)
    
    assert response['statusCode'] == 201
    
    body = json.loads(response['body'])
    assert "id" in body

@patch('boto3.client')
def test_handler_health(mock_boto_client):
    """Tests the health check route."""
    mock_event = {"path": "/collect/health", "httpMethod": "GET"}
    response = handler(mock_event, None)
    
    assert response['statusCode'] == 200
    assert json.loads(response['body'])['status'] == "healthy"