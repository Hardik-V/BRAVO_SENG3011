import json
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory (collection) to the path so it can find handler.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from handler import handler

@patch('boto3.client')
def run_test_success(mock_boto_client):
    """Mocks AWS and tests a successful 201 response."""
    print("Running Test: Success Case...")
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    
    mock_event = {
        "path": "/collect/financial",
        "httpMethod": "POST",
        "headers": {"X-API-Key": "ecosystem-secret-123"},
        "body": json.dumps({"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"})
    }

    response = handler(mock_event, None)
    
    if response['statusCode'] == 201:
        print("Success: Status 201 received")
    else:
        print(f"Failed: Status {response['statusCode']} received.")
        print(f"Body: {response['body']}")

@patch('boto3.client')
def run_test_health(mock_boto_client):
    """Tests the health check route."""
    print("Running Test: Health Check...")
    mock_event = {"path": "/collect/health", "httpMethod": "GET"}
    response = handler(mock_event, None)
    if response['statusCode'] == 200:
        print("Success: Health check passed.")
    else:
        print(f"Failed: Health check returned {response['statusCode']}")

if __name__ == "__main__":
    print("--- Starting Manual Unit Tests ---")
    try:
        run_test_success()
        run_test_health()
        print("--- All Tests Completed ---")
    except Exception as e:
        print(f"--- Test Suite Crashed: {e} ---")