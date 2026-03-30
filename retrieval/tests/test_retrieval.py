import json
import importlib.util
import os
import sys
from unittest.mock import patch, MagicMock

retrieval_dir = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.abspath(retrieval_dir))

os.environ["AWS_BUCKET_NAME"] = "bravo-adage-event-store"
os.environ["ENVIRONMENT"] = "dev"

spec = importlib.util.spec_from_file_location(
    "retrieval_handler",
    os.path.join(os.path.dirname(__file__), '..', 'handler.py')
)
module = importlib.util.module_from_spec(spec)
sys.modules["retrieval_handler"] = module
spec.loader.exec_module(module)
handler = module.handler
get_s3_client = module.get_s3_client


# Helper to build a mock S3 client with list and get behaviour
def make_mock_s3(keys_and_data):
    mock_s3 = MagicMock()

    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": key} for key in keys_and_data.keys()]
    }

    def mock_get_object(Bucket, Key):
        data = keys_and_data.get(Key, {})
        return {
            "Body": MagicMock(
                read=lambda: json.dumps(data).encode("utf-8")
            )
        }

    mock_s3.get_object.side_effect = mock_get_object
    return mock_s3


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
    mock_s3 = make_mock_s3({
        "dev/financial/AAPL_2025-01-01_2025-01-31.json": {
            "data_source": "Yahoo Finance",
            "dataset_type": "Financial Records",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2025-01-10T00:00:00Z"
                    },
                    "event_attributes": {
                        "ticker": "AAPL",
                        "open": 180.0,
                        "high": 182.0,
                        "low": 179.0,
                        "close": 181.0,
                        "volume": 1000000
                    }
                }
            ]
        }
    })
    mock_get_s3.return_value = mock_s3

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
    assert body["data_source"] == "Yahoo Finance"
    assert len(body["events"]) == 1


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_auto_collection_triggered_when_s3_empty(mock_get_s3, mock_collect):
    """When S3 has no data, collection is called and data is returned."""
    s3_data = {
        "dev/financial/AAPL_2025-01-01_2025-01-31.json": {
            "data_source": "Yahoo Finance",
            "dataset_type": "Financial Records",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2025-01-10T00:00:00Z"
                    },
                    "event_attributes": {
                        "ticker": "AAPL",
                        "open": 180.0,
                        "high": 182.0,
                        "low": 179.0,
                        "close": 181.0,
                        "volume": 1000000
                    }
                }
            ]
        }
    }

    mock_s3 = MagicMock()
    # First call returns empty, second call (after collection) returns data
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": []},
        {"Contents": [
            {"Key": "dev/financial/AAPL_2025-01-01_2025-01-31.json"}
        ]}
    ]

    def mock_get_object(Bucket, Key):
        data = s3_data.get(Key, {})
        return {
            "Body": MagicMock(
                read=lambda: json.dumps(data).encode("utf-8")
            )
        }

    mock_s3.get_object.side_effect = mock_get_object
    mock_get_s3.return_value = mock_s3
    mock_collect.return_value = True

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
    mock_collect.assert_called_once_with("AAPL", "2025-01-01", "2025-01-31")
    body = json.loads(response["body"])
    assert len(body["events"]) == 1


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_auto_collection_triggered_when_no_overlap(mock_get_s3, mock_collect):
    """When S3 has files but none overlap the range, collection is called."""
    s3_data = {
        "dev/financial/AAPL_2025-01-01_2025-01-31.json": {
            "data_source": "Yahoo Finance",
            "dataset_type": "Financial Records",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2025-01-10T00:00:00Z"
                    },
                    "event_attributes": {
                        "ticker": "AAPL",
                        "open": 180.0,
                        "high": 182.0,
                        "low": 179.0,
                        "close": 181.0,
                        "volume": 1000000
                    }
                }
            ]
        }
    }

    mock_s3 = MagicMock()
    # First call has files but none in the requested range
    # Second call (after collection) returns the new file
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": [
            {"Key": "dev/financial/AAPL_2024-01-01_2024-06-30.json"}
        ]},
        {"Contents": [
            {"Key": "dev/financial/AAPL_2025-01-01_2025-01-31.json"}
        ]}
    ]

    def mock_get_object(Bucket, Key):
        data = s3_data.get(Key, {})
        return {
            "Body": MagicMock(
                read=lambda: json.dumps(data).encode("utf-8")
            )
        }

    mock_s3.get_object.side_effect = mock_get_object
    mock_get_s3.return_value = mock_s3
    mock_collect.return_value = True

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
    mock_collect.assert_called_once_with("AAPL", "2025-01-01", "2025-01-31")


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_404_when_ticker_does_not_exist(mock_get_s3, mock_collect):
    """When collection returns False (bad ticker), return 404."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    mock_get_s3.return_value = mock_s3
    mock_collect.return_value = False

    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "INVALID",
            "from": "2025-01-01",
            "to": "2025-01-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 404
    mock_collect.assert_called_once_with(
        "INVALID", "2025-01-01", "2025-01-31"
    )


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_no_overlapping_files(mock_get_s3, mock_collect):
    mock_s3 = make_mock_s3({
        "dev/financial/AAPL_2024-01-01_2024-06-30.json": {
            "data_source": "Yahoo Finance",
            "events": []
        }
    })
    mock_get_s3.return_value = mock_s3
    mock_collect.return_value = False

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
    mock_collect.assert_called_once_with("AAPL", "2025-01-01", "2025-01-31")


@patch("retrieval_handler.get_s3_client")
def test_ticker_uppercased(mock_get_s3):
    mock_s3 = make_mock_s3({
        "dev/financial/AAPL_2025-01-01_2025-01-31.json": {
            "data_source": "Yahoo Finance",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2025-01-10T00:00:00Z"
                    },
                    "event_attributes": {
                        "ticker": "AAPL",
                        "open": 180.0,
                        "high": 182.0,
                        "low": 179.0,
                        "close": 181.0,
                        "volume": 1000000
                    }
                }
            ]
        }
    })
    mock_get_s3.return_value = mock_s3

    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "aapl",   # lowercase
            "from": "2025-01-01",
            "to": "2025-01-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200


@patch("retrieval_handler.get_s3_client")
def test_multi_file_merge(mock_get_s3):
    mock_s3 = make_mock_s3({
        "dev/financial/AAPL_2024-01-01_2024-06-30.json": {
            "data_source": "Yahoo Finance",
            "dataset_type": "Financial Records",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2024-03-01T00:00:00Z"
                    },
                    "event_attributes": {
                        "ticker": "AAPL",
                        "open": 170.0,
                        "high": 172.0,
                        "low": 169.0,
                        "close": 171.0,
                        "volume": 900000
                    }
                }
            ]
        },
        "dev/financial/AAPL_2024-07-01_2024-12-31.json": {
            "data_source": "Yahoo Finance",
            "dataset_type": "Financial Records",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2024-09-01T00:00:00Z"
                    },
                    "event_attributes": {
                        "ticker": "AAPL",
                        "open": 180.0,
                        "high": 182.0,
                        "low": 179.0,
                        "close": 181.0,
                        "volume": 1000000
                    }
                }
            ]
        }
    })
    mock_get_s3.return_value = mock_s3

    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-03-01",
            "to": "2024-09-30"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body["events"]) == 2
    dates = [
        e["event_time_object"]["timestamp"][:10]
        for e in body["events"]
    ]
    assert dates == sorted(dates)


@patch("retrieval_handler.get_s3_client")
def test_deduplication(mock_get_s3):
    mock_s3 = make_mock_s3({
        "dev/financial/AAPL_2024-01-01_2024-06-30.json": {
            "data_source": "Yahoo Finance",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2024-03-01T00:00:00Z"
                    },
                    "event_attributes": {"ticker": "AAPL", "close": 171.0}
                }
            ]
        },
        "dev/financial/AAPL_2024-03-01_2024-09-30.json": {
            "data_source": "Yahoo Finance",
            "events": [
                {
                    "event_time_object": {
                        "timestamp": "2024-03-01T00:00:00Z"
                    },
                    "event_attributes": {"ticker": "AAPL", "close": 171.0}
                }
            ]
        }
    })
    mock_get_s3.return_value = mock_s3

    event = {
        "path": "/retrieve/financial",
        "httpMethod": "GET",
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-03-01",
            "to": "2024-03-31"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body["events"]) == 1


def test_route_not_found():
    event = {"path": "/invalid", "httpMethod": "GET"}
    response = handler(event, None)
    assert response["statusCode"] == 404
    