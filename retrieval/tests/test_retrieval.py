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


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_successful_retrieval(mock_get_s3, mock_collect):
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
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": []},
        {"Contents": [
            {"Key": "dev/financial/AAPL_2025-01-01_2025-01-31.json"}
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
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": [
            {"Key": "dev/financial/AAPL_2024-01-01_2024-06-30.json"}
        ]},
        {"Contents": [
            {"Key": "dev/financial/AAPL_2025-01-01_2025-01-31.json"}
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
    mock_collect.assert_called_with("AAPL", "2025-01-01", "2025-01-31")


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


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_collection_triggered_when_data_incomplete(mock_get_s3, mock_collect):
    """When S3 has partial data, collection is called for full range."""
    partial_data = {
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
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": [
            {"Key": "dev/financial/AAPL_2025-01-01_2025-01-31.json"}
        ]},
        {"Contents": [
            {"Key": "dev/financial/AAPL_2025-01-01_2025-01-31.json"}
        ]}
    ]

    def mock_get_object(Bucket, Key):
        data = partial_data.get(Key, {})
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
def test_no_collection_when_data_complete(mock_get_s3, mock_collect):
    """When S3 has data for a single day range, collection is not called."""
    mock_s3 = make_mock_s3({
        "dev/financial/AAPL_2025-01-10_2025-01-10.json": {
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
            "from": "2025-01-10",
            "to": "2025-01-10"
        }
    }
    response = handler(event, None)
    assert response["statusCode"] == 200
    mock_collect.assert_not_called()


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_ticker_uppercased(mock_get_s3, mock_collect):
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
    mock_collect.return_value = True

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


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_multi_file_merge(mock_get_s3, mock_collect):
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
    mock_collect.return_value = True

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


@patch("retrieval_handler.call_collection_service")
@patch("retrieval_handler.get_s3_client")
def test_deduplication(mock_get_s3, mock_collect):
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
    mock_collect.return_value = True

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

# Unit testing retrieval logic (is_valid_date, get_expected_dates, has_complete_data, fetch_from_s3)
from retrieval_handler import is_valid_date, get_expected_dates, has_complete_data, fetch_from_s3 # noqa


# is_valid_date

def test_is_valid_date_correct_format():
    """Standard YYYY-MM-DD returns True."""
    assert is_valid_date("2024-03-01") is True


def test_is_valid_date_wrong_format():
    """DD-MM-YYYY format returns False."""
    assert is_valid_date("01-03-2024") is False


def test_is_valid_date_partial():
    """Partial date string returns False."""
    assert is_valid_date("2024-03") is False


def test_is_valid_date_empty_string():
    """Empty string returns False."""
    assert is_valid_date("") is False


def test_is_valid_date_nonsense():
    """Random string returns False."""
    assert is_valid_date("not-a-date") is False


# get_expected_dates
def test_get_expected_dates_excludes_weekends():
    """Weekends are not included in expected trading dates."""
    # 2024-03-02 is a Saturday, 2024-03-03 is a Sunday
    dates = get_expected_dates("2024-03-01", "2024-03-04")
    assert "2024-03-02" not in dates
    assert "2024-03-03" not in dates


def test_get_expected_dates_includes_weekdays():
    """Weekdays are included in expected trading dates."""
    dates = get_expected_dates("2024-03-01", "2024-03-04")
    assert "2024-03-01" in dates  # Friday
    assert "2024-03-04" in dates  # Monday


def test_get_expected_dates_single_weekday():
    """A single weekday range returns exactly one date."""
    dates = get_expected_dates("2024-03-01", "2024-03-01")
    assert dates == {"2024-03-01"}


def test_get_expected_dates_single_weekend_day():
    """A range that is only a weekend returns an empty set."""
    dates = get_expected_dates("2024-03-02", "2024-03-03")
    assert dates == set()


def test_get_expected_dates_full_week():
    """A Mon-Fri week returns exactly 5 dates."""
    dates = get_expected_dates("2024-03-04", "2024-03-08")
    assert len(dates) == 5


# has_complete_data

def make_events(dates):
    """Helper — build minimal event list from a list of YYYY-MM-DD strings."""
    return [{"event_time_object": {"timestamp": d + "T00:00:00Z"}} for d in dates]


def test_has_complete_data_full_coverage():
    """Returns True when all expected trading days are present."""
    # 2024-03-01 is a Friday — only one trading day in range
    events = make_events(["2024-03-01"])
    assert has_complete_data(events, "2024-03-01", "2024-03-01") is True


def test_has_complete_data_missing_days():
    """Returns False when some trading days are missing."""
    # Mon-Fri week, only one day provided
    events = make_events(["2024-03-04"])
    assert has_complete_data(events, "2024-03-04", "2024-03-08") is False


def test_has_complete_data_weekend_range_no_events():
    """Weekend-only range has no expected dates, so empty events = complete."""
    events = make_events([])
    assert has_complete_data(events, "2024-03-02", "2024-03-03") is True


def test_has_complete_data_extra_events_still_passes():
    """Extra events outside the range don't break completeness check."""
    events = make_events(["2024-03-01", "2024-02-15"])  # 2024-02-15 is outside range
    assert has_complete_data(events, "2024-03-01", "2024-03-01") is True


# fetch_from_s3

def make_s3_for_fetch(keys):
    """Helper — mock s3 client that returns a fixed list of keys."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": k} for k in keys]
    }
    return mock_s3


def test_fetch_from_s3_returns_none_when_empty():
    """Returns (None, None) when S3 has no objects at all."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    keys, objects = fetch_from_s3(mock_s3, "AAPL", "2024-03-01", "2024-03-31")
    assert keys is None
    assert objects is None


def test_fetch_from_s3_overlapping_file_returned():
    """A file whose range overlaps the query is included in overlapping_keys."""
    mock_s3 = make_s3_for_fetch(["dev/financial/AAPL_2024-01-01_2024-06-30.json"])
    keys, objects = fetch_from_s3(mock_s3, "AAPL", "2024-03-01", "2024-03-31")
    assert "dev/financial/AAPL_2024-01-01_2024-06-30.json" in keys


def test_fetch_from_s3_non_overlapping_file_excluded():
    """A file whose range is entirely before the query is not in overlapping_keys."""
    mock_s3 = make_s3_for_fetch(["dev/financial/AAPL_2023-01-01_2023-06-30.json"])
    keys, objects = fetch_from_s3(mock_s3, "AAPL", "2024-03-01", "2024-03-31")
    assert keys == []


def test_fetch_from_s3_multiple_files_only_overlap_returned():
    """Only overlapping files are returned when multiple exist."""
    mock_s3 = make_s3_for_fetch([
        "dev/financial/AAPL_2023-01-01_2023-06-30.json",  # before range
        "dev/financial/AAPL_2024-01-01_2024-06-30.json",  # overlaps
    ])
    keys, objects = fetch_from_s3(mock_s3, "AAPL", "2024-03-01", "2024-03-31")
    assert len(keys) == 1
    assert "dev/financial/AAPL_2024-01-01_2024-06-30.json" in keys


def test_fetch_from_s3_exact_date_match_included():
    """A file whose range exactly matches the query is included."""
    mock_s3 = make_s3_for_fetch(["dev/financial/AAPL_2024-03-01_2024-03-31.json"])
    keys, objects = fetch_from_s3(mock_s3, "AAPL", "2024-03-01", "2024-03-31")
    assert "dev/financial/AAPL_2024-03-01_2024-03-31.json" in keys
