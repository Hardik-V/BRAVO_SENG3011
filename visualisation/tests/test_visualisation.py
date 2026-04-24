import sys
import os
import json
import importlib.util
from unittest.mock import patch, MagicMock


visualisation_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))    # noqa
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))    # noqa
sys.path.insert(0, visualisation_dir)
sys.path.insert(0, repo_root)

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
                "event_time_object": {"timestamp": "2024-01-09T00:00:00Z"},
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

    m = MagicMock()
    m.__enter__ = MagicMock(return_value=MagicMock(read=MagicMock(return_value=b"fakepngbytes")))    # noqa
    m.__exit__ = MagicMock(return_value=False)

    with patch("builtins.open", return_value=m):
        event = {
            "path": "/visualise/financial",
            "httpMethod": "GET",
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
        "queryStringParameters": {
            "ticker": "AAPL",
            "from": "2024-01-01",
            "to": "2024-01-10"
        }
    }
    response = handler(event, None)
    print("RESPONSE:", response)
    assert response["statusCode"] == 404


@patch("visualisation_handler.get_financial_data")
def test_visualise_no_events(mock_get_financial_data):
    mock_get_financial_data.return_value = {"data_source": "Yahoo Finance"}

    event = {
        "path": "/visualise/financial",
        "httpMethod": "GET",
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

# Unit tests for get_financial_data and create_graph.

from retrieval_service import get_financial_data # noqa
from graph_service import create_graph # noqa

# helpers


def make_adage_data(dates_and_prices=None):
    """Build minimal ADAGE-format data with one event per entry."""
    if dates_and_prices is None:
        dates_and_prices = [("2024-01-09", 180.0, 185.0, 179.0, 182.0)]
    events = []
    for date, o, h, l, c in dates_and_prices:
        events.append({
            "event_time_object": {"timestamp": f"{date}T00:00:00Z"},
            "event_attributes": {
                "ticker": "AAPL",
                "open": o, "high": h, "low": l, "close": c,
                "volume": 1000000
            }
        })
    return {"data_source": "Yahoo Finance", "events": events}


# get_financial_data

@patch("retrieval_service.requests.get")
def test_get_financial_data_success(mock_get):
    """Returns parsed JSON on a 200 response."""
    mock_response = MagicMock()
    mock_response.json.return_value = make_adage_data()
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = get_financial_data("AAPL", "2024-01-01", "2024-01-10")
    assert result["data_source"] == "Yahoo Finance"
    assert len(result["events"]) == 1


@patch("retrieval_service.requests.get")
def test_get_financial_data_passes_correct_params(mock_get):
    """Ticker, from, and to are forwarded as query params."""
    mock_response = MagicMock()
    mock_response.json.return_value = make_adage_data()
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    get_financial_data("TSLA", "2024-03-01", "2024-03-31")

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["ticker"] == "TSLA"
    assert kwargs["params"]["from"] == "2024-03-01"
    assert kwargs["params"]["to"] == "2024-03-31"


@patch("retrieval_service.requests.get")
def test_get_financial_data_raises_on_404(mock_get):
    """Raises HTTPError when retrieval service returns 404."""
    from requests.exceptions import HTTPError
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError(
        response=MagicMock(status_code=404)
    )
    mock_get.return_value = mock_response

    try:
        get_financial_data("FAKE", "2024-01-01", "2024-01-10")
        assert False, "Expected HTTPError"
    except HTTPError:
        pass


@patch("retrieval_service.requests.get")
def test_get_financial_data_raises_on_500(mock_get):
    """Raises HTTPError when retrieval service returns 500."""
    from requests.exceptions import HTTPError
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError(
        response=MagicMock(status_code=500)
    )
    mock_get.return_value = mock_response

    try:
        get_financial_data("AAPL", "2024-01-01", "2024-01-10")
        assert False, "Expected HTTPError"
    except HTTPError:
        pass


# create_graph

@patch("graph_service.plt")
def test_create_graph_returns_filepath(mock_plt):
    """Returns the expected tmp file path on valid data."""
    mock_plt.subplots.return_value = (MagicMock(), MagicMock())
    result = create_graph(make_adage_data())
    assert result == "/tmp/financial_graph.png"


@patch("graph_service.plt")
def test_create_graph_returns_none_on_empty_events(mock_plt):
    """Returns None when events list is empty."""
    result = create_graph({"data_source": "Yahoo Finance", "events": []})
    assert result is None
    mock_plt.subplots.assert_not_called()


@patch("graph_service.plt")
def test_create_graph_returns_none_on_missing_events_key(mock_plt):
    """Returns None when events key is absent entirely."""
    result = create_graph({"data_source": "Yahoo Finance"})
    assert result is None


@patch("graph_service.plt")
def test_create_graph_skips_invalid_timestamps(mock_plt):
    """Events with unparseable timestamps are skipped, not crashed on."""
    mock_plt.subplots.return_value = (MagicMock(), MagicMock())
    data = {
        "events": [
            {
                "event_time_object": {"timestamp": "not-a-date"},
                "event_attributes": {
                    "ticker": "AAPL",
                    "open": 180.0, "high": 185.0, "low": 179.0, "close": 182.0
                }
            },
            {
                "event_time_object": {"timestamp": "2024-01-09T00:00:00Z"},
                "event_attributes": {
                    "ticker": "AAPL",
                    "open": 180.0, "high": 185.0, "low": 179.0, "close": 182.0
                }
            }
        ]
    }
    result = create_graph(data)
    assert result == "/tmp/financial_graph.png"


@patch("graph_service.plt")
def test_create_graph_returns_none_when_all_timestamps_invalid(mock_plt):
    """Returns None when every event has a bad timestamp (no valid dates)."""
    data = {
        "events": [
            {
                "event_time_object": {"timestamp": "bad"},
                "event_attributes": {
                    "ticker": "AAPL",
                    "open": 180.0, "high": 185.0, "low": 179.0, "close": 182.0
                }
            }
        ]
    }
    result = create_graph(data)
    assert result is None


@patch("graph_service.plt")
def test_create_graph_bullish_and_bearish_both_handled(mock_plt):
    """Doesn't crash with a mix of bullish (c>=o) and bearish (c<o) candles."""
    mock_ax = MagicMock()
    mock_plt.subplots.return_value = (MagicMock(), mock_ax)
    data = make_adage_data([
        ("2024-01-08", 180.0, 185.0, 179.0, 183.0),  # bullish: close > open
        ("2024-01-09", 183.0, 184.0, 178.0, 179.0),  # bearish: close < open
    ])
    result = create_graph(data)
    assert result == "/tmp/financial_graph.png"
    assert mock_ax.bar.call_count == 2


@patch("graph_service.plt")
def test_create_graph_calls_savefig(mock_plt):
    """plt.savefig is called with the correct output path."""
    mock_plt.subplots.return_value = (MagicMock(), MagicMock())
    create_graph(make_adage_data())
    mock_plt.savefig.assert_called_once_with("/tmp/financial_graph.png", dpi=150)


@patch("graph_service.plt")
def test_create_graph_closes_figure(mock_plt):
    """plt.close() is always called to free memory."""
    mock_plt.subplots.return_value = (MagicMock(), MagicMock())
    create_graph(make_adage_data())
    mock_plt.close.assert_called_once()
