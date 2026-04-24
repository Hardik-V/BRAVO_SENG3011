# tests for collection/collection.py — function-level unit tests
import pytest
import os
import sys
from unittest.mock import patch, MagicMock
import pandas as pd

# path setup
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["ENVIRONMENT"] = "dev"

from collection import fetch_and_standardize_finance, generate_s3_key

def make_mock_df():
    """Build a minimal fake yfinance DataFrame with the shape yf.download returns."""
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    df = pd.DataFrame({
        "Open":   [185.0, 186.5],
        "High":   [187.0, 188.0],
        "Low":    [184.0, 185.5],
        "Close":  [186.0, 187.0],
        "Volume": [50000000, 52000000],
    }, index=dates)
    return df


# fetch_and_standardize_finance

@patch('collection.yf.download')
def test_fetch_returns_correct_structure(mock_download):
    """Returned dict has all required top-level ADAGE keys."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    assert result is not None
    assert result["data_source"] == "Yahoo Finance"
    assert result["dataset_type"] == "Financial Records"
    assert "events" in result
    assert "dataset_time_object" in result


@patch('collection.yf.download')
def test_fetch_event_count_matches_rows(mock_download):
    """One event is created per row in the DataFrame."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    assert len(result["events"]) == 2


@patch('collection.yf.download')
def test_fetch_event_attributes_correct(mock_download):
    """Each event has the right OHLCV values and types."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    ev = result["events"][0]
    attrs = ev["event_attributes"]

    assert attrs["ticker"] == "AAPL"
    assert isinstance(attrs["open"], float)
    assert isinstance(attrs["high"], float)
    assert isinstance(attrs["low"], float)
    assert isinstance(attrs["close"], float)
    assert isinstance(attrs["volume"], int)
    assert attrs["open"] == 185.0
    assert attrs["volume"] == 50000000


@patch('collection.yf.download')
def test_fetch_event_time_object_format(mock_download):
    """Timestamp ends with Z and duration is 86400 seconds (1 day)."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    time_obj = result["events"][0]["event_time_object"]
    assert time_obj["timestamp"].endswith("Z")
    assert time_obj["duration"] == 86400
    assert time_obj["unit"] == "seconds"
    assert time_obj["timezone"] == "UTC"


@patch('collection.yf.download')
def test_fetch_events_sorted_chronologically(mock_download):
    """Events come back in ascending date order."""
    # reverse the dates to make sure sorting actually does work
    df = make_mock_df().iloc[::-1]
    mock_download.return_value = df
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    dates = [e["event_time_object"]["timestamp"] for e in result["events"]]
    assert dates == sorted(dates)


@patch('collection.yf.download')
def test_fetch_returns_none_on_empty_df(mock_download):
    """Returns None when yfinance finds no data (e.g. bad ticker or weekend range)."""
    mock_download.return_value = pd.DataFrame()
    result = fetch_and_standardize_finance("FAKE", "2024-01-01", "2024-01-02")

    assert result is None


@patch('collection.yf.download')
def test_fetch_dataset_id_is_pending(mock_download):
    """dataset_id starts as PENDING before S3 assigns a real ID."""
    mock_download.return_value = make_mock_df()
    result = fetch_and_standardize_finance("AAPL", "2024-01-01", "2024-01-10")

    assert result["dataset_id"] == "PENDING"


# generate_s3_key

def test_s3_key_format():
    """Key follows the expected pattern: stage/financial/TICKER_from_to.json"""
    key = generate_s3_key("AAPL", "2024-01-01", "2024-01-10")
    assert key == "dev/financial/AAPL_2024-01-01_2024-01-10.json"


def test_s3_key_uses_environment_stage():
    """Key prefix changes when ENVIRONMENT variable changes."""
    os.environ["ENVIRONMENT"] = "prod"
    # re-import so the module picks up the new env var
    import importlib
    import collection
    importlib.reload(collection)
    from collection import generate_s3_key as gen

    key = gen("TSLA", "2024-06-01", "2024-06-30")
    assert key.startswith("prod/")

    # reset
    os.environ["ENVIRONMENT"] = "dev"
    importlib.reload(collection)


def test_s3_key_ticker_in_filename():
    """Ticker symbol appears in the filename portion of the key."""
    key = generate_s3_key("MSFT", "2024-01-01", "2024-01-31")
    assert "MSFT" in key


def test_s3_key_dates_in_filename():
    """Both date bounds appear in the key."""
    key = generate_s3_key("AAPL", "2024-03-01", "2024-03-31")
    assert "2024-03-01" in key
    assert "2024-03-31" in key


def test_s3_key_ends_with_json():
    """Key always ends with .json extension."""
    key = generate_s3_key("AAPL", "2024-01-01", "2024-01-10")
    assert key.endswith(".json")