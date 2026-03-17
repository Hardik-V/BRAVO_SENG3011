import yfinance as yf  # type: ignore
from datetime import datetime, timezone
import os

# Get the stage (dev or prod) from environment variables
STAGE = os.getenv("STAGE", "dev")


# Logic for fetching and structuring the data
def fetch_and_standardize_finance(ticker: str, date_from: str, date_to: str):
    """
    Fetches data from Yahoo Finance and maps it to the
    Ecosystem Data Model (ADAGE 3.0 style).
    """
    # Fetch specified data using yfinance
    df = yf.download(ticker, start=date_from, end=date_to)

    if df.empty:
        return None

    # Get latest record for the 'Event'
    latest = df.iloc[-1]

    # Helper to handle yfinance Series/Item conversion within line limits
    def get_val(col):
        val = latest[col]
        return val.item() if hasattr(val, 'item') else val

    # Construct the ADAGE 3.0 model
    standardized_data = {
        "data_source": "Yahoo Finance",
        "dataset_type": "Financial Records",
        "dataset_id": "PENDING",  # Set by the caller after S3 upload
        "dataset_time_object": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timezone": "UTC"
        },
        "event": {
            "event_time_object": {
                "timestamp": latest.name.isoformat() + "Z",
                "duration": 86400,
                "unit": "seconds",
                "timezone": "UTC"
            },
            "event_type": "financial_market_reading",
            "event_attributes": {
                "ticker": ticker,
                "open": float(get_val('Open')),
                "high": float(get_val('High')),
                "low": float(get_val('Low')),
                "close": float(get_val('Close')),
                "volume": int(get_val('Volume'))
            }
        }
    }
    return standardized_data


def generate_s3_key(ticker: str, date_from: str, date_to: str):
    """Generates a descriptive S3 path using ticker and date range."""
    return f"{STAGE}/financial/{ticker}_{date_from}_{date_to}.json"
