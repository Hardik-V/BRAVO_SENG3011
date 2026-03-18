import yfinance as yf  # type: ignore
from datetime import datetime, timezone
import os

# Get the stage (dev or prod) from environment variables
STAGE = os.getenv("STAGE", "dev")


# Logic for fetching and structuring the data
def fetch_and_standardize_finance(ticker: str, date_from: str, date_to: str):
    df = yf.download(ticker, start=date_from, end=date_to)

    if df.empty:
        return None

    events = []
    for date, row in df.iterrows():
        def get_val(col):
            val = row[col]
            return val.item() if hasattr(val, 'item') else val

        events.append({
            "event_time_object": {
                "timestamp": date.isoformat() + "Z",
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
        })

    standardized_data = {
        "data_source": "Yahoo Finance",
        "dataset_type": "Financial Records",
        "dataset_id": "PENDING",
        "dataset_time_object": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timezone": "UTC"
        },
        "events": events
    }
    return standardized_data


def generate_s3_key(ticker: str, date_from: str, date_to: str):
    """Generates a descriptive S3 path using ticker and date range."""
    return f"{STAGE}/financial/{ticker}_{date_from}_{date_to}.json"
