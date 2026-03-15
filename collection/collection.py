import json
import yfinance as yf
from datetime import datetime
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
   
    # Construct the ADAGE 3.0 model
    standardized_data = {
        "data_source": "Yahoo Finance",
        "dataset_type": "Financial Records",
        "dataset_id": "PENDING", # Set by the caller after S3 upload
        "dataset_time_object": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
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
                "open": float(latest['Open'].item() if hasattr(latest['Open'], 'item') else latest['Open']),
                "high": float(latest['High'].item() if hasattr(latest['High'], 'item') else latest['High']),
                "low": float(latest['Low'].item() if hasattr(latest['Low'], 'item') else latest['Low']),
                "close": float(latest['Close'].item() if hasattr(latest['Close'], 'item') else latest['Close']),
                "volume": int(latest['Volume'].item() if hasattr(latest['Volume'], 'item') else latest['Volume'])
            }
        }
    }
    return standardized_data

def generate_s3_key(ticker: str, date_from: str, date_to: str):
    """Generates a descriptive S3 path using ticker and date range."""
    # Path format: dev/financial/AAPL/AAPL_2024-01-01_2024-01-10.json
    filename = f"{date_from}_{date_to}.json"
    return f"{STAGE}/financial/{ticker}/{filename}"