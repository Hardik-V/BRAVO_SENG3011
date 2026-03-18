import os
import requests

ENV = os.getenv("ENVIRONMENT", "dev")

STAGE = "/prod" if ENV == "prod" else "/dev"

RETRIEVAL_API = f"https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com{STAGE}/retrieve/financial" # noqa


def get_financial_data(ticker, start, end):
    headers = {"x-api-key": os.environ.get("API_KEY", "")}
    params = {
        "ticker": ticker,
        "from": start,
        "to": end
    }
    response = requests.get(RETRIEVAL_API, params=params, headers=headers)
    response.raise_for_status()
    return response.json()
