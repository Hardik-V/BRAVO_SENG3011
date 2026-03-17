import requests

RETRIEVAL_API = "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/retrieve/financial"    # noqa


def get_financial_data(ticker, start, end, api_key):
    headers = {"x-api-key": api_key}
    params = {
        "ticker": ticker,
        "from": start,
        "to": end
    }
    response = requests.get(RETRIEVAL_API, params=params, headers=headers)
    response.raise_for_status()
    return response.json()
