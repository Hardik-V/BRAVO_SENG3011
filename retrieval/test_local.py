from handler import handler


tests = [
    {
        "name": "health check",
        "event": {
            "path": "/retrieve/health",
            "httpMethod": "GET"
        }
    },
    {
        "name": "financial request - expected 404 because file is not in S3 yet",
        "event": {
            "path": "/retrieve/financial",
            "httpMethod": "GET",
            "queryStringParameters": {
                "ticker": "AAPL",
                "from": "2025-01-01",
                "to": "2025-01-31"
            }
        }
    },
    {
        "name": "missing parameter - expected 400",
        "event": {
            "path": "/retrieve/financial",
            "httpMethod": "GET",
            "queryStringParameters": {
                "ticker": "AAPL",
                "from": "2025-01-01"
            }
        }
    }
]


for test in tests:
    print(f"\n--- {test['name']} ---")
    result = handler(test["event"], None)
    print(result)
