# BRAVO - Financial Event Data API

A set of microservices for collecting, retrieving and visualising financial market event data, normalised into the ADAGE 3.0 Standard Event Model.

## Services

- **Collection** - Fetches financial market data from Yahoo Finance and stores it in AWS S3
- **Retrieval** - Fetches stored financial event data from AWS S3
- **Visualisation** - Generates price charts from stored financial event data

## Prerequisites

- Python 3.11+
- AWS CLI configured with credentials

## API URLs

| Environment | URL |
|---|---|
| Dev | `https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev` |
| Prod | `https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/prod` |

## Authentication

All endpoints (except health checks) require an API key passed in the request header:
```
x-api-key: your-api-key
```

## Endpoints

### Health Checks (no API key required)
```bash
curl https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/collect/health
curl https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/retrieve/health
curl https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/visualise/health
```

### Collect Financial Data
```bash
curl -X POST https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/collect/financial \
  -H "x-api-key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "from": "2024-01-01", "to": "2024-01-10"}'
```

### Retrieve Financial Data
```bash
curl "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/retrieve/financial?ticker=AAPL&from=2024-01-01&to=2024-01-10" \
  -H "x-api-key: your-api-key"
```

### Visualise Financial Data
```bash
curl "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com/dev/visualise/financial?ticker=AAPL&from=2024-01-01&to=2024-01-10&format=png" \
  -H "x-api-key: your-api-key"
```

## Running Tests
Run through pipeline:
```bash
pytest --cov=. --cov-report=term-missing
```

Integration Tests:
API_KEY=your-key pytest test_integration.py -v --timeout=60

## Tech Stack

- **Runtime**: Python 3.11
- **Data Source**: Yahoo Finance (yfinance)
- **Storage**: AWS S3
- **Infrastructure**: AWS Lambda + API Gateway
- **CI/CD**: GitHub Actions