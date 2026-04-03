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

A dedicated testing microservice is deployed as an AWS Lambda function that runs automated unit and integration tests against the Bravo API and generates a PDF report.

**Endpoint:** `POST https://dhkeko3mb2.execute-api.ap-southeast-2.amazonaws.com/dev/test/run`

**Authentication:** `x-api-key` header required

**Query Parameters:**
- `phase` - `unit`, `integration`, or `both` (default: `both`), `all`

**Example:**
```bash
curl -X POST \
  https://dhkeko3mb2.execute-api.ap-southeast-2.amazonaws.com/dev/test/run \
  -H "x-api-key: YOUR_API_KEY"
```

**Response:**
```json
{
  "message": "Tests complete",
  "report_url": "https://s3.amazonaws.com/...",
  "phases_run": ["unit", "integration"],
  "s3_key": "dev/reports/Unit_Integration_Report.pdf"
}
```

The report is also downloadable as a GitHub Actions artifact via the **Generate Test Report** workflow (Actions tab → Generate Test Report → Run workflow).

### Test Coverage
- **Unit tests** - fully mocked, no live endpoints (`collection/tests`, `retrieval/tests`, `visualisation/tests`)
- **Integration tests** - hit live dev/prod endpoints (`test-service/tests/integration`)
- **E2E tests** - full pipeline validation via dedicated CI workflow (`test-service/tests/e2e`)


## Tech Stack

- **Runtime**: Python 3.11
- **Data Source**: Yahoo Finance (yfinance)
- **Storage**: AWS S3
- **Infrastructure**: AWS Lambda + API Gateway
- **CI/CD**: GitHub Actions