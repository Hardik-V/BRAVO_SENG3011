# BRAVO - Financial Event Data API

A set of microservices for collecting, retrieving and visualising financial market event data, normalised into the ADAGE 3.0 Standard Event Model.

## Services

- **Collection** - Fetches financial market data from Yahoo Finance and stores it in AWS S3
- **Retrieval** - Fetches stored financial event data from AWS S3
- **Visualisation** - Generates price charts from stored financial event data

## Prerequisites

- Python 3.11+
- Docker
- AWS CLI configured with credentials
- Terraform (for deployment)
  
## Running with Docker
```bash
# Collection
docker build -t bravo-collection ./collection
docker run -p 9001:8080 bravo-collection

# Then test it locally with:
curl -X POST "http://localhost:9001/2015-03-31/functions/function/invocations" \
  -d '{"body": "{\"ticker\": \"DX-Y.NYB\", \"from\": \"2024-01-01\", \"to\": \"2024-12-31\"}"}'

# Retrieval
docker build -t bravo-retrieval ./retrieval
docker run -p 9002:8080 bravo-retrieval

# Visualisation
docker build -t bravo-visualisation ./visualisation
docker run -p 9003:8080 bravo-visualisation
```

## Running Tests
```bash
pytest --cov=. --cov-report=term-missing
```

## API Documentation

API is documented using OpenAPI 3.0. Once deployed, Swagger UI is available at:
- `/docs` on each service endpoint

## Tech Stack

- **Runtime**: Python 3
- **Data Source**: Yahoo Finance (yfinance)
- **Storage**: AWS S3
- **Infrastructure**: AWS Lambda + API Gateway (Terraform)
- **CI/CD**: GitHub Actions
