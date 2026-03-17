import json
import os
import base64
from visualisation.retrieval import get_financial_data
from visualisation.graph_service import create_graph


EXPECTED_API_KEY = os.getenv("FINANCE_API_KEY", "ecosystem-secret-123")


def handler(event, context):
    """AWS Lambda handler for the Visualisation microservice."""

    path = event.get("path", "")
    method = event.get("httpMethod", "")
    headers = event.get("headers", {}) or {}

    if path == "/visualise/health":
        return respond(200, {
            "status": "healthy",
            "service": "bravo-visualisation",
            "version": "1.0.0"
        })

    elif path == "/visualise/financial" and method == "GET":
        # API Key check
        api_key = headers.get("X-API-Key") or headers.get("x-api-key")
        if api_key != EXPECTED_API_KEY:
            return respond(401, {"message": "missing or invalid API key"})

        # Parse query parameters
        query = event.get("queryStringParameters") or {}
        ticker = query.get("ticker")
        date_from = query.get("from")
        date_to = query.get("to")

        if not all([ticker, date_from, date_to]):
            return respond(400, {
                "message": "ticker, from, and to are required"
            })

        try:
            # 1️ Retrieve financial data from retrieval microservice
            data = get_financial_data(ticker, date_from, date_to, api_key)
            if not data or "events" not in data:
                return respond(404, {"message": "no financial data found"})

            # 2️ Generate graph
            graph_path = create_graph(data)
            if not graph_path:
                return respond(500, {"message": "failed to generate graph"})

            # 3️ Encode PNG as base64 and return JSON
            with open(graph_path, "rb") as f:
                img_bytes = f.read()
            img_base64 = base64.b64encode(img_bytes).decode("utf-8")

            return respond(200, {"image_base64": img_base64})

        except Exception as e:
            return respond(500, {"message": f"server error: {str(e)}"})

    else:
        return respond(404, {"message": "Route not found"})


def respond(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }
