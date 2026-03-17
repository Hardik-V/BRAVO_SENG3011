import json
import base64
from graph_service import create_graph
from retrieval_service import get_financial_data


def respond(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }


def handler(event, context):
    """AWS Lambda handler for the Visualisation microservice."""

    path = event.get("path", "")
    method = event.get("httpMethod", "")

    if path == "/visualise/health":
        return respond(200, {
            "status": "healthy",
            "service": "bravo-visualisation",
            "version": "1.0.0"
        })

    elif path == "/visualise/financial" and method == "GET":

        query = event.get("queryStringParameters") or {}
        ticker = query.get("ticker")
        date_from = query.get("from")
        date_to = query.get("to")
        fmt = query.get("format", "png")
         
        if not all([ticker, date_from, date_to]):
            return respond(400, {
                "message": "ticker, from, and to are required"
            })
         
        try:
            data = get_financial_data(ticker, date_from, date_to)
            if not data or "event" not in data:
                return respond(404, {"message": "no financial data found"})
         
            if fmt == "json":
                return respond(200, {
                    "chart_type": "time_series",
                    "ticker": ticker,
                    "from": date_from,
                    "to": date_to,
                    "chart_data": data
                })
         
            else:
                # Default to PNG
                graph_path = create_graph(data)
                if not graph_path:
                    return respond(500, {"message": "failed to generate graph"})
         
                with open(graph_path, "rb") as f:
                    img_bytes = f.read()
                img_base64 = base64.b64encode(img_bytes).decode("utf-8")
         
                return {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "image/png",
                        "Access-Control-Allow-Origin": "*"
                    },
                    "isBase64Encoded": True,
                    "body": img_base64
                }
         
        except Exception as e:
            return respond(500, {"message": f"server error: {str(e)}"})
    else:
        return respond(404, {"message": "Route not found"})
         
