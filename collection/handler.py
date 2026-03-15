import json
import os
import boto3
from .collection import fetch_and_standardize_finance, generate_s3_key
# Replace prev line with commment for local testing
# from collection import fetch_and_standardize_finance, generate_s3_key

def handler(event, context):
    # Initialize S3 client and Environment Variables
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='ap-southeast-2'
    )

    S3_BUCKET = os.environ.get('AWS_BUCKET_NAME', 'bravo-adage-event-store')
    EXPECTED_API_KEY = os.getenv("FINANCE_API_KEY", "ecosystem-secret-123")

    path = event.get("path", "")
    method = event.get("httpMethod", "")
    headers = event.get("headers", {}) or {}

    # Route: Health Check
    if path == "/collect/health":
        try:    
            return respond(200, {
            "status": "healthy", 
            "service": "bravo-collection", 
            "version": "1.0.0"
            })
        except Exception as e:
            return respond(500, {"message": "Internal Server Error"})

    # Route: Financial Collection
    elif path == "/collect/financial" and method == "POST":
        
        # Security Check: API Key (401)
        api_key = headers.get("X-API-Key") or headers.get("x-api-key")
        if api_key != EXPECTED_API_KEY:
            return respond(401, {"message": "missing or invalid API key"})

        try:
            # Parse and Validate Request Body (400)
            body = json.loads(event.get("body", "{}"))
            ticker = body.get("ticker")
            date_from = body.get("from")
            date_to = body.get("to")

            if not all([ticker, date_from, date_to]):
                return respond(400, {"message": "invalid parameters: ticker, from, and to are required"})

            # Data Collection & Standardization (Logic from collection.py)
            standardized_data = fetch_and_standardize_finance(ticker, date_from, date_to)
            
            if not standardized_data:
                return respond(400, {"message": "no data found for ticker in this range"})

            # Generate Key and Finalize Dataset ID
            file_key = generate_s3_key(ticker, date_from, date_to)
            standardized_data["dataset_id"] = f"s3://{S3_BUCKET}/{file_key}"

            # S3 Persistence
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Body=json.dumps(standardized_data),
                ContentType="application/json"
            )

            # Success (201)
            return respond(201, {"id": file_key})

        except json.JSONDecodeError:
            return respond(400, {"message": "invalid JSON body"})
        except Exception as e:
            # Server Error (500)
            return respond(500, {"message": f"server error: {str(e)}"})

    # Route: Not Found
    else:
        return respond(404, {"message": "Route not found"})

# Helper function to format the Lambda Proxy Response
def respond(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }