import json
import os
import boto3  # type: ignore

# Allows code to run locally and on AWS
try:
    from .collection import (  # type: ignore
        fetch_and_standardize_finance, generate_s3_key
    )
except ImportError:
    from collection import (  # type: ignore
        fetch_and_standardize_finance, generate_s3_key
    )


def handler(event, context):
    # Initialize S3 client and Environment Variables
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='ap-southeast-2'
    )

    S3_BUCKET = os.environ.get('AWS_BUCKET_NAME', 'bravo-adage-event-store')

    path = event.get("path", "")
    method = event.get("httpMethod", "")

    # Route: Health Check
    if path == "/collect/health":
        return respond(200, {
            "status": "healthy",
            "service": "bravo-collection",
            "version": "1.0.0"
        })

    # Route: Financial Collection
    elif path == "/collect/financial" and method == "POST":

        try:
            # Parse and Validate Request Body (400)
            body = json.loads(event.get("body", "{}"))
            ticker = body.get("ticker")
            date_from = body.get("from")
            date_to = body.get("to")

            if not all([ticker, date_from, date_to]):
                # E501 fix: Split long string
                msg = "invalid parameters: ticker, from, and to are required"
                return respond(400, {"message": msg})

            # Data Collection & Standardization (Logic from collection.py)
            data = fetch_and_standardize_finance(ticker, date_from, date_to)

            if not data:
                # E501 fix: Split long string
                msg = "no data found for ticker in this range"
                return respond(400, {"message": msg})

            # Generate Key and Finalize Dataset ID
            file_key = generate_s3_key(ticker, date_from, date_to)
            # E501 fix: Split long f-string
            data["dataset_id"] = f"s3://{S3_BUCKET}/{file_key}"

            # S3 Persistence
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Body=json.dumps(data),
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
