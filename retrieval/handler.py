import json
import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")
APP_ENV = os.environ.get("ENVIRONMENT", "dev")


def get_s3_client():
    return boto3.client("s3", region_name="ap-southeast-2")


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def handler(event, context):
    path = event.get("path", "")
    http_method = event.get("httpMethod", "")

    if path == "/retrieve/health" and http_method == "GET":
        return build_response(200, {
            "status": "healthy",
            "service": "bravo-retrieval",
            "version": "1.0.0"
        })

    elif path == "/retrieve/financial" and http_method == "GET":
        query_params = event.get("queryStringParameters") or {}

        ticker = query_params.get("ticker")
        from_date = query_params.get("from")
        to_date = query_params.get("to")

        if not ticker or not from_date or not to_date:
            return build_response(400, {
                "error": "Missing required query parameters: ticker, from, to"
            })

        if not is_valid_date(from_date) or not is_valid_date(to_date):
            return build_response(400, {
                "error": "Invalid date format. Use YYYY-MM-DD."
            })

        if from_date > to_date:
            return build_response(400, {
                "error": "'from' date cannot be later than 'to' date."
            })

        if not BUCKET_NAME:
            return build_response(500, {
                "error": "AWS_BUCKET_NAME is not configured"
            })

        ticker = ticker.upper()
        s3_key = f"{APP_ENV}/financial/{ticker}_{from_date}_{to_date}.json"

        try:
            s3 = get_s3_client()
            response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            file_content = response["Body"].read().decode("utf-8")
            data = json.loads(file_content)

            return build_response(200, data)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code in ["NoSuchKey", "404", "NoSuchObject"]:
                return build_response(404, {
                    "error": "No data found for the requested params"
                })

            return build_response(500, {
                "error": "Failed to retrieve data from S3",
                "details": str(e)
            })

        except json.JSONDecodeError:
            return build_response(500, {
                "error": "Stored file is not valid JSON"
            })

        except Exception as e:
            return build_response(500, {
                "error": "Internal server error",
                "details": str(e)
            })

    else:
        return build_response(404, {
            "error": "Route not found"
        })
