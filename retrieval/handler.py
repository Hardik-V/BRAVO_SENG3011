import json
import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")
APP_ENV = os.environ.get("ENVIRONMENT", "")


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
        prefix = f"{APP_ENV}/financial/{ticker}_"

        try:
            s3 = get_s3_client()

            list_response = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=prefix
            )
            objects = list_response.get("Contents", [])

            if not objects:
                return build_response(404, {
                    "error": "No collected data found for this ticker"
                })

            overlapping_keys = []
            for obj in objects:
                key = obj["Key"]
                filename = key.replace(prefix, "").replace(".json", "")
                parts = filename.split("_")
                if len(parts) == 2:
                    file_from, file_to = parts

                    if file_from <= to_date and file_to >= from_date:
                        overlapping_keys.append(key)

            if not overlapping_keys:
                return build_response(404, {
                    "error": (
                        "No collected data covers the requested date range. "
                        "Please collect data for this range first."
                    )
                })

            all_events = []
            base_data = None

            for key in overlapping_keys:
                s3_response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                file_content = s3_response["Body"].read().decode("utf-8")
                data = json.loads(file_content)

                if base_data is None:
                    base_data = data

                all_events.extend(data.get("events", []))

            filtered_events = [
                e for e in all_events
                if from_date
                <= e["event_time_object"]["timestamp"][:10]
                <= to_date
            ]

            seen = set()
            unique_events = []
            for e in filtered_events:
                ts = e["event_time_object"]["timestamp"][:10]
                if ts not in seen:
                    seen.add(ts)
                    unique_events.append(e)

            unique_events.sort(
                key=lambda e: e["event_time_object"]["timestamp"][:10]
            )

            if not unique_events:
                return build_response(404, {
                    "error": "No data found within the requested date range"
                })

            base_data["events"] = unique_events
            return build_response(200, base_data)

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
