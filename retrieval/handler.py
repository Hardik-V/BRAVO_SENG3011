import json
import os
import boto3
import urllib.request
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")
APP_ENV = os.environ.get("ENVIRONMENT", "")
API_KEY = os.environ.get("API_KEY", "")

COLLECTION_BASE_URL = (
    "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com"
)


def get_collection_url():
    stage = APP_ENV if APP_ENV else "dev"
    return f"{COLLECTION_BASE_URL}/{stage}/collect/financial"


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


def get_expected_dates(from_date, to_date):
    """
    Returns a set of all dates (YYYY-MM-DD) in the requested range,
    excluding weekends since markets are closed.
    """
    expected = set()
    current = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    while current <= end:
        if current.weekday() < 5:  # 0-4 are Monday to Friday
            expected.add(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return expected


def call_collection_service(ticker, from_date, to_date):
    """
    Calls the collection service to fetch and store data for the given
    ticker and date range. Returns True if successful, False otherwise.
    """
    url = get_collection_url()
    payload = json.dumps({
        "ticker": ticker,
        "from": from_date,
        "to": to_date
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": API_KEY
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.status == 201
    except urllib.error.HTTPError as e:
        # 400 means ticker doesn't exist or no data found
        if e.code == 400:
            return False
        raise
    except Exception:
        raise


def fetch_from_s3(s3, ticker, from_date, to_date):
    """
    Attempts to find and return matching financial data from S3.
    Returns (overlapping_keys, objects) tuple.
    """
    prefix = f"{APP_ENV}/financial/{ticker}_"

    list_response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )
    objects = list_response.get("Contents", [])

    if not objects:
        return None, None

    overlapping_keys = []
    for obj in objects:
        key = obj["Key"]
        filename = key.replace(prefix, "").replace(".json", "")
        parts = filename.split("_")
        if len(parts) == 2:
            file_from, file_to = parts
            if file_from <= to_date and file_to >= from_date:
                overlapping_keys.append(key)

    return overlapping_keys, objects


def has_complete_data(events, from_date, to_date):
    """
    Checks if the events cover all expected trading days in the range.
    Returns True if complete, False if any dates are missing.
    """
    expected_dates = get_expected_dates(from_date, to_date)
    actual_dates = set(
        e["event_time_object"]["timestamp"][:10] for e in events
    )
    return expected_dates.issubset(actual_dates)


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

        try:
            s3 = get_s3_client()

            overlapping_keys, objects = fetch_from_s3(
                s3, ticker, from_date, to_date
            )

            # No data in S3 at all, or no overlapping files
            if not objects or not overlapping_keys:
                collected = call_collection_service(
                    ticker, from_date, to_date
                )

                if not collected:
                    return build_response(404, {
                        "error": (
                            "No data found for this ticker and date range"
                        )
                    })

                # Retry S3 fetch after collection
                overlapping_keys, objects = fetch_from_s3(
                    s3, ticker, from_date, to_date
                )

                if not overlapping_keys:
                    return build_response(404, {
                        "error": "No data found after collection attempt"
                    })

            # Build events from overlapping keys
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

            # If data is incomplete, call collection for the full range
            if not has_complete_data(unique_events, from_date, to_date):
                collected = call_collection_service(
                    ticker, from_date, to_date
                )

                if collected:
                    # Retry S3 fetch after collection
                    overlapping_keys, objects = fetch_from_s3(
                        s3, ticker, from_date, to_date
                    )

                    if overlapping_keys:
                        all_events = []
                        base_data = None

                        for key in overlapping_keys:
                            s3_response = s3.get_object(
                                Bucket=BUCKET_NAME, Key=key
                            )
                            file_content = (
                                s3_response["Body"].read().decode("utf-8")
                            )
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
