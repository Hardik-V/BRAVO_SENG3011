
import json


def handler(event, context):
    path = event.get("path", "")

    if path == "/retrieve/health":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "healthy",
                "service": "bravo-retrieval",
                "version": "1.0.0"
            })
        }

    elif path == "/retrieve/financial":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Retrieval endpoint coming soon"
            })
        }

    else:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Route not found"})
        }
