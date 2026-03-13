
import json


def handler(event, context):
    path = event.get("path", "")

    if path == "/visualise/health":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "healthy",
                "service": "bravo-visualisation",
                "version": "1.0.0"
            })
        }

    elif path == "/visualise/financial":
        # main visualisation logic here
        pass

    else:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Route not found"})
        }
