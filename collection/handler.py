import json


def handler(event, context):
    path = event.get("path", "")

    if path == "/collect/health":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "healthy",
                "service": "bravo-collection",
                "version": "1.0.0"
            })
        }

    elif path == "/collect/financial":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Collection endpoint coming soon"
            })
        }

    else:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Route not found"})
        }
