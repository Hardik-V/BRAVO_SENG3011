"""
test-service/handler.py
Deployed testing microservice for Bravo Event Intelligence.
Runs unit and integration tests, generates a PDF report, uploads to S3,
and returns a presigned download URL.

Trigger: POST /test/run
Optional query param: ?phase=unit|integration|both (default: both)
"""

import json
import os
import sys
import boto3
import pytest

sys.path.insert(0, os.path.dirname(__file__))

from combined_report import main as generate_report  # noqa

BUCKET = os.environ.get("AWS_BUCKET_NAME", "bravo-adage-event-store")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
REPORTS_DIR = "/tmp/reports"
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TEST_SERVICE_DIR = os.path.dirname(__file__)

PHASE_PATHS = {
    "unit": [
        os.path.join(REPO_ROOT, "collection", "tests"),
        os.path.join(REPO_ROOT, "retrieval", "tests"),
        os.path.join(REPO_ROOT, "visualisation", "tests"),
    ],
    "integration": [
        os.path.join(TEST_SERVICE_DIR, "tests", "integration"),
    ]
}


def respond(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }


def run_phase(phase: str):
    """Run pytest for a phase, return path to JSON report or None on error."""
    out = f"{REPORTS_DIR}/{phase}_report.json"
    paths = PHASE_PATHS.get(phase, [])
    
    # Filter to only existing paths
    existing = [p for p in paths if os.path.exists(p)]
    if not existing:
        print(f"[WARN] No test paths found for phase: {phase}")
        return None

    try:
        pytest.main([
            *existing,
            "--tb=short",
            "--json-report",
            f"--json-report-file={out}",
            "--json-report-indent=2",
            "-q"
        ])
        return out if os.path.exists(out) else None
    except Exception as e:
        print(f"[ERROR] Failed to run {phase} tests: {e}")
        return None


def handler(event, context):
    os.makedirs(REPORTS_DIR, exist_ok=True)

    params = event.get("queryStringParameters") or {}
    phase = params.get("phase", "both")

    unit_json = None
    integration_json = None

    if phase in ("unit", "both"):
        unit_json = run_phase("unit")

    if phase in ("integration", "both"):
        integration_json = run_phase("integration")

    if not unit_json and not integration_json:
        return respond(500, {"error": "All test phases failed to generate reports"})

    pdf_path = f"{REPORTS_DIR}/Unit_Integration_Report.pdf"
    try:
        sys.argv = [
            "combined_report.py",
            "--output", pdf_path,
            "--test-dirs", os.path.join(TEST_SERVICE_DIR, "tests"),
        ]
        if unit_json:
            sys.argv += ["--unit", unit_json]
        if integration_json:
            sys.argv += ["--integration", integration_json]

        generate_report()
    except Exception as e:
        return respond(500, {"error": f"PDF generation failed: {str(e)}"})

    s3_key = f"{ENVIRONMENT}/reports/Unit_Integration_Report.pdf"
    try:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.upload_file(pdf_path, BUCKET, s3_key)
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET, "Key": s3_key},
            ExpiresIn=3600
        )
    except Exception as e:
        return respond(500, {"error": f"S3 upload failed: {str(e)}"})

    return respond(200, {
        "message": "Tests complete",
        "report_url": url,
        "phases_run": [p for p, j in [("unit", unit_json), ("integration", integration_json)] if j],
        "s3_key": s3_key
    })