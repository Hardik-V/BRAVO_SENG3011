
import subprocess
import sys
import os

REPORTS_DIR = "test-service/reports"
TESTS_DIR = "test-service/tests"
RUNNER = "test-service/combined_report.py"
UNIT_JSON = f"{REPORTS_DIR}/unit_report.json"
INT_JSON = f"{REPORTS_DIR}/integration_report.json"
E2E_JSON = f"{REPORTS_DIR}/e2e_report.json"
OUTPUT_PDF = f"{REPORTS_DIR}/Combined_Test_Report.pdf"


def header(msg): print(f"\n\033[1;36m{'='*52}\n  {msg}\n{'='*52}\033[0m")
def info(msg): print(f"\033[1;34m[INFO]\033[0m  {msg}")
def success(msg): print(f"\033[1;32m[PASS]\033[0m  {msg}")
def error(msg): print(f"\033[1;31m[FAIL]\033[0m  {msg}")


def run(cmd):
    """Run a command, don't exit on failure (so we still generate the report)."""
    result = subprocess.run(cmd, shell=True)
    return result.returncode == 0


def main():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    header("Bravo Event Intelligence  Full Test Suite")

    # Install dependencies
    info("Installing dependencies...")
    run(f"{sys.executable} -m pip install -q -r test-service/requirements.txt")
    success("Dependencies OK\n")

    # Unit tests
    info("Running UNIT tests...")
    run(f"pytest {TESTS_DIR}/unit -v --tb=short --json-report --json-report-file={UNIT_JSON} --json-report-indent=2")
    success("Unit tests complete\n")

    # Integration tests
    info("Running INTEGRATION tests...")
    run(f"pytest {TESTS_DIR}/integration -v --tb=short --json-report --json-report-file={INT_JSON} --json-report-indent=2")
    success("Integration tests complete\n")

    # E2E tests
    info("Running E2E tests...")
    run(f"pytest {TESTS_DIR}/e2e -v --tb=short --json-report --json-report-file={E2E_JSON} --json-report-indent=2")
    success("E2E tests complete\n")

    # Generate PDF
    info("Generating combined PDF report...")
    cmd = (
        f"{sys.executable} {RUNNER}"
        f" --unit        {UNIT_JSON}"
        f" --integration {INT_JSON}"
        f" --e2e         {E2E_JSON}"
        f" --output      {OUTPUT_PDF}"
        f" --test-dirs   {TESTS_DIR}"
    )
    if run(cmd):
        success(f"Report ready → {OUTPUT_PDF}")
    else:
        error("PDF generation failed , check output above.")

    print(f"\n\033[1;36m{'='*52}\033[0m\n")


if __name__ == "__main__":
    main()
