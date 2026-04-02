"""
test_runner.py   Bravo Event Intelligence
Generates a professional, dynamic PDF test report from a pytest JSON output file.
Reads every test result and renders pass/fail per test with colour coding,
failure messages, timing, and summary statistics.

Usage:
    python test_runner.py <json_report_path> <output_pdf_path> <"Phase Name">

Example:
    python test_runner.py reports/unit_report.json reports/Unit_Report.pdf "Unit Testing"
"""

import os
import json
import sys
from datetime import datetime

# Force UTF-8 output on Windows (cmd/PowerShell default to cp1252)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor


# ── Colour Palette ─────────────────────────────────────────────────────────────
BRAND_DARK = HexColor("#0D1117")   # near-black background for header
BRAND_BLUE = HexColor("#1F6FEB")   # primary accent blue
BRAND_GREEN = HexColor("#238636")   # pass green
BRAND_RED = HexColor("#DA3633")   # fail red
BRAND_ORANGE = HexColor("#D29922")   # skipped/warning
BRAND_GREY = HexColor("#8B949E")   # muted text
BRAND_LIGHT = HexColor("#F0F6FF")   # light tint for alternating rows
WHITE = colors.white
BLACK = colors.black

PAGE_W, PAGE_H = A4
MARGIN_L = 20 * mm
MARGIN_R = 20 * mm
CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_report(path: str) -> dict:
    if not os.path.exists(path):
        print(f"[ERROR] JSON report not found: {path}")
        sys.exit(1)
    with open(path, "r") as f:
        return json.load(f)


def _outcome_style(outcome: str):
    """Returns (label_text, badge_colour, text_colour) for an outcome string."""
    o = outcome.upper()
    if o == "PASSED":
        return "PASS", BRAND_GREEN, WHITE
    if o == "FAILED":
        return "FAIL", BRAND_RED, WHITE
    if o == "SKIPPED":
        return "SKIP", BRAND_ORANGE, WHITE
    if o == "ERROR":
        return "ERR", BRAND_RED, WHITE
    return o[:4], BRAND_GREY, WHITE


def _clean_nodeid(nodeid: str) -> tuple[str, str]:
    """Split nodeid into (file_path, function_name)."""
    parts = nodeid.split("::")
    func = parts[-1] if len(parts) > 1 else nodeid
    fpath = "::".join(parts[:-1]) if len(parts) > 1 else ""
    # Strip leading path up to tests/
    if "tests/" in fpath:
        fpath = "tests/" + fpath.split("tests/")[-1]
    return fpath, func


def _draw_rounded_rect(c: canvas.Canvas, x, y, w, h, radius, fill_colour):
    c.setFillColor(fill_colour)
    p = c.beginPath()
    p.roundRect(x, y, w, h, radius)
    c.drawPath(p, fill=1, stroke=0)


def _draw_badge(c: canvas.Canvas, x, y, label, bg_colour, text_colour, font_size=7):
    """Draws a pill-shaped status badge."""
    badge_w = 28
    badge_h = 11
    _draw_rounded_rect(c, x, y - 2, badge_w, badge_h, 3, bg_colour)
    c.setFillColor(text_colour)
    c.setFont("Helvetica-Bold", font_size)
    c.drawCentredString(x + badge_w / 2, y + 1, label)


def _draw_header(c: canvas.Canvas, phase_name: str, generated_at: str, duration: float):
    """Draws the full-width dark header block."""
    header_h = 55 * mm
    # Dark background
    c.setFillColor(BRAND_DARK)
    c.rect(0, PAGE_H - header_h, PAGE_W, header_h, fill=1, stroke=0)

    # Blue accent stripe on left
    c.setFillColor(BRAND_BLUE)
    c.rect(0, PAGE_H - header_h, 4, header_h, fill=1, stroke=0)

    # Title
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 20)
    c.drawString(MARGIN_L, PAGE_H - 22 * mm, "Bravo Event Intelligence")

    c.setFont("Helvetica", 13)
    c.setFillColor(BRAND_BLUE)
    c.drawString(MARGIN_L, PAGE_H - 31 * mm, f"{phase_name}   Test Report")

    # Meta line
    c.setFont("Helvetica", 8)
    c.setFillColor(BRAND_GREY)
    c.drawString(MARGIN_L, PAGE_H - 40 * mm,
                 f"Generated: {generated_at}   |   Duration: {duration:.2f}s   |   "
                 f"API: ap-southeast-2.amazonaws.com")

    # Decorative dots
    for i, col in enumerate([BRAND_BLUE, BRAND_GREEN, BRAND_RED]):
        c.setFillColor(col)
        c.circle(PAGE_W - MARGIN_R - (i * 10),
                 PAGE_H - 15 * mm, 3, fill=1, stroke=0)


def _draw_summary_cards(c: canvas.Canvas, y: float,
                        total: int, passed: int, failed: int,
                        skipped: int, duration: float) -> float:
    """Draws 4 metric cards horizontally. Returns new y position."""
    card_w = (CONTENT_W - 9 * mm) / 4
    card_h = 22 * mm
    gap = 3 * mm
    x_start = MARGIN_L
    y_bottom = y - card_h

    cards = [
        ("TOTAL TESTS",   str(total),   BRAND_BLUE,   WHITE),
        ("PASSED",        str(passed),  BRAND_GREEN,  WHITE),
        ("FAILED",        str(failed),  BRAND_RED,    WHITE),
        ("SKIPPED",       str(skipped), BRAND_ORANGE, WHITE),
    ]

    for i, (label, value, bg, fg) in enumerate(cards):
        x = x_start + i * (card_w + gap)
        _draw_rounded_rect(c, x, y_bottom, card_w, card_h, 4, bg)

        c.setFillColor(fg)
        c.setFont("Helvetica-Bold", 20)
        c.drawCentredString(x + card_w / 2, y_bottom + 10 * mm, value)

        c.setFont("Helvetica", 7)
        c.drawCentredString(x + card_w / 2, y_bottom + 5 * mm, label)

    # Pass rate bar
    pr_y = y_bottom - 8 * mm
    pass_rate = (passed / total * 100) if total > 0 else 0

    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(BLACK)
    c.drawString(MARGIN_L, pr_y + 2 * mm, f"Pass Rate: {pass_rate:.1f}%")

    # Background bar
    c.setFillColor(HexColor("#E0E0E0"))
    c.roundRect(MARGIN_L + 55 * mm, pr_y, CONTENT_W -
                55 * mm, 4 * mm, 2, fill=1, stroke=0)

    # Fill bar
    fill_w = (CONTENT_W - 55 * mm) * (pass_rate / 100)
    bar_colour = BRAND_GREEN if pass_rate >= 80 else (
        BRAND_ORANGE if pass_rate >= 50 else BRAND_RED)
    c.setFillColor(bar_colour)
    c.roundRect(MARGIN_L + 55 * mm, pr_y, fill_w, 4 * mm, 2, fill=1, stroke=0)

    return pr_y - 8 * mm


def _draw_section_header(c: canvas.Canvas, y: float, title: str, count: int) -> float:
    """Draws a coloured section divider. Returns new y."""
    c.setFillColor(BRAND_BLUE)
    c.rect(MARGIN_L, y - 6 * mm, CONTENT_W, 6 * mm, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(MARGIN_L + 3 * mm, y - 4 * mm, f"{title}  ({count} tests)")
    return y - 6 * mm - 3 * mm


def _draw_test_row(c: canvas.Canvas, y: float, test: dict, row_idx: int) -> float:
    """
    Draws a single test result row.
    Returns the y position after rendering (accounts for multi-line failure messages).
    """
    ROW_H = 9 * mm

    # Alternating row background
    if row_idx % 2 == 0:
        c.setFillColor(BRAND_LIGHT)
        c.rect(MARGIN_L, y - ROW_H, CONTENT_W, ROW_H, fill=1, stroke=0)

    nodeid = test.get("nodeid", "Unknown")
    outcome = test.get("outcome", "unknown")
    dur = test.get("duration", 0.0)

    file_path, func_name = _clean_nodeid(nodeid)
    label, badge_col, badge_txt = _outcome_style(outcome)

    # Badge
    _draw_badge(c, MARGIN_L + 1 * mm, y - 7 * mm, label, badge_col, badge_txt)

    # Function name (primary)
    c.setFillColor(BLACK)
    c.setFont("Helvetica-Bold", 8)
    max_fn_w = 100 * mm
    fn_display = func_name[:55] if len(func_name) > 55 else func_name
    c.drawString(MARGIN_L + 32 * mm, y - 4 * mm, fn_display)

    # File path (secondary, muted)
    c.setFillColor(BRAND_GREY)
    c.setFont("Helvetica", 6.5)
    fp_display = file_path[:70] if len(file_path) > 70 else file_path
    c.drawString(MARGIN_L + 32 * mm, y - 7.5 * mm, fp_display)

    # Duration (right-aligned)
    c.setFillColor(BRAND_GREY)
    c.setFont("Helvetica", 7)
    c.drawRightString(PAGE_W - MARGIN_R, y - 5 * mm, f"{dur:.2f}s")

    new_y = y - ROW_H

    # If failed, show the failure message indented below
    if outcome.upper() in ("FAILED", "ERROR"):
        call_info = test.get("call", {}) or {}
        longrepr = call_info.get("longrepr", "") or test.get("longrepr", "")
        if longrepr:
            # Take only the last meaningful line (the AssertionError / message)
            lines = [ln.strip()
                     for ln in str(longrepr).splitlines() if ln.strip()]
            # Find the assert/error line
            err_line = ""
            for ln in reversed(lines):
                if ln.startswith("assert") or ln.startswith("E ") or "Error" in ln or "assert" in ln.lower():
                    err_line = ln.replace("E ", "", 1).strip()
                    break
            if not err_line and lines:
                err_line = lines[-1]
            if err_line:
                err_line = err_line[:95] + ("…" if len(err_line) > 95 else "")
                c.setFillColor(BRAND_RED)
                c.setFont("Helvetica-Oblique", 6.5)
                c.drawString(MARGIN_L + 35 * mm, new_y +
                             1 * mm, f"↳ {err_line}")
                new_y -= 4 * mm

    return new_y


def _new_page(c: canvas.Canvas, phase_name: str, page_num: int) -> float:
    """Start a new page, draw a minimal header, return starting y."""
    c.showPage()
    # Thin top bar
    c.setFillColor(BRAND_DARK)
    c.rect(0, PAGE_H - 12 * mm, PAGE_W, 12 * mm, fill=1, stroke=0)
    c.setFillColor(BRAND_BLUE)
    c.rect(0, PAGE_H - 12 * mm, 4, 12 * mm, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(MARGIN_L, PAGE_H - 8 * mm,
                 f"Bravo Event Intelligence   {phase_name} Report")
    c.setFont("Helvetica", 7)
    c.setFillColor(BRAND_GREY)
    c.drawRightString(PAGE_W - MARGIN_R, PAGE_H - 8 * mm, f"Page {page_num}")
    return PAGE_H - 18 * mm


def _draw_footer(c: canvas.Canvas):
    """Draws a footer line on the current page."""
    c.setStrokeColor(BRAND_GREY)
    c.setLineWidth(0.3)
    c.line(MARGIN_L, 12 * mm, PAGE_W - MARGIN_R, 12 * mm)
    c.setFont("Helvetica", 6.5)
    c.setFillColor(BRAND_GREY)
    c.drawString(MARGIN_L, 8 * mm,
                 "Bravo Event Intelligence   SENG3011   Automated Test Report")
    c.drawRightString(PAGE_W - MARGIN_R, 8 * mm,
                      "Confidential   Internal Use Only")


# ── Main Report Builder ────────────────────────────────────────────────────────

def generate_report(json_path: str, pdf_path: str, phase_name: str):
    report = _load_report(json_path)
    summary = report.get("summary", {})
    tests = report.get("tests", [])

    total = summary.get("total",   0)
    passed = summary.get("passed",  0)
    failed = summary.get("failed",  0)
    skipped = summary.get("skipped", 0)
    duration = report.get("duration", 0.0)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    c = canvas.Canvas(pdf_path, pagesize=A4)
    page_n = 1

    # ── Page 1: Header + Summary Cards ────────────────────────────────────────
    _draw_header(c, phase_name, generated_at, duration)

    y = PAGE_H - 60 * mm
    y = _draw_summary_cards(c, y, total, passed, failed, skipped, duration)

    y -= 6 * mm

    # ── Group tests by file ────────────────────────────────────────────────────
    from collections import OrderedDict
    sections: dict[str, list] = OrderedDict()
    for t in tests:
        fp, _ = _clean_nodeid(t.get("nodeid", ""))
        sections.setdefault(fp, []).append(t)

    row_idx = 0
    for section_title, section_tests in sections.items():
        # Check if we need a new page for the section header + at least one row
        if y < 30 * mm:
            _draw_footer(c)
            page_n += 1
            y = _new_page(c, phase_name, page_n)

        y = _draw_section_header(
            c, y, section_title or "Tests", len(section_tests))

        # Column header row
        c.setFont("Helvetica-Bold", 7)
        c.setFillColor(BRAND_GREY)
        c.drawString(MARGIN_L + 32 * mm, y - 3 * mm, "TEST FUNCTION")
        c.drawRightString(PAGE_W - MARGIN_R, y - 3 * mm, "DURATION")
        y -= 5 * mm

        for test in section_tests:
            # Estimate row height (failed rows are taller)
            outcome = test.get("outcome", "").upper()
            call_info = test.get("call", {}) or {}
            has_err = outcome in ("FAILED", "ERROR") and (
                call_info.get("longrepr") or test.get("longrepr")
            )
            needed = (13 * mm) if has_err else (9 * mm)

            if y - needed < 20 * mm:
                _draw_footer(c)
                page_n += 1
                y = _new_page(c, phase_name, page_n)

            y = _draw_test_row(c, y, test, row_idx)
            row_idx += 1

        y -= 4 * mm  # spacing between sections

    # ── Final summary footer bar ───────────────────────────────────────────────
    if y < 35 * mm:
        _draw_footer(c)
        page_n += 1
        y = _new_page(c, phase_name, page_n)

    y -= 4 * mm
    _draw_rounded_rect(c, MARGIN_L, y - 14 * mm,
                       CONTENT_W, 14 * mm, 4, BRAND_DARK)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 9)
    status_str = "ALL TESTS PASSED ✓" if failed == 0 else f"{failed} TEST(S) FAILED ✗"
    c.drawString(MARGIN_L + 5 * mm, y - 8 * mm, status_str)
    c.setFont("Helvetica", 8)
    c.setFillColor(BRAND_GREY)
    c.drawRightString(PAGE_W - MARGIN_R - 5 * mm, y - 8 * mm,
                      f"{passed}/{total} passed in {duration:.2f}s")

    _draw_footer(c)
    c.save()
    print(
        f"[OK] PDF report generated -> {pdf_path}  ({page_n} page(s), {total} tests)")


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python test_runner.py <json_report> <output_pdf> <Phase Name>")
        print('Example: python test_runner.py reports/unit.json reports/Unit.pdf "Unit Testing"')
        sys.exit(1)

    generate_report(
        json_path=sys.argv[1],
        pdf_path=sys.argv[2],
        phase_name=sys.argv[3],
    )
