"""
combined_report.py   Bravo Event Intelligence
Merges unit, integration, and E2E JSON reports into a single
professional PDF with one section per phase, per-test descriptions,
and an overall executive summary on page 1.

Usage:
    python test-service/combined_report.py \
        --unit        test-service/reports/unit_report.json \
        --integration test-service/reports/integration_report.json \
        --e2e         test-service/reports/e2e_report.json \
        --output      test-service/reports/Combined_Test_Report.pdf

Any phase can be omitted if its JSON doesn't exist yet.
"""

import os
import ast
import json
import sys
import argparse
import textwrap
from datetime import datetime

# Force UTF-8 output on Windows (cmd/PowerShell default to cp1252)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor, white, black


# ── Palette ────────────────────────────────────────────────────────────────────
C_DARK = HexColor("#0D1117")
C_BLUE = HexColor("#1F6FEB")
C_GREEN = HexColor("#238636")
C_RED = HexColor("#DA3633")
C_ORANGE = HexColor("#D29922")
C_GREY = HexColor("#8B949E")
C_LGREY = HexColor("#F6F8FA")
C_PURPLE = HexColor("#8957E5")   # accent for E2E phase
C_TEAL = HexColor("#1A7F64")   # accent for integration phase

PHASE_COLOURS = {
    "Unit Testing":                    C_BLUE,
    "Integration Testing":             C_TEAL,
    "End-to-End Pipeline Validation":  C_PURPLE,
}

PAGE_W, PAGE_H = A4
ML = 18 * mm          # left margin
MR = 18 * mm          # right margin
CW = PAGE_W - ML - MR  # content width
BOTTOM_SAFE = 18 * mm  # don't draw below this


# ── AST: extract docstrings from source files ──────────────────────────────────

def _extract_docstrings(test_dirs: list[str]) -> dict[str, str]:
    """
    Walk test source files and return {function_name: docstring}.
    Handles duplicate names by keying on nodeid-style 'file::func'.
    """
    docs: dict[str, str] = {}
    for d in test_dirs:
        if not os.path.isdir(d):
            continue
        for root, _, files in os.walk(d):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath) as f:
                        src = f.read()
                    tree = ast.parse(src)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                            doc = ast.get_docstring(node) or ""
                            # key by short name AND by path::name for disambiguation
                            rel = os.path.relpath(fpath)
                            docs[node.name] = doc
                            docs[f"{rel}::{node.name}"] = doc
                except Exception:
                    pass
    return docs


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load(path: str) -> dict | None:
    if not path or not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _outcome_style(outcome: str):
    o = outcome.upper()
    if o == "PASSED":
        return "PASS", C_GREEN,  white
    if o == "FAILED":
        return "FAIL", C_RED,    white
    if o == "SKIPPED":
        return "SKIP", C_ORANGE, white
    return "ERR",  C_RED, white


def _clean(nodeid: str) -> tuple[str, str]:
    parts = nodeid.split("::")
    func = parts[-1] if len(parts) > 1 else nodeid
    fpath = "::".join(parts[:-1]) if len(parts) > 1 else ""
    if "tests/" in fpath:
        fpath = "tests/" + fpath.split("tests/")[-1]
    return fpath, func


def _rr(c, x, y, w, h, r, col):
    c.setFillColor(col)
    p = c.beginPath()
    p.roundRect(x, y, w, h, r)
    c.drawPath(p, fill=1, stroke=0)


def _badge(c, x, y, label, bg, fg, fs=7):
    bw, bh = 28, 11
    _rr(c, x, y - 2, bw, bh, 3, bg)
    c.setFillColor(fg)
    c.setFont("Helvetica-Bold", fs)
    c.drawCentredString(x + bw / 2, y + 1, label)


def _err_line(test: dict) -> str:
    call = test.get("call", {}) or {}
    raw = call.get("longrepr", "") or test.get("longrepr", "") or ""
    if not raw:
        return ""
    lines = [ln.strip() for ln in str(raw).splitlines() if ln.strip()]
    for ln in reversed(lines):
        if any(k in ln for k in ("assert", "Assert", "Error", "E ")):
            return ln.replace("E ", "", 1).strip()
    return lines[-1] if lines else ""


def _docstring_for(test: dict, docs: dict) -> str:
    nodeid = test.get("nodeid", "")
    _, func = _clean(nodeid)
    # Try full path key first, then short name
    rel_key = nodeid.replace("/", os.sep)
    return docs.get(rel_key) or docs.get(nodeid) or docs.get(func) or ""


# ── Canvas helpers ─────────────────────────────────────────────────────────────

class Doc:
    """Thin stateful wrapper around reportlab Canvas."""

    def __init__(self, path: str):
        self.c = canvas.Canvas(path, pagesize=A4)
        self.y = PAGE_H
        self.page = 0
        self._continuation_title = ""

    # ── page management ───────────────────────────────────────────────────────

    def new_page(self, title=""):
        if self.page > 0:
            self._footer()
        self.c.showPage()
        self.page += 1
        if title:
            self._continuation_title = title
        self._mini_header()
        self.y = PAGE_H - 16 * mm

    def _mini_header(self):
        c = self.c
        c.setFillColor(C_DARK)
        c.rect(0, PAGE_H - 12 * mm, PAGE_W, 12 * mm, fill=1, stroke=0)
        c.setFillColor(C_BLUE)
        c.rect(0, PAGE_H - 12 * mm, 4, 12 * mm, fill=1, stroke=0)
        c.setFillColor(white)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(ML, PAGE_H - 8 * mm,
                     "Bravo Event Intelligence   Combined Test Report")
        c.setFont("Helvetica", 7)
        c.setFillColor(C_GREY)
        c.drawRightString(PAGE_W - MR, PAGE_H - 8 * mm, f"Page {self.page}")

    def _footer(self):
        c = self.c
        c.setStrokeColor(C_GREY)
        c.setLineWidth(0.3)
        c.line(ML, 12 * mm, PAGE_W - MR, 12 * mm)
        c.setFont("Helvetica", 6.5)
        c.setFillColor(C_GREY)
        c.drawString(
            ML, 8 * mm, "Bravo Event Intelligence   SENG3011   Automated Combined Test Report")
        c.drawRightString(PAGE_W - MR, 8 * mm,
                          "Confidential   Internal Use Only")

    def check(self, needed: float):
        """Start new page if not enough space."""
        if self.y - needed < BOTTOM_SAFE:
            self.new_page(self._continuation_title)

    def save(self):
        self._footer()
        self.c.save()


# ── Drawing blocks ─────────────────────────────────────────────────────────────

def draw_cover_header(doc: Doc, generated_at: str):
    c = doc.c
    hh = 60 * mm
    c.setFillColor(C_DARK)
    c.rect(0, PAGE_H - hh, PAGE_W, hh, fill=1, stroke=0)
    # Blue left stripe
    c.setFillColor(C_BLUE)
    c.rect(0, PAGE_H - hh, 5, hh, fill=1, stroke=0)

    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 22)
    c.drawString(ML, PAGE_H - 22 * mm, "Bravo Event Intelligence")
    c.setFont("Helvetica", 14)
    c.setFillColor(C_BLUE)
    c.drawString(ML, PAGE_H - 32 * mm, "Full System Test Report   All Phases")
    c.setFont("Helvetica", 8)
    c.setFillColor(C_GREY)
    c.drawString(ML, PAGE_H - 41 * mm, f"Generated: {generated_at}")
    c.drawString(ML, PAGE_H - 48 * mm,
                 "Covers: Unit Testing  ·  Integration Testing  ·  End-to-End Pipeline Validation")

    # Decorative circles
    for i, col in enumerate([C_BLUE, C_GREEN, C_RED, C_PURPLE]):
        c.setFillColor(col)
        c.circle(PAGE_W - MR - i * 12, PAGE_H - 15 * mm, 4, fill=1, stroke=0)

    doc.y = PAGE_H - hh - 8 * mm


def draw_overall_summary(doc: Doc, phases: list[dict]):
    """Big 3-column summary cards + overall pass rate bar."""
    c = doc.c
    grand_total = sum(p["total"] for p in phases)
    grand_passed = sum(p["passed"] for p in phases)
    grand_failed = sum(p["failed"] for p in phases)
    grand_skipped = sum(p["skipped"] for p in phases)
    grand_dur = sum(p["duration"] for p in phases)

    doc.check(55 * mm)

    # Section label
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(C_DARK)
    c.drawString(ML, doc.y, "Overall Execution Summary")
    doc.y -= 6 * mm

    # 4 cards
    card_w = (CW - 9 * mm) / 4
    card_h = 20 * mm
    gap = 3 * mm
    cards = [
        ("TOTAL",   str(grand_total),   C_BLUE,   white),
        ("PASSED",  str(grand_passed),  C_GREEN,  white),
        ("FAILED",  str(grand_failed),  C_RED,    white),
        ("SKIPPED", str(grand_skipped), C_ORANGE, white),
    ]
    for i, (lbl, val, bg, fg) in enumerate(cards):
        x = ML + i * (card_w + gap)
        y = doc.y - card_h
        _rr(c, x, y, card_w, card_h, 4, bg)
        c.setFillColor(fg)
        c.setFont("Helvetica-Bold", 20)
        c.drawCentredString(x + card_w / 2, y + 10 * mm, val)
        c.setFont("Helvetica", 7)
        c.drawCentredString(x + card_w / 2, y + 4 * mm, lbl)

    doc.y -= card_h + 5 * mm

    # Pass rate bar
    pr = (grand_passed / grand_total * 100) if grand_total else 0
    bar_col = C_GREEN if pr >= 80 else (C_ORANGE if pr >= 50 else C_RED)
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(black)
    c.drawString(
        ML, doc.y, f"Overall Pass Rate: {pr:.1f}%  ({grand_passed}/{grand_total} tests in {grand_dur:.2f}s)")
    doc.y -= 5 * mm
    c.setFillColor(HexColor("#E0E0E0"))
    c.roundRect(ML, doc.y, CW, 4 * mm, 2, fill=1, stroke=0)
    c.setFillColor(bar_col)
    c.roundRect(ML, doc.y, CW * pr / 100, 4 * mm, 2, fill=1, stroke=0)
    doc.y -= 8 * mm

    # Per-phase mini summary table
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(C_DARK)
    c.drawString(ML, doc.y, "Per-Phase Breakdown")
    doc.y -= 5 * mm

    col_x = [ML, ML + 68*mm, ML + 90*mm, ML + 112*mm, ML + 134*mm, ML + 156*mm]
    headers = ["Phase", "Total", "Passed", "Failed", "Skipped", "Duration"]
    c.setFont("Helvetica-Bold", 8)
    c.setFillColor(C_GREY)
    for hdr, x in zip(headers, col_x):
        c.drawString(x, doc.y, hdr)
    doc.y -= 4 * mm

    c.setLineWidth(0.3)
    c.setStrokeColor(C_GREY)
    c.line(ML, doc.y, ML + CW, doc.y)
    doc.y -= 4 * mm

    for p in phases:
        col = PHASE_COLOURS.get(p["name"], C_BLUE)
        c.setFillColor(col)
        c.rect(ML, doc.y - 1 * mm, 3, 5 * mm, fill=1, stroke=0)
        c.setFillColor(black)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(ML + 5*mm, doc.y + 1*mm, p["name"])
        c.setFont("Helvetica", 8)
        vals = [str(p["total"]), str(p["passed"]), str(p["failed"]),
                str(p["skipped"]), f"{p['duration']:.2f}s"]
        for val, x in zip(vals, col_x[1:]):
            c.drawString(x, doc.y + 1*mm, val)
        doc.y -= 6 * mm

    doc.y -= 6 * mm


def draw_phase_header(doc: Doc, phase_name: str, summary: dict, colour):
    """Full-width coloured phase divider with summary stats."""
    doc.check(28 * mm)
    c = doc.c

    bh = 18 * mm
    _rr(c, ML, doc.y - bh, CW, bh, 4, colour)
    # Left accent
    _rr(c, ML, doc.y - bh, 5, bh, 2, HexColor("#FFFFFF"))

    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 13)
    c.drawString(ML + 8*mm, doc.y - 8*mm, phase_name)
    c.setFont("Helvetica", 8)
    total = summary.get("total",   0)
    passed = summary.get("passed",  0)
    failed = summary.get("failed",  0)
    skipped = summary.get("skipped", 0)
    dur = summary.get("duration", 0.0)
    c.drawString(ML + 8*mm, doc.y - 14*mm,
                 f"Total: {total}   Passed: {passed}   Failed: {failed}   Skipped: {skipped}   Duration: {dur:.2f}s")

    doc.y -= bh + 3 * mm

    # Column headers
    c.setFont("Helvetica-Bold", 7)
    c.setFillColor(C_GREY)
    c.drawString(ML + 32*mm, doc.y, "TEST FUNCTION")
    c.drawString(ML + 105*mm, doc.y, "DESCRIPTION")
    c.drawRightString(PAGE_W - MR, doc.y, "DURATION")
    doc.y -= 4 * mm


def draw_test_row(doc: Doc, test: dict, docs: dict, row_idx: int, phase_colour):
    """Draw one test row: badge | function name | description | duration."""
    c = doc.c
    outcome = test.get("outcome", "unknown")
    dur = test.get("duration", 0.0)
    nodeid = test.get("nodeid", "")
    _, func = _clean(nodeid)

    label, badge_col, badge_txt = _outcome_style(outcome)
    description = _docstring_for(test, docs)

    # Wrap description to fit column (~55 chars)
    desc_lines = textwrap.wrap(description, width=52) if description else [" "]
    row_h = max(9 * mm, len(desc_lines) * 4 * mm + 3 * mm)

    # Failed rows need extra space for error message
    err = ""
    if outcome.upper() in ("FAILED", "ERROR"):
        err = _err_line(test)
        if err:
            row_h += 4 * mm

    doc.check(row_h + 2 * mm)

    y = doc.y

    # Alternating background
    if row_idx % 2 == 0:
        c.setFillColor(C_LGREY)
        c.rect(ML, y - row_h, CW, row_h, fill=1, stroke=0)

    # Left phase colour accent bar
    c.setFillColor(phase_colour)
    c.rect(ML, y - row_h, 2, row_h, fill=1, stroke=0)

    # Badge
    _badge(c, ML + 3*mm, y - 7*mm, label, badge_col, badge_txt)

    # Function name
    fn_display = func[:38] + ("…" if len(func) > 38 else "")
    c.setFillColor(black)
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(ML + 33*mm, y - 5*mm, fn_display)

    # Description lines
    c.setFont("Helvetica", 7)
    c.setFillColor(HexColor("#444444"))
    for li, dl in enumerate(desc_lines):
        c.drawString(ML + 105*mm, y - 4*mm - li * 4*mm, dl)

    # Duration
    c.setFont("Helvetica", 7)
    c.setFillColor(C_GREY)
    c.drawRightString(PAGE_W - MR, y - 5*mm, f"{dur:.3f}s")

    # Error message if failed
    if err:
        err_y = y - row_h + (4*mm if err else 0)
        err_display = err[:90] + ("…" if len(err) > 90 else "")
        c.setFillColor(C_RED)
        c.setFont("Helvetica-Oblique", 6.5)
        c.drawString(ML + 35*mm, err_y + 1*mm, f"↳ {err_display}")

    doc.y -= row_h


def draw_phase_section(doc: Doc, phase_name: str, report: dict,
                       docs: dict, colour):
    """Draw the full section for one phase: header + all test rows."""
    summary = {
        "total":    report["summary"].get("total",   0),
        "passed":   report["summary"].get("passed",  0),
        "failed":   report["summary"].get("failed",  0),
        "skipped":  report["summary"].get("skipped", 0),
        "duration": report.get("duration", 0.0),
    }
    tests = report.get("tests", [])

    # Start a new page for each phase only if there isn't enough room for
    # the phase header (18mm) + at least one file sub-header + one row (~25mm total)
    if doc.page == 1 or doc.y < 45 * mm:
        doc.new_page(phase_name)
    else:
        doc.y -= 6 * mm  # breathing space between summary and first phase

    doc._continuation_title = phase_name
    draw_phase_header(doc, phase_name, summary, colour)

    # Group by file
    from collections import OrderedDict
    sections: dict[str, list] = OrderedDict()
    for t in tests:
        fp, _ = _clean(t.get("nodeid", ""))
        sections.setdefault(fp, []).append(t)

    row_idx = 0
    for file_path, file_tests in sections.items():
        # File sub-header
        doc.check(10 * mm)
        c = doc.c
        c.setFillColor(HexColor("#EAECEF"))
        c.rect(ML, doc.y - 6*mm, CW, 6*mm, fill=1, stroke=0)
        c.setFillColor(colour)
        c.rect(ML, doc.y - 6*mm, 2, 6*mm, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 7)
        c.setFillColor(HexColor("#333333"))
        short = file_path.split(
            "tests/")[-1] if "tests/" in file_path else file_path
        c.drawString(ML + 5*mm, doc.y - 4*mm, short)
        doc.y -= 8 * mm

        for test in file_tests:
            draw_test_row(doc, test, docs, row_idx, colour)
            row_idx += 1

        doc.y -= 3 * mm

    return summary


def draw_final_verdict(doc: Doc, phases: list[dict]):
    """Bottom of report   overall pass/fail verdict box."""
    doc.check(20 * mm)
    c = doc.c
    total = sum(p["total"] for p in phases)
    passed = sum(p["passed"] for p in phases)
    failed = sum(p["failed"] for p in phases)
    dur = sum(p["duration"] for p in phases)

    bg = C_GREEN if failed == 0 else C_RED
    msg = "ALL TESTS PASSED ✓" if failed == 0 else f"{failed} TEST(S) FAILED   ACTION REQUIRED ✗"

    _rr(c, ML, doc.y - 14*mm, CW, 14*mm, 4, bg)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(ML + 5*mm, doc.y - 9*mm, msg)
    c.setFont("Helvetica", 8)
    c.setFillColor(HexColor("#CCCCCC"))
    c.drawRightString(PAGE_W - MR - 5*mm, doc.y - 9*mm,
                      f"{passed}/{total} passed across all phases in {dur:.2f}s")
    doc.y -= 18 * mm


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate combined Bravo test report")
    parser.add_argument("--unit",        default=None,
                        help="Path to unit JSON report")
    parser.add_argument("--integration", default=None,
                        help="Path to integration JSON report")
    parser.add_argument("--e2e",         default=None,
                        help="Path to E2E JSON report")
    parser.add_argument("--output",      required=True, help="Output PDF path")
    parser.add_argument("--test-dirs",   nargs="*",
                        default=["test-service/tests"],
                        help="Directories to scan for test docstrings")
    args = parser.parse_args()

    phase_cfg = [
        ("Unit Testing",                   args.unit,        C_BLUE),
        ("Integration Testing",            args.integration, C_TEAL),
        ("End-to-End Pipeline Validation", args.e2e,         C_PURPLE),
    ]

    # Load whichever reports exist
    loaded = [(name, _load(path), col)
              for name, path, col in phase_cfg if _load(path) is not None]

    if not loaded:
        print("[ERROR] No valid JSON reports found. Provide at least one of --unit / --integration / --e2e")
        sys.exit(1)

    # Extract docstrings from source
    docs = _extract_docstrings(args.test_dirs)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(
        args.output) else ".", exist_ok=True)

    doc = Doc(args.output)
    doc.page = 1
    doc._mini_header()
    doc.y = PAGE_H - 16 * mm

    # Cover / page 1
    draw_cover_header(doc, generated_at)

    phase_summaries = []
    for name, report, col in loaded:
        s = report.get("summary", {})
        phase_summaries.append({
            "name":     name,
            "total":    s.get("total",   0),
            "passed":   s.get("passed",  0),
            "failed":   s.get("failed",  0),
            "skipped":  s.get("skipped", 0),
            "duration": report.get("duration", 0.0),
        })

    draw_overall_summary(doc, phase_summaries)

    # One section per phase   page breaks handled inside draw_phase_section
    for name, report, col in loaded:
        draw_phase_section(doc, name, report, docs, col)

    draw_final_verdict(doc, phase_summaries)
    doc.save()

    total_tests = sum(p["total"] for p in phase_summaries)
    print(
        f"[OK] Combined PDF -> {args.output}  ({doc.page} pages, {total_tests} tests across {len(loaded)} phase(s))")


if __name__ == "__main__":
    main()
