#!/usr/bin/env python3
"""
Build the before/after performance comparison table for Part F.

    python scripts/compare_timings.py

Reads results/timings/scenario_timings.csv (written by run_scenarios.py, one row
per (phase, scenario)) and the paired EXPLAIN text files in results/explain/, and
writes a Markdown comparison table to results/comparison.md.

This script only SUMMARIZES whatever has actually been measured -- it does not
run any query itself. If scenario_timings.csv has no 'after' rows yet (indexes
not yet created and re-run against a live database), the corresponding table
cells are left as 'n/a' rather than a guessed number.

Node-type detection is a plain regex scan of the EXPLAIN (FORMAT TEXT) output --
good enough to say "a Seq Scan disappeared / a Bitmap Index Scan appeared", not
a full plan-tree parser. Read the underlying .txt files for the exact plan shape.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TIMINGS = ROOT / "results" / "timings" / "scenario_timings.csv"
EXPLAIN_DIR = ROOT / "results" / "explain"
OUT = ROOT / "results" / "comparison.md"

NODE_TYPES = ["Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan",
              "Bitmap Index Scan", "Nested Loop", "Hash Join", "Merge Join"]

_BUFFERS = re.compile(r"Buffers:\s*(.+)")
_HIT = re.compile(r"hit=(\d+)")
_READ = re.compile(r"read=(\d+)")


def load_timings() -> dict[int, dict[str, dict]]:
    """{scenario_num: {"before": row_or_None, "after": row_or_None}}. Rows whose
    scenario number falls outside the 8 known scenarios (or an unrecognized
    phase) are skipped rather than raising -- this file only summarizes, it
    should never crash on a malformed or forward-compatible CSV row."""
    out: dict[int, dict[str, dict]] = {n: {"before": None, "after": None} for n in range(1, 9)}
    if not TIMINGS.exists():
        return out
    with open(TIMINGS, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            num = int(row["scenario"])
            phase = row["phase"]
            if num not in out or phase not in ("before", "after"):
                continue
            out[num][phase] = row
    return out


def buffer_totals(text: str) -> tuple[int, int]:
    """Shared hit/read from the OUTERMOST plan node's Buffers line only.

    EXPLAIN (ANALYZE, BUFFERS) per-node buffer counts are cumulative down the
    plan tree -- a parent's Buffers line already includes everything its
    children did, the same way its "actual time" already includes child
    execution time. The root node's line (the first "Buffers:" line in the
    text, since EXPLAIN prints top-down) is therefore already the whole
    plan's total; summing every node's line would count every descendant
    once for itself and again for every ancestor.

    A Buffers line lists comma-separated groups, e.g.
    "shared hit=100 read=5, temp read=3 written=1" -- "shared" is stated once
    per group, not once per hit=/read=/written=/dirtied= field, so hit=/read=
    must be searched within the "shared" group specifically, not matched
    against the whole line (which would also pick up an unrelated temp/local
    read= and misreport it as a shared disk read).
    """
    m = _BUFFERS.search(text)
    if not m:
        return 0, 0
    shared_segment = next(
        (part.strip() for part in m.group(1).split(",") if part.strip().startswith("shared")),
        None,
    )
    if shared_segment is None:
        return 0, 0
    hit_m = _HIT.search(shared_segment)
    read_m = _READ.search(shared_segment)
    return (int(hit_m.group(1)) if hit_m else 0,
            int(read_m.group(1)) if read_m else 0)


def node_summary(text: str) -> str:
    counts = {n: text.count(n) for n in NODE_TYPES}
    # "Index Scan" is a substring of "Bitmap Index Scan" -- every Bitmap Index
    # Scan occurrence also matches the plain "Index Scan" pattern, so subtract
    # the overlap or a GIN-backed Bitmap Index Scan gets double-reported as a
    # (nonexistent) plain Index Scan too.
    counts["Index Scan"] -= counts["Bitmap Index Scan"]
    counts = {n: c for n, c in counts.items() if c > 0}
    return ", ".join(f"{n}x{c}" if c > 1 else n for n, c in counts.items()) or "n/a"


def explain_text(num: int, suffix: str) -> str | None:
    path = EXPLAIN_DIR / f"scenario_{num:02d}{suffix}.txt"
    return path.read_text(encoding="utf-8") if path.exists() else None


def fmt_ms(row: dict | None) -> str:
    if row is None or not row.get("exec_ms"):
        return "n/a"
    return f"{float(row['exec_ms']):.1f}"


def pct_change(before_row: dict | None, after_row: dict | None) -> str:
    if not before_row or not after_row:
        return "n/a"
    if not before_row.get("exec_ms") or not after_row.get("exec_ms"):
        return "n/a"
    b, a = float(before_row["exec_ms"]), float(after_row["exec_ms"])
    if b <= 0:
        return "n/a"
    return f"{(b - a) / b * 100:+.1f}%"


def main() -> int:
    timings = load_timings()

    lines = [
        "# Baseline vs. Indexed Performance Comparison",
        "",
        "Auto-generated by scripts/compare_timings.py from "
        "results/timings/scenario_timings.csv and results/explain/*.txt. "
        "'n/a' means that phase has not actually been run yet.",
        "",
        "| # | Exec ms (before) | Exec ms (after) | Change | Plan nodes (before) | Plan nodes (after) | Buffers (before) | Buffers (after) |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for num in range(1, 9):
        before_row, after_row = timings[num]["before"], timings[num]["after"]
        before_ms, after_ms = fmt_ms(before_row), fmt_ms(after_row)
        change = pct_change(before_row, after_row)

        before_text = explain_text(num, "")
        after_text = explain_text(num, ".after")
        before_nodes = node_summary(before_text) if before_text else "n/a"
        after_nodes = node_summary(after_text) if after_text else "n/a"

        before_buf = "n/a"
        if before_text:
            hit, read = buffer_totals(before_text)
            before_buf = f"hit={hit} read={read}"
        after_buf = "n/a"
        if after_text:
            hit, read = buffer_totals(after_text)
            after_buf = f"hit={hit} read={read}"

        lines.append(
            f"| {num:02d} | {before_ms} | {after_ms} | {change} | "
            f"{before_nodes} | {after_nodes} | {before_buf} | {after_buf} |"
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
