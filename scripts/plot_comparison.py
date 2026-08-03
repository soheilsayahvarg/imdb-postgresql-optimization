#!/usr/bin/env python3
"""
Render results/timings/scenario_timings.csv into a before/after bar chart.

    python scripts/plot_comparison.py

Reads the real (before, after) exec_ms pair for each of the 8 scenarios and
draws one horizontal diverging bar per scenario: percent change in execution
time, green for faster and red for slower. Percent change (not raw ms) is the
right axis here because the raw times span four orders of magnitude (12.7ms to
147,701.3ms) -- a single linear or log axis of absolute times would flatten the
small scenarios to invisible slivers, while percent change puts all 8 on one
comparable scale, including scenario 1's genuine regression.

Labels are English on purpose: matplotlib has no built-in Persian shaping, and
this keeps the figure consistent with how the report already renders every
other precise number and identifier (via \\lr{}) in Latin script.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

ROOT = Path(__file__).resolve().parents[1]
TIMINGS = ROOT / "results" / "timings" / "scenario_timings.csv"
OUT = ROOT / "report" / "figures" / "performance_comparison.pdf"

# Short English titles for the y-axis; kept in sync with sql/04_scenarios.sql by scenario number.
SHORT_TITLE = {
    1: "1. Avg rating by genre",
    2: "2. Top-10 movies by votes",
    3: "3. Top-20 directors",
    4: "4. Yearly avg rating (2000+)",
    5: "5. Multi-genre actors",
    6: "6. Series by season count",
    7: "7. Most common genre pair",
    8: "8. Top-5 longest per genre",
}

# dataviz skill status palette (fixed, never themed): good / critical.
GOOD = "#0ca30c"
CRITICAL = "#d03b3b"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"


def load_pairs() -> dict[int, tuple[float, float]]:
    before: dict[int, float] = {}
    after: dict[int, float] = {}
    with open(TIMINGS, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            num = int(row["scenario"])
            ms = float(row["exec_ms"])
            (before if row["phase"] == "before" else after)[num] = ms
    return {n: (before[n], after[n]) for n in sorted(before) if n in after}


def main() -> int:
    pairs = load_pairs()
    if len(pairs) != 8:
        print(f"ERROR: expected 8 scenarios with both phases, found {len(pairs)}", file=sys.stderr)
        return 1

    nums = sorted(pairs, reverse=True)  # scenario 1 at top, 8 at bottom
    pct = [100.0 * (pairs[n][0] - pairs[n][1]) / pairs[n][0] for n in nums]
    colors = [GOOD if p >= 0 else CRITICAL for p in pct]
    labels = [SHORT_TITLE[n] for n in nums]

    sans = [f.name for f in fm.fontManager.ttflist if f.name in ("Arial", "Segoe UI", "Verdana")]
    plt.rcParams["font.family"] = sans[0] if sans else "DejaVu Sans"

    fig, ax = plt.subplots(figsize=(7.2, 4.0), dpi=200)
    y = range(len(nums))
    bars = ax.barh(y, pct, height=0.55, color=colors, zorder=3)

    ax.axvline(0, color=MUTED, linewidth=1.0, zorder=2)
    ax.set_yticks(list(y), labels)
    ax.tick_params(axis="y", length=0, labelsize=9, labelcolor=INK)
    ax.tick_params(axis="x", length=0, labelsize=8, labelcolor=MUTED)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_visible(False)

    xmin, xmax = min(pct) - 30, max(pct) + 15
    ax.set_xlim(xmin, xmax)
    for rect, p in zip(bars, pct):
        offset = 2.0 if p >= 0 else -6.0
        ha = "left" if p >= 0 else "right"
        ax.text(rect.get_width() + offset, rect.get_y() + rect.get_height() / 2,
                 f"{p:+.0f}%", va="center", ha=ha, fontsize=8.5, color=INK, zorder=4)

    ax.set_xlabel("Change in execution time after indexing  (green = faster, red = slower)",
                   fontsize=8.5, color=MUTED)
    ax.set_title("Before / after indexing -- real measured execution time",
                  fontsize=10.5, color=INK, loc="left", pad=10)

    fig.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT)
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
