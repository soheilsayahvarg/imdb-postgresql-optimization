#!/usr/bin/env python3
"""
Generate per-scenario EXPLAIN files from the single source sql/04_scenarios.sql.

    python scripts/gen_explain.py

Slices each block delimited by

    -- @scenario NN | title
    <one SQL statement ending in ;>
    -- @end

and writes sql/explain/scenario_NN.sql, each containing the SAME statement wrapped
in EXPLAIN (ANALYZE, BUFFERS, SETTINGS). The scenario SQL therefore lives in
exactly one place; the EXPLAIN files are always regenerated, never edited.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "sql" / "04_scenarios.sql"
OUT_DIR = ROOT / "sql" / "explain"

# EXPLAIN options. ANALYZE actually runs the query; BUFFERS reports shared/temp
# block I/O (meaningful because track_io_timing=on); SETTINGS records any non-default
# GUCs so the plan is self-documenting. FORMAT TEXT keeps the output diff-friendly.
EXPLAIN_PREFIX = "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)"

BLOCK = re.compile(
    r"^--\s*@scenario\s+(\d+)\s*\|\s*(.+?)\s*$"   # -- @scenario NN | title
    r"(.*?)"                                       # body
    r"^--\s*@end\s*$",                             # -- @end
    re.DOTALL | re.MULTILINE,
)


def main() -> int:
    text = SOURCE.read_text(encoding="utf-8")
    blocks = list(BLOCK.finditer(text))
    if not blocks:
        print(f"ERROR: no scenario blocks found in {SOURCE}")
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for m in blocks:
        num = int(m.group(1))
        title = m.group(2).strip()
        # Drop leading comment lines from the body; keep the SQL statement itself.
        body_lines = [ln for ln in m.group(3).splitlines()
                      if ln.strip() and not ln.lstrip().startswith("--")]
        query = "\n".join(body_lines).strip()
        if not query.endswith(";"):
            print(f"ERROR: scenario {num:02d} does not end in ';'")
            return 1
        query = query[:-1]                          # EXPLAIN takes the bare statement

        out = OUT_DIR / f"scenario_{num:02d}.sql"
        content = (
            f"-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.\n"
            f"-- Do not edit; edit the source scenario and regenerate.\n"
            f"\\set ON_ERROR_STOP on\n"
            f"SET search_path TO imdb, public;\n"
            f"\\qecho '===== SCENARIO {num:02d}: {title} ====='\n"
            f"{EXPLAIN_PREFIX}\n{query};\n"
        )
        out.write_text(content, encoding="utf-8")
        written.append((num, title, out))

    print(f"Wrote {len(written)} EXPLAIN files to {OUT_DIR}:")
    for num, title, out in written:
        print(f"  scenario_{num:02d}.sql  {title}")
    if len(written) != 8:
        print(f"\nWARNING: expected 8 scenarios, generated {len(written)}.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
