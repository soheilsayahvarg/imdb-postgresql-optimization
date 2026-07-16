#!/usr/bin/env python3
"""
Run the 8 scenarios and collect timings + EXPLAIN plans.

    python scripts/run_scenarios.py                 # ANALYZE, then explain + time all 8
    python scripts/run_scenarios.py --phase after   # label the timings 'after' (post-index)
    python scripts/run_scenarios.py --only 5 8       # just those scenarios
    python scripts/run_scenarios.py --repeat 3       # take the best of N timed runs
    python scripts/run_scenarios.py --no-analyze     # skip the ANALYZE step

For each scenario it:
  1. writes the EXPLAIN (ANALYZE, BUFFERS) plan to results/explain/scenario_NN[.after].txt
  2. times the plain query (best of --repeat) and appends a row to
     results/timings/scenario_timings.csv with (phase, scenario, seconds, rows)

The EXPLAIN files are produced by scripts/gen_explain.py from sql/04_scenarios.sql,
so this runner never contains the query text. The plain query it times is sliced
from the same source, guaranteeing the plan and the timing measure the same SQL.

This is the convenience wrapper. The equivalent raw `docker exec` commands are in
the Part E notes; both read the same generated files.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import connect  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "sql" / "04_scenarios.sql"
EXPLAIN_DIR = ROOT / "sql" / "explain"
OUT_EXPLAIN = ROOT / "results" / "explain"
OUT_TIMINGS = ROOT / "results" / "timings"

BLOCK = re.compile(
    r"^--\s*@scenario\s+(\d+)\s*\|\s*(.+?)\s*$(.*?)^--\s*@end\s*$",
    re.DOTALL | re.MULTILINE,
)


def load_scenarios() -> dict[int, tuple[str, str]]:
    text = SOURCE.read_text(encoding="utf-8")
    out = {}
    for m in BLOCK.finditer(text):
        num = int(m.group(1))
        title = m.group(2).strip()
        body = [ln for ln in m.group(3).splitlines()
                if ln.strip() and not ln.lstrip().startswith("--")]
        out[num] = (title, "\n".join(body).strip())
    return out


def apply_session(cur, args) -> None:
    """
    Session GUCs shared by every scenario run in this connection.

    docker-compose.yml keeps a conservative global work_mem=32MB precisely so the
    baseline shows the spills; the scenarios raise it PER SESSION here instead of
    globally, exactly as the compose comment promises. temp_file_limit and
    statement_timeout are seatbelts so a runaway heavy plan cannot fill the ~52 GB
    drive or run forever.
    """
    cur.execute("SET search_path TO imdb, public")
    cur.execute("SET work_mem = %s", (args.work_mem,))
    cur.execute("SET temp_file_limit = %s", (args.temp_file_limit,))
    cur.execute("SET statement_timeout = %s", (args.statement_timeout,))


def run_analyze(conn) -> None:
    print("ANALYZE (refresh planner statistics)...", flush=True)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO imdb, public")
        for t in ("title_basics", "title_ratings", "title_crew", "title_episode",
                  "name_basics", "title_principals", "title_akas"):
            started = time.monotonic()
            cur.execute(f"ANALYZE imdb.{t}")
            print(f"  analyzed {t:<18} {time.monotonic() - started:6.1f}s", flush=True)


_EXEC_TIME = re.compile(r"Execution Time:\s*([\d.]+)\s*ms")


def explain_to_file(conn, args, num: int, title: str, query: str, suffix: str) -> tuple[Path, float | None]:
    """Run EXPLAIN (ANALYZE, BUFFERS), save the plan, and return the server-side
    Execution Time in ms parsed from the plan -- the authoritative query timing,
    free of client fetch/transfer overhead."""
    OUT_EXPLAIN.mkdir(parents=True, exist_ok=True)
    path = OUT_EXPLAIN / f"scenario_{num:02d}{suffix}.txt"
    with conn.cursor() as cur:
        apply_session(cur, args)
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT) {query}")
        plan = "\n".join(r[0] for r in cur.fetchall())
    header = f"===== SCENARIO {num:02d}: {title} =====\n"
    path.write_text(header + plan + "\n", encoding="utf-8")
    m = _EXEC_TIME.search(plan)
    return path, (float(m.group(1)) if m else None)


def time_query(conn, args, query: str, repeat: int) -> tuple[float, int]:
    """Best-of-N client wall time (execute + fetchall). Recorded alongside the
    server-side Execution Time; the two together show transfer overhead."""
    best = None
    rows = 0
    with conn.cursor() as cur:
        apply_session(cur, args)
        for _ in range(repeat):
            started = time.monotonic()
            cur.execute(query)
            fetched = cur.fetchall()
            elapsed = time.monotonic() - started
            rows = len(fetched)
            best = elapsed if best is None else min(best, elapsed)
    return best or 0.0, rows


def merge_timings(path: Path, phase: str, new_rows: list) -> None:
    """Upsert by (phase, scenario): re-running a phase replaces its rows rather
    than appending duplicates, so the before/after report reads one row per pair."""
    fieldnames = ["phase", "scenario", "title", "exec_ms", "wall_s", "rows"]
    existing = []
    if path.exists():
        with open(path, newline="", encoding="utf-8") as fh:
            existing = [r for r in csv.DictReader(fh)]
    replaced = {(phase, str(r[1])) for r in new_rows}
    kept = [r for r in existing if (r["phase"], r["scenario"]) not in replaced]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(fieldnames)
        for r in kept:
            w.writerow([r[f] for f in fieldnames])
        w.writerows(new_rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run and time the 8 scenarios")
    ap.add_argument("--phase", default="before", help="label for the timings CSV (before/after)")
    ap.add_argument("--only", nargs="*", type=int, default=None, help="scenario numbers to run")
    ap.add_argument("--repeat", type=int, default=1, help="timed runs per scenario; best is kept")
    ap.add_argument("--no-analyze", dest="analyze", action="store_false", default=True)
    ap.add_argument("--explain-only", action="store_true", help="write plans, skip timing")
    ap.add_argument("--work-mem", default="64MB",
                    help="per-session work_mem for the runs (global stays 32MB)")
    ap.add_argument("--temp-file-limit", default="20GB",
                    help="per-session temp_file_limit seatbelt against a runaway spill")
    ap.add_argument("--statement-timeout", default="30min",
                    help="per-session statement_timeout seatbelt")
    args = ap.parse_args()

    scenarios = load_scenarios()
    if len(scenarios) != 8:
        print(f"ERROR: parsed {len(scenarios)} scenarios from {SOURCE}, expected 8")
        return 1
    if args.only:
        unknown = [n for n in args.only if n not in scenarios]
        if unknown:
            print(f"ERROR: unknown scenario number(s) {unknown}; valid are {sorted(scenarios)}")
            return 1
    chosen = sorted(args.only) if args.only else sorted(scenarios)

    suffix = "" if args.phase == "before" else f".{args.phase}"
    conn = connect(autocommit=True)
    try:
        if args.analyze:
            run_analyze(conn)

        OUT_TIMINGS.mkdir(parents=True, exist_ok=True)
        timings_path = OUT_TIMINGS / "scenario_timings.csv"
        rows_out = []

        for num in chosen:
            title, query = scenarios[num]
            print(f"\n[{num:02d}] {title}", flush=True)
            plan_path, exec_ms = explain_to_file(conn, args, num, title, query, suffix)
            print(f"     plan  -> {plan_path}  (exec {exec_ms:.1f} ms)"
                  if exec_ms is not None else f"     plan  -> {plan_path}", flush=True)
            if not args.explain_only:
                secs, nrows = time_query(conn, args, query, args.repeat)
                print(f"     time  -> exec {exec_ms or 0:.1f} ms | wall {secs:.3f}s "
                      f"({nrows:,} rows, best of {args.repeat})", flush=True)
                rows_out.append([args.phase, num, title,
                                 f"{exec_ms:.1f}" if exec_ms is not None else "",
                                 f"{secs:.3f}", nrows])

        if rows_out:
            merge_timings(timings_path, args.phase, rows_out)
            print(f"\nTimings written to {timings_path} (phase {args.phase!r} rows replaced)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
