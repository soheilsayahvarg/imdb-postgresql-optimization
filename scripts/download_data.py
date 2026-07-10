#!/usr/bin/env python3
"""
Download the IMDb .tsv.gz dumps into data/.

    python scripts/download_data.py            # the 6 datasets the scenarios need
    python scripts/download_data.py --all      # ...plus title.akas
    python scripts/download_data.py --verify-full   # re-check CRC of what is on disk

Design notes:

  * Downloads are RESUMABLE. The IMDb server closed two of our connections
    mid-transfer while probing it, so a 737 MB file finishing on the first try is
    not something to rely on. Each file lands in <name>.part and is only renamed
    once its length matches the server's Content-Length.

  * The .gz files are never decompressed here. producer.py streams them straight
    out of gzip, which keeps ~10 GB of intermediate TSV off a drive that only has
    ~53 GB free.

  * Before downloading anything the script refuses to start if the free space is
    less than 1.5x what the remaining files need.
"""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import sys
import time
from pathlib import Path

import requests
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import DATA_DIR, Dataset, human_bytes, select_datasets  # noqa: E402

CHUNK = 1 << 20          # 1 MiB
CONNECT_TIMEOUT = 15     # seconds
READ_TIMEOUT = 60        # seconds
MAX_ATTEMPTS = 6


def remote_size(session: requests.Session, ds: Dataset) -> int:
    """
    Content-Length of the dump, in bytes.

    HEAD works today, but it is the single request that would abort the whole run
    before a byte is fetched, so fall back to a one-byte ranged GET and read the
    total out of Content-Range.
    """
    try:
        resp = session.head(ds.url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
        resp.raise_for_status()
        length = resp.headers.get("Content-Length")
        if length is not None:
            return int(length)
    except requests.RequestException:
        pass

    resp = session.get(ds.url, headers={"Range": "bytes=0-0"}, stream=True,
                       timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    content_range = resp.headers.get("Content-Range", "")   # "bytes 0-0/8547396"
    resp.close()
    if "/" not in content_range:
        raise RuntimeError(f"{ds.filename}: server reports neither Content-Length nor Content-Range")
    return int(content_range.rsplit("/", 1)[1])


def verify_gzip(path: Path, full: bool) -> None:
    """
    Raise if the file is not a readable gzip stream.

    full=False reads only the header line, which catches a truncated *start*.
    full=True streams the whole member, so the gzip module checks the trailing
    CRC32 and ISIZE -- the only way to catch a silently truncated *end*.
    """
    with gzip.open(path, "rb") as fh:
        if full:
            while fh.read(CHUNK):
                pass
        else:
            header = fh.readline()
            if not header or b"\t" not in header:
                raise RuntimeError(f"{path.name}: first line is not a TSV header")


def download_one(session: requests.Session, ds: Dataset, force: bool) -> tuple[str, int]:
    """Returns (status, bytes_on_disk). Status is one of: downloaded, resumed, skipped."""
    total = remote_size(session, ds)
    part = ds.path.with_name(ds.path.name + ".part")

    if ds.path.exists() and not force:
        on_disk = ds.path.stat().st_size
        if on_disk == total:
            return "skipped", on_disk
        tqdm.write(f"  {ds.filename}: size mismatch ({on_disk:,} != {total:,}), re-downloading")
        ds.path.unlink()

    if force and part.exists():
        part.unlink()

    resumed = part.exists() and part.stat().st_size > 0

    for attempt in range(1, MAX_ATTEMPTS + 1):
        have = part.stat().st_size if part.exists() else 0
        if have > total:                      # server file shrank; start over
            part.unlink()
            have = 0
        if have == total:
            break

        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            resp = session.get(ds.url, headers=headers, stream=True,
                               timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            resp.raise_for_status()

            # A server that ignores Range answers 200 with the whole body.
            if have and resp.status_code != 206:
                tqdm.write(f"  {ds.filename}: server ignored Range, restarting from 0")
                part.unlink()
                have = 0

            mode = "ab" if have else "wb"
            with open(part, mode) as fh, tqdm(
                total=total, initial=have, unit="B", unit_scale=True, unit_divisor=1024,
                desc=f"{ds.name:<17}", leave=True, ncols=88,
            ) as bar:
                for chunk in resp.iter_content(CHUNK):
                    fh.write(chunk)
                    bar.update(len(chunk))

        except (requests.RequestException, ConnectionResetError, OSError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"{ds.filename}: giving up after {attempt} attempts") from exc
            backoff = min(2 ** attempt, 30)
            tqdm.write(f"  {ds.filename}: {type(exc).__name__} on attempt {attempt}, "
                       f"retrying in {backoff}s (have {human_bytes(part.stat().st_size if part.exists() else 0)})")
            time.sleep(backoff)
            continue

        if part.stat().st_size == total:
            break

        # The server hung up cleanly but early. Resume on the next attempt --
        # unless this was the last one, in which case do not sleep before failing.
        resumed = True
        if attempt < MAX_ATTEMPTS:
            time.sleep(min(2 ** attempt, 30))

    got = part.stat().st_size
    if got != total:
        raise RuntimeError(f"{ds.filename}: got {got:,} bytes, expected {total:,}")

    verify_gzip(part, full=False)
    os.replace(part, ds.path)                 # atomic: a .gz that exists is complete
    return ("resumed" if resumed else "downloaded"), total


def main() -> int:
    ap = argparse.ArgumentParser(description="Download the IMDb dumps into data/")
    ap.add_argument("--tables", nargs="*", default=None,
                    help="subset, e.g. title_basics title_ratings (default: all six required)")
    ap.add_argument("--all", action="store_true",
                    help="also fetch title.akas, which no report scenario reads")
    ap.add_argument("--force", action="store_true", help="re-download even if complete")
    ap.add_argument("--verify-full", action="store_true",
                    help="stream every local .gz end-to-end so its CRC32 is checked")
    args = ap.parse_args()

    datasets = select_datasets(args.tables, include_optional=args.all)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.verify_full:
        print("Verifying local files (full CRC check)...")
        bad = 0
        for ds in datasets:
            if not ds.path.exists():
                print(f"  [MISSING] {ds.filename}")
                bad += 1
                continue
            try:
                verify_gzip(ds.path, full=True)
                print(f"  [OK]      {ds.filename}  {human_bytes(ds.path.stat().st_size)}")
            except Exception as exc:
                print(f"  [CORRUPT] {ds.filename}: {exc}")
                bad += 1
        return 1 if bad else 0

    session = requests.Session()
    session.headers["User-Agent"] = "imdb-db-project/1.0 (university coursework)"

    print("Planned downloads:")
    needed = 0.0
    for ds in datasets:
        flag = "" if ds.required else "   (optional: no scenario reads it)"
        print(f"  {ds.name:<17} ~{ds.gz_megabytes:>7,.1f} MB -> {ds.table}{flag}")
        if not ds.path.exists():
            needed += ds.gz_megabytes * 1024 * 1024

    free = shutil.disk_usage(DATA_DIR).free
    print(f"\n  to fetch : {human_bytes(needed)}")
    print(f"  free     : {human_bytes(free)}")
    if free < needed * 1.5:
        print("\nERROR: not enough free space (want 1.5x headroom). Free some disk first.")
        return 1

    print()
    results = []
    for ds in datasets:
        status, size = download_one(session, ds, args.force)
        results.append((ds, status, size))

    print("\nSummary")
    total = 0
    for ds, status, size in results:
        total += size
        print(f"  {status:<11} {ds.filename:<26} {human_bytes(size):>12}")
    print(f"  {'':<11} {'TOTAL':<26} {human_bytes(total):>12}")
    print("\nFiles stay compressed. producer.py streams them straight out of gzip.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
