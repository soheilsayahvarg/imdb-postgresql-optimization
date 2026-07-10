"""
Shared configuration for the IMDb ingestion scripts.

Holds the dataset registry, the database connection factory, and clean_value().
Imported by download_data.py, producer.py and consumer.py so that the six
datasets are described in exactly one place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

load_dotenv(ROOT / "resources" / ".env")

IMDB_BASE_URL = "https://datasets.imdbws.com"

DEFAULT_QUEUE = "imdb_ingest"


# -----------------------------------------------------------------------------
#  Dataset registry
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Dataset:
    name: str                      # "title.basics"
    table: str                     # "title_basics"
    gz_megabytes: float            # measured from the live server's Content-Length
    scenarios: tuple[int, ...]     # which of the 8 report scenarios need this table

    @property
    def filename(self) -> str:
        return f"{self.name}.tsv.gz"

    @property
    def url(self) -> str:
        return f"{IMDB_BASE_URL}/{self.filename}"

    @property
    def path(self) -> Path:
        return DATA_DIR / self.filename

    @property
    def required(self) -> bool:
        """False only for title.akas, which no scenario reads."""
        return bool(self.scenarios)


# Ordered by compressed size, ascending. Loading smallest-first means the real
# on-disk cost of each table can be measured with pg_total_relation_size() before
# committing to the two giants at the end.
DATASETS: tuple[Dataset, ...] = (
    Dataset("title.ratings",    "title_ratings",     8.2, (1, 2, 3, 4)),
    Dataset("title.episode",    "title_episode",    51.5, (6,)),
    Dataset("title.crew",       "title_crew",       78.4, (3,)),
    Dataset("title.basics",     "title_basics",    213.6, (1, 2, 4, 5, 7, 8)),
    Dataset("name.basics",      "name_basics",     292.1, (3, 5)),
    Dataset("title.principals", "title_principals", 736.9, (5,)),
    # No scenario reads title_akas. It is downloaded and loaded only with --all,
    # and it is the first thing to drop if the ~53 GB of free disk runs short.
    Dataset("title.akas",       "title_akas",      482.6, ()),
)

BY_TABLE = {d.table: d for d in DATASETS}
BY_NAME = {d.name: d for d in DATASETS}

REQUIRED = tuple(d for d in DATASETS if d.required)


def select_datasets(tables: list[str] | None, include_optional: bool) -> tuple[Dataset, ...]:
    """Resolve a --tables/--all selection into an ordered dataset tuple."""
    if tables:
        chosen = []
        for t in tables:
            key = t.replace(".", "_")
            if key not in BY_TABLE:
                raise SystemExit(f"unknown table {t!r}; known: {', '.join(BY_TABLE)}")
            chosen.append(BY_TABLE[key])
        # Preserve registry order (ascending size) regardless of CLI order.
        return tuple(d for d in DATASETS if d in chosen)
    return DATASETS if include_optional else REQUIRED


# -----------------------------------------------------------------------------
#  Database
# -----------------------------------------------------------------------------

def connect(autocommit: bool = True):
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "imdb"),
        user=os.getenv("PGUSER", "imdb"),
        password=os.getenv("PGPASSWORD", "imdb_pass"),
    )
    conn.autocommit = autocommit
    # The payloads carry non-ASCII titles verbatim (ensure_ascii=False), so the
    # client encoding must not be left to the platform default.
    conn.set_client_encoding("UTF8")
    return conn


# -----------------------------------------------------------------------------
#  Value handling
# -----------------------------------------------------------------------------

def clean_value(value: str | None) -> str | None:
    r"""
    IMDb encodes SQL NULL as the two-character sequence \N (backslash, capital N).

    Returns None for that marker and for genuinely empty fields, otherwise the
    value unchanged. Deliberately does NOT strip whitespace: a handful of real
    titles begin or end with a space, and silently trimming them would corrupt
    the primary_title column.
    """
    if value is None or value == "" or value == "\\N":
        return None
    return value


def human_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:,.1f} {unit}"
        n /= 1024.0
    return f"{n:,.1f} PiB"
