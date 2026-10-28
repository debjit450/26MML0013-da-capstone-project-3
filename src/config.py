"""Shared paths and connection settings. Override with environment variables."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "outputs"
CHARTS = OUT / "charts"
ANALYTICS = OUT / "analytics"

PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://library:library@localhost:5433/library")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_BORROWING = os.getenv("TOPIC_BORROWING", "library.borrowing")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "library.borrowing.dlq")

# Reference "today" for the historical dataset. Loans without a return date
# and past their due date on this day are OVERDUE.
AS_OF = os.getenv("AS_OF", "2026-10-27")

UNKNOWN_MEMBER = "M-UNKNOWN"

for d in (RAW, PROCESSED, CHARTS, ANALYTICS):
    d.mkdir(parents=True, exist_ok=True)
