"""Batch ingestion: CSV files -> clean -> PostgreSQL (dimensions, facts, analytics tables).
Safe to re-run: batch rows are replaced, rows that arrived through Kafka are kept."""
import uuid

import pandas as pd
from sqlalchemy import create_engine, text

import analysis
import clean
from config import PG_URL, UNKNOWN_MEMBER


def load():
    run_id = "batch-" + uuid.uuid4().hex[:8]
    books, members, txn, report = clean.run_all()
    a = analysis.compute_analytics(books, members, txn)

    mem = members[["member_id", "first_name", "last_name", "email", "phone", "join_date", "membership_type",
                   "home_branch_id", "age_group", "join_date_imputed", "email_missing"]].copy()
    mem["join_date"] = mem["join_date"].dt.date
    fact = txn.copy()
    fact["issue_ts"] = fact["issue_ts"].dt.tz_localize("UTC")
    for c in ("due_date", "return_date"):
        fact[c] = fact[c].dt.date
    fact["ingest_mode"], fact["source_ref"], fact["ingest_run_id"] = "batch", "borrowing.csv", run_id

    month = lambda df: df.assign(month=df["month"].dt.date)
    bs = a["book_stats"].copy()
    bs["last_borrowed"] = bs["last_borrowed"].dt.tz_localize("UTC")
    tables = [("dim_books", books), ("dim_members", mem), ("fact_borrowing", fact),
              ("analytics_monthly_issues", month(a["monthly"])), ("analytics_book_stats", bs),
              ("analytics_membership_monthly", month(a["membership"]))]

    eng = create_engine(PG_URL, future=True)
    with eng.begin() as cx:
        cx.execute(text("DELETE FROM fact_borrowing WHERE ingest_mode = 'batch'"))
        cx.execute(text("DELETE FROM dim_members WHERE member_id <> :u"), {"u": UNKNOWN_MEMBER})
        for t in ("dim_books", "analytics_monthly_issues", "analytics_book_stats", "analytics_membership_monthly"):
            cx.execute(text(f"DELETE FROM {t}"))
        for name, df in tables:
            df.astype(object).where(df.notna(), None).to_sql(name, cx, if_exists="append", index=False,
                                                              method="multi", chunksize=2000)
    print(f"batch load complete, run_id={run_id}")
    for k, v in report.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    load()
