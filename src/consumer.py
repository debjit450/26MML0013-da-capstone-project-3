"""Consume borrowing events from Kafka, apply the same cleaning rules as the
batch job and upsert into PostgreSQL. Invalid events go to the dead letter topic.

  python consumer.py
"""
import json
import uuid
from datetime import datetime

from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine, text

from config import KAFKA_BOOTSTRAP, PG_URL, TOPIC_BORROWING, TOPIC_DLQ, UNKNOWN_MEMBER

INSERT = text("""
INSERT INTO fact_borrowing (txn_id, member_id, book_id, branch_id, issue_ts, due_date, status,
    member_missing, member_recovered, ingest_mode, source_ref, ingest_run_id)
VALUES (:txn_id, :member_id, :book_id, :branch_id, :issue_ts, :due_date, 'ISSUED',
    :member_missing, :member_recovered, 'stream', :source_ref, :run_id)
ON CONFLICT (txn_id) DO NOTHING""")


def transform(event: dict, email_to_id: dict, known_members: set, known_books: set):
    """Validate and clean one event. Returns a row dict, or raises ValueError."""
    for f in ("txn_id", "book_id", "branch_id", "issue_ts"):
        if not event.get(f):
            raise ValueError(f"missing required field: {f}")
    if event["book_id"] not in known_books:
        raise ValueError(f"unknown book_id {event['book_id']}")
    datetime.fromisoformat(event["issue_ts"])  # raises ValueError if malformed

    member, recovered = event.get("member_id"), False
    if not member and event.get("borrower_email"):
        member = email_to_id.get(event["borrower_email"].strip().lower())
        recovered = member is not None
    missing = member not in known_members
    return {**{k: event[k] for k in ("txn_id", "book_id", "branch_id", "issue_ts")},
            "due_date": event.get("due_date"), "member_id": UNKNOWN_MEMBER if missing else member,
            "member_missing": missing, "member_recovered": recovered}


def main():
    eng = create_engine(PG_URL, future=True)
    with eng.connect() as cx:
        members = cx.execute(text("SELECT member_id, email FROM dim_members")).all()
        known_books = {r[0] for r in cx.execute(text("SELECT book_id FROM dim_books"))}
    known_members = {m for m, _ in members}
    email_to_id = {e.lower(): m for m, e in members if e}

    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "library-pg-loader",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    dlq = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    c.subscribe([TOPIC_BORROWING])
    run_id = "stream-" + uuid.uuid4().hex[:8]
    print(f"consuming {TOPIC_BORROWING}, run_id={run_id}. Ctrl+C to stop.")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("kafka error:", msg.error()); continue
            ref = f"kafka:{msg.topic()}/{msg.partition()}/{msg.offset()}"
            try:
                row = transform(json.loads(msg.value()), email_to_id, known_members, known_books)
                with eng.begin() as cx:
                    cx.execute(INSERT, {**row, "source_ref": ref, "run_id": run_id})
            except (ValueError, json.JSONDecodeError) as e:
                dlq.produce(TOPIC_DLQ, value=json.dumps({"ref": ref, "error": str(e), "raw": msg.value().decode(errors="replace")}))
                dlq.poll(0)
            c.commit(msg)  # commit only after the row is stored or dead-lettered
    except KeyboardInterrupt:
        pass
    finally:
        c.close(); dlq.flush()


if __name__ == "__main__":
    main()
