"""Stream borrowing transactions to Kafka.

Simulates the circulation desk: picks a random member and book from the loaded
data and publishes one JSON event per borrow. About 5% of events have the member
id missing to exercise the missing-borrower rules.

  python producer.py --count 200 --rate 5      # 200 events, 5 per second
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import KAFKA_BOOTSTRAP, PROCESSED, TOPIC_BORROWING, TOPIC_DLQ


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(t, 1, 1) for t in (TOPIC_BORROWING, TOPIC_DLQ) if t not in existing]
    if not new:
        return
    for f in admin.create_topics(new).values():
        f.result()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--rate", type=float, default=5, help="events per second")
    args = ap.parse_args()

    members = pd.read_csv(PROCESSED / "members_clean.csv")[["member_id", "email"]].dropna().to_dict("records")
    books = pd.read_csv(PROCESSED / "books_clean.csv")["book_id"].tolist()
    branches = ["B01", "B02", "B03", "B04", "B05"]

    ensure_topics()
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for i in range(args.count):
        m, now = random.choice(members), datetime.now(timezone.utc)
        event = {
            "txn_id": "S" + uuid.uuid4().hex[:10].upper(),
            "member_id": m["member_id"], "borrower_email": m["email"],
            "book_id": random.choice(books), "branch_id": random.choice(branches),
            "issue_ts": now.isoformat(), "due_date": (now + timedelta(days=14)).date().isoformat(),
        }
        if random.random() < 0.05:
            event["member_id"] = None
            if random.random() < 0.5:
                event["borrower_email"] = None
        p.produce(TOPIC_BORROWING, key=event["txn_id"], value=json.dumps(event))
        p.poll(0)
        time.sleep(1 / args.rate)
    p.flush()
    print(f"published {args.count} events to {TOPIC_BORROWING}")


if __name__ == "__main__":
    main()
