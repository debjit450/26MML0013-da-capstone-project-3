"""Serialize member data into JSON (nested documents) and verify the round trip.

Output:
  data/processed/members.json    one JSON array of member documents
  data/processed/members.jsonl   same records, one per line (stream/Kafka friendly)
"""
import json
from dataclasses import asdict, dataclass, field
from typing import Optional

import pandas as pd

from config import PROCESSED


@dataclass
class Contact:
    email: Optional[str]
    phone: Optional[str]


@dataclass
class Membership:
    type: str
    join_date: Optional[str]
    home_branch_id: str
    join_date_imputed: bool = False


@dataclass
class Member:
    member_id: str
    first_name: str
    last_name: str
    age_group: str
    contact: Contact
    membership: Membership
    flags: dict = field(default_factory=dict)


def _s(v):
    return None if pd.isna(v) else str(v)


def row_to_member(r) -> Member:
    return Member(
        member_id=r["member_id"], first_name=r["first_name"], last_name=r["last_name"],
        age_group=r["age_group"],
        contact=Contact(email=_s(r["email"]), phone=_s(r["phone"])),
        membership=Membership(
            type=r["membership_type"],
            join_date=None if pd.isna(r["join_date"]) else pd.Timestamp(r["join_date"]).strftime("%Y-%m-%d"),
            home_branch_id=r["home_branch_id"], join_date_imputed=bool(r["join_date_imputed"])),
        flags={"email_missing": bool(r["email_missing"])},
    )


def serialize(df: pd.DataFrame) -> list[dict]:
    return [asdict(row_to_member(r)) for _, r in df.iterrows()]


def deserialize(docs: list[dict]) -> list[Member]:
    return [Member(**{**d, "contact": Contact(**d["contact"]), "membership": Membership(**d["membership"])})
            for d in docs]


def main():
    df = pd.read_csv(PROCESSED / "members_clean.csv", dtype={"phone": "string"})
    docs = serialize(df)
    (PROCESSED / "members.json").write_text(json.dumps(docs, indent=2, ensure_ascii=False))
    with open(PROCESSED / "members.jsonl", "w") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    # round trip check: JSON -> objects -> dicts must equal what we wrote
    back = json.loads((PROCESSED / "members.json").read_text())
    assert [asdict(m) for m in deserialize(back)] == docs, "round trip mismatch"
    print(f"serialized {len(docs)} members -> members.json / members.jsonl (round trip OK)")


if __name__ == "__main__":
    main()
