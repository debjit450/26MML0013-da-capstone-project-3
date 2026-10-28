"""Cleaning rules for the library datasets, including missing borrower handling.

Missing borrower strategy (borrowing transactions):
  1. member_id present and known        -> keep.
  2. member_id missing, email present
     and matches a member               -> RECOVER member_id from email (member_recovered = True).
  3. still unresolved, or member_id not
     found in the member master         -> assign the sentinel member 'M-UNKNOWN' and set
                                           member_missing = True. The loan is kept so book and
                                           branch metrics stay correct, but membership metrics
                                           ignore it.
Rejected (written to data/processed/rejects.csv): unknown book_id, unparseable issue date.
"""
import json
import re

import numpy as np
import pandas as pd

from config import AS_OF, PROCESSED, RAW, UNKNOWN_MEMBER

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def clean_books(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ("book_id", "title", "author", "genre", "publisher", "home_branch_id"):
        df[c] = df[c].astype("string").str.strip()
    df = df.drop_duplicates("book_id", keep="first")
    df["has_ebook"] = df["has_ebook"].astype(bool)
    return df.reset_index(drop=True)


def clean_members(df: pd.DataFrame, first_borrow: pd.Series | None = None) -> pd.DataFrame:
    df = df.copy()
    df["member_id"] = df["member_id"].astype("string").str.strip().str.upper()
    df = df.drop_duplicates("member_id", keep="first")
    for c in ("first_name", "last_name"):
        df[c] = df[c].astype("string").str.strip().str.title()
    df["email"] = df["email"].astype("string").str.strip().str.lower()
    df.loc[~df["email"].fillna("").str.match(EMAIL_RE), "email"] = pd.NA
    df["phone"] = df["phone"].astype("string").str.replace(r"\D", "", regex=True).replace("", pd.NA)
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")
    df["join_date_imputed"] = False
    if first_borrow is not None:
        # a member cannot borrow before joining, so the first loan date is a safe upper bound
        fill = df["join_date"].isna() & df["member_id"].isin(first_borrow.index)
        df.loc[fill, "join_date"] = df.loc[fill, "member_id"].map(first_borrow).dt.normalize()
        df.loc[fill, "join_date_imputed"] = True
    df["age_group"] = df["age_group"].fillna("Unknown")
    df["email_missing"] = df["email"].isna()
    return df.reset_index(drop=True)


def clean_borrowing(raw: pd.DataFrame, members: pd.DataFrame, books: pd.DataFrame):
    """Return (clean_df, rejects_df, report_dict)."""
    report = {"rows_in": int(len(raw))}
    df = raw.copy()
    for c in ("txn_id", "member_id", "borrower_email", "book_id", "branch_id"):
        df[c] = df[c].astype("string").str.strip()
    df["member_id"] = df["member_id"].str.upper().replace("", pd.NA)
    df["borrower_email"] = df["borrower_email"].str.lower().replace("", pd.NA)

    # 1. exact duplicates by transaction id
    before = len(df)
    df = df.drop_duplicates("txn_id", keep="first")
    report["duplicates_removed"] = int(before - len(df))

    # 2. dates
    df["issue_ts"] = pd.to_datetime(df["issue_datetime"], errors="coerce")
    df["due_date"] = pd.to_datetime(df["due_date"], errors="coerce")
    df["return_date"] = pd.to_datetime(df["return_date"], errors="coerce")
    bad_issue = df["issue_ts"].isna()

    # 3. referential checks
    bad_book = ~df["book_id"].isin(books["book_id"])
    rejects = df[bad_issue | bad_book].copy()
    rejects["reject_reason"] = np.where(bad_issue[bad_issue | bad_book], "bad_issue_date", "unknown_book_id")
    df = df[~(bad_issue | bad_book)].copy()
    report["rejected"] = int(len(rejects))

    # 4. return date earlier than issue date is invalid -> treat as not returned yet
    bad_ret = df["return_date"].notna() & (df["return_date"] < df["issue_ts"].dt.normalize())
    df["return_date_invalid"] = bad_ret
    df.loc[bad_ret, "return_date"] = pd.NaT
    report["invalid_return_dates_nulled"] = int(bad_ret.sum())

    # 5. missing borrower handling
    email_to_id = members.dropna(subset=["email"]).drop_duplicates("email").set_index("email")["member_id"]
    known = set(members["member_id"])
    df["member_recovered"] = False
    need = df["member_id"].isna() & df["borrower_email"].notna()
    recovered = df.loc[need, "borrower_email"].map(email_to_id)
    ok = recovered.notna()
    df.loc[recovered[ok].index, "member_id"] = recovered[ok]
    df.loc[recovered[ok].index, "member_recovered"] = True
    report["member_recovered_from_email"] = int(ok.sum())

    unresolved = df["member_id"].isna() | ~df["member_id"].isin(known)
    df["member_missing"] = unresolved
    df.loc[unresolved, "member_id"] = UNKNOWN_MEMBER
    report["member_unresolved_marked_unknown"] = int(unresolved.sum())

    # 6. derived fields
    as_of = pd.Timestamp(AS_OF)
    df["status"] = np.where(df["return_date"].notna(), "RETURNED",
                            np.where(df["due_date"] < as_of, "OVERDUE", "ISSUED"))
    df["loan_days"] = (df["return_date"] - df["issue_ts"].dt.normalize()).dt.days
    df["days_late"] = (df["return_date"] - df["due_date"]).dt.days.clip(lower=0)

    cols = ["txn_id", "member_id", "book_id", "branch_id", "issue_ts", "due_date", "return_date",
            "status", "loan_days", "days_late", "member_missing", "member_recovered", "return_date_invalid"]
    df = df[cols].sort_values("txn_id").reset_index(drop=True)
    report["rows_out"] = int(len(df))
    return df, rejects.reset_index(drop=True), report


def run_all(save=True):
    books = clean_books(pd.read_csv(RAW / "books.csv"))
    members_raw = pd.read_csv(RAW / "members.csv", dtype={"phone": "string"})
    borrow_raw = pd.read_csv(RAW / "borrowing.csv")
    first_borrow = (pd.to_datetime(borrow_raw["issue_datetime"], errors="coerce")
                    .groupby(borrow_raw["member_id"].astype("string").str.upper()).min())
    members = clean_members(members_raw, first_borrow)
    borrowing, rejects, report = clean_borrowing(borrow_raw, members, books)
    report["members_email_missing"] = int(members["email_missing"].sum())
    report["members_join_date_imputed"] = int(members["join_date_imputed"].sum())
    report["members_age_group_unknown"] = int((members["age_group"] == "Unknown").sum())
    if save:
        books.to_csv(PROCESSED / "books_clean.csv", index=False)
        members.to_csv(PROCESSED / "members_clean.csv", index=False)
        borrowing.to_csv(PROCESSED / "borrowing_clean.csv", index=False)
        rejects.to_csv(PROCESSED / "rejects.csv", index=False)
        (PROCESSED / "quality_report.json").write_text(json.dumps(report, indent=2))
    return books, members, borrowing, report


if __name__ == "__main__":
    *_, rep = run_all()
    print(json.dumps(rep, indent=2))
