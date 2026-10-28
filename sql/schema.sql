-- Smart Library Analytics Hub schema (auto-run on first Postgres start)

CREATE TABLE IF NOT EXISTS dim_books (
    book_id TEXT PRIMARY KEY, title TEXT NOT NULL, author TEXT, genre TEXT, isbn TEXT,
    publisher TEXT, publication_year INT, copies_total INT, home_branch_id TEXT, has_ebook BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_members (
    member_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT, phone TEXT,
    join_date DATE, membership_type TEXT, home_branch_id TEXT, age_group TEXT,
    join_date_imputed BOOLEAN DEFAULT FALSE, email_missing BOOLEAN DEFAULT FALSE
);
-- placeholder borrower for transactions where the member cannot be identified
INSERT INTO dim_members (member_id, first_name, last_name, age_group, membership_type)
VALUES ('M-UNKNOWN', 'Unknown', 'Borrower', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_borrowing (
    txn_id TEXT PRIMARY KEY,
    member_id TEXT NOT NULL REFERENCES dim_members(member_id),
    book_id TEXT NOT NULL REFERENCES dim_books(book_id),
    branch_id TEXT NOT NULL,
    issue_ts TIMESTAMPTZ NOT NULL,
    due_date DATE, return_date DATE, status TEXT NOT NULL,
    loan_days INT, days_late INT,
    member_missing BOOLEAN DEFAULT FALSE, member_recovered BOOLEAN DEFAULT FALSE,
    return_date_invalid BOOLEAN DEFAULT FALSE,
    -- lineage columns
    ingest_mode TEXT NOT NULL CHECK (ingest_mode IN ('batch','stream')),
    source_ref TEXT NOT NULL,          -- borrowing.csv  or  kafka:<topic>/<partition>/<offset>
    ingest_run_id TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fb_issue_ts ON fact_borrowing (issue_ts);

-- analytics tables written by the Python analysis step
CREATE TABLE IF NOT EXISTS analytics_monthly_issues (
    month DATE PRIMARY KEY, issues INT, unique_borrowers INT,
    avg_loan_days NUMERIC(6,2), overdue_rate NUMERIC(6,4));
CREATE TABLE IF NOT EXISTS analytics_book_stats (
    book_id TEXT PRIMARY KEY, title TEXT, genre TEXT, times_borrowed INT,
    last_borrowed TIMESTAMPTZ, avg_loan_days NUMERIC(6,2));
CREATE TABLE IF NOT EXISTS analytics_membership_monthly (
    month DATE PRIMARY KEY, new_members INT, cumulative_members INT, mom_growth_pct NUMERIC(8,3));
