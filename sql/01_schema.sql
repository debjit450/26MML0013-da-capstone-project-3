-- Smart Library Analytics Hub: PostgreSQL schema
-- Runs automatically on first container start (docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS dim_branch (
    branch_id   TEXT PRIMARY KEY,
    branch_name TEXT NOT NULL
);
INSERT INTO dim_branch VALUES
  ('B01','Central'),('B02','North'),('B03','South'),('B04','East'),('B05','West')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_books (
    book_id          TEXT PRIMARY KEY,
    title            TEXT NOT NULL,
    author           TEXT,
    genre            TEXT,
    isbn             TEXT,
    publisher        TEXT,
    publication_year INT,
    copies_total     INT,
    home_branch_id   TEXT REFERENCES dim_branch(branch_id),
    has_ebook        BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_members (
    member_id         TEXT PRIMARY KEY,
    first_name        TEXT,
    last_name         TEXT,
    email             TEXT,
    phone             TEXT,
    join_date         DATE,
    membership_type   TEXT,
    home_branch_id    TEXT REFERENCES dim_branch(branch_id),
    age_group         TEXT,
    join_date_imputed BOOLEAN DEFAULT FALSE,
    email_missing     BOOLEAN DEFAULT FALSE
);
-- sentinel used when the borrower on a transaction cannot be identified
INSERT INTO dim_members (member_id, first_name, last_name, age_group, membership_type)
VALUES ('M-UNKNOWN', 'Unknown', 'Borrower', 'Unknown', 'Unknown')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_borrowing (
    txn_id              TEXT PRIMARY KEY,
    member_id           TEXT NOT NULL REFERENCES dim_members(member_id),
    book_id             TEXT NOT NULL REFERENCES dim_books(book_id),
    branch_id           TEXT NOT NULL REFERENCES dim_branch(branch_id),
    issue_ts            TIMESTAMPTZ NOT NULL,
    due_date            DATE,
    return_date         DATE,
    status              TEXT NOT NULL,
    loan_days           INT,
    days_late           INT,
    member_missing      BOOLEAN DEFAULT FALSE,
    member_recovered    BOOLEAN DEFAULT FALSE,
    return_date_invalid BOOLEAN DEFAULT FALSE,
    -- lineage columns
    ingest_mode         TEXT NOT NULL CHECK (ingest_mode IN ('batch','stream')),
    source_ref          TEXT NOT NULL,   -- e.g. borrowing.csv or kafka:library.borrowing/0/1532
    ingest_run_id       TEXT NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fb_issue_ts ON fact_borrowing (issue_ts);
CREATE INDEX IF NOT EXISTS ix_fb_book     ON fact_borrowing (book_id);
CREATE INDEX IF NOT EXISTS ix_fb_member   ON fact_borrowing (member_id);

CREATE TABLE IF NOT EXISTS fact_ebook_access (
    log_id               TEXT PRIMARY KEY,
    member_id            TEXT,
    book_id              TEXT,
    access_ts            TIMESTAMPTZ,
    device               TEXT,
    session_minutes      INT,
    pages_read           INT,
    app_version          TEXT,
    network_type         TEXT,
    network_offline_sync BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_feedback (
    feedback_id  TEXT PRIMARY KEY,
    member_id    TEXT,
    book_id      TEXT,
    rating       INT,
    comment      TEXT,
    channel      TEXT,
    submitted_at DATE,
    sentiment    TEXT
);

-- Analytics tables written by the Python batch job (analysis.py)
CREATE TABLE IF NOT EXISTS analytics_monthly_issues (
    month DATE PRIMARY KEY, issues INT, unique_borrowers INT,
    avg_loan_days NUMERIC(6,2), overdue_rate NUMERIC(6,4)
);
CREATE TABLE IF NOT EXISTS analytics_book_stats (
    book_id TEXT PRIMARY KEY, title TEXT, genre TEXT, times_borrowed INT,
    last_borrowed TIMESTAMPTZ, avg_loan_days NUMERIC(6,2)
);
CREATE TABLE IF NOT EXISTS analytics_genre_monthly (
    month DATE, genre TEXT, issues INT, PRIMARY KEY (month, genre)
);
CREATE TABLE IF NOT EXISTS analytics_membership_monthly (
    month DATE PRIMARY KEY, new_members INT, cumulative_members INT, mom_growth_pct NUMERIC(8,3)
);

-- Pipeline audit trail (supports lineage and data quality review)
CREATE TABLE IF NOT EXISTS etl_run_log (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL,
    stage       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       BIGINT NOT NULL,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stream messages that failed validation
CREATE TABLE IF NOT EXISTS stream_dead_letter (
    id         BIGSERIAL PRIMARY KEY,
    source_ref TEXT,
    reason     TEXT,
    payload    TEXT,
    logged_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
