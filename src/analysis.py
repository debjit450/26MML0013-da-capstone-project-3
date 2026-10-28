"""Book issue trend analysis and Python visualizations.

compute_analytics() returns DataFrames that are also loaded into PostgreSQL
(analytics_* tables) by load_batch.py. make_charts() writes PNGs to outputs/charts.
"""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from config import ANALYTICS, AS_OF, CHARTS, PROCESSED, UNKNOWN_MEMBER

plt.rcParams.update({"figure.dpi": 110, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": .25, "font.size": 9})
BLUE, ORANGE, GREY = "#2f6db5", "#e07b39", "#8a8f98"


def load_processed():
    b = pd.read_csv(PROCESSED / "books_clean.csv")
    m = pd.read_csv(PROCESSED / "members_clean.csv", parse_dates=["join_date"], dtype={"phone": "string"})
    t = pd.read_csv(PROCESSED / "borrowing_clean.csv", parse_dates=["issue_ts", "due_date", "return_date"])
    return b, m, t


def compute_analytics(books, members, txn):
    t = txn.merge(books[["book_id", "title", "genre"]], on="book_id", how="left")
    t["month"] = t["issue_ts"].dt.to_period("M").dt.to_timestamp()
    known = t[~t["member_missing"]]

    monthly = t.groupby("month").agg(
        issues=("txn_id", "count"),
        unique_borrowers=("member_id", lambda s: s[s != UNKNOWN_MEMBER].nunique()),
        avg_loan_days=("loan_days", "mean"),
        overdue_rate=("status", lambda s: (s == "OVERDUE").mean()),
    ).reset_index()

    book_stats = t.groupby(["book_id", "title", "genre"]).agg(
        times_borrowed=("txn_id", "count"), last_borrowed=("issue_ts", "max"),
        avg_loan_days=("loan_days", "mean")).reset_index().sort_values("times_borrowed", ascending=False)

    genre_monthly = t.groupby(["month", "genre"]).size().rename("issues").reset_index()

    mem = members.dropna(subset=["join_date"]).copy()
    mem["month"] = mem["join_date"].dt.to_period("M").dt.to_timestamp()
    growth = mem.groupby("month").size().rename("new_members").reset_index()
    growth["cumulative_members"] = growth["new_members"].cumsum()
    growth["mom_growth_pct"] = growth["cumulative_members"].pct_change() * 100

    return {"monthly": monthly, "book_stats": book_stats, "genre_monthly": genre_monthly,
            "membership": growth, "txn": t, "known": known}


def trend_summary(a):
    monthly = a["monthly"]
    full = monthly[monthly["month"] < pd.Timestamp(AS_OF).to_period("M").to_timestamp()]  # drop partial month
    x = np.arange(len(full))
    slope, intercept = np.polyfit(x, full["issues"], 1)
    pred = slope * x + intercept
    ss_res, ss_tot = ((full["issues"] - pred) ** 2).sum(), ((full["issues"] - full["issues"].mean()) ** 2).sum()
    t = a["txn"]
    dow = t.groupby(t["issue_ts"].dt.day_name()).size()
    return {
        "as_of": AS_OF,
        "total_transactions": int(len(t)),
        "transactions_with_unknown_borrower": int(t["member_missing"].sum()),
        "distinct_books_borrowed": int(t["book_id"].nunique()),
        "avg_loan_days": round(float(t["loan_days"].mean()), 2),
        "overdue_rate_pct": round(float((t["status"] == "OVERDUE").mean() * 100), 2),
        "late_return_rate_pct": round(float((t["days_late"].fillna(0) > 0).sum() / t["return_date"].notna().sum() * 100), 2),
        "busiest_weekday": dow.idxmax(), "quietest_weekday": dow.idxmin(),
        "monthly_trend_slope_issues_per_month": round(float(slope), 2),
        "monthly_trend_r2": round(float(1 - ss_res / ss_tot), 3),
        "top_genre": t["genre"].value_counts().idxmax(),
        "top_5_books": a["book_stats"].head(5)[["title", "times_borrowed"]].to_dict("records"),
        "members_total": int(a["membership"]["new_members"].sum()),
        "busiest_branch": t["branch_id"].value_counts().idxmax(),
    }


def make_charts(a):
    t, monthly, mem = a["txn"], a["monthly"], a["membership"]
    last_full = pd.Timestamp(AS_OF).to_period("M").to_timestamp()

    # 1 monthly issues with trend line
    fig, ax = plt.subplots(figsize=(8, 3.6))
    full, part = monthly[monthly["month"] < last_full], monthly[monthly["month"] >= last_full]
    ax.plot(full["month"], full["issues"], marker="o", color=BLUE, label="Books issued")
    ax.plot(part["month"], part["issues"], marker="o", ls="none", mfc="white", color=BLUE, label="Partial month")
    z = np.polyfit(np.arange(len(full)), full["issues"], 1)
    ax.plot(full["month"], np.polyval(z, np.arange(len(full))), "--", color=ORANGE, label=f"Trend (+{z[0]:.0f}/month)")
    ax.set(title="Books issued per month", ylabel="Transactions"); ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(CHARTS / "01_monthly_issues.png"); plt.close(fig)

    # 2 top books
    top = a["book_stats"].head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(7, 4.6))
    ax.barh(top["title"], top["times_borrowed"], color=BLUE)
    ax.set(title="Top 15 most borrowed books", xlabel="Times borrowed"); ax.grid(axis="y", visible=False)
    fig.tight_layout(); fig.savefig(CHARTS / "02_top_books.png"); plt.close(fig)

    # 3 genre share
    g = t["genre"].value_counts().iloc[::-1]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.barh(g.index, g.values, color=BLUE)
    ax.set(title="Issues by genre", xlabel="Transactions"); ax.grid(axis="y", visible=False)
    fig.tight_layout(); fig.savefig(CHARTS / "03_genre_share.png"); plt.close(fig)

    # 4 weekday x month heatmap
    piv = (t.assign(dow=t["issue_ts"].dt.dayofweek, m=t["issue_ts"].dt.to_period("M").astype(str))
           .pivot_table(index="dow", columns="m", values="txn_id", aggfunc="count", fill_value=0))
    fig, ax = plt.subplots(figsize=(9, 3.4))
    im = ax.imshow(piv.values, aspect="auto", cmap="Blues")
    ax.set_yticks(range(7), ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
    ax.set_xticks(range(len(piv.columns)), piv.columns, rotation=90, fontsize=7); ax.grid(False)
    ax.set_title("Issue volume by weekday and month"); fig.colorbar(im, ax=ax, label="Transactions")
    fig.tight_layout(); fig.savefig(CHARTS / "04_weekday_heatmap.png"); plt.close(fig)

    # 5 membership growth
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.bar(mem["month"], mem["new_members"], width=20, color=GREY, label="New members")
    ax2 = ax.twinx(); ax2.plot(mem["month"], mem["cumulative_members"], color=BLUE, lw=2, label="Cumulative")
    ax2.grid(False); ax2.spines["right"].set_visible(True)
    ax.set(title="Membership growth", ylabel="New members / month"); ax2.set_ylabel("Cumulative members")
    fig.tight_layout(); fig.savefig(CHARTS / "05_membership_growth.png"); plt.close(fig)

    # 6 loan duration distribution
    d = t["loan_days"].dropna()
    fig, ax = plt.subplots(figsize=(7, 3.6))
    ax.hist(d, bins=40, color=BLUE, alpha=.85)
    ax.axvline(14, color=ORANGE, ls="--", label="14 day loan period"); ax.axvline(d.mean(), color="k", ls=":", label=f"Mean {d.mean():.1f}d")
    ax.set(title="Loan duration", xlabel="Days", ylabel="Loans"); ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(CHARTS / "06_loan_duration.png"); plt.close(fig)

    # 7 branch comparison
    br = t.groupby("branch_id").agg(issues=("txn_id", "count"), overdue=("status", lambda s: (s == "OVERDUE").mean() * 100))
    fig, ax = plt.subplots(figsize=(6.5, 3.6))
    ax.bar(br.index, br["issues"], color=BLUE); ax2 = ax.twinx()
    ax2.plot(br.index, br["overdue"], "o-", color=ORANGE); ax2.grid(False); ax2.spines["right"].set_visible(True)
    ax.set(title="Branch volume and overdue rate", ylabel="Transactions"); ax2.set_ylabel("Overdue %", color=ORANGE)
    fig.tight_layout(); fig.savefig(CHARTS / "07_branch_comparison.png"); plt.close(fig)


def main():
    books, members, txn = load_processed()
    a = compute_analytics(books, members, txn)
    for k in ("monthly", "book_stats", "genre_monthly", "membership"):
        a[k].to_csv(ANALYTICS / f"{k}.csv", index=False)
    summary = trend_summary(a)
    (ANALYTICS / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    make_charts(a)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
