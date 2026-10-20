"""Daily driver incentive payouts (daily Airflow DAG, 01:00 UTC).

    python -m orchestration.payouts compute   # Databricks Gold driver_daily_utilisation -> Gold driver_payouts
    python -m orchestration.payouts report    # report files + the HTML e-mail body

Rule: USD 20 bonus when utilisation >= 78 % and trips >= 12, plus USD 1.50 for every trip with surge >= 1.5x.
The payout is computed with one SQL statement on the Databricks SQL warehouse, for the latest day in Gold.
Report: /lake/reports/driver_payouts_latest.csv (every driver, e-mail attachment), driver_payouts_summary.json,
        driver_payouts_latest.html (summary + top-10 drivers). The HTML is also printed as the LAST stdout line, so
        Airflow passes it (XCom) to the EmailOperator.
Exit code 99 (= Airflow 'skipped') while Gold has no driver data yet or Databricks is not configured.
"""
import html
import json
import os
import sys

from common.config import (PAYOUT_BONUS, PAYOUT_MIN_TRIPS, PAYOUT_SURGE_EXTRA, REPORTS, SKIP, TARGET_UTILISATION,
                           databricks_configured)


def build_html(summary: dict, top) -> str:
    """summary dict + top-10 rows (DataFrame) -> one-line HTML body."""
    style = 'style="border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px"'
    td = 'style="border:1px solid #ccc;padding:4px 8px"'
    th = 'style="border:1px solid #ccc;padding:4px 8px;background:#f0f0f0;text-align:left"'
    srows = [("Date", summary["date"]), ("Drivers", f"{summary['drivers']:,}"),
             ("Drivers with bonus (utilisation >= 78% and >= 12 trips)", f"{summary['drivers_with_bonus']:,}"),
             ("Total bonus (USD 20 each)", f"USD {summary['total_bonus']:,.2f}"),
             ("Total surge extra (USD 1.50 per surge trip)", f"USD {summary['total_surge_extra']:,.2f}"),
             ("Total payout", f"USD {summary['total_payout']:,.2f}")]
    out = [f"<h2>Daily driver incentive payouts - {html.escape(str(summary['date']))}</h2>", f"<table {style}>"]
    out += [f"<tr><th {th}>{html.escape(k)}</th><td {td}>{html.escape(str(v))}</td></tr>" for k, v in srows]
    out += ["</table>", "<h3>Top 10 drivers by payout</h3>", f"<table {style}><tr>"]
    cols = [("driver_id", "Driver"), ("trips", "Trips"), ("utilisation", "Utilisation"), ("surge_trips", "Surge trips"),
            ("bonus", "Bonus USD"), ("surge_extra", "Surge extra USD"), ("total_payout", "Total USD")]
    out += [f"<th {th}>{h}</th>" for _, h in cols] + ["</tr>"]
    for _, r in top.iterrows():
        cells = [str(int(r["driver_id"])), str(int(r["trips"])), f"{float(r['utilisation']):.1%}", str(int(r["surge_trips"])),
                 f"{float(r['bonus']):.2f}", f"{float(r['surge_extra']):.2f}", f"{float(r['total_payout']):.2f}"]
        out += ["<tr>"] + [f"<td {td}>{c}</td>" for c in cells] + ["</tr>"]
    out += ["</table>", "<p>All drivers of the day are in the attached CSV (driver_payouts_latest.csv).</p>"]
    return "".join(out)


def compute() -> int:
    from cloud.databricks.workspace import exists, execute, query, table
    src, dst = table("driver_daily_utilisation"), table("driver_payouts")
    day = query(f"SELECT CAST(MAX(date) AS STRING) AS d FROM {src}")
    if not day or not day[0]["d"]:
        print("no driver data in Gold yet - skipping", file=sys.stderr)
        return SKIP
    d = day[0]["d"]
    bonus = (f"CASE WHEN utilisation >= {TARGET_UTILISATION} AND trips >= {PAYOUT_MIN_TRIPS} "
             f"THEN {PAYOUT_BONUS} ELSE 0 END")
    select = f"""
        SELECT driver_id, date, trips, online_min, utilisation, revenue, surge_trips,
               CAST({bonus} AS DOUBLE) AS bonus,
               CAST(ROUND(surge_trips * {PAYOUT_SURGE_EXTRA}, 2) AS DOUBLE) AS surge_extra,
               CAST(ROUND({bonus} + surge_trips * {PAYOUT_SURGE_EXTRA}, 2) AS DOUBLE) AS total_payout
        FROM {src} WHERE date = CAST(? AS DATE)"""
    if exists("driver_payouts"):
        execute(f"DELETE FROM {dst} WHERE date = CAST(? AS DATE)", [d])     # re-running a day replaces it
        execute(f"INSERT INTO {dst} {select}", [d])
    else:
        execute(f"CREATE TABLE {dst} AS {select}", [d])
    n = query(f"SELECT COUNT(*) AS n, SUM(CASE WHEN bonus > 0 THEN 1 ELSE 0 END) AS b FROM {dst} WHERE date = CAST(? AS DATE)", [d])
    print(f"[payouts] {d}: {n[0]['n']} drivers, {n[0]['b']} with bonus")
    return 0


def report() -> int:
    import pandas as pd
    from cloud.databricks.workspace import query, table
    rows = query(f"SELECT * FROM {table('driver_payouts')} WHERE date = (SELECT MAX(date) FROM {table('driver_payouts')})")
    if not rows:
        print("driver_payouts is empty - skipping", file=sys.stderr)
        return SKIP
    d = pd.DataFrame(rows).sort_values("total_payout", ascending=False)
    day = d["date"].iloc[0]
    os.makedirs(REPORTS, exist_ok=True)
    d.to_csv(f"{REPORTS}/driver_payouts_latest.csv", index=False)
    summary = {"date": str(day), "drivers": int(len(d)), "drivers_with_bonus": int((d["bonus"] > 0).sum()),
               "total_bonus": float(d["bonus"].sum()), "total_surge_extra": float(d["surge_extra"].sum()),
               "total_payout": float(d["total_payout"].sum())}
    json.dump(summary, open(f"{REPORTS}/driver_payouts_summary.json", "w"), indent=2)
    body = build_html(summary, d.head(10))
    open(f"{REPORTS}/driver_payouts_latest.html", "w").write(body)
    print(summary)
    print(body)                       # last line -> XCom -> e-mail body
    return 0


def main() -> int:
    if not databricks_configured():
        print("Databricks is not configured - skipping", file=sys.stderr)
        return SKIP
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    return compute() if cmd == "compute" else report()


if __name__ == "__main__":
    sys.exit(main())
