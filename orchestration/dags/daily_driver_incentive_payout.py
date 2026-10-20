"""Daily 01:00 UTC - driver incentive payouts, e-mailed to operations.

compute_payouts      Gold driver_daily_utilisation -> Gold driver_payouts (one SQL statement on Databricks)
build_payout_report  CSV of every driver + HTML summary with the top-10 drivers
email_payout_report  HTML body + CSV attachment (MailHog locally: http://localhost:8026)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

from dag_settings import DEFAULT_ARGS, SKIP, TOOLS

with DAG("daily_driver_incentive_payout", default_args=DEFAULT_ARGS, schedule="0 1 * * *",
         start_date=datetime(2025, 1, 1), catchup=False, max_active_runs=1, tags=["ridehail", "payout"]) as dag:
    compute = BashOperator(task_id="compute_payouts", skip_on_exit_code=SKIP,
                           bash_command=f"{TOOLS} python -m orchestration.payouts compute ")
    # the job prints the HTML report as its last line -> XCom -> e-mail body
    report = BashOperator(task_id="build_payout_report", skip_on_exit_code=SKIP,
                          bash_command=f"{TOOLS} python -m orchestration.payouts report ")
    mail = EmailOperator(task_id="email_payout_report", to="ops@ridehail.local",
                         subject="Daily driver incentive payouts {{ ds }}",
                         html_content="{{ ti.xcom_pull(task_ids='build_payout_report') }}",
                         files=["/lake/reports/driver_payouts_latest.csv"])
    compute >> report >> mail
