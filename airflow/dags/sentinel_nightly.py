from datetime import datetime, date
from airflow.decorators import dag, task


@dag(
    dag_id="sentinel_nightly",
    schedule="0 2 * * *",
    # schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 12),
    catchup=False,
)
def sentinel_nightly():

    @task
    def review_amount_alerts_task():
        from jobs.db import get_connection
        from jobs.tasks import review_amount_alerts
        conn = get_connection()
        try:
            return sorted(review_amount_alerts(conn))
        finally:
            conn.close()

    @task
    def compute_risk_profiles_task(dismissed_ids):
        from jobs.db import get_connection
        from jobs.tasks import compute_risk_profiles
        conn = get_connection()
        try:
            compute_risk_profiles(conn)
        finally:
            conn.close()

    @task
    def generate_report_task():
        from jobs.db import get_connection
        from jobs.tasks import generate_report
        conn = get_connection()
        try:
            return generate_report(conn, "/opt/airflow/reports", date.today().isoformat())
        finally:
            conn.close()

    dismissed = review_amount_alerts_task()
    profiles = compute_risk_profiles_task(dismissed)
    generate_report_task().set_upstream(profiles)


sentinel_nightly()