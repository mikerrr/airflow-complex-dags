import logging
import shutil 
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import settings

# List of email address to send email alerts to if this job fails
monitoring_recipients_emails = Variable.get("monitoring_recipients_emails")
ALERT_EMAIL_ADDRESSES = [x.strip() for x in monitoring_recipients_emails.split(',')]

default_args = {
    "owner": "operations",
    'depends_on_past': False,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True
}

MAX_LOG_DAYS = 14
LOG_DIR = '/opt/airflow/logs'

def find_old_logs():
    # Query old dag runs and build the log file paths to be deleted
    # Example log directory looks like this:
    # '/path/to/logs/dag_name/task_name/2021-01-11T12:25:00+00:00'
    # airflow 2.4.1 update
    # /path/to/logs/dag_id=dag_name/run_id=scheduled__2022-10-09T00:00:00+00:00/task_id=task_name/attempt=5.log'
    sql= f"""
        SET TIME ZONE 'UTC';
        SELECT  '{LOG_DIR}/dag_id=' || dag_id || '/run_id=' || run_id AS log_dir
        FROM dag_run
        WHERE execution_date::DATE <= now()::DATE - INTERVAL '{MAX_LOG_DAYS} days';
    """
    logging.info("Fetching old logs to purge...")
    session = settings.Session()
    res = session.execute(sql)
    rows = res.fetchall()
    logging.info(f"Found {len(rows)} log directories to delete...")
    
    for row in rows:
            delete_log_dir(row[0])

def delete_log_dir(log_dir):
    try:
        # Recursively delete the log directory and its log contents (e.g, 1.log, 2.log, etc)
        shutil.rmtree(log_dir)
        logging.info(f"Deleted directory and log contents: {log_dir}")
    except OSError as e:
        logging.info(f"Unable to delete: {e.filename} - {e.strerror}")

with DAG(
        dag_id="airflow_log_cleanup",
        start_date=airflow.utils.dates.days_ago(20),
        schedule_interval="@daily",
        default_args=default_args,
        max_active_runs=1,
        catchup=False,
        tags=['airflow-maintenance-dags']
) as dag:
    log_cleanup_op = PythonOperator(
        task_id="delete_old_logs",
        python_callable=find_old_logs
    )