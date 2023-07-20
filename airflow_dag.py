from datetime import datetime as dt, timedelta as td
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from ep2023.airflow_objects import RedditMessageOperator

MAIN_DAG_NAME = "europython_2023_talk_example"
DAGRUN_TIMEOUT = td(hours=1)
MAX_ACTIVE_TASKS = 1
MAX_ACTIVE_RUNS = 1
SCHEDULE = "0 5 * * *"
EMAIL = "sebastien.crocquevieille@numberly.com"
RETRIES = 1
RETRY_DELAY = td(minutes=5)
START_DATE = dt(2023, 7, 18)

DEFAULT_ARGS = {
    "email": EMAIL,
    "email_on_failure": True,
    "email_on_retry": False,
    "owner": "Numberly",
    "retries": RETRIES,
    "retry_delay": RETRY_DELAY,
    "start_date": START_DATE,
}

with DAG(
    MAIN_DAG_NAME,
    dagrun_timeout=DAGRUN_TIMEOUT,
    default_args=DEFAULT_ARGS,
    max_active_tasks=MAX_ACTIVE_TASKS,
    max_active_runs=MAX_ACTIVE_RUNS,
    schedule=SCHEDULE,
) as main_dag:
    task_working_hard = EmptyOperator(
        task_id="working_hard",
    )
    task_hardly_working = RedditMessageOperator(
        task_id="hardly_working",
        reddit_conn="reddit_api_conn",
        target_user="StopReadingMemes",
        message="Your pipeline has finished, get off the toilet"
    )
    task_working_hard >> task_hardly_working
