import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator

from training.custom_operators.nifi_operator import NifiOperator

with DAG(
        dag_id="schedule-nifi-flow",
        schedule_interval=None,
        start_date=datetime.datetime.today(),
        catchup=False
) as dag:
    schedule_task = NifiOperator(task_id="schedule-nifi-flow", name="trigger")
    quality_check_task = SQLTableCheckOperator(task_id="quality-check",
                                               conn_id="postgres_connection",
                                               table="vulnerability_vulnerability",
                                               checks={
                                                   "row_count_check": {"check_statement": "COUNT(*) = 1966"}
                                               }
                                               )
    schedule_task >> quality_check_task
