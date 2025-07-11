from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator #didn't work
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from google.cloud import storage

import csv
import pandas as pd

import ast
from datetime import datetime
import pendulum
from pa_py_libraries import bq_load_util

PROJECT_ID = Variable.get("project_id")
GCS_BUCKET = Variable.get("default_gcs_bucket")
DAGS_FOLDER = Variable.get("dags_folder")
SQL_PATH = DAGS_FOLDER + "/bq_codes/dc_dashboard/"
EST_TZ = pendulum.timezone("America/Toronto")

default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'depends_on_past': False,
    'project_id': PROJECT_ID,
}



with DAG(
    dag_id='talent_metrics_pipeline',
    description='Talent Metrics pipeline',
    schedule=None,
    start_date=datetime(2025, 3, 12, tzinfo=EST_TZ),
    catchup=False,
    default_args=default_args
) as dag:



    # Submit BQ jobs to Execute SQL on BigQuery

    # load SDM
    execute_bq_job_sdm = BigQueryInsertJobOperator(
         task_id="talent_metrics_sdm",
         configuration={
         "query": {
            "query": "{% include 'bq_codes/talent_metrics/talent_metrics_sdm.sql' %}",
            "useLegacySql": False,
            "priority": "BATCH",
            }
        },
        location='northamerica-northeast1',
        deferrable=False
   )

    # load DC
    execute_bq_job_dc = BigQueryInsertJobOperator(
         task_id="talent_metrics_dc",
         configuration={
         "query": {
            "query": "{% include 'bq_codes/talent_metrics/talent_metrics_dc.sql' %}",
            "useLegacySql": False,
            "priority": "BATCH",
            }
        },
        location='northamerica-northeast1',
        deferrable=False
   )

    # load Retail
    execute_bq_job_retail = BigQueryInsertJobOperator(
         task_id="talent_metrics_retail",
         configuration={
         "query": {
            "query": "{% include 'bq_codes/talent_metrics/talent_metrics_retail.sql' %}",
            "useLegacySql": False,
            "priority": "BATCH",
            }
        },
        location='northamerica-northeast1',
        deferrable=False
   )

    # Submit BQ jobs to Execute SQL on BigQuery
    execute_bq_job_all = BigQueryInsertJobOperator(
         task_id="talent_metrics_all",
         configuration={
         "query": {
            "query": "{% include 'bq_codes/talent_metrics/talent_metrics_all.sql' %}",
            "useLegacySql": False,
            "priority": "BATCH",
            }
        },
        location='northamerica-northeast1',
        deferrable=False
   )

    # Setting the task dependencies
    execute_bq_job_dc >> execute_bq_job_retail >> execute_bq_job_sdm >> execute_bq_job_all
