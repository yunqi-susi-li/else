from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

import ast
import datetime
import pendulum

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

def manipulate_files_on_gcs(**kwargs):
    from google.cloud import storage

    # Initialize a client
    client = storage.Client()

    # Specify your bucket and file names
    bucket_name = GCS_BUCKET

    print(f"GCS bucket " +  bucket_name)
    print(f"SQL PATH: " +  SQL_PATH)

    file_name = 'airflow_test/testfile.txt'

    # Example manipulation: Upload a file
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename('/opt/airflow/dags/repo/airflow-job/dags/test_files/testfile.txt')

    print(f"Uploaded {file_name} to GCS bucket {bucket_name}")


with DAG(
    dag_id='gcs_and_bigquery_dag',
    description='This job to test connecting to GCS and Bigquery',
    schedule=None,
    start_date=datetime.datetime(2025, 3, 12, tzinfo=EST_TZ),
    catchup=False,
    default_args=default_args
) as dag:

    # Task 1: Manipulate files on Google Cloud Storage
    push_testfile_to_gcs_bucket = PythonOperator(
        task_id='push_testfile_to_gcs_bucket',
        python_callable=manipulate_files_on_gcs,
        provide_context=True
    )

    # Task 2: Execute SQL on BigQuery
    execute_bq_job = BigQueryInsertJobOperator(
         task_id="execute_bq_job",
         configuration={
         "query": {
            "query":  f"create or replace table {PROJECT_ID}.airflow_test.for_bq_operator_test2 as select * from {PROJECT_ID}.dc_dashboard_v2.team_levels_hierarchy",
            "useLegacySql": False,
            "priority": "BATCH",
            }
        },
        location='northamerica-northeast1',
        deferrable=False
   )

    # Setting the task dependencies
    push_testfile_to_gcs_bucket >> execute_bq_job