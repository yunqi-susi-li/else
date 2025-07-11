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


def gcs_prep_wdweekly_hc_file (**kwargs):

    # Initialize a client
    client_gsc = storage.Client()

    # Specify your bucket and file names
    bucket_name = GCS_BUCKET

    print(f"GCS bucket " +  bucket_name)
    
    num_top_row_to_remove = 5
    folder_path = 'data_ingestions/workday_data/headcount/'
    processed_folder_path = 'data_ingestions/workday_data/headcount/Processed/'
    file_list = []
    for blob in client_gsc.list_blobs(bucket_name, prefix=folder_path):
        if blob.name.count('/') == folder_path.count('/'):
            file_list.append(blob.name.split('/')[-1])
            print("file: " + blob.name.split('/')[-1])
    
    file_list = file_list[1:]
    print("Preparing file: " + file_list[0])    

    file_path = f"gs://{bucket_name}/{folder_path}{file_list[0]}"   
    df=pd.read_csv(file_path, skiprows=num_top_row_to_remove, dtype=str)
    df=df.drop(['Worker', 'Top Talent?', 'Hot Skill?', 'Performance Rating', 'Potential Rating', 'Retention Risk'], axis=1)
    df.columns = df.columns.str.replace('-', '_').str.replace(' ', '_').str.replace(r'_{2,}', '_', regex=True).str.replace('?', '').str.lower()
    df['rec_cre_tms']=datetime.now()
    df['src_file_nm']=file_list[0]

    df.to_csv(file_path, index=False) 

    print(f"Processed {file_path} on GCS bucket {bucket_name}")


with DAG(
    dag_id='sample_pipeline',
    description='This job to test connecting to GCS and Bigquery',
    schedule=None,
    start_date=datetime(2025, 3, 12, tzinfo=EST_TZ),
    catchup=False,
    default_args=default_args
) as dag:

    # Task 1: Transform files on GCS folder if requruired before loading into BQ table
    gcs_file_prep = PythonOperator(
        task_id='gcs_file_prep',
        python_callable=gcs_prep_wdweekly_hc_file,
        provide_context=True
    )

    # Task 2: use GCSToBigQueryOperator to load files from GCS to BQ table

    # a - load the schema from the JSON file
    schema_fields = bq_load_util.load_schema_from_json(DAGS_FOLDER+"/bq_schemas/schema_workday_weekly_headcount.json")  # Specify the correct path to your JSON file

    # b - load data from GCS to BigQuery
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=GCS_BUCKET,  # GCS bucket name
        source_objects=["data_ingestions/workday_data/headcount/*.csv"],  # GCS file path
        destination_project_dataset_table=f"{PROJECT_ID}.airflow_test.workday_weekly_headcount",  # BigQuery table (project.dataset.table)
        schema_fields=schema_fields,  # Pass the loaded schema here
        allow_quoted_newlines=True,
        skip_leading_rows=1,  # Optional: skip header row in CSV
        write_disposition="WRITE_APPEND",  # Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
        source_format="CSV",  # Other options: 'NEWLINE_DELIMITED_JSON', 'AVRO', 'PARQUET', etc.
    )

    # Task 3: Submit BQ job to Execute SQL on BigQuery
    execute_bq_job = BigQueryInsertJobOperator(
         task_id="execute_bq_job",
         configuration={
         "query": {
            "query": "{% include 'bq_codes/dc_dashboard/stub.sql' %}",
            "useLegacySql": False,
            "priority": "BATCH",
            }
        },
        location='northamerica-northeast1',
        deferrable=False
   )

    # Task 4: Cleanup gcs folder 
    ''' didint work
    gcs_cleanup = GCSToGCSOperator(
        task_id="gcs_cleanup",
        source_bucket=GCS_BUCKET,
        source_object="data_ingestions/workday_data/headcount/*.csv",
        destination_object="data_ingestions/workday_data/headcount/Processed/",
        match_glob='*.csv',
        move_object=True
    )'''
    #gsutil mv gs://ppl-analytics-cfs/data_ingestions/workday_data/headcount/*.csv gs://ppl-analytics-cfs/data_ingestions/workday_data/headcount/Processed

    gcs_cleanup = BashOperator(
            task_id='gcs_cleanup',
            bash_command=f"gsutil mv gs://{GCS_BUCKET}/data_ingestions/workday_data/headcount/*.csv gs://{GCS_BUCKET}/data_ingestions/workday_data/headcount/Processed/"
        )
    # Setting the task dependencies
    gcs_file_prep >> load_gcs_to_bigquery 
    load_gcs_to_bigquery >> execute_bq_job
    load_gcs_to_bigquery >> gcs_cleanup
    #gcs_file_prep >> gcs_cleanup