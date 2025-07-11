from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator #didn't work
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
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
    
    print("Preparing file: " + file_list[0])    
    #file_list = file_list[1:]
    #print("Preparing file: " + file_list[0])

    file_path = f"gs://{bucket_name}/{folder_path}{file_list[0]}"   
    df=pd.read_csv(file_path, skiprows=num_top_row_to_remove, dtype=str)
    df=df.drop(['Worker', 'Top Talent?', 'Hot Skill?', 'Performance Rating', 'Potential Rating', 'Retention Risk'], axis=1)
    df.columns = df.columns.str.replace('-', '_').str.replace(' ', '_').str.replace(r'_{2,}', '_', regex=True).str.replace('?', '').str.lower()
    df['rec_cre_tms']=datetime.now()
    df['src_file_nm']=file_list[0]

    df.to_csv(file_path, index=False) 

    print(f"Processed {file_path} on GCS bucket {bucket_name}")


with DAG(
    dag_id='sensor_example',
    schedule=None,
    start_date=datetime(2025, 3, 12, tzinfo=EST_TZ),
    catchup=False,
    default_args=default_args
) as dag:


    # Task 1: Use GCSObjectExistenceSensor to check if any file exists in a folder
    file_sensor_task = GCSObjectExistenceSensor(
        task_id='file_sensor_task',
        bucket=GCS_BUCKET,  
        object='data_ingestions/workday_data/headcount/test_load.csv',  # Wildcard to match any file in the folder
        poke_interval=120,  # Interval in seconds between checks (default 60)
        timeout=30000,  # Maximum time in seconds to wait for the file (default 600)
        mode='reschedule',  # The mode to use (either poke or reschedule)
        soft_fail=False  # If True, the task will not fail if the file is not found
    )

    # Task 2: Transform files on GCS folder if requruired before loading into BQ table
    gcs_file_prep = PythonOperator(
        task_id='gcs_file_prep',
        python_callable=gcs_prep_wdweekly_hc_file,
        provide_context=True
    )


    # Setting the task dependencies
    file_sensor_task >> gcs_file_prep