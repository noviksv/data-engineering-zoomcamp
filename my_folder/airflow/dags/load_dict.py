from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os
import pyarrow.csv as pv
import pyarrow.parquet as pq

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

dataset_url="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
dataset_file=os.path.basename(dataset_url)

path_to_local_file="/tmp/"
cmd_2=f"curl -sSL {dataset_url} > {path_to_local_file}/{dataset_file}"
cmd_3=f"gcloud auth activate-service-account --key-file={GOOGLE_APPLICATION_CREDENTIALS};gsutil -m cp {path_to_local_file}/{dataset_file.replace('.csv', '.parquet')} gs://{BUCKET}"
cmd_4=f"rm -f {path_to_local_file}/{dataset_file.replace('.csv', '.parquet')}"
#cmd_2=f"echo {dataset_file}"
#cmd_3=f"echo {dataset_file}"
#cmd_4=f"echo {dataset_file}"

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        print("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

with DAG(
    f"load_dict",
    #end_date=datetime(2019,3,1),
    default_args=default_args,
    schedule_interval="0 10 2 * *",
    catchup=True,
    max_active_runs=1,
) as dag:
    op1 = DummyOperator(task_id="task1")
    op2 = BashOperator(
        task_id="copy_file_from_www_to_local",
        bash_command=cmd_2,
        #bash_command=f"env"
        )
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_file}/{dataset_file}",
        },
    )
    op3 = BashOperator(
        task_id="uplopad_to_gcs",
        bash_command=cmd_3,
        #bash_command=f"env"
        )
    op4 = BashOperator(
        task_id="remove_local",
        bash_command=cmd_4,
        #bash_command=f"env"
        )        
    op1 >> op2 >> format_to_parquet_task >> op3 >> op4
  