from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
#    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")



#cmd_2=f"echo {dataset_file}"
#cmd_3=f"echo {dataset_file}"
#cmd_4=f"echo {dataset_file}"
def create_dag(symbol):
    dataset_url="https://d37ci6vzurychx.cloudfront.net/trip-data/" + symbol +"_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
    dataset_file=os.path.basename(dataset_url)
    path_to_local_file="/tmp/"

    cmd_2=f"curl -sSL {dataset_url} > {path_to_local_file}/{dataset_file}"
    cmd_3=f"gcloud auth activate-service-account --key-file={GOOGLE_APPLICATION_CREDENTIALS};gsutil -m cp {path_to_local_file}/formatted_{dataset_file} gs://{BUCKET}/raw/{symbol}/"
    cmd_4=f"rm -f {path_to_local_file}/*{dataset_file}"
    cmd_format_pq=f"hostname;python /opt/airflow/utils/convert_pq.py --input_file {path_to_local_file}/{dataset_file} --output_file {path_to_local_file}/formatted_{dataset_file} --schema {symbol}"
    #cmd_format_pq=f"mv {path_to_local_file}/{dataset_file} {path_to_local_file}/formatted_{dataset_file}"
    
    with DAG(
        f"process_ny_taxi_{symbol}_1",
        #end_date=datetime(2019,3,1),
        default_args=default_args,
        start_date=datetime(2019,1,1),
        end_date=datetime(2021,1,1),
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
        format_pq = BashOperator(
            task_id="format_pq",
            bash_command=cmd_format_pq,
            #bash_command=f"env"
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

        op1 >> op2 >>format_pq >> op3 >> op4
        return dag
for symbol in ("yellow", "green", "fhv"):
    globals()[f"dag_{symbol}"] = create_dag(symbol)    