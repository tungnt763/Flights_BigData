import os
import sys
HOME = os.getenv('AIRFLOW_HOME')
sys.path.append(HOME)

from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lib.utils import get_rundate as _get_rundate
from resources.business.dim.l1_dim_landing import landing_layer
import json

config_path = os.path.join(HOME, 'config', 'metadata_schema.json')
with open(config_path, 'r') as inp:
    config_content = inp.read()
    metadata_schema = json.loads(config_content)

# Default arguments
default_args = {
    'owner': 'tungnt',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
    'start_date': datetime.today()
}

@dag(
    dag_id="landing_layer_dag",
    default_args=default_args,
    schedule=None,
    tags=['landing', 'gcs', 'bigquery'],
    catchup=False
)
def landing_layer_dag():
    bucket_name = 'team17_flights_data'
    prefix_name = 'raw/Combined_Flights_'
    project_name = 'festive-ellipse-441310-b5'
    dataset_name = 'edw_ld'
    table_name = 'metadata'

    @task(provide_context=True) 
    def get_rundate(**context):
        run_dt = _get_rundate()
        print(f">> Rundate: {run_dt}")
        context['ti'].xcom_push(key="rundate", value=run_dt)

    sensor_task = GCSObjectsWithPrefixExistenceSensor(
        task_id='sensor_combined_flights_file',
        bucket=bucket_name,
        prefix=prefix_name, 
        google_cloud_conn_id='gcp',
        timeout=600, 
        poke_interval=30,  
    )

    @task
    def get_file_name(bucket_name, prefix):
        hook = GCSHook(gcp_conn_id='gcp')

        blobs = hook.list(bucket_name, prefix=prefix)

        if blobs:
            return blobs[0]  # Return the first file found
        return None

    ld_layer = landing_layer()

    archive_file = GCSToGCSOperator(
        task_id='archive_file',
        source_bucket=bucket_name,
        source_object="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value') }}",
        destination_bucket=bucket_name,
        destination_object="archive/{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value').split('/')[-1].split('.')[0] }}_{{ ts_nodash }}.csv",
        move_object=True,
        gcp_conn_id='gcp',
    )

    @task_group
    def trigger_dag():

        trigger_airline_dag = TriggerDagRunOperator(
            task_id="trigger_airline_dag",
            trigger_dag_id="airline_dag",
            wait_for_completion=False
        )

        trigger_marketing_airline_dag = TriggerDagRunOperator(
            task_id="trigger_marketing_airline_dag",
            trigger_dag_id="marketing_airline_dag",
            wait_for_completion=False
        )

        trigger_operating_airline_dag = TriggerDagRunOperator(
            task_id="trigger_operating_airline_dag",
            trigger_dag_id="operating_airline_dag",
            wait_for_completion=False
        )

        trigger_date_dag = TriggerDagRunOperator(
            task_id="trigger_date_dag",
            trigger_dag_id="date_dag",
            wait_for_completion=False
        )
        
        trigger_airport_dag = TriggerDagRunOperator(
            task_id="trigger_airport_dag",
            trigger_dag_id="airport_dag",
            wait_for_completion=False
        )
    
    return_value = get_file_name(bucket_name, prefix_name)
    get_rundate() >> sensor_task >> return_value >> ld_layer >> archive_file >> trigger_dag()

landing_layer_dag()