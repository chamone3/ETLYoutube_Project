from airflow import DAG
from airflow.decorators import dag
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlistId,
    get_video_ids,
    extract_video_data,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("Europe/Malta")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
}

@dag(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",  # todo dia às 14:00
    catchup=False,
    tags=["youtube"],
)
def produce_json():
    # Aqui estamos usando TaskFlow:
    playlist_id = get_playlistId()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json(extracted_data)   # não precisa guardar em variável

dag = produce_json()

# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # trigger_data_quality = TriggerDagRunOperator(
    #     task_id="trigger_data_quality",
    #     trigger_dag_id="data_quality",
    # )

    # Define dependencies
    update_staging >> update_core #>> trigger_data_quality

