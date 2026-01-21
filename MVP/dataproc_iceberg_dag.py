from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator, DataprocStopClusterOperator

import time
# Replace these values
PROJECT_ID = 'wf-mdss-bq'
timestr = time.strftime("%Y-%m-%d-%H%M")
REGION = 'us-central1'
CLUSTER_NAME = 'wf-mdss-bq-dpk'
BUCKET = 'wellshadoopmigration-bucket'
OUTPUT_PATH = f'gs://{BUCKET}/iceberg_table'
# SCRIPT_URI = f'gs://{BUCKET}/iceberg_table/scripts/spark_iceberg_writer.py'
SCRIPT_URI = f'gs://{BUCKET}/iceberg_table/scripts/table_creation_spark.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dataproc_iceberg_writer',
    default_args=default_args,
    description='Submit PySpark job to create Iceberg table on Dataproc',
    schedule_interval=None,
    start_date=days_ago(1),
)

# Define the PySpark job
pyspark_job = {
    'reference': {'job_id': f'iceberg_writer_job_{timestr}'},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {
        'main_python_file_uri': SCRIPT_URI,
        'args': [OUTPUT_PATH],
        'jar_file_uris':['gs://wellshadoopmigration-bucket/iceberg_table/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar']
    }
}

# start_cluster = DataprocStartClusterOperator(
#         task_id="start_cluster",
#         project_id=PROJECT_ID,
#         region=REGION,
#         cluster_name=CLUSTER_NAME,
#         gcp_conn_id="gcp_wf_mdss_bq"
#     )


submit_iceberg = DataprocSubmitJobOperator(
    task_id='submit_iceberg_job',
    job=pyspark_job,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

# stop_cluster = DataprocStopClusterOperator(
#     task_id="stop_cluster",
#     project_id=PROJECT_ID,
#     region=REGION,
#     cluster_name=CLUSTER_NAME,
# )


# start_cluster >> submit_iceberg >> stop_cluster
submit_iceberg