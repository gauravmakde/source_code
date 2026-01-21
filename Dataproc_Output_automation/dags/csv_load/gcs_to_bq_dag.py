from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    dag_id="gcs_to_bigquery_example",
    default_args=default_args,
    description="Load data from GCS to BigQuery using GCSToBigQueryOperator",
    schedule_interval=None,
    start_date=days_ago(1),
)

delimiter = "\x1F"
DATASET_NAME = "dev_nap_stg"
TABLE_NAME = "product_online_purchasable_item_ldg"

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example1",
    bucket="test-data-datametica",
    source_objects=[
        "app09392/isf-artifacts/data-files/PRODUCT_SELLABILITY_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/live-on-site-282fae9c-5836-4c45-bed6-1d6be6cf27d0-0.csv.gz"
    ],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    write_disposition="WRITE_TRUNCATE",
    field_delimiter=f"{delimiter}",
    autodetect=True,
    gcp_conn_id="gcp-onix-connection-nonprod",
    dag=dag,
)


load_csv
