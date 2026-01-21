from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from datetime import datetime


def get_batch_id(dag_id, task_id):

    current_datetime = datetime.today()
    if len(dag_id) <= 22:
        dag_id = dag_id.replace("_", "-").rstrip("-")
        ln_task_id = 45 - len(dag_id)
        task_id = task_id[-ln_task_id:].replace("_", "-").strip("-")
    elif len(task_id) <= 22:
        task_id = task_id.replace("_", "-").strip("-")
        ln_dag_id = 45 - len(task_id)
        dag_id = dag_id[:ln_dag_id].replace("_", "-").rstrip("-")
    else:
        dag_id = dag_id[:23].replace("_", "-").rstrip("-")
        task_id = task_id[-22:].replace("_", "-").strip("-")

    date = current_datetime.strftime("%Y%m%d%H%M%S")
    return f"""{dag_id}--{task_id}--{date}"""


# Define DAG arguments
default_args = {
    "start_date": days_ago(1),
    "catchup": False,
}

# Define your DAG
with DAG(
    dag_id="dataproc_pyspark_job",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    session_name = get_batch_id(
        dag_id="dataproc_pyspark_job", task_id="run_pyspark_job"
    )
    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_pyspark_job",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://test-data-datametica/app09392/isf-artifacts/pyspark/jwn_load_spark_code.py",
                "args": ["dev_nap_stg","product_online_purchasable_item_ldg",
                    "gs://test-data-datametica/app09392/isf-artifacts/data-files/PRODUCT_SELLABILITY_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/live-on-site-282fae9c-5836-4c45-bed6-1d6be6cf27d0-0.csv.gz",
                    "jwn-nap-user1-nonprod-zmnb.dev_nap_stg.product_online_purchasable_item_ldg",
                ],
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "airflow-gcp-onix-testing@jwn-nap-user1-nonprod-zmnb.iam.gserviceaccount.com",
                    "subnetwork_uri": "projects/jwn-dataprocvpc-nonprod-2lds/regions/us-west1/subnetworks/nap-user1-01",
                }
            },
        },
        region="us-west1",
        project_id="jwn-nap-user1-nonprod-zmnb",
        gcp_conn_id="gcp-onix-connection-nonprod",
        batch_id=session_name,
    )

    pyspark_task
