from airflow import models
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from datetime import datetime

PROJECT_ID = 'wf-mdss-bq'  # Replace with your actual GCP project ID
TOPIC_NAME = 'my-airflow-topic'  # The topic connected to the Cloud Function

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with models.DAG(
    dag_id='trigger_pubsub_topic_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pubsub', 'cloud-function'],
) as dag:

    publish_message = PubSubPublishMessageOperator(
        task_id="publish_to_pubsub",
        project_id=PROJECT_ID,
        topic=TOPIC_NAME,
        messages=[
            {
                "data": "Trigger downstream DAG".encode("utf-8"),
                # Optional attributes if needed:
                # "attributes": {"source": "airflow"}
            }
        ],
    )
