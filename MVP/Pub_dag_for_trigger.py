from airflow import models
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from datetime import datetime
from airflow.models import Variable
import json


PROJECT_ID = 'wf-mdss-bq'
TOPIC = 'cloudfunction_topic'

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with models.DAG(
    dag_id='trigger_pubsub_topic_dag',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['pubsub'],
) as dag:
    
    dag_conf_variable = Variable.get(key="MVP_Hist_K", deserialize_json=True)
    json_message_string = json.dumps(dag_conf_variable)
    message_data_bytes = json_message_string.encode("utf-8")

    publish_message = PubSubPublishMessageOperator(
        task_id="publish_to_pubsub",
        project_id=PROJECT_ID,
        topic=TOPIC,
        messages=[
            {"data": message_data_bytes}
        ]
    )
