import pendulum
import os, configparser
from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
import logging
from nordstrom.utils.cloud_creds import cloud_creds
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

# Airflow variables
airflow_environment = os.environ.get("ENVIRONMENT", "local")
root_path = (
    path.dirname(__file__).split("APP05039-gcp-stylingisf-dags")[0]
    + "APP05039-gcp-stylingisf-dags/"
)
configs_path = root_path + "configs/"
sql_path = root_path + "sql/"
module_path = root_path + "modules/"

# Fetch data from config file
config_env = (
    "development" if airflow_environment in ["nonprod", "development"] else "production"
)
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f"env-configs-{config_env}.cfg"))
config.read(os.path.join(configs_path, f"env-dag-specific-{config_env}.cfg"))

dag_id = "gcp_sqs_to_demand_forecast_hourly_v1_17168_styling_styling_insights_kpi"
sql_config_dict = dict(config[dag_id + "_db_param"])


os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)

from modules.pelican import Pelican

start_date = pendulum.datetime(2022, 2, 24)
cron = (
    None if config.get(dag_id, "cron").lower() == "none" else config.get(dag_id, "cron")
)
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = "{{ ts_nodash }}"
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

# Get the Teradata connection details based on the environment

# bigquery_environment = config.get(env, 'bigquery_environment')
# service_account_email = config.get(env, "service_account_email")
# subnet_url = config.get(env, "subnet_url")
# project_id = config.get(env, "gcp_project_id")

# region = config.get(env, "region")
# gcp_conn_id=config.get(env, 'gcp_conn')


# Initializing BQ hook
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
    return f"""{dag_id}--{task_id}--{date}""".lower()


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get("dag", "nauth_conn_id"),
        cloud_conn_id=config.get("dag", "gcp_conn"),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()


def setup_creds_aws(aws_iam_role_arn: str):
    @cloud_creds(
        nauth_conn_id="nauth-connection-nonprod",
        cloud_conn_id="aws_default",
        aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer",
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()


# Adding Pelican task dependencies
def pelican_check():
    return Pelican.validate(dag_name=dag_id, env=airflow_environment)


class PelicanJobSensor(BaseSensorOperator):
    def poke(self, context):
        return Pelican.check_job_status(
            context, dag_name=dag.dag_id, env=airflow_environment
        )


def toggle_pelican_validator(pelican_flag, dag):
    if pelican_flag == True:
        pelican_sensor_task = PelicanJobSensor(
            task_id="pelican_job_sensor",
            poke_interval=300,  # check every 5 minutes
            timeout=4500,  # timeout after 75 minutes
            mode="poke",
        )

        return pelican_sensor_task
    else:
        dummy_sensor = DummyOperator(task_id="dummy_sensor", dag=dag)

        return dummy_sensor


def toggle_pelican_sensor(pelican_flag, dag):
    if pelican_flag == True:
        pelican_validation = PythonOperator(
            task_id="pelican_validation", python_callable=pelican_check, dag=dag
        )

        return pelican_validation
    else:
        dummy_validation = DummyOperator(task_id="dummy_validation", dag=dag)

        return dummy_validation


default_args = {
    "retries": 3,
    "description": "sqs_to_demand_forecast_hourly_v1_17168_styling_styling_insights_kpi DAG Description",
    "retry_delay": timedelta(minutes=10),
    # "email": config.get(dag_id, "email").split(","),
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": start_date,
    "catchup": False,
    "depends_on_past": False,
    "sla": dag_sla,
}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=cron,
    max_active_runs=set_max_active_runs,
    concurrency=set_concurrency,
) as dag:

    project_id = config.get("dag", "gcp_project_id")
    service_account_email = config.get("dag", "service_account_email")
    region = config.get("dag", "region")
    gcp_conn = config.get("dag", "gcp_conn")
    subnet_url = config.get("dag", "subnet_url")
    pelican_flag = config.getboolean("dag", "pelican_flag")
    # Bq load configs:
    gcs_bucket = config.get("dag", "gcs_bucket")
    s3_bucket = config.get("dag", "s3_bucket")
    s3_prefix = config.get("dag", "s3_prefix")
    aws_conn_id = config.get("dag", "aws_conn_id")
    pyspark_path = config.get("dag", "pyspark_path")

    env = os.getenv("ENVIRONMENT")

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )
    creds_setup_aws = PythonOperator(
        task_id="setup_creds_aws",
        python_callable=setup_creds_aws,
        op_args=["arn:aws:iam::290445963451:role/gcp_data_transfer"],
    )

    pelican_validator = toggle_pelican_validator(pelican_flag, dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag, dag)

    demand_forecast_hourly = open(
        os.path.join(sql_path, "demand_forecast_hourly.sql"), "r"
    ).read()

    launch_read_from_s3_stage_0_job_1 = BigQueryInsertJobOperator(
        task_id="read_from_s3_stage_0_job_1",
        configuration={
            "query": {"query": demand_forecast_hourly, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    session_name = get_batch_id(dag_id=f"{dag_id}", task_id="read_from_s3_stage_0_job")

    read_from_s3_stage_0_job = DataprocCreateBatchOperator(
        task_id="read_from_s3_stage_0_job",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": pyspark_path,
                "args": [
                    f"{project_id}",
                    sql_config_dict.get("schema_name"),
                    sql_config_dict.get("object_name"),
                    f"{gcs_bucket+s3_prefix}/*",
                ],
            },
            "environment_config": {
                "execution_config": {
                    "service_account": service_account_email,
                    "subnetwork_uri": subnet_url,
                }
            },
        },
        region=region,
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        batch_id=session_name,
        dag=dag,
    )
    # read_from_s3_stage_0_job_0 = dataproc_gcs_to_bq(
    #     "read_from_s3_stage_0_job_0",
    #     dag_id,
    #     sql_config_dict.get("schema_name"),
    #     sql_config_dict.get("object_name"),
    #     gcp_conn,
    #     gcs_bucket + s3_prefix,
    #     dag,
    # )

    # product_image_asset_s3_to_gcs = call_s3_to_gcs(
    #     "product_image_asset_s3_to_gcs",
    #     aws_conn_id,
    #     gcp_conn,
    #     s3_bucket,
    #     s3_prefix,
    #     gcs_bucket,
    #     dag,
    # )
    read_from_s3_stage_0_job_0 = S3ToGCSOperator(
        task_id="read_from_s3_stage_0_job_0",
        aws_conn_id=aws_conn_id,
        bucket=s3_bucket,
        prefix=s3_prefix,
        gcp_conn_id=gcp_conn,
        dest_gcs=gcs_bucket,
        replace=False,
        dag=dag,
    )
    # def call_s3_to_gcs(task_id,aws_conn_id,gcp_conn_id,s3_bucket,s3_prefix,dest_gcs,dag):
    #     return S3ToGCSOperator(
    #             task_id=task_id,
    #             aws_conn_id=aws_conn_id,
    #             bucket=s3_bucket,
    #             prefix=s3_prefix,
    #             gcp_conn_id=gcp_conn_id,
    #             dest_gcs=dest_gcs,
    #             replace=False,
    #             dag=dag,
    #     )

    # Email needs to be changed

    (
        creds_setup
        >> creds_setup_aws
        >> read_from_s3_stage_0_job_0
        >> read_from_s3_stage_0_job
        >> launch_read_from_s3_stage_0_job_1
        >> pelican_validator
        >> pelican_sensor
    )
