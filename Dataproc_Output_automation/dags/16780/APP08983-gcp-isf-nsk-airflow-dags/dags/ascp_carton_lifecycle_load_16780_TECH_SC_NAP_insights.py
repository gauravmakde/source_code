import pendulum
import os
import configparser
import yaml
from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator
from nordstrom.utils.cloud_creds import cloud_creds
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models.dagrun import DagRun

from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor
# from airflow.operators.python import get_current_context
# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator


# Airflow variables
airflow_environment = os.environ.get("ENVIRONMENT", "local")
root_path = (
    path.dirname(__file__).split("APP08983-gcp-isf-nsk-airflow-dags")[0]
    + "APP08983-gcp-isf-nsk-airflow-dags/"
)
configs_path = root_path + "configs/"
sql_path = root_path + "sql/"
module_path = root_path + "modules/"


# Fetch data from config file
config_env = (
    "development"
    if airflow_environment == "nonprod" or airflow_environment == "development"
    else "production"
)
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f"env-configs-{config_env}.cfg"))
config.read(os.path.join(configs_path, f"env-dag-specific-{config_env}.cfg"))

dag_id = "gcp_ascp_carton_lifecycle_load_16780_TECH_SC_NAP_insights"
sql_config_dict = dict(config[dag_id + "_db_param"])

os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)

from modules.pelican import Pelican
from modules.date_functions import upstream_dependency

# sys.path.append(path + '/../')

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config_env == "development" else config.get(dag_id, "cron")
dataplex_project_id = config.get("dag", "dataplex_project_id")
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = "{{ ts_nodash.lower() }}"
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get("dag", "nauth_conn_id"),
        cloud_conn_id=config.get("dag", "gcp_conn"),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

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
    "retries": 0 if config_env == "development" else 3,
    "description": "ascp_carton_lifecycle_load_16780_TECH_SC_NAP_insights DAG Description",
    "retry_delay": timedelta(minutes=10),
    # 'email': config.get(dag_id, 'email').split(','),
    # 'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    # 'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    "start_date": start_date,
    "catchup": False,
    "depends_on_past": False,
    "sla": dag_sla,
}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=(
        None if config_env == "development" else config.get(dag_id, "cron")
    ),
    max_active_runs=set_max_active_runs,
    template_searchpath=[sql_path],
    concurrency=set_concurrency,
) as dag:

    project_id = config.get("dag", "gcp_project_id")
    service_account_email = config.get("dag", "service_account_email")
    region = config.get("dag", "region")
    gcp_conn = config.get("dag", "gcp_conn")
    subnet_url = config.get("dag", "subnet_url")
    pelican_flag = config.getboolean("dag", "pelican_flag")
    env = os.getenv("ENVIRONMENT")

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )

    pelican_validator = toggle_pelican_validator(pelican_flag, dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag, dag)

    launch_other_appId_upstream_dependencies_sensor_0 = ApiDagRunLatestSensor(
    dag = dag,
    task_id='check_other_appId_upstream_dependencies_0',
    conn_id='okta_conn_app08649',
    external_dag_id='gcp_ascp_store_inventory_transfer_from_kafka_to_teradata_17610_DAS_SC_OUTBOUND_APP08649_insights_v2',
    date_query_fn = upstream_dependency.hours12_query_function,
    timeout = 3600,
    retries = 1,
    poke_interval = 120)

    launch_other_appId_upstream_dependencies_sensor_1 = ApiDagRunLatestSensor(
    dag = dag,
    task_id='check_other_appId_upstream_dependencies_1',
    conn_id='okta_conn_app07999',
    external_dag_id='gcp_vendor_asn_lifecycle_kafka_to_orc_td_17268_REDRIVER_SCOT',
    date_query_fn = upstream_dependency.hours12_query_function,
    timeout = 3600,
    retries = 1,
    poke_interval = 120)

    launch_other_appId_upstream_dependencies_sensor_2 = ApiDagRunLatestSensor(
    dag = dag,
    task_id='check_other_appId_upstream_dependencies_2',
    conn_id='okta_conn_app02592',
    external_dag_id='gcp_nrlt_wm_inbound_shipment_lifecycle_16753_TECH_SC_NAP_insights',
    date_query_fn = upstream_dependency.hours12_query_function,
    timeout = 3600,
    retries = 1,
    poke_interval = 120)

    launch_other_appId_upstream_dependencies_sensor_3 = ApiDagRunLatestSensor(
    dag = dag,
    task_id='check_other_appId_upstream_dependencies_3',
    conn_id='okta_conn_app02592',
    external_dag_id='gcp_nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights',
    date_query_fn = upstream_dependency.hours12_query_function,
    timeout = 3600,
    retries = 1,
    poke_interval = 120)

    launch_other_appId_upstream_dependencies_sensor_4 = ApiDagRunLatestSensor(
    dag = dag,
    task_id='check_other_appId_upstream_dependencies_4',
    conn_id='okta_conn_app02592',
    external_dag_id='gcp_nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights',
    date_query_fn = upstream_dependency.hours12_query_function,
    timeout = 3600,
    retries = 1,
    poke_interval = 120)

    # launch_other_appId_upstream_dependencies_sensor_0 = DummyOperator( task_id="check_other_appId_upstream_dependencies_0")
    # launch_other_appId_upstream_dependencies_sensor_1 = DummyOperator( task_id="check_other_appId_upstream_dependencies_1")
    # launch_other_appId_upstream_dependencies_sensor_2 = DummyOperator( task_id="check_other_appId_upstream_dependencies_2")
    # launch_other_appId_upstream_dependencies_sensor_3 = DummyOperator( task_id="check_other_appId_upstream_dependencies_3")
    # launch_other_appId_upstream_dependencies_sensor_4 = DummyOperator( task_id="check_other_appId_upstream_dependencies_4")


    start_batch = open(os.path.join(sql_path, "common_sql/start_batch.sql"), "r").read()

    launch_start_batch_load_start_batch = BigQueryInsertJobOperator(
        task_id="start_batch_load_start_batch",
        configuration={"query": {"query": start_batch, "useLegacySql": False}},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    ascp_carton_lifecycle_load = open(
        os.path.join(
            sql_path, "ascp_carton_lifecycle_load/ascp_carton_lifecycle_load.sql"
        ),
        "r",
    ).read()

    launch_teradata_load_teradata = BigQueryInsertJobOperator(
        task_id="teradata_load_teradata",
        configuration={
            "query": {"query": ascp_carton_lifecycle_load, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    end_batch = open(os.path.join(sql_path, "common_sql/end_batch.sql"), "r").read()

    launch_end_batch_load_end_batch = BigQueryInsertJobOperator(
        task_id="end_batch_load_end_batch",
        configuration={"query": {"query": end_batch, "useLegacySql": False}},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    creds_setup >> launch_other_appId_upstream_dependencies_sensor_0 >>[launch_start_batch_load_start_batch]
    launch_other_appId_upstream_dependencies_sensor_1 >>[launch_start_batch_load_start_batch]
    launch_other_appId_upstream_dependencies_sensor_2 >>[launch_start_batch_load_start_batch]
    launch_other_appId_upstream_dependencies_sensor_3 >>[launch_start_batch_load_start_batch]
    launch_other_appId_upstream_dependencies_sensor_4 >>[launch_start_batch_load_start_batch]
    launch_start_batch_load_start_batch >>[launch_teradata_load_teradata]
    launch_teradata_load_teradata >>[launch_end_batch_load_end_batch]
    launch_end_batch_load_end_batch >> pelican_validator >> pelican_sensor