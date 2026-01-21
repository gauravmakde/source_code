# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        f4ceb236e834760fcadbdbd01832fa141062be7c
# CI pipeline URL:      https://git.jwn.app/TM01007/APP07324-gcp-Analytics-Insights/-/pipelines/7430782
# CI commit timestamp:  2024-11-05T17:08:43+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os, configparser
from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging


# Airflow variables
airflow_environment = os.environ.get("ENVIRONMENT", "local")
root_path = (
    path.dirname(__file__).split("APP07324-gcp-Analytics-Insights")[0]
    + "APP07324-gcp-Analytics-Insights/"
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

dag_id = "gcp_inventory_position_master_table_11521_ACE_ENG"
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


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get("dag", "nauth_conn_id"),
        cloud_conn_id=config.get("dag", "gcp_conn"),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()


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
    "description": "inventory_position_master_table_11521_ACE_ENG DAG Description",
    "retry_delay": timedelta(minutes=10),
    "email": (
        None
        if config.get(dag_id, "email").lower() == "none"
        else config.get(dag_id, "email").split(",")
    ),
    "email_on_failure": False,  # config.getboolean(dag_id, 'email_on_failure'),
    "email_on_retry": False,  # config.getboolean(dag_id, 'email_on_retry'),
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
    env = os.getenv("ENVIRONMENT")

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )

    pelican_validator = toggle_pelican_validator(pelican_flag, dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag, dag)

    inventory_position_inventory_position_master = open(
        os.path.join(sql_path, "inventory_position/inventory_position_master.sql"), "r"
    ).read()

    launch_run_bigquery_sql_refresh_master = BigQueryInsertJobOperator(
        task_id="run_bigquery_sql_refresh_master",
        configuration={
            "query": {
                "query": inventory_position_inventory_position_master,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    """launch_run_bigquery_sql_refresh_master_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ACE_ENGINEERING_APP07324_K8S_GUMBO_CONN_PROD',
    namespace='app07324',
    task_id='run_bigquery_sql_refresh_master_sensor',
    poke_interval=100,
    time_out=3600,
    job_name='run-bigquery-sql-refresh-master-inventory-position-master-table',
    nsk_cluster='nsk-gumbo-prod')"""

    (
        creds_setup
        >> launch_run_bigquery_sql_refresh_master
        >> pelican_validator
        >> pelican_sensor
    )
