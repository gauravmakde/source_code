# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        e56289031cac5b0ff6d0f69dc3841146a1416441
# CI pipeline URL:      https://git.jwn.app/TM00593/das_merch/APP04070-gcp-MFP_kafka_to_TD_IF/-/pipelines/6430191
# CI commit timestamp:  2024-07-04T06:27:04+00:00
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
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor
from airflow.sensors.external_task import ExternalTaskSensor


# Airflow variables
airflow_environment = os.environ.get("ENVIRONMENT", "local")
root_path = (
    path.dirname(__file__).split("APP04070-gcp-MFP_kafka_to_TD_IF")[0]
    + "APP04070-gcp-MFP_kafka_to_TD_IF/"
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

dag_id = "gcp_inventory_day_fact_load_dly_10976_tech_nap_merch"
sql_config_dict = dict(config[dag_id + "_db_param"])

os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)

from modules.pelican import Pelican
from modules.sub1 import upstream_dependency

start_date = pendulum.datetime(2022, 2, 24, tz = "US/Pacific")
cron = None if config.get(dag_id, 'cron').lower() == 'none' else config.get(dag_id, 'cron')

paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = "{{ ts_nodash }}"
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes = int(-1)) if int(-1) > 1 else None

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

def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id = config.get("dag", "nauth_conn_id"),
        cloud_conn_id = config.get("dag", "gcp_conn"),
        service_account_email = service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def pelican_check():
    return Pelican.validate(dag_name = dag_id, env = airflow_environment)

class PelicanJobSensor(BaseSensorOperator):
    def poke(self, context):
        return Pelican.check_job_status(
            context, dag_name = dag.dag_id, env = airflow_environment
        )

def toggle_pelican_validator(pelican_flag, dag):
    if pelican_flag == True:
        pelican_sensor_task = PelicanJobSensor(
            task_id = "pelican_job_sensor",
            poke_interval = 300,  # check every 5 minutes
            timeout = 4500,  # timeout after 75 minutes
            mode = "poke",
        )

        return pelican_sensor_task
    else:
        dummy_sensor = DummyOperator(task_id = "dummy_sensor", dag = dag)

        return dummy_sensor

def toggle_pelican_sensor(pelican_flag, dag):
    if pelican_flag == True:
        pelican_validation = PythonOperator(
            task_id = "pelican_validation", python_callable = pelican_check, dag = dag
        )

        return pelican_validation
    else:
        dummy_validation = DummyOperator(task_id = "dummy_validation", dag = dag)

        return dummy_validation

default_args = {
    "retries": 0 if config_env == "development" else 2,
    "description": "inventory_day_fact_load_dly_10976_tech_nap_merch DAG Description",
    "retry_delay": timedelta(minutes = 15),
    # "email": (
    #     None
    #     if config.get(dag_id, "email").lower() == "none"
    #     else config.get(dag_id, "email").split(",")
    # ),
    # "email_on_failure": config.getboolean(dag_id, "email_on_failure"),
    # "email_on_retry": config.getboolean(dag_id, "email_on_retry"),
    "start_date": start_date,
    "catchup": False,
    "depends_on_past": False,
    # "sla": dag_sla,
}

def default_query_function():
    current_date_time = datetime.now()

    return {"execution_date_lte": current_date_time.isoformat()}

with DAG(
    dag_id = dag_id,
    default_args = default_args,
    schedule_interval = cron,
    max_active_runs = set_max_active_runs,
    concurrency = set_concurrency,
) as dag:

    project_id = config.get("dag", "gcp_project_id")
    service_account_email = config.get("dag", "service_account_email")
    region = config.get("dag", "region")
    gcp_conn = config.get("dag", "gcp_conn")
    subnet_url = config.get("dag", "subnet_url")
    pelican_flag = config.getboolean("dag", "pelican_flag")
    env = os.getenv("ENVIRONMENT")

    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args=[service_account_email],
    )

    pelican_validator = toggle_pelican_validator(pelican_flag, dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag, dag)


    launch_other_appId_upstream_dependencies_sensor_0 = ApiDagRunLatestSensor(
        dag = dag,
        task_id = "check_other_appId_upstream_dependencies_0",
        conn_id = "okta_conn_to_APP08983_team",
        external_dag_id = "gcp_ascp_rms_all_inventory_stock_quantity_load_v1_16780_TECH_SC_NAP_insights",
        date_query_fn = upstream_dependency.date_query_function,
        timeout = 3600,
        retries = 1,
        poke_interval = 120,
    )

    merch_inventory_sku_store_day_fct_load_dly = open(
        os.path.join(sql_path, "merch_inventory_sku_store_day_fct_load_dly.sql"), "r"
    ).read()

    launch_inventory_day_fact_load_dly_inv_day_fct_load = BigQueryInsertJobOperator(
        task_id = "inventory_day_fact_load_dly_inv_day_fct_load",
        configuration={
            "query": {
                "query": merch_inventory_sku_store_day_fct_load_dly,
                "useLegacySql": False,
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag,
    )

    inv_dly_batch_update = open(
        os.path.join(sql_path, "inv_dly_batch_update.sql"), "r"
    ).read()

    launch_inventory_day_fact_load_dly_inv_dw_batch_update = BigQueryInsertJobOperator(
        task_id = "inventory_day_fact_load_dly_inv_dw_batch_update",
        configuration={"query": {"query": inv_dly_batch_update, "useLegacySql": False}},
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag,
    )


    (
        creds_setup
        >> launch_other_appId_upstream_dependencies_sensor_0
        >> [launch_inventory_day_fact_load_dly_inv_day_fct_load]
    )
    launch_inventory_day_fact_load_dly_inv_day_fct_load >> [launch_inventory_day_fact_load_dly_inv_dw_batch_update]
    (
        launch_inventory_day_fact_load_dly_inv_dw_batch_update
        >> pelican_validator
        >> pelican_sensor
    )
