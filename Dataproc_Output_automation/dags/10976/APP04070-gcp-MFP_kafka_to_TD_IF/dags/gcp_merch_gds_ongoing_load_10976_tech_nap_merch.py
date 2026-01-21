# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        d8d4673c8dea33f2fbf0626ccb3c8ad78d049999
# CI pipeline URL:      https://git.jwn.app/TM00593/das_merch/APP04070-gcp-MFP_kafka_to_TD_IF/-/pipelines/6967821
# CI commit timestamp:  2024-09-13T13:20:23+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import configparser
import logging
import os
import pendulum
import sys
from datetime import datetime, timedelta
from os import path
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor

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

dag_id = "gcp_merch_gds_ongoing_load_10976_tech_nap_merch"
sql_config_dict = dict(config[dag_id + "_db_param"])

os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)


from modules.pelican import Pelican
from modules.sub1 import upstream_dependency

start_date = pendulum.datetime(2022, 2, 24, tz="US/Pacific")
cron = (
    None if config.get(dag_id, "cron").lower() == "none" else config.get(dag_id, "cron")
)
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = "{{ ts_nodash.lower() }}"
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None


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
    "retries": 0 if config_env == "development" else 2,
    "description": "merch_gds_ongoing_load_10976_tech_nap_merch DAG Description",
    "retry_delay": timedelta(minutes=15),
    # 'email': None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
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

    # launch_other_appId_upstream_dependencies_sensor_0 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_0",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_inventory_day_fact_load_dly_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.date_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    # launch_other_appId_upstream_dependencies_sensor_1 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_1",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_merch_nap_sales_fact_load_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.weekly_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    # launch_other_appId_upstream_dependencies_sensor_2 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_2",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_transfer_apt_load_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.weekly_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    # launch_other_appId_upstream_dependencies_sensor_3 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_3",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_demand_fact_load_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.weekly_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    # launch_other_appId_upstream_dependencies_sensor_4 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_4",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_receipt_sku_store_fact_load_poreceipt_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.weekly_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    # launch_other_appId_upstream_dependencies_sensor_5 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_5",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_wac_object_model_to_sematic_load_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.weekly_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    # launch_other_appId_upstream_dependencies_sensor_6 = ApiDagRunLatestSensor(
    #     dag=dag,
    #     task_id="check_other_appId_upstream_dependencies_6",
    #     conn_id="okta_conn_to_APP04070_team",
    #     external_dag_id="gcp_wac_channel_dim_load_10976_tech_nap_merch",
    #     date_query_fn=upstream_dependency.weekly_query_function,
    #     timeout=3600,
    #     retries=1,
    #     poke_interval=120,
    # )

    merch_inv_sku_store_day_eoh_load = open(
        os.path.join(sql_path, "merch_inv_sku_store_day_eoh_load.sql"), "r"
    ).read()

    launch_gds_load_gds_inv_eoh_load = BigQueryInsertJobOperator(
        task_id="gds_load_gds_inv_eoh_load",
        configuration={
            "query": {"query": merch_inv_sku_store_day_eoh_load, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    merch_inv_sku_store_day_boh_load = open(
        os.path.join(sql_path, "merch_inv_sku_store_day_boh_load.sql"), "r"
    ).read()

    launch_gds_load_gds_inv_boh_load = BigQueryInsertJobOperator(
        task_id="gds_load_gds_inv_boh_load",
        configuration={
            "query": {"query": merch_inv_sku_store_day_boh_load, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    merch_sale_sku_store_day_work_load = open(
        os.path.join(sql_path, "merch_sale_sku_store_day_work_load.sql"), "r"
    ).read()

    launch_gds_load_gds_sales_load = BigQueryInsertJobOperator(
        task_id="gds_load_gds_sales_load",
        configuration={
            "query": {
                "query": merch_sale_sku_store_day_work_load,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    merch_tran_sku_store_day_work_load = open(
        os.path.join(sql_path, "merch_tran_sku_store_day_work_load.sql"), "r"
    ).read()

    launch_gds_load_gds_tran_work_load = BigQueryInsertJobOperator(
        task_id="gds_load_gds_tran_work_load",
        configuration={
            "query": {
                "query": merch_tran_sku_store_day_work_load,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    merch_tran_sku_store_day_fact_load = open(
        os.path.join(sql_path, "merch_tran_sku_store_day_fact_load.sql"), "r"
    ).read()

    launch_gds_load_gds_tran_fact_load = BigQueryInsertJobOperator(
        task_id="gds_load_gds_tran_fact_load",
        configuration={
            "query": {
                "query": merch_tran_sku_store_day_fact_load,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    gds_batch_date_update = open(
        os.path.join(sql_path, "gds_batch_date_update.sql"), "r"
    ).read()

    launch_gds_load_gds_batch_date_update = BigQueryInsertJobOperator(
        task_id="gds_load_gds_batch_date_update",
        configuration={
            "query": {"query": gds_batch_date_update, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # (
    #     creds_setup
    #     >> launch_other_appId_upstream_dependencies_sensor_0
    #     >> [launch_gds_load_gds_inv_eoh_load]
    # )
    # launch_other_appId_upstream_dependencies_sensor_1 >> [
    #     launch_gds_load_gds_inv_eoh_load
    # ]
    # launch_other_appId_upstream_dependencies_sensor_2 >> [
    #     launch_gds_load_gds_inv_eoh_load
    # ]
    # launch_other_appId_upstream_dependencies_sensor_3 >> [
    #     launch_gds_load_gds_inv_eoh_load
    # ]
    # launch_other_appId_upstream_dependencies_sensor_4 >> [
    #     launch_gds_load_gds_inv_eoh_load
    # ]
    # launch_other_appId_upstream_dependencies_sensor_5 >> [
    #     launch_gds_load_gds_inv_eoh_load
    # ]
    # launch_other_appId_upstream_dependencies_sensor_6 >> [
    #     launch_gds_load_gds_inv_eoh_load
    # ]
    # launch_gds_load_gds_inv_eoh_load >> [launch_gds_load_gds_inv_boh_load]
    # launch_gds_load_gds_inv_boh_load >> [launch_gds_load_gds_sales_load]
    # launch_gds_load_gds_sales_load >> [launch_gds_load_gds_tran_work_load]
    # launch_gds_load_gds_tran_work_load >> [launch_gds_load_gds_tran_fact_load]
    # launch_gds_load_gds_tran_fact_load >> [launch_gds_load_gds_batch_date_update]
    # launch_gds_load_gds_batch_date_update >> pelican_validator >> pelican_sensor

    creds_setup >> launch_gds_load_gds_inv_eoh_load
    launch_gds_load_gds_inv_eoh_load >> [launch_gds_load_gds_inv_boh_load]
    launch_gds_load_gds_inv_boh_load >> [launch_gds_load_gds_sales_load]
    launch_gds_load_gds_sales_load >> [launch_gds_load_gds_tran_work_load]
    launch_gds_load_gds_tran_work_load >> [launch_gds_load_gds_tran_fact_load]
    launch_gds_load_gds_tran_fact_load >> [launch_gds_load_gds_batch_date_update]
    launch_gds_load_gds_batch_date_update >> pelican_validator >> pelican_sensor
