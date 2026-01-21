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
from airflow.models.dagrun import DagRun
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

dag_id = "gcp_smartmarkdown_insights_10976_tech_nap_merch"
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
    "description": "smartmarkdown_insights_10976_tech_nap_merch DAG Description",
    "retry_delay": timedelta(minutes=15),
    # 'email': None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
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

    # def get_most_recent_dag_run(execution_date, task, **kwargs):
    #     dag_runs = DagRun.find(dag_id=task.external_dag_id)
    #     dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    #     if dag_runs:
    #         return dag_runs[0].execution_date

    # launch_check_external_upstream_dependencies_sensor_0 = ExternalTaskSensor(
    # dag=dag,
    # task_id='check_external_upstream_dependencies_0',
    # external_dag_id='gcp_sales_amount_10976_tech_nap_merch',
    # timeout=1800,
    # execution_delta=timedelta(minutes=60),
    # retries=3)

    # launch_check_external_upstream_dependencies_sensor_1 = ExternalTaskSensor(
    # dag=dag,
    # task_id='check_external_upstream_dependencies_1',
    # external_dag_id='gcp_clearance_markdown_applied_resets_10976_tech_nap_merch',
    # timeout=3600,
    # execution_date_fn=get_most_recent_dag_run,
    # poke_interval=120,
    # retries=1)

    cmd_batch_date_update = open(
        os.path.join(sql_path, "smart_markdown_insights/cmd_batch_date_update.sql"), "r"
    ).read()

    launch_run_cmd_batch_dt_update = BigQueryInsertJobOperator(
        task_id="run_cmd_batch_dt_update",
        configuration={
            "query": {"query": cmd_batch_date_update, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    clear_clearance_markdown_stock_quantity_end_of_wk_fact = open(
        os.path.join(
            sql_path,
            "smart_markdown_insights/clear_clearance_markdown_stock_quantity_end_of_wk_fact.sql",
        ),
        "r",
    ).read()

    launch_run_clear_inv_fact = BigQueryInsertJobOperator(
        task_id="run_clear_inv_fact",
        configuration={
            "query": {
                "query": clear_clearance_markdown_stock_quantity_end_of_wk_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_clearance_markdown_stock_quantity_end_of_wk_fact = open(
        os.path.join(
            sql_path,
            "smart_markdown_insights/load_clearance_markdown_stock_quantity_end_of_wk_fact.sql",
        ),
        "r",
    ).read()

    launch_run_load_inv_fact = BigQueryInsertJobOperator(
        task_id="run_load_inv_fact",
        configuration={
            "query": {
                "query": load_clearance_markdown_stock_quantity_end_of_wk_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    clear_clearance_markdown_insights_by_wk_fact = open(
        os.path.join(
            sql_path,
            "smart_markdown_insights/clear_clearance_markdown_insights_by_wk_fact.sql",
        ),
        "r",
    ).read()

    launch_run_clear_insights_fact = BigQueryInsertJobOperator(
        task_id="run_clear_insights_fact",
        configuration={
            "query": {
                "query": clear_clearance_markdown_insights_by_wk_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_common_inventory = open(
        os.path.join(sql_path, "smart_markdown_insights/load_common_inventory.sql"), "r"
    ).read()

    launch_run_load_common_inventory = BigQueryInsertJobOperator(
        task_id="run_load_common_inventory",
        configuration={
            "query": {"query": load_common_inventory, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_non_sellable_stock = open(
        os.path.join(sql_path, "smart_markdown_insights/load_non_sellable_stock.sql"),
        "r",
    ).read()

    launch_run_load_non_sellable_stock = BigQueryInsertJobOperator(
        task_id="run_load_non_sellable_stock",
        configuration={
            "query": {"query": load_non_sellable_stock, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    clear_inventory_receipts_insights_fact = open(
        os.path.join(
            sql_path,
            "smart_markdown_insights/clear_inventory_receipts_insights_fact.sql",
        ),
        "r",
    ).read()

    launch_run_clear_first_last_receipts = BigQueryInsertJobOperator(
        task_id="run_clear_first_last_receipts",
        configuration={
            "query": {
                "query": clear_inventory_receipts_insights_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_inventory_receipts_insights_fact = open(
        os.path.join(
            sql_path,
            "smart_markdown_insights/load_inventory_receipts_insights_fact.sql",
        ),
        "r",
    ).read()

    launch_run_load_first_last_receipts = BigQueryInsertJobOperator(
        task_id="run_load_first_last_receipts",
        configuration={
            "query": {
                "query": load_inventory_receipts_insights_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_sales_insights = open(
        os.path.join(sql_path, "smart_markdown_insights/load_sales_insights.sql"), "r"
    ).read()

    launch_run_load_sales_insights = BigQueryInsertJobOperator(
        task_id="run_load_sales_insights",
        configuration={"query": {"query": load_sales_insights, "useLegacySql": False}},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_additional_sales_insights = open(
        os.path.join(
            sql_path, "smart_markdown_insights/load_additional_sales_insights.sql"
        ),
        "r",
    ).read()

    launch_run_load_additional_sales_insights = BigQueryInsertJobOperator(
        task_id="run_load_additional_sales_insights",
        configuration={
            "query": {"query": load_additional_sales_insights, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_product = open(
        os.path.join(sql_path, "smart_markdown_insights/load_product.sql"), "r"
    ).read()

    launch_run_load_product = BigQueryInsertJobOperator(
        task_id="run_load_product",
        configuration={"query": {"query": load_product, "useLegacySql": False}},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_aggregates = open(
        os.path.join(sql_path, "smart_markdown_insights/load_aggregates.sql"), "r"
    ).read()

    launch_run_load_aggregates = BigQueryInsertJobOperator(
        task_id="run_load_aggregates",
        configuration={"query": {"query": load_aggregates, "useLegacySql": False}},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    load_sell_thru_and_markdown_insights = open(
        os.path.join(
            sql_path, "smart_markdown_insights/load_sell_thru_and_markdown_insights.sql"
        ),
        "r",
    ).read()

    launch_run_load_sell_thru_and_markdown_insights = BigQueryInsertJobOperator(
        task_id="run_load_sell_thru_and_markdown_insights",
        configuration={
            "query": {
                "query": load_sell_thru_and_markdown_insights,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # (
    #     creds_setup
    #     >> launch_check_external_upstream_dependencies_sensor_0
    #     >> [launch_run_cmd_batch_dt_update]
    # )
    # launch_check_external_upstream_dependencies_sensor_1 >> [
    #     launch_run_cmd_batch_dt_update
    # ]
    # launch_run_cmd_batch_dt_update >> [launch_run_clear_inv_fact]
    # launch_run_clear_inv_fact >> [launch_run_load_inv_fact]
    # launch_run_load_inv_fact >> [launch_run_clear_insights_fact]
    # launch_run_clear_insights_fact >> [launch_run_load_common_inventory]
    # launch_run_load_common_inventory >> [launch_run_load_non_sellable_stock]
    # launch_run_load_non_sellable_stock >> [launch_run_clear_first_last_receipts]
    # launch_run_clear_first_last_receipts >> [launch_run_load_first_last_receipts]
    # launch_run_load_first_last_receipts >> [launch_run_load_sales_insights]
    # launch_run_load_sales_insights >> [launch_run_load_additional_sales_insights]
    # launch_run_load_additional_sales_insights >> [launch_run_load_product]
    # launch_run_load_product >> [launch_run_load_aggregates]
    # launch_run_load_aggregates >> [launch_run_load_sell_thru_and_markdown_insights]
    # (
    #     launch_run_load_sell_thru_and_markdown_insights
    #     >> pelican_validator
    #     >> pelican_sensor
    # )

    creds_setup >> launch_run_cmd_batch_dt_update
    launch_run_cmd_batch_dt_update >> [launch_run_clear_inv_fact]
    launch_run_clear_inv_fact >> [launch_run_load_inv_fact]
    launch_run_load_inv_fact >> [launch_run_clear_insights_fact]
    launch_run_clear_insights_fact >> [launch_run_load_common_inventory]
    launch_run_load_common_inventory >> [launch_run_load_non_sellable_stock]
    launch_run_load_non_sellable_stock >> [launch_run_clear_first_last_receipts]
    launch_run_clear_first_last_receipts >> [launch_run_load_first_last_receipts]
    launch_run_load_first_last_receipts >> [launch_run_load_sales_insights]
    launch_run_load_sales_insights >> [launch_run_load_additional_sales_insights]
    launch_run_load_additional_sales_insights >> [launch_run_load_product]
    launch_run_load_product >> [launch_run_load_aggregates]
    launch_run_load_aggregates >> [launch_run_load_sell_thru_and_markdown_insights]
    (
        launch_run_load_sell_thru_and_markdown_insights
        >> pelican_validator
        >> pelican_sensor
    )
