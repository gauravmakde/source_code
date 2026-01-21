# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        9abab657b849d9392975402a32c34550e62618ae
# CI pipeline URL:      https://git.jwn.app/TM01478/app08983-gcp-isf-nsk-airflow-dags/-/pipelines/7194774
# CI commit timestamp:  2024-10-10T16:25:13+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os
import configparser

from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging


# from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor


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
    "development" if airflow_environment in ["nonprod", "development"] else "production"
)
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f"env-configs-{config_env}.cfg"))
config.read(os.path.join(configs_path, f"env-dag-specific-{config_env}.cfg"))

dag_id = "gcp_nap_jwn_metrics_clarity_td_load_16780_TECH_SC_NAP_insights"
sql_config_dict = dict(config[dag_id + "_db_param"])

os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)


from modules.pelican import Pelican


# sys.path.append(path + '/../')
# from modules.date_functions import upstream_dependency

start_date = pendulum.datetime(2022, 2, 24)
cron = (
    None if config.get(dag_id, "cron").lower() == "none" else config.get(dag_id, "cron")
)
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
    "description": "nap_jwn_metrics_clarity_td_load_16780_TECH_SC_NAP_insights DAG Description",
    "retry_delay": timedelta(minutes=10),
    "email": (
        None
        if config.get(dag_id, "email").lower() == "none"
        else config.get(dag_id, "email").split(",")
    ),
    "email_on_failure": config.getboolean(dag_id, "email_on_failure"),
    "email_on_retry": config.getboolean(dag_id, "email_on_retry"),
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
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_0',
    # conn_id='okta_conn_app08983',
    # external_dag_id='gcp_purchase_order_lifecycle_kafka_to_orc_td_16780_TECH_SC_NAP_insights',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_1 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_1',
    # conn_id='okta_conn_app07999',
    # external_dag_id='gcp_vendor_asn_lifecycle_kafka_to_orc_td_17268_REDRIVER_SCOT',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_2 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_2',
    # conn_id='okta_conn_app08983',
    # external_dag_id='gcp_ascp_rms_all_inventory_stock_quantity_load_v1_16780_TECH_SC_NAP_insights',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_3 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_3',
    # conn_id='okta_conn_app08983',
    # external_dag_id='gcp_ascp_po_receipt_v1_16780_TECH_SC_NAP_insights',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_4 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_4',
    # conn_id='okta_conn_app08983',
    # external_dag_id='gcp_ascp_transfer_logical_16780_TECH_SC_NAP_insights',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_5 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_5',
    # conn_id='okta_conn_app08983',
    # external_dag_id='gcp_ascp_rtv_logical_16780_TECH_SC_NAP_insights',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_6 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_6',
    # conn_id='okta_conn_app08918',
    # external_dag_id='gcp_TECH_Merch_NAP_DEMAND_KPI_PRODUCT_LOAD',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    # launch_other_appId_upstream_dependencies_sensor_7 = ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_7',
    # conn_id='okta_conn_app08918',
    # external_dag_id='gcp_PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    nap_jwn_metrics_clarity_load_clarity_batch_start = open(
        os.path.join(sql_path, "nap_jwn_metrics_clarity_load/clarity_batch_start.sql"),
        "r",
    ).read()

    launch_Batch_start_job0_batch_start = BigQueryInsertJobOperator(
        task_id="Batch_start_job0_batch_start",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_clarity_batch_start,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_inventory_sku_loc_day_fact_ld = open(
        os.path.join(
            sql_path,
            "nap_jwn_metrics_clarity_load/jwn_inventory_sku_loc_day_fact_ld.sql",
        ),
        "r",
    ).read()

    launch_SKULOC_inventory_sku_loc_day_fact_ld = BigQueryInsertJobOperator(
        task_id="SKULOC_inventory_sku_loc_day_fact_ld",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_inventory_sku_loc_day_fact_ld,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_init_zero_count_dq_metrics_fact = open(
        os.path.join(
            sql_path,
            "nap_jwn_metrics_clarity_load/jwn_init_zero_count_dq_metrics_fact.sql",
        ),
        "r",
    ).read()

    launch_DQ_metrics_init_jwn_init_zero_count_dq_metrics_fact = BigQueryInsertJobOperator(
        task_id="DQ_metrics_init_jwn_init_zero_count_dq_metrics_fact",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_init_zero_count_dq_metrics_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_dq_timeliness_start = open(
        os.path.join(sql_path, "nap_jwn_metrics_clarity_load/dq_timeliness_start.sql"),
        "r",
    ).read()

    launch_Timeliness_start_load01 = BigQueryInsertJobOperator(
        task_id="Timeliness_start_load01",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_dq_timeliness_start,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_purchase_order_rms_receipts_fact_ld = open(
        os.path.join(
            sql_path,
            "nap_jwn_metrics_clarity_load/jwn_purchase_order_rms_receipts_fact_ld.sql",
        ),
        "r",
    ).read()

    launch_Receipt_purchase_order_rms_receipts_fact_ld = BigQueryInsertJobOperator(
        task_id="Receipt_purchase_order_rms_receipts_fact_ld",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_purchase_order_rms_receipts_fact_ld,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_transfers_rms_fact_ld = open(
        os.path.join(
            sql_path, "nap_jwn_metrics_clarity_load/jwn_transfers_rms_fact_ld.sql"
        ),
        "r",
    ).read()

    launch_Transfer_rms_fact_ld = BigQueryInsertJobOperator(
        task_id="Transfer_rms_fact_ld",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_transfers_rms_fact_ld,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_rtv_delta_ld = open(
        os.path.join(sql_path, "nap_jwn_metrics_clarity_load/jwn_rtv_delta_ld.sql"), "r"
    ).read()

    launch_RTV_jwn_rtv_delta_ld = BigQueryInsertJobOperator(
        task_id="RTV_jwn_rtv_delta_ld",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_rtv_delta_ld,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_external_in_transit_ld = open(
        os.path.join(
            sql_path, "nap_jwn_metrics_clarity_load/jwn_external_in_transit_ld.sql"
        ),
        "r",
    ).read()

    launch_Intrans_jwn_external_in_transit_ld = BigQueryInsertJobOperator(
        task_id="Intrans_jwn_external_in_transit_ld",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_external_in_transit_ld,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_aoi_last_receipt_date = open(
        os.path.join(
            sql_path, "nap_jwn_metrics_clarity_load/jwn_aoi_last_receipt_date.sql"
        ),
        "r",
    ).read()

    launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt = BigQueryInsertJobOperator(
        task_id="AOI_merge_jwn_aoi_last_receipt_date_delta_updt",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_aoi_last_receipt_date,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_aoi_network_delta_updt = open(
        os.path.join(
            sql_path, "nap_jwn_metrics_clarity_load/jwn_aoi_network_delta_updt.sql"
        ),
        "r",
    ).read()

    launch_AOI_merge_jwn_aoi_network_delta_updt = BigQueryInsertJobOperator(
        task_id="AOI_merge_jwn_aoi_network_delta_updt",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_aoi_network_delta_updt,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_aoi_exposed_delta_updt = open(
        os.path.join(
            sql_path, "nap_jwn_metrics_clarity_load/jwn_aoi_exposed_delta_updt.sql"
        ),
        "r",
    ).read()

    launch_AOI_merge_jwn_aoi_exposed_delta_updt = BigQueryInsertJobOperator(
        task_id="AOI_merge_jwn_aoi_exposed_delta_updt",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_aoi_exposed_delta_updt,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_product_ld = open(
        os.path.join(sql_path, "nap_jwn_metrics_clarity_load/jwn_product_ld.sql"), "r"
    ).read()

    launch_AOI_merge_product = BigQueryInsertJobOperator(
        task_id="AOI_merge_product",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_product_ld,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_jwn_dq_metrics_fact_complete = open(
        os.path.join(
            sql_path, "nap_jwn_metrics_clarity_load/jwn_dq_metrics_fact_complete.sql"
        ),
        "r",
    ).read()

    launch_DQ_metrics_complete_jwn_dq_metrics_fact_complete = BigQueryInsertJobOperator(
        task_id="DQ_metrics_complete_jwn_dq_metrics_fact_complete",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_jwn_dq_metrics_fact_complete,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_dq_timeliness_end = open(
        os.path.join(sql_path, "nap_jwn_metrics_clarity_load/dq_timeliness_end.sql"),
        "r",
    ).read()

    launch_Timeliness_end_load02 = BigQueryInsertJobOperator(
        task_id="Timeliness_end_load02",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_dq_timeliness_end,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    nap_jwn_metrics_clarity_load_clarity_batch_end = open(
        os.path.join(sql_path, "nap_jwn_metrics_clarity_load/clarity_batch_end.sql"),
        "r",
    ).read()

    launch_Batch_end_job4_batch_end = BigQueryInsertJobOperator(
        task_id="Batch_end_job4_batch_end",
        configuration={
            "query": {
                "query": nap_jwn_metrics_clarity_load_clarity_batch_end,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # creds_setup >> launch_other_appId_upstream_dependencies_sensor_0 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_1 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_2 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_3 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_4 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_5 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_6 >>[launch_Batch_start_job0_batch_start]
    # launch_other_appId_upstream_dependencies_sensor_7 >>[launch_Batch_start_job0_batch_start]
    # launch_Batch_start_job0_batch_start >>[launch_DQ_metrics_init_jwn_init_zero_count_dq_metrics_fact, launch_SKULOC_inventory_sku_loc_day_fact_ld, launch_Timeliness_start_load01]
    # launch_SKULOC_inventory_sku_loc_day_fact_ld >>[launch_RTV_jwn_rtv_delta_ld, launch_Receipt_purchase_order_rms_receipts_fact_ld, launch_Transfer_rms_fact_ld]
    # launch_DQ_metrics_init_jwn_init_zero_count_dq_metrics_fact >>    launch_Timeliness_start_load01 >>    launch_Receipt_purchase_order_rms_receipts_fact_ld >>[launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt, launch_Intrans_jwn_external_in_transit_ld]
    # launch_Transfer_rms_fact_ld >>[launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt]
    # launch_RTV_jwn_rtv_delta_ld >>[launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt]
    # launch_Intrans_jwn_external_in_transit_ld >>    launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt >>[launch_AOI_merge_jwn_aoi_network_delta_updt]
    # launch_AOI_merge_jwn_aoi_network_delta_updt >>[launch_AOI_merge_jwn_aoi_exposed_delta_updt]
    # launch_AOI_merge_jwn_aoi_exposed_delta_updt >>[launch_AOI_merge_product]
    # launch_AOI_merge_product >>[launch_DQ_metrics_complete_jwn_dq_metrics_fact_complete, launch_Timeliness_end_load02]
    # launch_DQ_metrics_complete_jwn_dq_metrics_fact_complete >>[launch_Batch_end_job4_batch_end]
    # launch_Timeliness_end_load02 >>[launch_Batch_end_job4_batch_end]
    # launch_Batch_end_job4_batch_end >> pelican_validator >> pelican_sensor

    (
        creds_setup
        >> launch_Batch_start_job0_batch_start
        >> launch_DQ_metrics_init_jwn_init_zero_count_dq_metrics_fact
    )
    launch_Batch_start_job0_batch_start >> launch_SKULOC_inventory_sku_loc_day_fact_ld
    launch_Batch_start_job0_batch_start >> launch_Timeliness_start_load01
    launch_SKULOC_inventory_sku_loc_day_fact_ld >> launch_RTV_jwn_rtv_delta_ld
    (
        launch_SKULOC_inventory_sku_loc_day_fact_ld
        >> launch_Receipt_purchase_order_rms_receipts_fact_ld
    )
    launch_SKULOC_inventory_sku_loc_day_fact_ld >> launch_Transfer_rms_fact_ld

    (
        launch_Receipt_purchase_order_rms_receipts_fact_ld
        >> launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt
    )
    (
        launch_Receipt_purchase_order_rms_receipts_fact_ld
        >> launch_Intrans_jwn_external_in_transit_ld
    )

    launch_Transfer_rms_fact_ld >> launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt

    launch_RTV_jwn_rtv_delta_ld >> launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt

    (
        launch_AOI_merge_jwn_aoi_last_receipt_date_delta_updt
        >> launch_AOI_merge_jwn_aoi_network_delta_updt
    )

    (
        launch_AOI_merge_jwn_aoi_network_delta_updt
        >> launch_AOI_merge_jwn_aoi_exposed_delta_updt
    )

    launch_AOI_merge_jwn_aoi_exposed_delta_updt >> launch_AOI_merge_product

    launch_AOI_merge_product >> launch_DQ_metrics_complete_jwn_dq_metrics_fact_complete
    launch_AOI_merge_product >> launch_Timeliness_end_load02

    (
        launch_DQ_metrics_complete_jwn_dq_metrics_fact_complete
        >> launch_Batch_end_job4_batch_end
    )

    (
        launch_Timeliness_end_load02
        >> launch_Batch_end_job4_batch_end
        >> pelican_validator
        >> pelican_sensor
    )
