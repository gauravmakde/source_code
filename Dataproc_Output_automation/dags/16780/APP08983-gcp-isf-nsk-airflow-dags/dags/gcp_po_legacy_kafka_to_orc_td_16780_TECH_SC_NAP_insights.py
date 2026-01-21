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
import yaml
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
from airflow.models import Variable

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

dag_id = "gcp_po_legacy_kafka_to_orc_td_16780_TECH_SC_NAP_insights"
sql_config_dict = dict(config[dag_id + "_db_param"])

os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican

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
dag_sla = timedelta(minutes=int(90)) if int(90) > 1 else None

delta_core_jar = config.get("dataproc", "delta_core_jar")
etl_jar_version = config.get("dataproc", "etl_jar_version")
metastore_service_path = config.get("dataproc", "metastore_service_path")
spark_jar_path = config.get("dataproc", "spark_jar_path")
user_config_path = config.get("dataproc", "user_config_path")


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
    "retries": 0 if config_env == "development" else 3,
    "description": "po_legacy_kafka_to_orc_td_16780_TECH_SC_NAP_insights DAG Description",
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

    # Please add the consumer_group_name,topic_name from kafka json file and remove if kafka is not present
    fetch_batch_run_id_task = FetchKafkaBatchRunIdOperator(
        task_id="fetch_batch_run_id_task",
        gcp_project_id=project_id,
        gcp_connection_id=gcp_conn,
        gcp_region=region,
        topic_name="ascp-purchase-order-object-model-avro",
        consumer_group_name="onix-ascp-purchase-order-object-model-avro_po_legacy_kafka_to_orc_td_16780_TECH_SC_NAP_insights_kafka_kafka_to_orc",
        offsets_table="jwn-nap-user1-nonprod-zmnb.onehop_etl_app_db.kafka_consumer_offset_batch_details",
        # source_table = f"`{{dataplex_project_id}}.onehop_etl_app_db.source_kafka_consumer_offset_batch_details`",
        default_latest_run_id=int(
            (datetime.today() - timedelta(days=1)).strftime("%Y%m%d000000")
        ),
        dag=dag,
    )
    # 1.kafka_bd_start_batch

    po_legacy_po_legacy_bd_start_batch = open(
        os.path.join(sql_path, "po_legacy/po_legacy_bd_start_batch.sql"), "r"
    ).read()

    livy_kafka_bd_start_batch = BigQueryInsertJobOperator(
        task_id="kafka_bd_start_batch",
        configuration={
            "query": {
                "query": po_legacy_po_legacy_bd_start_batch,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # 2. kafka_kafka_to_orc
    session_name = get_batch_id(dag_id=dag_id, task_id="kafka_kafka_to_orc")
    livy_kafka_kafka_to_orc = DataprocCreateBatchOperator(
        task_id="kafka_kafka_to_orc",
        project_id=project_id,
        region=region,
        gcp_conn_id=gcp_conn,
        batch={
            "runtime_config": {
                "version": "1.1",
                "properties": {
                    "spark.sql.legacy.avro.datetimeRebaseModeInWrite": "LEGACY",
                    "spark.speculation": "false",
                    "spark.airflow.run_id": "{{ ti.xcom_pull(task_ids = 'fetch_batch_run_id_task')}}",
                    "spark.sql.avro.datetimeRebaseModeInRead": "LEGACY",
                    "spark.sql.parquet.int96AsTimestamp": "false",
                    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
                    "spark.sql.parquet.binaryAsString": "true",
                },
            },
            "spark_batch": {
                "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                "jar_file_uris": [
                    f"{spark_jar_path}/{delta_core_jar}",
                    f"{spark_jar_path}/uber-onehop-etl-pipeline.jar",
                ],
                "args": [
                    f"{user_config_path}/argument_po_legacy_kafka_to_orc_td_16780_TECH_SC_NAP_insights_kafka_kafka_to_orc.json",
                    # "--aws_user_role_external_id",
                    # Variable.get("aws_role_externalid"),
                ],
            },
            "environment_config": {
                "execution_config": {
                    "service_account": service_account_email,
                    "subnetwork_uri": subnet_url,
                },
                
            },
        },
        batch_id=session_name,
        dag=dag,
    )

    # 3. orc_orc_to_orc
    po_legacy_po_legacy_orc_to_orc = open(
        os.path.join(sql_path, "po_legacy/po_legacy_orc_to_orc.sql"), "r"
    ).read()

    livy_orc_orc_to_orc = BigQueryInsertJobOperator(
        task_id="orc_orc_to_orc",
        configuration={
            "query": {"query": po_legacy_po_legacy_orc_to_orc, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # 4. orc_to_csv
    po_legacy_po_legacy_orc_to_csv = open(
        os.path.join(sql_path, "po_legacy/po_legacy_orc_to_csv.sql"), "r"
    ).read()

    livy_csv_orc_to_csv = BigQueryInsertJobOperator(
        task_id="csv_orc_to_csv",
        configuration={
            "query": {"query": po_legacy_po_legacy_orc_to_csv, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # 5. bd_end_batch

    po_legacy_po_legacy_bd_end_batch = open(
        os.path.join(sql_path, "po_legacy/po_legacy_bd_end_batch.sql"), "r"
    ).read()

    livy_bd_end_batch = BigQueryInsertJobOperator(
        task_id="bd_end_batch",
        configuration={
            "query": {"query": po_legacy_po_legacy_bd_end_batch, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    po_legacy_po_legacy_bq_start_batch = open(
        os.path.join(sql_path, "po_legacy/po_legacy_bq_start_batch.sql"), "r"
    ).read()

    launch_bq_start_batch = BigQueryInsertJobOperator(
        task_id="bq_start_batch",
        configuration={
            "query": {
                "query": po_legacy_po_legacy_bq_start_batch,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # tpt_header_tpt_load_load_tbl = SSHOperator(
    # task_id='tpt_header_tpt_load_load_tbl',
    # command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_UTL -j PRD_NAP_STG_PURCHASE_ORDER_LDG_JOB -h bqnapprod.nordstrom.net -l 'N' -u SCH_NAP_SCINV_BATCH_PRD -p \"\$bqwallet(SCH_NAP_SCINV_BATCH_PRD_PWD)\" -f s3://tf-nap-scinv-prod-ascp-extract-csv/data/po_legacy_header_csv/*.csv.gz -o UPDATE",
    # timeout = 1800,
    # ssh_conn_id="TECH_ISF_NAP_SCINV_bqUTIL_SERVER_S3EXT_PROD")

    # tpt_ship_tpt_load_load_tbl = SSHOperator(
    # task_id='tpt_ship_tpt_load_load_tbl',
    # command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_UTL -j PRD_NAP_STG_PURCHASE_ORDER_ITEM_SHIPLOCATION_LDG_JOB -h bqnapprod.nordstrom.net -l 'N' -u SCH_NAP_SCINV_BATCH_PRD -p \"\$bqwallet(SCH_NAP_SCINV_BATCH_PRD_PWD)\" -f s3://tf-nap-scinv-prod-ascp-extract-csv/data/po_legacy_ship_csv/*.csv.gz -o UPDATE",
    # timeout = 1800,
    # ssh_conn_id="TECH_ISF_NAP_SCINV_bqUTIL_SERVER_S3EXT_PROD")

    # tpt_dist_tpt_load_load_tbl = SSHOperator(
    # task_id='tpt_dist_tpt_load_load_tbl',
    # command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_UTL -j PRD_NAP_STG_PURCHASE_ORDER_ITEM_DISTRIBUTELOCATION_LDG_JOB -h bqnapprod.nordstrom.net -l 'N' -u SCH_NAP_SCINV_BATCH_PRD -p \"\$bqwallet(SCH_NAP_SCINV_BATCH_PRD_PWD)\" -f s3://tf-nap-scinv-prod-ascp-extract-csv/data/po_legacy_dist_csv/*.csv.gz -o UPDATE",
    # timeout = 1800,
    # ssh_conn_id="TECH_ISF_NAP_SCINV_bqUTIL_SERVER_S3EXT_PROD")

    po_legacy_po_legacy_header_to_fact = open(
        os.path.join(sql_path, "po_legacy/po_legacy_header_to_fact.sql"), "r"
    ).read()

    launch_header_ldg_to_fact = BigQueryInsertJobOperator(
        task_id="header_ldg_to_fact",
        configuration={
            "query": {
                "query": po_legacy_po_legacy_header_to_fact,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    po_legacy_po_legacy_ship_to_fact = open(
        os.path.join(sql_path, "po_legacy/po_legacy_ship_to_fact.sql"), "r"
    ).read()

    launch_ship_ldg_to_fact = BigQueryInsertJobOperator(
        task_id="ship_ldg_to_fact",
        configuration={
            "query": {"query": po_legacy_po_legacy_ship_to_fact, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    po_legacy_po_legacy_dist_to_fact = open(
        os.path.join(sql_path, "po_legacy/po_legacy_dist_to_fact.sql"), "r"
    ).read()

    launch_dist_ldg_to_fact = BigQueryInsertJobOperator(
        task_id="dist_ldg_to_fact",
        configuration={
            "query": {"query": po_legacy_po_legacy_dist_to_fact, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    po_legacy_po_legacy_bq_end_batch = open(
        os.path.join(sql_path, "po_legacy/po_legacy_bq_end_batch.sql"), "r"
    ).read()

    launch_bq_end_batch = BigQueryInsertJobOperator(
        task_id="bq_end_batch",
        configuration={
            "query": {"query": po_legacy_po_legacy_bq_end_batch, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    (
        creds_setup
        >> fetch_batch_run_id_task
        >> [
            livy_bd_end_batch,
            livy_csv_orc_to_csv,
            livy_kafka_bd_start_batch,
            livy_orc_orc_to_orc,
        ]
    )
    livy_kafka_bd_start_batch >> [livy_kafka_kafka_to_orc]
    livy_kafka_kafka_to_orc >> [livy_csv_orc_to_csv, livy_orc_orc_to_orc]
    livy_orc_orc_to_orc >> [livy_bd_end_batch]
    livy_csv_orc_to_csv >> [livy_bd_end_batch]
    (
        livy_bd_end_batch
        >> launch_bq_start_batch
        >> [launch_dist_ldg_to_fact, launch_header_ldg_to_fact, launch_ship_ldg_to_fact]
    )
    launch_header_ldg_to_fact >> [launch_bq_end_batch]
    launch_ship_ldg_to_fact >> [launch_bq_end_batch]
    launch_dist_ldg_to_fact >> [launch_bq_end_batch]
    launch_bq_end_batch >> pelican_validator >> pelican_sensor
