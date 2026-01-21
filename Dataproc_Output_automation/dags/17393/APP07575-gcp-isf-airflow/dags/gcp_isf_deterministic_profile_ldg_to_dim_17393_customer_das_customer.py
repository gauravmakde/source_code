# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        0bd98fc32fb04c45fbd4e3c11e8d8b9fec95d520
# CI pipeline URL:      https://git.jwn.app/TM01190/semantic-and-presentation-layer/APP07575-gcp-isf-airflow/-/pipelines/6879555
# CI commit timestamp:  2024-09-04T12:24:24+00:00
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
    path.dirname(__file__).split("APP07575-gcp-isf-airflow")[0]
    + "APP07575-gcp-isf-airflow/"
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

dag_id = "gcp_isf_deterministic_profile_ldg_to_dim_17393_customer_das_customer"
sql_config_dict = dict(config[dag_id + "_db_param"])

os.environ["NEWRELIC_API_KEY"] = "TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV"

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)


from modules.pelican import Pelican
from modules.metrics.main_common import main


start_date = pendulum.datetime(2022, 2, 24)
cron = (
    None if config.get(dag_id, "cron").lower() == "none" else config.get(dag_id, "cron")
)
dataplex_project_id = config.get("dag", "dataplex_project_id")
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = "{{ ts_nodash }}"
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
    "retries": 0 if config_env == "development" else 3,
    "description": "isf_deterministic_profile_ldg_to_dim_17393_customer_das_customer DAG Description",
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

    deterministic_profile_deterministic_profile_ldg_to_wrk = open(
        os.path.join(
            sql_path, "deterministic_profile/deterministic_profile_ldg_to_wrk.sql"
        ),
        "r",
    ).read()

    launch_deterministic_profile_ldg_to_wrk = BigQueryInsertJobOperator(
        task_id="deterministic_profile_ldg_to_wrk",
        configuration={
            "query": {
                "query": deterministic_profile_deterministic_profile_ldg_to_wrk,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    deterministic_profile_deterministic_profile_wrk_to_dim = open(
        os.path.join(
            sql_path, "deterministic_profile/deterministic_profile_wrk_to_dim.sql"
        ),
        "r",
    ).read()

    launch_deterministic_profile_wrk_to_dim = BigQueryInsertJobOperator(
        task_id="deterministic_profile_wrk_to_dim",
        configuration={
            "query": {
                "query": deterministic_profile_deterministic_profile_wrk_to_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    deterministic_profile_deterministic_profile_elt_control_set_elt_from_and_to = open(
        os.path.join(
            sql_path,
            "deterministic_profile/deterministic_profile_elt_control_set_elt_from_and_to.sql",
        ),
        "r",
    ).read()

    launch_deterministic_profile_elt_control_set_elt_from_and_to = BigQueryInsertJobOperator(
        task_id="deterministic_profile_elt_control_set_elt_from_and_to",
        configuration={
            "query": {
                "query": deterministic_profile_deterministic_profile_elt_control_set_elt_from_and_to,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    deterministic_profile_deterministic_profile_dq_audit = open(
        os.path.join(
            sql_path, "deterministic_profile/deterministic_profile_dq_audit.sql"
        ),
        "r",
    ).read()

    launch_deterministic_profile_dq_audit = BigQueryInsertJobOperator(
        task_id="deterministic_profile_dq_audit",
        configuration={
            "query": {
                "query": deterministic_profile_deterministic_profile_dq_audit,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    launch_deterministic_profile_send_newrelic_metric = PythonOperator(
        task_id="deterministic_profile_send_newrelic_metric",
        python_callable=main,
        op_kwargs={
            "newrelic_connection": config.get("dag", "newrelic_connection"),
            "hook": BigQueryHook(
                gcp_conn_id=gcp_conn, location=region, use_legacy_sql=False
            ),
            "bigquery_environment": sql_config_dict["dbenv"],
            "metric_yaml_file": os.path.join(
                module_path, "yaml/deterministic_profile.yaml"
            ),
            # 'isf_dag_name' : sql_config_dict['dag_name'],
            # 'tpt_job_name' : sql_config_dict['tpt_job_name'],
            # 'subject_area_name' : sql_config_dict['subject_area'],
            # 'ldg_table_name': sql_config_dict['ldg_table_name']
        },
    )

    deterministic_profile_deterministic_profile_elt_control_deactivate = open(
        os.path.join(
            sql_path,
            "deterministic_profile/deterministic_profile_elt_control_deactivate.sql",
        ),
        "r",
    ).read()

    launch_deterministic_profile_elt_control_deactivate = BigQueryInsertJobOperator(
        task_id="deterministic_profile_elt_control_deactivate",
        configuration={
            "query": {
                "query": deterministic_profile_deterministic_profile_elt_control_deactivate,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    (
        creds_setup
        >> launch_deterministic_profile_ldg_to_wrk
        >> [launch_deterministic_profile_wrk_to_dim]
    )
    launch_deterministic_profile_wrk_to_dim >> [
        launch_deterministic_profile_elt_control_set_elt_from_and_to
    ]
    launch_deterministic_profile_elt_control_set_elt_from_and_to >> [
        launch_deterministic_profile_dq_audit
    ]
    launch_deterministic_profile_dq_audit >> [
        launch_deterministic_profile_send_newrelic_metric
    ]
    launch_deterministic_profile_send_newrelic_metric >> [
        launch_deterministic_profile_elt_control_deactivate
    ]
    (
        launch_deterministic_profile_elt_control_deactivate
        >> pelican_validator
        >> pelican_sensor
    )
