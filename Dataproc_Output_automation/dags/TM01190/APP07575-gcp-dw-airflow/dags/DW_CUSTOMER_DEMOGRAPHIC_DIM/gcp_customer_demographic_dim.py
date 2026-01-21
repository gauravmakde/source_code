import logging
import os
import sys
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from datetime import timedelta
from google.cloud import bigquery

# Add python modules path to PYTHONPATH
current_path = os.path.split(__file__)[0]
sys.path.append(os.path.normpath(os.path.join(current_path, "../../common/utils")))

# Import custom python modules
from config.config_provider import ConfigProvider
from metrics.send_metrics import send_newrelic_metrics
from bigquery.big_query import execute_elt_control_load_macro
from nordstrom.utils.cloud_creds import cloud_creds
from sensor_datetime_resolver import get_datetime_for_sensor


def xcom_pull_template(task_id):
    return f"task_instance.xcom_pull(task_ids='{task_id}')"


env = "DEFAULT"

config_provider = ConfigProvider(section=env)
common_config = config_provider.load(config_name="common")
airflow_connections_config = config_provider.load(config_name="airflow_connections")
aws_resources_config = config_provider.load(config_name="aws_resources")
teradata_config = config_provider.load(config_name="teradata")
metamorph_config = config_provider.load(config_name="metamorph")
local_config = config_provider.load_from_path(os.path.join(current_path, "local.cfg"))
gcp_config = config_provider.load(config_name="gcp_resources")

# Get properties from config files
dag_owner = common_config.get_property("dag_owner")
alert_email_addr = common_config.get_property("alert_email_addr")
dw_airflow_repo_tag = common_config.get_property("dw_airflow_repo_tag")
dw_airflow_repo_prefix = common_config.get_property("dw_airflow_repo_prefix")

newrelic_connection = airflow_connections_config.get_property("newrelic_connection")
gcp_conn_id = gcp_config.get_property("gcp_conn_id")
location = gcp_config.get_property("location")
bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location, use_legacy_sql=False)
bigquery_environment = gcp_config.get_property("bigquery_environment")
gcp_project_id = gcp_config.get_property("project_id")
parameters = {"dbenv": bigquery_environment, "project_id": gcp_project_id}

# GCP connections
gcp_conn = gcp_config.get_property("gcp_conn")
nauth_conn = gcp_config.get_property("nauth_conn_id")
service_account_email = gcp_config.get_property("service_account_email")

py_scripts_relative_path = teradata_config.get_property("py_scripts_relative_path")
metamorph_data_path = metamorph_config.get_property("metamorph_data_path")

# local
dag_schedule = local_config.get_property("customer_demographic_schedule")
# customer_demographic_teradata_user = local_config.get_property('customer_demographic_teradata_user')
customer_demographic_control_db_schema = local_config.get_property(
    "customer_demographic_control_db_schema"
)
customer_demographic_job_name = local_config.get_property(
    "customer_demographic_job_name"
)
customer_demographic_subject_area = local_config.get_property(
    "customer_demographic_subject_area"
)
customer_demographic_sql_prefix = local_config.get_property(
    "customer_demographic_sql_prefix"
)
demographics_tag = local_config.get_property("demographics_tag")

GET_ELT_CONTROL_TABLE_DATES_TASK_ID = "GET_ELT_CONTROL_TABLE_DATES"

EXTRACT_FROM_TMSTP = f"{{{{ {xcom_pull_template(GET_ELT_CONTROL_TABLE_DATES_TASK_ID)}.strip().split('_')[0] }}}}"
EXTRACT_TO_TMSTP = f"{{{{ {xcom_pull_template(GET_ELT_CONTROL_TABLE_DATES_TASK_ID)}.strip().split('_')[1] }}}}"
year = f"{{{{ {xcom_pull_template(GET_ELT_CONTROL_TABLE_DATES_TASK_ID)}.strip().split('_')[0].split('-')[0] }}}}"
month = f"{{{{ {xcom_pull_template(GET_ELT_CONTROL_TABLE_DATES_TASK_ID)}.strip().split('_')[0].split('-')[1] }}}}"
day = f"{{{{ {xcom_pull_template(GET_ELT_CONTROL_TABLE_DATES_TASK_ID)}.strip().split('_')[0].split('-')[2] }}}}"
ymd_path = f"year={year}/month={month}/day={day}"
data_subject = "customer_demographic"
data_subject_upper = data_subject.upper()
py_scripts_path = os.path.normpath(os.path.join(current_path, py_scripts_relative_path))
incremental_load_task_id = "INCREMENTAL_LDG_TO_DIM"
full_refresh_task_id = "FULL_REFRESH_LDG_TO_DIM"


def setup_creds_bq(nauth_conn_id, cloud_conn_id, bq_service_account_email):
    @cloud_creds(
        nauth_conn_id=nauth_conn_id,
        cloud_conn_id=cloud_conn_id,
        service_account_email=bq_service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()


def check_load_type(**kwargs):
    is_full_refresh_check = kwargs["ti"].xcom_pull(
        dag_id="PL_AMPERITY_CUSTOMER_DEMOGRAPHICS_ONGOING_INGESTION",
        task_ids="RESOLVE_SQS_MESSAGE",
        key="IS_DEMOGRAPHIC_FULL_REFRESH",
    )

    print(f"IS_DEMOGRAPHIC_FULL_REFRESH XCOM value is '{is_full_refresh_check}'")

    if is_full_refresh_check:
        return full_refresh_task_id
    else:
        return incremental_load_task_id


default_args = {
    "owner": dag_owner,
    # "email": alert_email_addr.split(","),
    "start_date": datetime(2023, 9, 10),
    "email_on_failure": False,  # if env == "nonprod" else True,
    "email_on_retry": False,
    "depends_on_past": False,
    # "sla": timedelta(minutes=180),
}

with DAG(
    dag_id=f"gcp_{dw_airflow_repo_prefix}_CUSTOMER_DEMOGRAPHIC_DIM",
    default_args=default_args,
    schedule_interval=None,
    tags=[dw_airflow_repo_tag, demographics_tag],
    catchup=False,
    template_searchpath=os.path.join(current_path, "sql/"),
) as dag:
    creds_setup_bq = PythonOperator(
        task_id="setup_creds_bq",
        python_callable=setup_creds_bq,
        op_kwargs={
            "nauth_conn_id": nauth_conn,
            "cloud_conn_id": gcp_conn,
            "bq_service_account_email": service_account_email,
        },
    )

    ELT_BATCH_START = PythonOperator(
        task_id="ELT_BATCH_START",
        python_callable=execute_elt_control_load_macro,
        op_kwargs={
            "bq_conn_id": gcp_conn_id,
            "subject_area": customer_demographic_subject_area,
            "bigquery_environment": bigquery_environment,
            "location": location,
            "task_type": "START",
            "cust_schema": True,
        },
    )

    GET_LOAD_TYPE = BranchPythonOperator(
        task_id="GET_LOAD_TYPE", python_callable=check_load_type
    )

    INCREMENTAL_LDG_TO_DIM = BigQueryInsertJobOperator(
        task_id=incremental_load_task_id,
        configuration={
            "query": {
                "query": open(
                    os.path.join(
                        current_path,
                        "../../sql/DW_CUSTOMER_DEMOGRAPHIC_DIM/customer_experian_demographic_daily_incremental_ld.sql",
                    ),
                    "r",
                ).read(),
                "useLegacySql": False,
            }
        },
        project_id=gcp_project_id,
        params=parameters,
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )

    FULL_REFRESH_LDG_TO_DIM = BigQueryInsertJobOperator(
        task_id=full_refresh_task_id,
        configuration={
            "query": {
                "query": open(
                    os.path.join(
                        current_path,
                        "../../sql/DW_CUSTOMER_DEMOGRAPHIC_DIM/customer_experian_demographic_full_refresh_ld.sql",
                    ),
                    "r",
                ).read(),
                "useLegacySql": False,
            }
        },
        project_id=gcp_project_id,
        params=parameters,
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )

    SEND_NEWRELIC_METRIC = PythonOperator(
        task_id="SEND_NEWRELIC_METRIC",
        python_callable=send_newrelic_metrics,
        op_kwargs={
            "newrelic_connection": newrelic_connection,
            "bq_hook": bq_hook,
            "bigquery_environment": bigquery_environment,
            "metric_file_path": os.path.join(
                current_path, "customer_demographic_metrics.yaml"
            ),
            "bq_conn_id": gcp_conn_id,
            "region": location,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    ELT_BATCH_END = PythonOperator(
        task_id="ELT_BATCH_END",
        python_callable=execute_elt_control_load_macro,
        op_kwargs={
            "bq_conn_id": gcp_conn_id,
            "subject_area": customer_demographic_subject_area,
            "bigquery_environment": bigquery_environment,
            "location": location,
            "task_type": "END",
            "cust_schema": True,
        },
    )

    creds_setup_bq >> ELT_BATCH_START >> GET_LOAD_TYPE
    GET_LOAD_TYPE >> INCREMENTAL_LDG_TO_DIM >> SEND_NEWRELIC_METRIC
    GET_LOAD_TYPE >> FULL_REFRESH_LDG_TO_DIM >> SEND_NEWRELIC_METRIC
    SEND_NEWRELIC_METRIC >> ELT_BATCH_END
