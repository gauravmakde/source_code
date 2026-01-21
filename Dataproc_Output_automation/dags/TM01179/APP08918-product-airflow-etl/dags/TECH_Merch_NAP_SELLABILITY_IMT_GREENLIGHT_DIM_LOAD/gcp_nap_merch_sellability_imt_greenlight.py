from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from datetime import datetime, timedelta
from nordstrom.utils.cloud_creds import cloud_creds
from nordstrom.utils.setup_module import setup_module
from os import path
from pathlib import Path
import configparser
import logging
import os
import sys

# added comment for testing

file_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/__init__.py",
)
setup_module(module_name="ds_product_common", file_path=file_path)

from ds_product_common.servicenow_util import invoke_servicenow_api

# fetch environment from OS variable
env = os.environ["ENVIRONMENT"]

# Fetch the data from config file
config = configparser.ConfigParser()
current_path = os.path.split(__file__)[0]
sys.path.append(path)
abspath = os.path.dirname(os.path.abspath(__file__))
config.read(
    os.path.join(
        current_path,
        "../../configs/TECH_Merch_NAP_SELLABILITY_IMT_GREENLIGHT_DIM_LOAD/nap_merch_sellability_imt_greenlight.cfg",
    )
)
owner = config.get(env, "dag_owner")
# email = config.get(env, 'dag_alert_email_addr')
retry_limit = config.getint(env, "retries")
retry_delay = config.getint(env, "retry_delay")

# Teradata variabless
sql_path = os.path.join(
    current_path, "../../sql/TECH_Merch_NAP_SELLABILITY_IMT_GREENLIGHT_DIM_LOAD/"
)

# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")
s3_bucket = config.get(env, "s3_bucket")
s3_prefix = config.get(env, "s3_prefix")
gcs_destination_bucket = config.get(env, "gcs_destination_bucket")
gcs_source = config.get(env, "gcs_source")

# AWS,GCP variables
aws_conn_id = config.get(env, "aws_conn_id")
s3bucket_date_format = (
    "year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/"
    "hour={{ ts_nodash[9:11]}}/min={{ ts_nodash[11:13]}}/"
)

s3_path_imt_geenlight = config.get(env, "s3_path_imt_geenlight") + s3bucket_date_format
project_id = config.get(env, "project_id")
dataset_Id = config.get(env, "dataset_Id")
table_Id = config.get(env, "table_Id")
gcp_conn = config.get(env, "gcp_conn")
s3_bucket_name = config.get(env, "s3_bucket_name")
gcs_bucket_name = config.get(env, "gcs_bucket_name")

service_account_email = config.get(env, "service_account_email")

sql_config_dict = dict(config["db"])

region = config.get(env, "region")

gcs_root_bucket_name = config.get(env, "gcs_root_bucket_name")

# Airflow variables
schedule = None


# method to invoke the callback function to create service now incident
def create_servicenow_incident(context):
    invoke_servicenow_api(context, sn_incident_severity)


default_args = {
    "owner": owner,
    "start_date": datetime(2020, 5, 7),
    # 'email': email.split(','),
    "email_on_failure": False,  # if env == 'development' else True,
    "email_on_retry": False,
    "retries": retry_limit,
    "retry_delay": timedelta(seconds=retry_delay),
    "on_failure_callback": create_servicenow_incident,
}


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get(env, "nauth_conn_id"),
        cloud_conn_id=config.get(env, "gcp_conn"),
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


with DAG(
    "gcp_" + owner + "_SELLABILITY_IMT_GREENLIGHT_DIM_LOAD",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
) as dag:

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

    # Step 1 - Dummy task for the begin task
    extract_start = DummyOperator(task_id="extract_start", dag=dag)

    # Step 2: Take backup of the IMT Greenlight dimension table
    imt_greenlight_backup_dim = open(
        os.path.join(sql_path, "IMT_GREENLIGHT_BACKUP_DIM.sql"), "r"
    ).read()

    imt_greenlight_backup_dim = BigQueryInsertJobOperator(
        task_id="imt_greenlight_backup_dim",
        configuration={
            "query": {"query": imt_greenlight_backup_dim, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    imt_greenlight_s3_to_gcs = S3ToGCSOperator(
        task_id="imt_greenlight_s3_to_gcs",
        aws_conn_id="aws_default",
        bucket=s3_bucket,
        prefix=s3_prefix,
        gcp_conn_id=gcp_conn,
        dest_gcs=gcs_destination_bucket,
        replace=False,
        dag=dag,
    )

    imt_greenlight_gcs_to_landing = BigQueryInsertJobOperator(
        task_id="imt_greenlight_gcs_to_landing",
        configuration={
            "load": {
                "sourceUris": gcs_source,  # GCS URI of your file
                "destinationTable": {
                    "projectId": project_id,  # Google Cloud project ID
                    "datasetId": dataset_Id,  # BigQuery dataset ID
                    "tableId": table_Id,  # BigQuery table ID
                },
                "sourceFormat": "CSV",  # CSV format
                "fieldDelimiter": ",",  # Specify the custom delimiter (Unit Separator ␟)
                "skipLeadingRows": 1,  # Skip header row if present (optional)
                "writeDisposition": "WRITE_APPEND",  # Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        location=region,
        dag=dag,
    )
    # Step 5: Load the Dimension table with daily delta
    imt_greenlight_landing_to_dim = open(
        os.path.join(sql_path, "IMT_GREENLIGHT_LANDING_TO_DIM.sql"), "r"
    ).read()

    imt_greenlight_landing_to_dim = BigQueryInsertJobOperator(
        task_id="imt_greenlight_landing_to_dim",
        configuration={
            "query": {"query": imt_greenlight_landing_to_dim, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # Step 6 - Dummy the task for the end task
    extract_end = DummyOperator(task_id="extract_end", dag=dag)

    # Task dependency
    (
        creds_setup
        >> creds_setup_aws
        >> extract_start
        >> imt_greenlight_backup_dim
        >> imt_greenlight_s3_to_gcs
        >> imt_greenlight_gcs_to_landing
        >> imt_greenlight_landing_to_dim
        >> extract_end
    )
