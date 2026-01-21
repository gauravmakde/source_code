import os
import sys, logging
import configparser
from datetime import timedelta, datetime
import random
from os import path
from pathlib import Path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from nordstrom.utils.cloud_creds import cloud_creds
from airflow.operators.python import PythonOperator
from nordstrom.utils.setup_module import setup_module
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
import ast


modules_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/",
)
sys.path.append(modules_path)

from bigquery_util import call_s3_to_gcs, dataproc_gcs_to_bq, get_batch_load_status
from servicenow_util import invoke_servicenow_api

# from ds_product_common.sqs_msg_processor_util_nsk import sqs_file_processor
# from ds_product_common.s3_util import check_s3_object_availability

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = "development"

DAG_NAME = "TECH_Merch_NAP_FEE_CODE_LOAD"

# Fetch the data from config file
config = configparser.ConfigParser()
current_path = os.path.split(__file__)[0]
sys.path.append(path)
sql_path = os.path.join(current_path, "../../sql/TECH_Merch_NAP_FEE_CODE_LOAD/")
config.read(
    os.path.join(
        current_path, "../../configs/TECH_Merch_NAP_FEE_CODE_LOAD/nap_merch_feecode.cfg"
    )
)
# Airflow variables
owner = config.get(env, "dag_owner")
email = config.get(env, "dag_alert_email_addr")
retry_limit = int(config.get(env, "retries"))
retry_delay = int(config.getint(env, "retry_delay"))
schedule = config.get(env, "dag_schedule_interval")
# service_account_email = config.get(env, "service_account")
# BigqueryInsertJob Op parameters
bq_bucket = config.get(env, "bq_bucket")
# EC2 Path variables
# s3_sql_path = config.get(env, 'sql_path')
project_id = config.get(env, "project_id")
gcp_conn = config.get(env, "gcp_conn")
region = config.get(env, "region")
service_account_email = config.get(env, "service_account_email")
sql_config_dict = ast.literal_eval(config[env]["sql_config_dict"])


subject_area_name = config.get(env, "subject_area_name")
aws_conn_id = config.get(env, "aws_conn_id")
gcs_bucket = config.get(env, "gcs_bucket")
s3_bucket = config.get(env, "s3_bucket")

s3bucket_date_format = "year=2024/month=10/day=17/hour=04/"
feecode_ec2_sql_path = config.get(env, "ec2_sql_path") + "feecode/"
# feecode_prefix = config.get(env, "feecode_prefix") + s3bucket_date_format
feecode_prefix = config.get(env, "feecode_prefix")

# TPT variables
feecode_queue_url = config.get(env, "feecode_queue_url")
feecode_file_identifier = config.get(env, "feecode_file_identifier")
subject_area_name = config.get(env, "subject_area_name")
# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")
# Handshake details
handshake_bucket = config.get(env, "handshake_bucket")
handshake_file_key = config.get(env, "handshake_file_key")
handshake_execution_dt = "{{ ds_nodash[:4]}}-{{ ds_nodash[4:6]}}-{{ ds_nodash[6:8]}}"


# method to invoke the callback function to create service now incident
def create_servicenow_incident(context):
    """
    This function triggers servicenow api
      Parameters : context, severity
      Returns    : operator
    """
    invoke_servicenow_api(context, sn_incident_severity)


default_args = {
    "owner": owner,
    "start_date": datetime(2020, 5, 7),
    # 'email'              : email.split(','),
    # 'email_on_failure'   : False if env == 'development' else True,
    # 'email_on_retry'     : False,
    # 'retries'            : retry_limit,
    "retry_delay": timedelta(seconds=retry_delay),
    # 'on_failure_callback': create_servicenow_incident
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
        nauth_conn_id=config.get(env, "nauth_conn_id"),
        cloud_conn_id=config.get(env, "aws_conn_id"),
        aws_iam_role_arn=aws_iam_role_arn,
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()


with DAG(
    owner + "_FEE_CODE_LOAD",
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

    # Step 1 - Task which checks for previous failed process and fails in case of previous unsuccessful run
    # Pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_FEE_CODE
    start_load = get_batch_load_status("feecode_load", subject_area_name, "START")

    # Step 2 - Task for processing messages from AWS SQS queue.
    # If the queue has any Selling Rights or Status files then the xcom value will be
    # updated as 'File received' or 'File Not Received'
    # feecode_sqs_processing = sqs_file_processor("feecode_sqs_processing", feecode_queue_url,
    #                                              feecode_file_identifier,
    #                                              feecode_s3_out_path,
    #                                              max_messages=5000)

    feecode_sqs_processing = DummyOperator(task_id="feecode_sqs_processing", dag=dag)

    # Step 3 - Check if there are any files in the output, if not skips the Teradata load
    # feecode_check_file_availability = check_s3_object_availability('feecode_check_file_availability',
    #                                                                 feecode_s3_out_path, 'feecode_skip_load', 'feecode_refresh_sqls')

    feecode_check_file_availability = DummyOperator(
        task_id="feecode_check_file_availability", dag=dag
    )

    # Step 4 - Copies all the SQL files from S3 to EC2 to work with Teradata
    # feecode_refresh_sqls = copy_s3_to_ec2("feecode_refresh_sqls", s3_sql_path, feecode_ec2_sql_path)

    # Step 6 - TPT load for initial landing tables
    # feecode_s3_to_landing = get_tpt_load('fee_code_ldg_tpt_load', feecode_s3_out_path)

    # Step 7 - Load the DIM tables for the feecode
    # feecode_landing_to_dim = get_bteq_load('feecode_landing_to_dim',
    #                                        'PRODUCT_FEECODE_LDG_TO_PRODUCT_FEECODE_DIM.sql',
    #                                        feecode_ec2_sql_path)

    feecode_landing_to_dim_sql = open(
        os.path.join(sql_path, "PRODUCT_FEECODE_LDG_TO_PRODUCT_FEECODE_DIM.sql"), "r"
    ).read()

    feecode_landing_to_dim = BigQueryInsertJobOperator(
        task_id="feecode_landing_to_dim",
        configuration={
            "query": {"query": feecode_landing_to_dim_sql, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    feecode_s3_to_GCS = call_s3_to_gcs(
        "feecode_s3_to_GCS",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        feecode_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    feecode_to_GCS_to_BQ = dataproc_gcs_to_bq(
        "feecode_to_GCS_to_BQ",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "fee_table"),
        gcp_conn,
        gcs_bucket + feecode_prefix + s3bucket_date_format,
        dag,
    )

    # Step 8 - Task which pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_FEECODE
    # which indicates the current feeecode dag load is successful
    end_load = get_batch_load_status("end_feecode_load", subject_area_name, "END")

    # Step 12 - Dummy task if no files found by the SQS file processor task
    feecode_skip_load = DummyOperator(task_id="feecode_skip_load")

    # Task dependency
    # start_load >> feecode_sqs_processing >> feecode_check_file_availability
    # start_load >> s3_to_gcs >> feecode_gcs_to_bq_landing >> feecode_landing_to_dim >> end_load
    # s3_to_gcs >> feecode_skip_load >> end_load

    (
        creds_setup
        >> creds_setup_aws
        >> start_load
        >> feecode_sqs_processing
        >> feecode_check_file_availability
        >> feecode_s3_to_GCS
        >> feecode_to_GCS_to_BQ
        >> feecode_landing_to_dim
    )
    feecode_landing_to_dim >> feecode_skip_load >> end_load
