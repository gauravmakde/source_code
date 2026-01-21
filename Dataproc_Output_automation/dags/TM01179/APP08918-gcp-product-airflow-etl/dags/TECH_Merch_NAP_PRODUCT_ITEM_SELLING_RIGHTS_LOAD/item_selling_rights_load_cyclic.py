# from airflow.providers.amazon.aws.transfers.s3_to_gcs import S3ToGCSOperator
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime
from datetime import timedelta
from nordstrom.utils.setup_module import setup_module
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from os import path
from pathlib import Path
import configparser
import os
import re
import sys
import ast


# file_path = os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent,'../modules/common/packages/__init__.py')
# setup_module(module_name="ds_product_common", file_path=file_path)

modules_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/",
)
sys.path.append(modules_path)
from bigquery_util import call_s3_to_gcs, dataproc_gcs_to_bq, get_batch_load_status


# from ds_product_common.sqs_msg_processor_util_nsk import (sqs_file_processor, sqs_check_file_availability)
# from ds_product_common.s3_util import check_s3_object_availability


# from ds_product_common.teradata_util import get_batch_load_status
# from ds_product_common.bigquery_util import get_batch_load_status
from servicenow_util import invoke_servicenow_api

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = "development"

config = configparser.ConfigParser()
current_path = os.path.split(__file__)[0]
sys.path.append(current_path)
abspath = os.path.dirname(os.path.abspath(__file__))
# Airflow variables
config.read(
    os.path.join(
        current_path,
        "../../configs/TECH_Merch_NAP_PRODUCT_ITEM_SELLING_RIGHTS_LOAD/nap_merch_item_selling_rights.cfg",
    )
)
owner = config.get(env, "dag_owner")
# email = config.get(env, 'dag_alert_email_addr')
retry_limit = int(config.get(env, "retries"))
retry_delay = int(config.getint(env, "retry_delay"))
schedule = config.get(env, "dag_schedule_interval")
# EC2 Path variables
# s3_sql_path = config.get(env, 'sql_path')
sql_path = os.path.join(
    current_path, "../../sql/TECH_Merch_NAP_PRODUCT_ITEM_SELLING_RIGHTS_LOAD/"
)
# s3bucket_date_format = 'year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/' \
# 					   'hour={{ ts_nodash[9:11]}}/'

s3bucket_date_format = "year=2024/month=10/day=08/" "hour=00/"

selling_rights_ec2_sql_path = config.get(env, "ec2_sql_path") + "item_selling_rights/"
selling_rights_s3_out_path = (
    config.get(env, "selling_rights_s3_out_path") + s3bucket_date_format
)
selling_status_ec2_sql_path = config.get(env, "ec2_sql_path") + "item_selling_status/"
selling_status_s3_out_path = (
    config.get(env, "selling_status_s3_out_path") + s3bucket_date_format
)
# TPT variables
selling_rights_queue_url = config.get(env, "selling_rights_queue_url")
selling_rights_file_identifier = config.get(env, "selling_rights_file_identifier")
selling_status_queue_url = config.get(env, "selling_status_queue_url")
selling_status_file_identifier = config.get(env, "selling_status_file_identifier")
# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")
project_id = config.get(env, "project_id")
gcp_conn = config.get(env, "gcp_conn")
region = config.get(env, "region")
service_account_email = config.get(env, "service_account_email")
sql_config_dict = ast.literal_eval(config[env]["sql_config_dict"])


subject_area_name = config.get(env, "subject_area_name")
aws_conn_id = config.get(env, "aws_conn_id")
gcs_bucket = config.get(env, "gcs_bucket")
s3_bucket = config.get(env, "s3_bucket")
selling_rights_s3_out_path_prefix = config.get(env, "selling_rights_s3_out_path_prefix")
selling_status_s3_out_path_prefix = config.get(env, "selling_status_s3_out_path_prefix")


DAG_NAME = "TECH_MERCH_NAP_PRODUCT_ITEM_SELLING_RIGHTS_LOAD"
# Fetch the data from config file


# method to invoke the callback function to create service now incident
def create_servicenow_incident(context):
    invoke_servicenow_api(context, sn_incident_severity)


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


default_args = {
    "owner": owner,
    "start_date": datetime(2020, 5, 7),
    # 'email'              : email.split(','),
    # 'email_on_failure'   : False if env == 'development' else False,
    # 'email_on_retry'     : False,
    "retries": retry_limit,
    "retry_delay": timedelta(seconds=retry_delay),
    # 'on_failure_callback': create_servicenow_incident
}

with DAG(
    owner + "_PRODUCT_ITEM_SELLING_RIGHTS_LOAD",
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

    # Step 1 - Task which checks for previous failed process and fails incase of previous unsucessfull run
    # Pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_ITEM_SELLING_RIGHTS
    start_load = get_batch_load_status(
        "start_selling_rights_load", subject_area_name, "START"
    )

    # Step 2 - Task for processing messages from AWS SQS queue. If the queue has any Selling Rights or Status files then the xcom value will be
    # updated as 'File received' or 'File Not Received'
    # 	selling_rights_sqs_processing = sqs_file_processor("selling_rights_sqs_processing", selling_rights_queue_url,
    # 													   selling_rights_file_identifier,
    # 													   selling_rights_s3_out_path,
    # 													   max_messages=5000)
    selling_rights_sqs_processing = DummyOperator(
        task_id="selling_rights_sqs_processing", dag=dag
    )

    # 	selling_status_sqs_processing = sqs_file_processor("selling_status_sqs_processing", selling_status_queue_url,
    # 													   selling_status_file_identifier,
    # 													   selling_status_s3_out_path, max_messages=5000)

    selling_status_sqs_processing = DummyOperator(
        task_id="selling_status_sqs_processing", dag=dag
    )

    # Step 3 - Check if the queue processing task found any files in the queue, if not skips the Teradata load
    # 	selling_rights_check_file_availability = check_s3_object_availability('selling_rights_check_file_availability',
    # 	                            selling_rights_s3_out_path, 'selling_rights_skip_load', 'refresh_selling_rights_sqls')

    selling_rights_check_file_availability = DummyOperator(
        task_id="selling_rights_check_file_availability", dag=dag
    )

    # 	selling_status_check_file_availability = check_s3_object_availability('selling_status_check_file_availability',
    # 	                            selling_status_s3_out_path, 'selling_status_skip_load', 'refresh_selling_status_sqls')

    selling_status_check_file_availability = DummyOperator(
        task_id="selling_status_check_file_availability", dag=dag
    )


    selling_right_landing_to_opr = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_RIGHTS_LDG_2_PRODUCT_ITEM_SELLING_RIGHTS_OPR.sql",
        ),
        "r",
    ).read()

    selling_right_landing_to_opr = BigQueryInsertJobOperator(
        task_id="selling_right_landing_to_opr",
        configuration={
            "query": {"query": selling_right_landing_to_opr, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    selling_right_landing_to_store_dim = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_RIGHTS_LDG_2_PRODUCT_ITEM_SELLING_RIGHTS_STORE_DIM.sql",
        ),
        "r",
    ).read()

    selling_right_landing_to_store_dim = BigQueryInsertJobOperator(
        task_id="selling_right_landing_to_store_dim",
        configuration={
            "query": {
                "query": selling_right_landing_to_store_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    selling_right_landing_to_audience_dim = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_RIGHTS_LDG_2_PRODUCT_ITEM_SELLABLE_AUDIENCE_DIM.sql",
        ),
        "r",
    ).read()

    selling_right_landing_to_audience_dim = BigQueryInsertJobOperator(
        task_id="selling_right_landing_to_audience_dim",
        configuration={
            "query": {
                "query": selling_right_landing_to_audience_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    item_selling_status_landing_to_dim = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_STATUS_LDG_2_PRODUCT_ITEM_SELLING_STATUS_DIM.sql",
        ),
        "r",
    ).read()

    item_selling_status_landing_to_dim = BigQueryInsertJobOperator(
        task_id="item_selling_status_landing_to_dim",
        configuration={
            "query": {
                "query": item_selling_status_landing_to_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    selling_right_landing_to_dim = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_RIGHTS_LDG_2_PRODUCT_ITEM_SELLING_RIGHTS_DIM.sql",
        ),
        "r",
    ).read()

    selling_right_landing_to_dim = BigQueryInsertJobOperator(
        task_id="selling_right_landing_to_dim",
        configuration={
            "query": {"query": selling_right_landing_to_dim, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    item_selling_status_dim_landing_to_selling_rights_dim = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_STATUS_DIM_2_PRODUCT_ITEM_SELLING_RIGHTS_DIM.sql",
        ),
        "r",
    ).read()

    item_selling_status_dim_landing_to_selling_rights_dim = BigQueryInsertJobOperator(
        task_id="item_selling_status_dim_landing_to_selling_rights_dim",
        configuration={
            "query": {
                "query": item_selling_status_dim_landing_to_selling_rights_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    item_selling_rights_xref_date_update = open(
        os.path.join(sql_path, "PRODUCT_ITEM_SELLING_RIGHTS_XREF_DATE_UPDATE.sql"), "r"
    ).read()

    item_selling_rights_xref_date_update = BigQueryInsertJobOperator(
        task_id="item_selling_rights_xref_date_update",
        configuration={
            "query": {
                "query": item_selling_rights_xref_date_update,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    item_selling_rights_dim_to_selling_rights_xref = open(
        os.path.join(
            sql_path,
            "PRODUCT_ITEM_SELLING_RIGHTS_DIM_2_PRODUCT_ITEM_SELLING_RIGHTS_XREF.sql",
        ),
        "r",
    ).read()

    item_selling_rights_dim_to_selling_rights_xref = BigQueryInsertJobOperator(
        task_id="item_selling_rights_dim_to_selling_rights_xref",
        configuration={
            "query": {
                "query": item_selling_rights_dim_to_selling_rights_xref,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    selling_rights_s3_to_GCS = call_s3_to_gcs(
        "selling_rights_s3_to_GCS",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        selling_rights_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    selling_status_s3_to_GCS = call_s3_to_gcs(
        "selling_status_s3_to_GCS",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        selling_status_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    selling_rights_GCS_to_BQ = dataproc_gcs_to_bq(
        "selling_rights_GCS_to_BQ",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "rights_table"),
        gcp_conn,
        gcs_bucket + selling_rights_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    selling_status_GCS_to_BQ = dataproc_gcs_to_bq(
        "selling_status_GCS_to_BQ",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "status_table"),
        gcp_conn,
        gcs_bucket + selling_status_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    # Step 14 - Task which pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_ITEM_SELLING_RIGHTS which
    # indicates the current Selling Rights dag load is sucessful
    end_load = get_batch_load_status(
        "end_selling_rights_load", subject_area_name, "END"
    )

    # Step 15 - Dummy task if no files found by the sqs file processor task
    selling_rights_skip_load = DummyOperator(task_id="selling_rights_skip_load")
    selling_status_skip_load = DummyOperator(task_id="selling_status_skip_load")

    # Task dependency
    (
        creds_setup >> creds_setup_aws >> start_load
        >> selling_rights_sqs_processing
        >> selling_rights_check_file_availability
    )
    (
        start_load
        >> selling_status_sqs_processing
        >> selling_status_check_file_availability
    )
    (
        selling_rights_check_file_availability
        >> selling_rights_s3_to_GCS
        >> selling_rights_GCS_to_BQ
        >> selling_right_landing_to_opr
        >> selling_right_landing_to_store_dim
        >> selling_right_landing_to_audience_dim
        >> selling_right_landing_to_dim
        >> item_selling_status_dim_landing_to_selling_rights_dim
    )
    selling_rights_check_file_availability >> selling_rights_skip_load >> end_load
    (
        selling_status_check_file_availability
        >> selling_status_s3_to_GCS
        >> selling_status_GCS_to_BQ
        >> item_selling_status_landing_to_dim
        >> selling_right_landing_to_dim
        >> item_selling_status_dim_landing_to_selling_rights_dim
        >> item_selling_rights_xref_date_update
        >> item_selling_rights_dim_to_selling_rights_xref
        >> end_load
    )
    selling_status_check_file_availability >> selling_status_skip_load >> end_load

    # start_load >> selling_right_landing_to_opr >> selling_right_landing_to_store_dim >> # selling_right_landing_to_audience_dim
    # selling_right_landing_to_dim >> item_selling_status_dim_landing_to_selling_rights_dim
    # start_load >> selling_rights_skip_load >> end_load
    # item_selling_status_landing_to_dim >> selling_right_landing_to_dim
    # item_selling_status_dim_landing_to_selling_rights_dim >> item_selling_rights_xref_date_update
    # item_selling_rights_dim_to_selling_rights_xref >> end_load
