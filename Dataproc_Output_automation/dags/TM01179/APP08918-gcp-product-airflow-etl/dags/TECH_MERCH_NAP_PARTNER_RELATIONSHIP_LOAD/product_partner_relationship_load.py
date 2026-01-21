import os
import sys
import configparser
import re
from datetime import datetime
from datetime import timedelta
from os import path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# from nordstrom.utils.setup_module import setup_module
from pathlib import Path
from nordstrom.utils.cloud_creds import cloud_creds
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import logging
import ast

# file_path = os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent, 'common/packages/__init__.py')
# setup_module(module_name="ds_product_common", file_path=file_path)
modules_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/",
)
sys.path.append(modules_path)
from bigquery_util import (
    get_batch_load_status,
    call_s3_to_gcs,
    dataproc_gcs_to_bq,
    call_bqinsert_operator,
)

# from servicenow_util import invoke_servicenow_api
# from handshake_util import s3_create_handshake_files


# from ds_product_common.servicenow_util import invoke_servicenow_api
# from ds_product_common.handshake_util import s3_create_handshake_files
# from ds_product_common.s3_util import check_s3_object_availability
# from ds_product_common.sqs_msg_processor_util_nsk import sqs_file_processor

# fetch environment from OS variable
env = "nonprod"

# Fetch the data from config file
config = configparser.ConfigParser()
# path = os.path.split(__file__)[0]
path = (
    path.dirname(__file__).split("APP08918-gcp-product-airflow-etl")[0]
    + "APP08918-gcp-product-airflow-etl/"
)
sys.path.append(path)
# Airflow variables
config.read(
    os.path.join(
        path,
        "configs/TECH_MERCH_NAP_PARTNER_RELATIONSHIP_LOAD/nap_product_partner_relationship.cfg",
    )
)
sql_path = path + "sql/TECH_MERCH_NAP_PARTNER_RELATIONSHIP_LOAD/"

owner = config.get(env, "dag_owner")
# email = config.get(env, 'dag_alert_email_addr')
retry_limit = int(config.get(env, "retries"))
retry_delay = int(config.getint(env, "retry_delay"))
schedule = config.get(env, "dag_schedule_interval")
# EC2 Path variables
# s3_sql_path = config.get(env, 'sql_path')
# ec2_sql_path = config.get(env, 'ec2_sql_path') + 'product_partner_relationship/'
# s3bucket_date_format = 'year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/' \
#    'hour={{ ts_nodash[9:11]}}/'
s3bucket_date_format = "/year=2024/month=10/day=08/hour=08/"
product_partner_relationship_detail_s3_out_path = (
    config.get(env, "product_partner_relationship_detail_s3_out_path")
    + s3bucket_date_format
)
product_partner_relation_shipping_policy_s3_out_path = (
    config.get(env, "product_partner_relation_shipping_policy_s3_out_path")
    + s3bucket_date_format
)
product_partner_relation_cust_return_policy_s3_out_path = (
    config.get(env, "product_partner_relation_cust_return_policy_s3_out_path")
    + s3bucket_date_format
)
# TPT variables
product_partner_relationship_detail_queue_url = config.get(
    env, "product_partner_relationship_detail_queue_url"
)
product_partner_relation_shipping_policy_queue_url = config.get(
    env, "product_partner_relation_shipping_policy_queue_url"
)
product_partner_relation_cust_return_policy_queue_url = config.get(
    env, "product_partner_relation_cust_return_policy_queue_url"
)

product_partner_relationship_detail_file_identifier = config.get(
    env, "product_partner_relationship_detail_file_identifier"
)
product_partner_relation_shipping_policy_file_identifier = config.get(
    env, "product_partner_relation_shipping_policy_file_identifier"
)
product_partner_relation_cust_return_policy_file_identifier = config.get(
    env, "product_partner_relation_cust_return_policy_file_identifier"
)

subject_area_name = config.get(env, "subject_area_name")
s3_bucket = config.get(env, "s3_bucket")
# s3_file_path = config.get(env, "s3_file_path")
aws_conn_id = config.get(env, "aws_conn_id")
gcs_bucket = config.get(env, "gcs_bucket")
# s3_connection_id = config.get(env, "s3_connection_id")
service_account_email = config.get(env, "service_account_email")
project_id = config.get(env, "project_id")
bq_env = config.get(env, "bq_env")
product_partner_relationship_detail_s3_out_path_prefix = config.get(
    env, "product_partner_relationship_detail_s3_out_path_prefix"
)
product_partner_relation_shipping_policy_s3_out_path_prefix = config.get(
    env, "product_partner_relation_shipping_policy_s3_out_path_prefix"
)
product_partner_relation_cust_return_policy_s3_out_path_prefix = config.get(
    env, "product_partner_relation_cust_return_policy_s3_out_path_prefix"
)
gcp_conn_id = config.get(env, "gcp_conn")

parameters = {"gcp_project_id": project_id, "dbenv": bq_env}
region = config.get(env, "region")
nauth_conn_id = config.get(env, "nauth_conn_id")
cloud_conn_id = config.get(env, "gcp_conn")


# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")

# method to invoke the callback function to create service now incident
# def create_servicenow_incident(context):
#     invoke_servicenow_api(context, sn_incident_severity)

default_args = {
    "owner": owner,
    "start_date": datetime(2020, 5, 7),
    # 'email': email.split(','),
    # 'email_on_failure': False if env == 'development' else True,
    # 'email_on_retry': False,
    "retries": retry_limit,
    "retry_delay": timedelta(seconds=retry_delay),
    # 'on_failure_callback': create_servicenow_incident
}


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=nauth_conn_id,
        cloud_conn_id=cloud_conn_id,
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


DAG_NAME = "gcp_" + owner + "_PARTNER_RELATIONSHIP_LOAD"
with DAG(
    dag_id=DAG_NAME, default_args=default_args, schedule_interval=None, catchup=False
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

    # Step 1 - Task which checks for previous failed process and fails in case of previous unsuccessfully run
    # Pushes xcom value as 'Y' for key ETL_BATCH_PAARTNER_RELATIONSHIP
    start_load = get_batch_load_status("start_load", subject_area_name, "START")

    # Step 2 - Task for processing messages from AWS SQS queue. If the queue has any  PARTNER RELATIONSHIP files then the
    # xcom value will be updated as 'File received' or 'File Not Received'
    # sqs_processing_partner_relationship_detail = sqs_file_processor("sqs_processing_partner_relationship_detail",
    #                                           product_partner_relationship_detail_queue_url,
    #                                           product_partner_relationship_detail_file_identifier,
    #                                           product_partner_relationship_detail_s3_out_path,
    #                                           max_messages=5000)
    sqs_processing_partner_relationship_detail = DummyOperator(
        task_id="sqs_processing_partner_relationship_detail", dag=dag
    )

    # sqs_processing_partner_relation_shipping_policy = sqs_file_processor("sqs_processing_partner_relation_shipping_policy",
    #                                               product_partner_relation_shipping_policy_queue_url,
    #                                               product_partner_relation_shipping_policy_file_identifier,
    #                                               product_partner_relation_shipping_policy_s3_out_path,
    #                                               max_messages=5000)
    sqs_processing_partner_relation_shipping_policy = DummyOperator(
        task_id="sqs_processing_partner_relation_shipping_policy", dag=dag
    )

    # sqs_processing_partner_relation_cust_return_policy = sqs_file_processor("sqs_processing_partner_relation_cust_return_policy",
    #                                               product_partner_relation_cust_return_policy_queue_url,
    #                                               product_partner_relation_cust_return_policy_file_identifier,
    #                                               product_partner_relation_cust_return_policy_s3_out_path,
    #                                               max_messages=5000)
    sqs_processing_partner_relation_cust_return_policy = DummyOperator(
        task_id="sqs_processing_partner_relation_cust_return_policy", dag=dag
    )

    # Step 3 - Check if the queue processing task found any files in the queue, if not skips the Teradata load
    # partner_relationship_detail_check_file_availability = check_s3_object_availability('partner_relationship_detail_check_file_availability',
    #    product_partner_relationship_detail_s3_out_path,
    #    'partner_relationship_detail_skip_load',
    #    'product_partner_relationship_detail_landing')

    # partner_relation_shipping_policy_check_file_availability = check_s3_object_availability('partner_relation_shipping_policy_check_file_availability',
    #    product_partner_relation_shipping_policy_s3_out_path,
    #    'partner_relation_shipping_policy_skip_load',
    #    'product_partner_relation_shipping_policy_landing')

    # partner_relation_cust_return_policy_check_file_availability = check_s3_object_availability('partner_relation_cust_return_policy_check_file_availability',
    #    product_partner_relation_cust_return_policy_s3_out_path,
    #    'partner_relation_cust_return_policy_skip_load',
    #    'product_partner_relation_cust_return_policy_landing')

    # Step 4 - Copies all the sql files from s3 to ec2 to work with teradata
    # refresh_sql = copy_s3_to_ec2("refresh_sql", s3_sql_path, ec2_sql_path)

    # Step 5 - TPT load for initial landing table
    product_partner_relationship_detail_s3_to_gcs = call_s3_to_gcs(
        "product_partner_relationship_detail_s3_to_gcs",
        aws_conn_id,
        gcp_conn_id,
        s3_bucket,
        product_partner_relationship_detail_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )
    product_partner_relation_shipping_policy_s3_to_gcs = call_s3_to_gcs(
        "product_partner_relation_shipping_policy_s3_to_gcs",
        aws_conn_id,
        gcp_conn_id,
        s3_bucket,
        product_partner_relation_shipping_policy_s3_out_path_prefix
        + s3bucket_date_format,
        gcs_bucket,
        dag,
    )
    product_partner_relation_cust_return_policy_s3_to_gcs = call_s3_to_gcs(
        "product_partner_relation_cust_return_policy_s3_to_gcs",
        aws_conn_id,
        gcp_conn_id,
        s3_bucket,
        product_partner_relation_cust_return_policy_s3_out_path_prefix
        + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    product_partner_relationship_detail_gcs_bq = dataproc_gcs_to_bq(
        "product_partner_relationship_detail_gcs_bq",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "relationship_detail_table_name"),
        gcp_conn_id,
        gcs_bucket
        + product_partner_relationship_detail_s3_out_path_prefix
        + s3bucket_date_format,
        dag,
    )
    product_partner_relation_shipping_policy_gcs_bq = dataproc_gcs_to_bq(
        "product_partner_relation_shipping_policy_gcs_bq",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "relation_shipping_policy_table_name"),
        gcp_conn_id,
        gcs_bucket
        + product_partner_relation_shipping_policy_s3_out_path_prefix
        + s3bucket_date_format,
        dag,
    )
    product_partner_relation_cust_return_policy_gcs_bq = dataproc_gcs_to_bq(
        "product_partner_relation_cust_return_policy_gcs_bq",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "relation_cust_return_policy_table_name"),
        gcp_conn_id,
        gcs_bucket
        + product_partner_relation_cust_return_policy_s3_out_path_prefix
        + s3bucket_date_format,
        dag,
    )

    # Step 6 - This step will execute partner relationship load procedure
    product_partner_relationship_detail_dim_load = call_bqinsert_operator(
        dag,
        "product_partner_relationship_detail_dim_load",
        os.path.join(sql_path, "PRODUCT_PARTNER_RELATIONSHIP_LDG_2_DIM_LOAD.sql"),
        project_id,
        parameters,
        region,
        gcp_conn_id,
    )

    product_partner_relation_shipping_policy_dim_load = call_bqinsert_operator(
        dag,
        "product_partner_relation_shipping_policy_dim_load",
        os.path.join(
            sql_path, "PRODUCT_PARTNER_RELATN_SHIPPING_POLICY_LDG_2_DIM_LOAD.sql"
        ),
        project_id,
        parameters,
        region,
        gcp_conn_id,
    )

    product_partner_relation_cust_return_policy_opr_load = call_bqinsert_operator(
        dag,
        "product_partner_relation_cust_return_policy_opr_load",
        os.path.join(
            sql_path,
            "PRODUCT_PARTNER_RELATIONSHIP_CUST_RETURN_POLICY_LDG_TO_OPR_LOAD.sql",
        ),
        project_id,
        parameters,
        region,
        gcp_conn_id,
    )

    product_partner_relation_cust_return_policy_dim_load = call_bqinsert_operator(
        dag,
        "product_partner_relation_cust_return_policy_dim_load",
        os.path.join(
            sql_path,
            "PRODUCT_PARTNER_RELATIONSHIP_CUST_RETURN_POLICY_OPR_TO_DIM_LOAD.sql",
        ),
        project_id,
        parameters,
        region,
        gcp_conn_id,
    )

    # Step 7 - Task which pushes xcom value as 'N' for key ETL_BATCH_PROGRESS_PARTNER_RELATIONSHIP which
    # indicates the current dag load is successful
    end_load = get_batch_load_status("end_load", subject_area_name, "END")

    # Step 8 - Dummy task if no files found by the sqs file processor task
    # partner_relationship_detail_skip_load = DummyOperator(task_id='partner_relationship_detail_skip_load')
    # partner_relation_shipping_policy_skip_load = DummyOperator(task_id='partner_relation_shipping_policy_skip_load')
    # partner_relation_cust_return_policy_skip_load = DummyOperator(task_id='partner_relation_cust_return_policy_skip_load')

    # Dependency between tasks

    (
        creds_setup
        >> creds_setup_aws
        >> start_load
        >> sqs_processing_partner_relationship_detail
        >> product_partner_relationship_detail_s3_to_gcs
        >> product_partner_relationship_detail_gcs_bq
        >> product_partner_relationship_detail_dim_load
        >> end_load
    )

    (
        start_load
        >> sqs_processing_partner_relation_shipping_policy
        >> product_partner_relation_shipping_policy_s3_to_gcs
        >> product_partner_relation_shipping_policy_gcs_bq
        >> product_partner_relation_shipping_policy_dim_load
        >> end_load
    )
    (
        start_load
        >> sqs_processing_partner_relation_cust_return_policy
        >> product_partner_relation_cust_return_policy_s3_to_gcs
        >> product_partner_relation_cust_return_policy_gcs_bq
        >> product_partner_relation_cust_return_policy_opr_load
        >> product_partner_relation_cust_return_policy_dim_load
        >> end_load
    )

