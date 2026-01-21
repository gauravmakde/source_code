import os
import sys
import configparser
from datetime import datetime
from datetime import timedelta
import ast
import logging
from nordstrom.utils.cloud_creds import cloud_creds
from os import path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from nordstrom.utils.setup_module import setup_module
from pathlib import Path
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

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

# env = os.environ['ENVIRONMENT']
env = "nonprod"
# Fetch the data from config file
config = configparser.ConfigParser()
# path = os.path.split(__file__)[0]
path = (
    path.dirname(__file__).split("APP08918-gcp-product-airflow-etl")[0]
    + "APP08918-gcp-product-airflow-etl/"
)
sys.path.append(path)
config.read(
    os.path.join(path, "configs/TECH_Merch_NAP_RP_SKU_LOCATION_LOAD/nap_merch_rp.cfg")
)

owner = config.get(env, "dag_owner")
dag_id = "gcp_" + owner + "_RP_SKU_LOCATION_LOAD"
retry_limit = int(config.get(env, "retries"))
retry_delay = int(config.getint(env, "retry_delay"))
schedule = config.get(env, "dag_schedule_interval")

project_id = config.get(env, "project_id")
gcp_conn = config.get(env, "gcp_conn")
sql_config_dict = ast.literal_eval(config[env]["sql_config_dict"])
region = config.get(env, "region")


sql_path = path + "sql/TECH_Merch_NAP_RP_SKU_LOCATION_LOAD/"
# s3bucket_date_format = '/year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/' \
#                        'hour={{ ts_nodash[9:11]}}/'
s3bucket_date_format = "/year=2024/month=10/day=08/hour=10/"
sqs_s3_out_path_rp = config.get(env, "s3_out_path") + s3bucket_date_format
# Replenishment Indicator variables
rp_queue_url = config.get(env, "queue_url")
rp_file_identifier = config.get(env, "file_identifier")
s3_bucket = config.get(env, "s3_bucket")
s3_file_path = config.get(env, "s3_file_path")
gcs_dest_path = config.get(env, "gcs_dest_path")
gcs_bucket = config.get(env, "gcs_bucket")
s3_connection_id = config.get(env, "s3_connection_id")
service_account_email = config.get(env, "service_account_email")
subject_area_name = config.get(env, "subject_area_name")
nauth_conn_id = config.get(env, "nauth_conn_id")
cloud_conn_id = config.get(env, "gcp_conn")
aws_conn_id = config.get(env, "aws_conn_id")

# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")

# method to invoke the callback function to create service now incident
# def create_servicenow_incident(context):
#     invoke_servicenow_api(context, sn_incident_severity)

default_args = {
    "owner": owner,
    "start_date": datetime(2020, 5, 7),
    "retry_delay": timedelta(seconds=retry_delay),
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


DAG_NAME = "gcp_" + owner + "_RP_SKU_LOCATION_LOAD"
with DAG(
    DAG_NAME, default_args=default_args, schedule_interval=None, catchup=False
) as dag:

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
        dag=dag,
    )

    creds_setup_aws = PythonOperator(
        task_id="setup_creds_aws",
        python_callable=setup_creds_aws,
        op_args=["arn:aws:iam::290445963451:role/gcp_data_transfer"],
    )

    # Step 1 - Task which checks for previous failed process and fails incase of previous unsucessfull run
    # Pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_REPLENISHMENT_INDICATOR
    start_replenishment_load = get_batch_load_status(
        "start_replenishment_load", subject_area_name, "START"
    )

    # Step 2 - Task for processing messages from AWS SQS queue. If the queue has any RP files then the xcom value will be
    # updated as 'File received' or 'File Not Received'
    # rp_sqs_processing = sqs_file_processor("rp_sqs_processing",rp_queue_url, rp_file_identifier, sqs_s3_out_path_rp)
    rp_sqs_processing = DummyOperator(task_id="rp_sqs_processing", dag=dag)

    # Step 3 - Check if the queue processing task found any files in the queue, if not skips the Teradata load
    # rp_file_availability_check = check_s3_object_availability('rp_file_availability_check',
    #                                                           sqs_s3_out_path_rp,
    #                                                          'skip_teradata_load',
    #                                                          'copy_rp_sql_from_s3_to_ec2')
    rp_file_availability_check = DummyOperator(
        task_id="rp_file_availability_check", dag=dag
    )

    rp_file_s3_to_gcs = call_s3_to_gcs(
        "rp_file_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        s3_file_path + s3bucket_date_format,
        gcs_bucket,
        dag,
    )
    # Step 6 - GCS TO BQ load for initial landing table

    rp_file_gcs_to_bq = dataproc_gcs_to_bq(
        "rp_file_gcs_to_bq",
        DAG_NAME,
        config.get(env, "dataset_name"),
        config.get(env, "rp_table_name"),
        gcp_conn,
        gcs_bucket + s3_file_path + s3bucket_date_format,
        dag,
    )

    # rp_gcs_to_landing = DummyOperator(task_id='rp_sku_loc_ldg_bq_load')
    # Step 7 - This step will load opr table
    sql_string = open(
        os.path.join(sql_path, "RP_SKU_LOC_LDG_2_RP_SKU_LOC_DIM.sql"), "r"
    ).read()
    rp_landing_to_dimension = BigQueryInsertJobOperator(
        task_id="rp_landing_to_dimension",
        configuration={"query": {"query": sql_string, "useLegacySql": False}},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # Step 8 - Task which pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_REPLENISHMENT_INDICATOR which
    # indicates the current RP dag load is successful
    end_replenishment_load = get_batch_load_status(
        "end_replenishment_load", subject_area_name, "END"
    )

    # Step 9 - Dummy task if no files found by the sqs file processor task
    skip_teradata_load = DummyOperator(task_id="skip_teradata_load", dag=dag)

    # Task dependency

    (
        creds_setup
        >> creds_setup_aws
        >> start_replenishment_load
        >> rp_sqs_processing
        >> rp_file_availability_check
    )
    (
        rp_file_availability_check
        >> rp_file_s3_to_gcs
        >> rp_file_gcs_to_bq
        >> rp_landing_to_dimension
        >> end_replenishment_load
    )
    rp_file_availability_check >> skip_teradata_load >> end_replenishment_load
  
