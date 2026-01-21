import os
import sys, logging
import configparser
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from nordstrom.utils.setup_module import setup_module
from nordstrom.utils.cloud_creds import cloud_creds
from pathlib import Path
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import ast

# file_path = os.path.join(
#     Path(os.path.dirname(os.path.abspath(__file__))).parent,
#     "../modules/common/packages/__init__.py",
# )
# setup_module(module_name="ds_product_common", file_path=file_path)

# from ds_product_common.bigquery_util import get_batch_load_status


# from ds_product_common.servicenow_util import invoke_servicenow_api
# from ds_product_common.sqs_msg_processor_util_nsk import (sqs_file_processor, sqs_check_file_availability)
# from ds_product_common.s3_util import check_s3_object_availability
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

# fetch environment from OS variable
#env = os.environ["ENVIRONMENT"]
env = "development"

# Fetch the data from config file
config = configparser.ConfigParser()
current_path = os.path.split(__file__)[0]
sql_path = os.path.join(current_path, "../../sql/TECH_Merch_NAP_CATALOG_LOAD/")
config.read(
    os.path.join(
        current_path, "../../configs/TECH_Merch_NAP_CATALOG_LOAD/nap_merch_catalog.cfg"
    )
)
project_id = config.get(env, "gcp_project_id")
owner = config.get(env, "dag_owner")
s3_file_path = config.get(env, "s3_file_path")
aws_conn_id = config.get(env, "aws_conn_id")
gcs_bucket = config.get(env, "gcs_bucket")
s3_connection_id = config.get(env, "s3_connection_id")
s3_bucket = config.get(env, "s3_bucket")
s3_out_path_catalog_prefix = config.get(
    env, "s3_out_path_catalog_prefix"
)
s3_out_path_item_catalog_prefix = config.get(
    env, "s3_out_path_item_catalog_prefix"
)

# email = config.get(env, 'dag_alert_email_addr')
# retry_limit = int(config.get(env, 'retries'))
# retry_delay = int(config.getint(env, 'retry_delay'))
schedule = config.get(env, "dag_schedule_interval")
service_account_email = config.get(env, "service_account")
item_catalog_s3_to_ldr = config.get(env, "item_catalog_s3_to_landing")
catalog_s3_to_ldr = config.get(env, "catalog_s3_to_landing")
# s3bucket_date_format = (
#     "year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/"
#     "hour={{ ts_nodash[9:11]}}/"
# )
s3bucket_date_format = (  "year=2024/month=10/day=08/hour=16/")
s3bucket_date_format_item = ( "year=2024/month=10/day=08/hour=04/")
s3_out_path_catalog = config.get(env, "s3_out_path_catalog") + s3bucket_date_format
gcs_out_path_catalog = config.get(env, "gcs_out_path_catalog") + s3bucket_date_format
gcs_bucket = config.get(env, "gcs_bucket")
s3_out_path_item_catalog = (
    config.get(env, "s3_out_path_item_catalog") + s3bucket_date_format
)
gcs_out_path_item_catalog = (
    config.get(env, "gcs_out_path_item_catalog") + s3bucket_date_format
)

# TPT variables
queue_url_catalog = config.get(env, "queue_url_catalog")
queue_url_item_catalog = config.get(env, "queue_url_item_catalog")
file_identifier = config.get(env, "file_identifier")
gcp_conn = config.get(env, "gcp_conn")
region = config.get(env, "region")
sql_config_dict = ast.literal_eval(config[env]["sql_config_dict"])
# s3_out_path = config.get(env, 's3_out_path')
subject_area_name = config.get(env, "subject_area_name")
# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")
trigger_rule = "none_failed"


# method to invoke the callback function to create service now incident
# def create_servicenow_incident(context):
#     invoke_servicenow_api(context, sn_incident_severity)


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
    #'email': email.split(','),
    #   'email_on_failure': False if env == 'development' else True,
    #  'email_on_retry': False,
    # 'retries': retry_limit,
    #'retry_delay': timedelta(seconds=retry_delay),
    # 'on_failure_callback': create_servicenow_incident
}

with DAG(
    owner + "_CATALOG_LOAD",
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

    # Step 1 - Task which checks for previous failed process and fails in case of previous unsucessfull run
    # Pushes xcom value as 'Y' for key ETL_BATCH_PROGRESS_CATALOG
    start_load = get_batch_load_status("start_load", subject_area_name, "START")

    # Step 2 - Task for processing messages from AWS SQS queue. If the queue has any Catalog files then the xcom value will be
    # updated as 'File received' or 'File Not Received'
    # sqs_processing_catalog = sqs_file_processor("sqs_processing_catalog", queue_url_catalog, file_identifier,
    #                                             s3_out_path_catalog)
    # sqs_processing_item_catalog = sqs_file_processor("sqs_processing_item_catalog", queue_url_item_catalog,
    #                                                  file_identifier, s3_out_path_item_catalog)

    # Step 3 - Check if the queue processing task found any files in the queue, if not skips the Teradata load
    # check_file_availability_catalog = check_s3_object_availability('check_file_availability_catalog',
    #                                                                s3_out_path_catalog, 'skip_load_catalog',
    #                                                                'catalog_s3_to_landing')
    # check_file_availability_item_catalog = check_s3_object_availability('check_file_availability_item_catalog',
    #                                                                     s3_out_path_item_catalog,
    #                                                                     'skip_load_item_catalog',
    #                                                                     'item_catalog_s3_to_landing')
    sqs_processing_catalog = DummyOperator(task_id="sqs_processing_catalog", dag=dag)
    sqs_processing_item_catalog = DummyOperator(task_id="sqs_processing_item_catalog", dag=dag)
    check_file_availability_catalog = DummyOperator(task_id="check_file_availability_catalog", dag=dag)
    check_file_availability_item_catalog = DummyOperator(task_id="check_file_availability_item_catalog", dag=dag)




    catalog_s3_to_gcs = call_s3_to_gcs(
        "catalog_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        s3_out_path_catalog_prefix+ s3bucket_date_format,
        gcs_bucket,
        dag,
    )
    item_catalog_s3_to_gcs = call_s3_to_gcs(
        "item_catalog_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        s3_out_path_item_catalog_prefix + s3bucket_date_format_item,
        gcs_bucket,
        dag,
    )

    catalog_gcs_to_landing = dataproc_gcs_to_bq(
        "catalog_gcs_to_landing",
        owner + "_CATALOG_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "catalog_s3_to_landing"),
        gcp_conn,
        gcs_bucket + s3_out_path_catalog_prefix + s3bucket_date_format,
        dag,
    )
    
    
    item_catalog_gcs_to_landing = dataproc_gcs_to_bq(
        "item_catalog_gcs_to_landing",
        owner + "_CATALOG_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "item_catalog_s3_to_landing"),
        gcp_conn,
        gcs_bucket + s3_out_path_item_catalog_prefix + s3bucket_date_format_item,
        dag,
    )
   


    
    # Step 6 -  load the ORP table

    catalog_LDG_to_OPR = BigQueryInsertJobOperator(
        task_id="catalog_opr_load",
        configuration={
            "query": {
                "query": open(
                    os.path.join(
                        sql_path, "PRODUCT_CATALOG_LDG_2_PRODUCT_CATALOG_OPR.sql"
                    ),
                    "r",
                ).read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    item_catalog_LDG_to_OPR = BigQueryInsertJobOperator(
        task_id="item_catalog_opr_load",
        configuration={
            "query": {
                "query": open(
                    os.path.join(
                        sql_path, "PRODUCT_ITEM_CATALOG_LDG_2_PRODUCT_CATALOG_OPR.sql"
                    ),
                    "r",
                ).read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # Step 7 - This step will load the DIM table
    catalog_OPR_to_DIM = BigQueryInsertJobOperator(
        task_id="catalog_dim_load",
        configuration={
            "query": {
                "query": open(
                    os.path.join(
                        sql_path, "PRODUCT_CATALOG_OPR_2_PRODUCT_CATALOG_DIM.sql"
                    ),
                    "r",
                ).read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # Step 8 - Task which pushes xcom value as 'Y' for key ETL_BATCH_CATALOG which
    # indicates the current Catalog dag load is successful
    end_load = get_batch_load_status("end_load", subject_area_name, "END")

    # Step 9 - Dummy task if no files found by the sqs file processor task
    skip_load_catalog = DummyOperator(task_id='skip_load_catalog')
    skip_load_item_catalog = DummyOperator(task_id='skip_load_item_catalog')

    opr_end_load = get_batch_load_status("opr_end_load", subject_area_name, "END")

    # Task dependency
    creds_setup >> creds_setup_aws >> start_load

    #Catalog Processing Flow
    start_load >> sqs_processing_catalog
    sqs_processing_catalog >> check_file_availability_catalog
    check_file_availability_catalog >> skip_load_catalog >> end_load
    check_file_availability_catalog >> catalog_s3_to_gcs >> catalog_gcs_to_landing >> catalog_LDG_to_OPR

    #Item Catalog Processing Flow
    start_load >> sqs_processing_item_catalog
    sqs_processing_item_catalog >> check_file_availability_item_catalog

    check_file_availability_item_catalog >> item_catalog_s3_to_gcs >> item_catalog_gcs_to_landing >> item_catalog_LDG_to_OPR
    check_file_availability_item_catalog >> skip_load_item_catalog >> end_load
    #OPR to DIM Flow
    catalog_LDG_to_OPR >> opr_end_load
    item_catalog_LDG_to_OPR >> opr_end_load
    opr_end_load >> catalog_OPR_to_DIM >> end_load

  