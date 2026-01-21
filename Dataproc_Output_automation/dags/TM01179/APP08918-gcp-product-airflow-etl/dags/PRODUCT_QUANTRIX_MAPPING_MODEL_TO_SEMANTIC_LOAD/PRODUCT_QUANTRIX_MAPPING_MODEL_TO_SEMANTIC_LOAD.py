import ast
import configparser
import logging
import os
import sys

from datetime import timedelta
from datetime import datetime, timezone
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from nordstrom.utils.cloud_creds import cloud_creds
from nordstrom.utils.setup_module import setup_module

file_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/__init__.py",
)
setup_module(module_name="ds_product_common", file_path=file_path)
sql_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent, "../sql/"
)
config_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent, "../configs/"
)
# setup_module(module_name="ds_product_common", file_path=file_path)

sys.path.append(file_path)

from ds_product_common.bigquery_util import get_elt_control_load

# from ds_product_common.servicenow_util import invoke_servicenow_api
from ds_product_common.sensor_util import s3_file_check

# fetch environment from OS variable
env = os.environ["ENVIRONMENT"]

# Fetch the data from config file
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
sys.path.append(path)
# Airflow variables
config.read(
    os.path.join(
        config_path,
        "PRODUCT_QUANTRIX_MAPPING_MODEL_TO_SEMANTIC_LOAD/product_quantrix.cfg",
    )
)
owner = config.get(env, "dag_owner")
email = config.get(env, "dag_alert_email_addr")
# schedule = config.get(env, "dag_schedule_interval")
schedule = None
retries = config.getint(env, "retries")
retry_delay = config.getint(env, "retry_delay")
CATG_SUBCLASS_TABLE_ID = config.get(env, "CATG_SUBCLASS_TABLE_ID")
SUPP_DEPT_TABLE_ID = config.get(env, "SUPP_DEPT_TABLE_ID")

# Teradata configs
sql_config_dict = ast.literal_eval(config.get(env, "sql_config_dict"))
s3_sql_path = config.get(env, "sql_path")
ec2_sql_path = config.get(env, "ec2_sql_path") + "quantrix/"
subject_area_name = config.get(env, "subject_area_name")
aws_iam_role_arn = config.get(env, "aws_iam_role_arn")
# folders
category_subclass_map_s3_bucket = config.get(env, "category_subclass_map_s3_bucket")
category_subclass_map_s3_key = config.get(env, "category_subclass_map_s3_key")
supp_dept_map_s3_bucket = config.get(env, "supp_dept_map_s3_bucket")
supp_dept_map_s3_key = config.get(env, "supp_dept_map_s3_key")
s3_catg_subclass_process_path = config.get(env, "s3_catg_subclass_process_path")

# Setting up arguments
Dyear = "{{ds_nodash[:4]}}"
Dmonth = "{{ds_nodash[4:6]}}"
Dday = "{{ds_nodash[6:8]}}"
Dhour = "{{ ts_nodash[9:11]}}"
FROM_DATE = "{{ ds }}"
TO_DATE = "{{ tomorrow_ds }}"

s3bucket_date_format = (
    "year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/"
)

processing_bucket = config.get(env, "processing_bucket")
dataset_Id = config.get(env, "dataset_Id")

s3_supp_dept_process_path = config.get(env, "s3_supp_dept_process_path")


DAG_NAME = "gcp_PRODUCT_QUANTRIX_MAPPING_MODEL_TO_SEMANTIC_LOAD"


# Service now incident
# sn_incident_severity = config.get(env, 'sn_incident_severity')


# def create_servicenow_incident(context):
#     """
#        This function triggers servicenow api
#          Parameters : context, severity
#          Returns    : operator
#     """
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
        nauth_conn_id="nauth-connection-nonprod",
        cloud_conn_id="aws_default",
        aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer",
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()


default_args = {
    "owner": owner,
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 1),
    "email": None,
    "email_on_failure": False,
    "email_on_retry": False,
    # 'email': email.split(','),
    # 'email_on_failure': False if env == 'development' else True,
    # 'email_on_retry': False,
    "sla": timedelta(minutes=180),
    # 'on_failure_callback': create_servicenow_incident
}
with DAG(
    DAG_NAME, default_args=default_args, schedule_interval=schedule, catchup=False
) as dag:
    project_id = config.get(env, "gcp_project_id")
    service_account_email = config.get(env, "service_account_email")
    region = config.get(env, "region")
    gcp_conn = config.get(env, "gcp_conn")
    subnet_url = config.get(env, "subnet_url")

    # List all Tasks
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

    # Step 1 - ETL Batch process to set the ACTIVE_LOAD_IND to Y/N
    bq_hook = BigQueryHook(
        gcp_conn_id=config.get(env, "gcp_conn"),
        use_legacy_sql=False,
        location="us-west1",
    )
    start_quantrix_load = get_elt_control_load(
        "start_quantrix_load", bq_hook, subject_area_name, "START"
    )
    end_quantrix_load = get_elt_control_load(
        "end_quantrix_load", bq_hook, subject_area_name, "END"
    )

    # Step 2 - Check the availability of files
    check_catg_subclass_map_file = s3_file_check(
        "check_catg_subclass_map_file",
        category_subclass_map_s3_bucket,
        category_subclass_map_s3_key,
    )
    check_supp_dept_map_file = s3_file_check(
        "check_supp_dept_map_file", supp_dept_map_s3_bucket, supp_dept_map_s3_key
    )

    # Step 3 - Copies files to processing bucket
    copy_catg_subclass_map_files = S3ToGCSOperator(
        task_id="copy_catg_subclass_map_files",
        aws_conn_id="aws_default",
        bucket=category_subclass_map_s3_bucket,
        prefix=category_subclass_map_s3_key,
        gcp_conn_id=gcp_conn,
        dest_gcs=s3_catg_subclass_process_path,
        replace=False,
        dag=dag,
    )

    copy_supp_dept_map_files = S3ToGCSOperator(
        task_id="copy_supp_dept_map_files",
        aws_conn_id="aws_default",
        bucket=supp_dept_map_s3_bucket,
        prefix=supp_dept_map_s3_key,
        gcp_conn_id=gcp_conn,
        dest_gcs=s3_supp_dept_process_path,
        replace=False,
        dag=dag,
    )

    # Step 4 - Copies all the sql files from s3 to ec2 to work with teradata
    product_catg_subclass_map_gcs_to_landing = BigQueryInsertJobOperator(
        task_id="CATG_SUBCLASS_MAP_LDG",
        configuration={
            "load": {
                "sourceUris": [
                    s3_catg_subclass_process_path + "madm/*.txt"
                ],  # GCS URI of your TXT file
                "destinationTable": {
                    "projectId": project_id,  # Google Cloud project ID
                    "datasetId": dataset_Id,  # BigQuery dataset ID
                    "tableId": CATG_SUBCLASS_TABLE_ID,  # BigQuery table ID
                },
                "sourceFormat": "CSV",  # Treat TXT file as CSV
                "fieldDelimiter": "|",  # Custom delimiter (pipe |)
                "skipLeadingRows": 1,  # Skip the header row if present
                "writeDisposition": "WRITE_TRUNCATE",  # Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        location=region,
        dag=dag,
    )

    product_supp_dept_map_gcs_to_landing = BigQueryInsertJobOperator(
        task_id="SUPP_DEPT_MAP_LDG",
        configuration={
            "load": {
                "sourceUris": [
                    s3_supp_dept_process_path + "madm/*.txt"
                ],  # GCS URI of your TXT file
                "destinationTable": {
                    "projectId": project_id,  # Google Cloud project ID
                    "datasetId": dataset_Id,  # BigQuery dataset ID
                    "tableId": SUPP_DEPT_TABLE_ID,  # BigQuery table ID
                },
                "sourceFormat": "CSV",  # Treat TXT file as CSV
                "fieldDelimiter": "|",  # Custom delimiter (pipe |)
                "skipLeadingRows": 1,  # Skip the header row if present
                "writeDisposition": "WRITE_TRUNCATE",  # Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        location=region,
        dag=dag,
    )

    # Step 6 - BQ Script for stage to
    PRODUCT_QUANTRIX_CATG_SUBCLASS_MAP_LDG_TO_DIM = open(
        os.path.join(
            sql_path,
            "PRODUCT_QUANTRIX_MAPPING_MODEL_TO_SEMANTIC_LOAD/PRODUCT_QUANTRIX_CATG_SUBCLASS_MAP_LDG_TO_DIM.sql",
        ),
        "r",
    ).read()

    load_product_catg_subclass_map_dimension = BigQueryInsertJobOperator(
        task_id="product_catg_subclass_map_dimension_table",
        configuration={
            "query": {
                "query": PRODUCT_QUANTRIX_CATG_SUBCLASS_MAP_LDG_TO_DIM,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    PRODUCT_QUANTRIX_SUPP_DEP_MAP_LDG_TO_DIM = open(
        os.path.join(
            sql_path,
            "PRODUCT_QUANTRIX_MAPPING_MODEL_TO_SEMANTIC_LOAD/PRODUCT_QUANTRIX_SUPP_DEP_MAP_LDG_TO_DIM.sql",
        ),
        "r",
    ).read()

    load_product_supp_dept_map_dimension = BigQueryInsertJobOperator(
        task_id="product_supp_dept_map_dimension_table",
        configuration={
            "query": {
                "query": PRODUCT_QUANTRIX_SUPP_DEP_MAP_LDG_TO_DIM,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag=dag,
    )

    # Dependency between tasks
    (
        creds_setup
        >> creds_setup_aws
        >> start_quantrix_load
        >> check_catg_subclass_map_file
        >> copy_catg_subclass_map_files
    )
    copy_catg_subclass_map_files >> product_catg_subclass_map_gcs_to_landing
    (
        start_quantrix_load
        >> check_supp_dept_map_file
        >> copy_supp_dept_map_files
        >> product_supp_dept_map_gcs_to_landing
    )
    (
        product_catg_subclass_map_gcs_to_landing
        >> load_product_catg_subclass_map_dimension
        >> end_quantrix_load
    )
    (
        product_supp_dept_map_gcs_to_landing
        >> load_product_supp_dept_map_dimension
        >> end_quantrix_load
    )
