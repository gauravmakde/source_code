from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from datetime import datetime
from datetime import timedelta
from nordstrom.utils.cloud_creds import cloud_creds
# from nordstrom.utils.setup_module import setup_module
from os import path
from pathlib import Path
import configparser
import logging
import os
import sys
import random
import ast

# file_path=os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent,'../modules/common/packages/__init__.py')
# setup_module(module_name="ds_product_common", file_path=file_path)
modules_path = os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent,'../modules/common/packages/')
sys.path.append(modules_path)
from bigquery_util import (get_cntrl_tbl_dates,get_product_elt_control_load,call_s3_to_gcs,dataproc_gcs_to_bq,call_bqinsert_operator)

# from flink_job_helper import FlinkJobHelper
from s3_util import s3_copy_files
from sensor_util import s3_check_success_file
from servicenow_util import invoke_servicenow_api

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = 'development'

# env='nonprod'
# Fetch the data from config file
config = configparser.ConfigParser()
current_path = os.path.split(__file__)[0]
sql_path = os.path.join(current_path, '../../sql/PRODUCT_SELLING_EVENTS_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/')
config.read(os.path.join(current_path, '../../configs/PRODUCT_SELLING_EVENTS_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/product_selling_events.cfg'))
owner = config.get(env, 'dag_owner')
# email = config.get(env, 'dag_alert_email_addr')
# schedule = config.get(env, 'dag_schedule_interval')
retries = config.getint(env,'retries')
retry_delay = config.getint(env,'retry_delay')

# Teradata configs

subject_area_name = config.get(env, 'subject_area_name')
aws_conn_id = config.get(env, 'aws_conn_id')

# Setting up arguments
# Dyear = '{{ds_nodash[:4]}}'
# Dmonth = '{{ds_nodash[4:6]}}'
# Dday = '{{ds_nodash[6:8]}}'
# Dhour = '{{ ts_nodash[9:11]}}'

Dyear = '2024'
Dmonth = '10'
Dday = '08'
Dhour = '04'

DAG_NAME = 'gcp_PRODUCT_SELLING_EVENTS_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1'

# Service now incident configs
sn_incident_severity=config.get(env,'sn_incident_severity')

curr_day_path = '/year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/hour=' + Dhour + '/'


# flink variables
kafka_bootstrap_servers = config.get(env, 'kafka_bootstrap_servers')
kafka_schema_registry_url = config.get(env, 'kafka_schema_registry_url')
flink_offset_path = config.get(env, 'flink_offset_path')
flink_offset_backup_path = config.get(env, 'flink_offset_backup_path')

# flink variables - Selling Event
product_selling_event_flink_deployment = config.get(env, 'product_selling_event_flink_deployment')
product_selling_event_flink_service_url = config.get(env, 'product_selling_event_flink_service_url')
product_selling_event_kafka_input_topic = config.get(env, 'product_selling_event_kafka_input_topic')
product_selling_event_kafka_consumer_group_id = config.get(env, 'product_selling_event_kafka_consumer_group_id')
product_selling_event_offset_file = flink_offset_path + config.get(env, 'product_selling_event_offset_path_suffix')
product_selling_event_output_path_category = config.get(env, 'product_selling_event_output_path_category')
product_selling_event_output_path_category_prefix = config.get(env, 'product_selling_event_output_path_category_prefix')
product_selling_event_output_path_extract = config.get(env, 'product_selling_event_output_path_extract')
product_selling_event_output_path_extract_prefix = config.get(env, 'product_selling_event_output_path_extract_prefix')
product_selling_event_flink_patch_template_file = os.path.join(current_path, 'configs/flink/product-selling-event-job.json')

# flink variables - Selling Event Sku
product_selling_event_sku_flink_deployment = config.get(env, 'product_selling_event_sku_flink_deployment')
product_selling_event_sku_flink_service_url = config.get(env, 'product_selling_event_sku_flink_service_url')
product_selling_event_sku_kafka_input_topic = config.get(env, 'product_selling_event_sku_kafka_input_topic')
product_selling_event_sku_kafka_consumer_group_id = config.get(env, 'product_selling_event_sku_kafka_consumer_group_id')
product_selling_event_sku_offset_file = flink_offset_path + config.get(env, 'product_selling_event_sku_offset_path_suffix')
product_selling_event_sku_output_path_sku = config.get(env, 'product_selling_event_sku_output_path_sku')
product_selling_event_sku_output_path_sku_prefix = config.get(env, 'product_selling_event_sku_output_path_sku_prefix')
product_selling_event_sku_output_path_tag = config.get(env, 'product_selling_event_sku_output_path_tag')
product_selling_event_sku_output_path_tag_prefix = config.get(env, 'product_selling_event_sku_output_path_tag_prefix')
product_selling_event_sku_flink_patch_template_file = os.path.join(current_path, 'configs/flink/product-selling-event-sku-job.json')
gcp_conn_id=config.get(env, 'gcp_conn')
bq_bucket=config.get(env, 'bq_bucket')
location=config.get(env, 'region')
service_account_email=config.get(env, 'service_account_email')
gcs_bucket=config.get(env, 'gcs_bucket')
s3_bucket=config.get(env, 's3_bucket')
project_id=config.get(env, 'gcp_project_id')
sql_config_dict = ast.literal_eval(config[env]['sql_config_dict'])

def create_servicenow_incident(context):
    """
       This function triggers servicenow api
         Parameters : context, severity
         Returns    : operator
    """
    invoke_servicenow_api(context, sn_incident_severity)


def dummy_task(task_id, dag):
    """
       This function calls dummy operator
         Parameters : TaskId
         Returns    : None
     """
    return DummyOperator(task_id=task_id, trigger_rule="none_failed", dag=dag)


default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 1),
    # 'email': email.split(','),
    'email_on_failure': False,
    # 'email_on_retry': False,
    'sla': timedelta(minutes=180),
    # 'on_failure_callback': create_servicenow_incident
}

def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get(env, 'nauth_conn_id'),
        cloud_conn_id=config.get(env, 'gcp_conn'),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def setup_creds_aws(aws_iam_role_arn: str):
    @cloud_creds(
        nauth_conn_id=config.get(env, 'nauth_conn_id'),
        cloud_conn_id=config.get(env, 'aws_conn_id'),
        aws_iam_role_arn=aws_iam_role_arn,
    )
    def setup_credential():
        logging.info("AWS connection is set up")
    
    setup_credential()
    
    
# Initializing BQ hook
hook = BigQueryHook(gcp_conn_id=gcp_conn_id,location=location, use_legacy_sql=False)

with DAG(DAG_NAME, default_args=default_args, schedule_interval=None, catchup=False) as dag:

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )

    creds_setup_aws = PythonOperator(
        task_id="setup_creds_aws",
        python_callable=setup_creds_aws,
        op_args=['arn:aws:iam::290445963451:role/gcp_data_transfer'],
    )

    # List all Tasks

    # Initialize FlinkJobHelper
    # flink_job_helper = FlinkJobHelper()

    # Step 1, 2, 6 - Create cluster, disable/resume idle termination - skipped
    # backup_offset_files = s3_copy_files('backup_offset_files', flink_offset_path, flink_offset_backup_path)
    backup_offset_files = dummy_task('backup_offset_files',dag)


    # Step 4 - Triggering the flink converter for extracting the objects - 
    # product_selling_event_flink_extract = \
    #     flink_job_helper.create_submit_job_operator(task_id="trigger_product_selling_event_flink_extract",
    #                                                 pod_name="product-selling-event-airflow-flink-job-trigger",
    #                                                 flink_deployment=product_selling_event_flink_deployment,
    #                                                 flink_service_url=product_selling_event_flink_service_url,
    #                                                 patch_template_file=product_selling_event_flink_patch_template_file,
    #                                                 env_vars={
    #                                                     "TEMPLATE_RESTART_NONCE": str(random.randint(1, 10)),
    #                                                     "TEMPLATE_KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
    #                                                     "TEMPLATE_SCHEMA_REGISTRY_URL": kafka_schema_registry_url,
    #                                                     "TEMPLATE_KAFKA_CONSUMER_GROUP_ID": product_selling_event_kafka_consumer_group_id,
    #                                                     "TEMPLATE_KAFKA_INPUT_TOPIC": product_selling_event_kafka_input_topic,
    #                                                     "TEMPLATE_INPUT_OFFSET_FILE": product_selling_event_offset_file,
    #                                                     "TEMPLATE_OUTPUT_BASE_PATH_CATEGORY": product_selling_event_output_path_category + curr_day_path,
    #                                                     "TEMPLATE_OUTPUT_BASE_PATH_EXTRACT": product_selling_event_output_path_extract + curr_day_path
    #                                                 })

    # product_selling_event_sku_flink_extract = \
    #     flink_job_helper.create_submit_job_operator(task_id="trigger_product_selling_event_sku_flink_extract",
    #                                                 pod_name="product-selling-event-sku-airflow-flink-job-trigger",
    #                                                 flink_deployment=product_selling_event_sku_flink_deployment,
    #                                                 flink_service_url=product_selling_event_sku_flink_service_url,
    #                                                 patch_template_file=product_selling_event_sku_flink_patch_template_file,
    #                                                 env_vars={
    #                                                     "TEMPLATE_RESTART_NONCE": str(random.randint(1, 10)),
    #                                                     "TEMPLATE_KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
    #                                                     "TEMPLATE_SCHEMA_REGISTRY_URL": kafka_schema_registry_url,
    #                                                     "TEMPLATE_KAFKA_CONSUMER_GROUP_ID": product_selling_event_sku_kafka_consumer_group_id,
    #                                                     "TEMPLATE_KAFKA_INPUT_TOPIC": product_selling_event_sku_kafka_input_topic,
    #                                                     "TEMPLATE_INPUT_OFFSET_FILE": product_selling_event_sku_offset_file,
    #                                                     "TEMPLATE_OUTPUT_BASE_PATH_SKU": product_selling_event_sku_output_path_sku + curr_day_path,
    #                                                     "TEMPLATE_OUTPUT_BASE_PATH_TAG": product_selling_event_sku_output_path_tag + curr_day_path
    #                                                 })

    # # Step 5 - Monitor the Flink jobs submitted in the previous steps
    # product_selling_event_flink_status = \
    #     flink_job_helper.create_check_job_operator(prev_task_id="trigger_product_selling_event_flink_extract",
    #                                                task_id="monitor_product_selling_event_flink_extract",
    #                                                pod_name="product-selling-event-airflow-flink-job-monitor",
    #                                                flink_service_url=product_selling_event_flink_service_url)

    # product_selling_event_sku_flink_status = \
    #     flink_job_helper.create_check_job_operator(prev_task_id="trigger_product_selling_event_sku_flink_extract",
    #                                                task_id="monitor_product_selling_event_sku_flink_extract",
    #                                                pod_name="product-selling-event-sku-airflow-flink-job-monitor",
    #                                                flink_service_url=product_selling_event_sku_flink_service_url)
    
    # Please remove all the dumpy operator for flink and use the above flink for fetching 
    product_selling_event_flink_extract = dummy_task("product_selling_event_flink_extract", dag)
    product_selling_event_sku_flink_extract = dummy_task("product_selling_event_sku_flink_extract", dag)
    product_selling_event_flink_status = dummy_task("product_selling_event_flink_status", dag)
    product_selling_event_sku_flink_status = dummy_task("product_selling_event_sku_flink_status", dag)

    # Step 7 - ETL Batch process to set the ACTIVE_LOAD_IND to Y/N

    start_selling_event_load = get_product_elt_control_load("start_selling_event_load",hook, subject_area_name, 'START')
    end_selling_event_load = get_product_elt_control_load("end_selling_event_load",hook,subject_area_name, 'END')

    # # Step 8 - Task to get control table dates
    # cntrl_tbl_dt_ext = get_cntrl_tbl_dates('cntrl_tbl_dt_ext',subject_area_name,hook)
    cntrl_tbl_dt_ext = dummy_task('cntrl_tbl_dt_ext',dag = dag)
    
    # productSellingEventSKUS3Check = s3_check_success_file('productSellingEventSKUS3Check',
    #                                                       product_selling_event_sku_output_path_sku + curr_day_path)
    # productSellingEventS3Check = s3_check_success_file('productSellingEventS3Check',
    #                                                    product_selling_event_output_path_extract + curr_day_path)
    # productSellingEventTagsS3Check = s3_check_success_file('productSellingEventTagsS3Check',
    #                                                        product_selling_event_sku_output_path_tag + curr_day_path)
    # productSellingEventCategoryS3Check = s3_check_success_file('productSellingEventCategoryS3Check',
    #                                                            product_selling_event_output_path_category + curr_day_path)

    productSellingEventSKUS3Check = dummy_task('productSellingEventSKUS3Check',dag)
    productSellingEventS3Check = dummy_task('productSellingEventS3Check',dag)
    productSellingEventTagsS3Check = dummy_task('productSellingEventTagsS3Check',dag)
    productSellingEventCategoryS3Check = dummy_task('productSellingEventCategoryS3Check',dag)

    product_selling_event_sku_s3_to_GCS = call_s3_to_gcs('product_selling_event_sku_s3_to_GCS',aws_conn_id,gcp_conn_id,s3_bucket,product_selling_event_sku_output_path_sku_prefix+curr_day_path,gcs_bucket,dag)
    product_selling_event_categories_s3_to_GCS = call_s3_to_gcs('product_selling_event_categories_s3_to_GCS',aws_conn_id,gcp_conn_id,s3_bucket,product_selling_event_output_path_category_prefix+curr_day_path,gcs_bucket,dag)
    product_selling_event_s3_to_GCS = call_s3_to_gcs('product_selling_event_s3_to_GCS',aws_conn_id,gcp_conn_id,s3_bucket,product_selling_event_output_path_extract_prefix+curr_day_path,gcs_bucket,dag)
    product_selling_event_tags_s3_to_GCS = call_s3_to_gcs('product_selling_event_tags_s3_to_GCS',aws_conn_id,gcp_conn_id,s3_bucket,product_selling_event_sku_output_path_tag_prefix+curr_day_path,gcs_bucket,dag)
    

    product_selling_event_sku_GCS_to_BQ = dataproc_gcs_to_bq('product_selling_event_sku_GCS_to_BQ',DAG_NAME,config.get(env,'dataset_name'),config.get(env,'sku_table_name'),gcp_conn_id,gcs_bucket+product_selling_event_sku_output_path_sku_prefix+curr_day_path,dag)
    product_selling_event_tags_GCS_to_BQ = dataproc_gcs_to_bq('product_selling_event_tags_GCS_to_BQ',DAG_NAME,config.get(env,'dataset_name'),config.get(env,'tags_table_name'),gcp_conn_id,gcs_bucket+product_selling_event_sku_output_path_tag_prefix+curr_day_path,dag)
    product_selling_event_categories_GCS_to_BQ = dataproc_gcs_to_bq('product_selling_event_categories_GCS_to_BQ',DAG_NAME,config.get(env,'dataset_name'),config.get(env,'category_table_name'),gcp_conn_id,gcs_bucket+product_selling_event_output_path_category_prefix+curr_day_path,dag)
    product_selling_event_GCS_to_BQ = dataproc_gcs_to_bq('product_selling_event_GCS_to_BQ',DAG_NAME,config.get(env,'dataset_name'),config.get(env,'event_table_name'),gcp_conn_id,gcs_bucket+product_selling_event_output_path_extract_prefix+curr_day_path,dag)




    # Step 12 - Dummy tasks to mark the completion of Teradata landing load
    product_landing_tbl_load_complete = dummy_task("product_landing_table_load_complete", dag)

    # Step 13 - TD Script for stage to dimension table load
    

    load_product_selling_event_categories_dimension = call_bqinsert_operator(dag,'product_selling_event_categories_dimension_table_load',os.path.join(sql_path,'PRODUCT_SELLING_EVENT_CATEGORIES_LDG_2_SELLING_EVENT_CATEGORIES_DIM.sql'),project_id,sql_config_dict,location,gcp_conn_id)


    load_product_selling_event_tags_dimension = call_bqinsert_operator(dag,'product_selling_event_tags_dimension_table_load',os.path.join(sql_path,'PRODUCT_SELLING_EVENT_TAGS_LDG_2_SELLING_EVENT_TAGS_DIM.sql'),project_id,sql_config_dict,location,gcp_conn_id)

    load_product_selling_event_xref = call_bqinsert_operator(dag,'product_selling_event_xref_table_load',os.path.join(sql_path,'PRODUCT_SELLING_EVENT_LDG_2_SELLING_EVENT_XREF.sql'),project_id,sql_config_dict,location,gcp_conn_id)

    load_product_selling_event_sku_dimension = call_bqinsert_operator(dag,'product_selling_event_sku_dimension_table_load',os.path.join(sql_path,'PRODUCT_SELLING_EVENT_SKU_LDG_2_SELLING_EVENT_SKU_DIM.sql'),project_id,sql_config_dict,location,gcp_conn_id)
    
    creds_setup >> creds_setup_aws >> start_selling_event_load >> backup_offset_files
    backup_offset_files >> product_selling_event_flink_extract >> product_selling_event_flink_status
    backup_offset_files >> product_selling_event_sku_flink_extract >> product_selling_event_sku_flink_status
    product_selling_event_flink_status >> cntrl_tbl_dt_ext
    product_selling_event_sku_flink_status >> cntrl_tbl_dt_ext
    cntrl_tbl_dt_ext >> productSellingEventSKUS3Check >> product_selling_event_sku_s3_to_GCS >> product_selling_event_sku_GCS_to_BQ >> product_landing_tbl_load_complete
    cntrl_tbl_dt_ext >> productSellingEventS3Check >> product_selling_event_s3_to_GCS >> product_selling_event_GCS_to_BQ >> product_landing_tbl_load_complete
    cntrl_tbl_dt_ext >> productSellingEventTagsS3Check >> product_selling_event_tags_s3_to_GCS >> product_selling_event_tags_GCS_to_BQ >> product_landing_tbl_load_complete
    cntrl_tbl_dt_ext >> productSellingEventCategoryS3Check >> product_selling_event_categories_s3_to_GCS >> product_selling_event_categories_GCS_to_BQ >> product_landing_tbl_load_complete
    product_landing_tbl_load_complete >> load_product_selling_event_categories_dimension >> load_product_selling_event_xref
    product_landing_tbl_load_complete >> load_product_selling_event_tags_dimension >> load_product_selling_event_xref
    load_product_selling_event_xref >> load_product_selling_event_sku_dimension >> end_selling_event_load
