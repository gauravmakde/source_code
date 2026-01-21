from datetime import timedelta, datetime
import configparser
import os
import random
import sys, logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from nordstrom.utils.cloud_creds import cloud_creds
from nordstrom.utils.setup_module import setup_module
from pathlib import Path

file_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/__init__.py",
)
setup_module(module_name="ds_product_common", file_path=file_path)

from ds_product_common.bigquery_util import (get_cntrl_tbl_dates, get_elt_control_load,call_s3_to_gcs,dataproc_gcs_to_bq,call_bqinsert_operator)
from ds_product_common.servicenow_util import invoke_servicenow_api

# from ds_product_common.s3_util import s3_copy_files
# from ds_product_common.sensor_util import s3_check_success_file
# from ds_product_common.flink_job_helper import FlinkJobHelper

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = 'development'

# Fetch the data from config file
config = configparser.ConfigParser()
current_path = os.path.split(__file__)[0]
sql_path = os.path.join(
    current_path, "../../sql/PRODUCT_SELLABILITY_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/"
)
config.read(
    os.path.join(
        current_path,
        "../../configs/PRODUCT_SELLABILITY_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/product_sellability.cfg",
    )
)
owner = config.get(env, "dag_owner")
email = config.get(env, "dag_alert_email_addr")
schedule = config.get(env, "dag_schedule_interval")
retries = config.getint(env, "retries")
retry_delay = config.getint(env, "retry_delay")
service_account_email = config.get(env, "service_account_email")
subnet_url = config.get(env, "subnet_url")
gcs_bucket=config.get(env, 'gcs_bucket')
s3_bucket=config.get(env, 's3_bucket')
# BigqueryInsertJob Op parameters
project_id = config.get(env, "gcp_project_id")
gcp_conn = config.get(env, "gcp_conn")
sql_config_dict = dict(config["db"])
region = config.get(env, "region")
module_path = os.path.join(current_path, "../../modules/")
sys.path.append(module_path)



# Teradata configs
# s3_sql_path = config.get(env, 'sql_path')
# ec2_sql_path = config.get(env, 'ec2_sql_path') + 'sellability/'
subject_area_name = config.get(env, "subject_area_name")
aws_conn_id = config.get(env, 'aws_conn_id')
# Setting up arguments
# Dyear = "{{ds_nodash[:4]}}"
# Dmonth = "{{ds_nodash[4:6]}}"
# Dday = "{{ds_nodash[6:8]}}"
# Dhour = "{{ ts_nodash[9:11]}}"


# FROM_DATE = "{{ ds }}"
# TO_DATE = "{{ tomorrow_ds }}"

Dyear = '2024'
Dmonth = '10'
Dday = '08'
Dhour = '08'

DAG_NAME = "PRODUCT_SELLABILITY_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1"

# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")

curr_day_path = (
    "/year=" + Dyear + "/month=" + Dmonth + "/day=" + Dday + "/hour=" + Dhour + "/"
)

# flink variables
kafka_bootstrap_servers = config.get(env, "kafka_bootstrap_servers")
kafka_schema_registry_url = config.get(env, "kafka_schema_registry_url")
flink_offset_path = config.get(env, "flink_offset_path")
flink_offset_backup_path = config.get(env, "flink_offset_backup_path")

# flink variables - Image Asset
product_image_asset_flink_deployment = config.get(
    env, "product_image_asset_flink_deployment"
)
product_image_asset_flink_service_url = config.get(
    env, "product_image_asset_flink_service_url"
)
product_image_asset_kafka_input_topic = config.get(
    env, "product_image_asset_kafka_input_topic"
)
product_image_asset_kafka_consumer_group_id = config.get(
    env, "product_image_asset_kafka_consumer_group_id"
)
product_image_asset_offset_file = flink_offset_path + config.get(
    env, "product_image_asset_offset_path_suffix"
)
product_image_asset_output_path = config.get(env, "product_image_asset_output_path")
product_image_asset_flink_patch_template_file = os.path.join(
    current_path, "configs/flink/product-image-asset-job.json"
)

# flink variables - Live On Site
product_live_on_site_flink_deployment = config.get(
    env, "product_live_on_site_flink_deployment"
)
product_live_on_site_flink_service_url = config.get(
    env, "product_live_on_site_flink_service_url"
)
product_live_on_site_kafka_input_topic = config.get(
    env, "product_live_on_site_kafka_input_topic"
)
product_live_on_site_kafka_consumer_group_id = config.get(
    env, "product_live_on_site_kafka_consumer_group_id"
)
product_live_on_site_offset_file = flink_offset_path + config.get(
    env, "product_live_on_site_offset_path_suffix"
)
product_live_on_site_output_path = config.get(env, "product_live_on_site_output_path")
product_live_on_site_flink_patch_template_file = os.path.join(
    current_path, "configs/flink/product-live-on-site-job.json"
)


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
    return f"""{dag_id}--{task_id}--{date}""".lower()


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
    "owner": owner,
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 1),
    # 'email': email.split(','),
    # 'email_on_failure': False if env == 'development' else True,
    'email_on_failure': False,
    # 'email_on_retry': False,
    "sla": timedelta(minutes=180),
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
        nauth_conn_id="nauth-connection-nonprod",
        cloud_conn_id="aws_default",
        aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer",
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()

# Initializing BQ hook
hook = BigQueryHook(gcp_conn_id=gcp_conn, location=region, use_legacy_sql=False)


with DAG(
    f"gcp_{DAG_NAME}",
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

    # List all Tasks

    # Initialize FlinkJobHelper
    # flink_job_helper = FlinkJobHelper()

    # # Step 3 - Take backup of the kafka offset files before triggering the metamorph scripts
    # backup_offset_files = s3_copy_files('backup_offset_files', flink_offset_path, flink_offset_backup_path)
    
    
    # These offset files are being copied on s3 bucket hence used dummyoperator for testing
    backup_offset_files = DummyOperator(task_id='backup_offset_files',dag=dag)


    # # Step 4 - Triggering the flink converter for extracting the objects
    # product_image_asset_flink_extract = \
    #     flink_job_helper.create_submit_job_operator(task_id="trigger_product_image_asset_flink_extract",
    #                                                 pod_name="product-image-asset-airflow-flink-job-trigger",
    #                                                 flink_deployment=product_image_asset_flink_deployment,
    #                                                 flink_service_url=product_image_asset_flink_service_url,
    #                                                 patch_template_file=product_image_asset_flink_patch_template_file,
    #                                                 env_vars={
    #                                                     "TEMPLATE_RESTART_NONCE": str(random.randint(1, 10)),
    #                                                     "TEMPLATE_KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
    #                                                     "TEMPLATE_SCHEMA_REGISTRY_URL": kafka_schema_registry_url,
    #                                                     "TEMPLATE_KAFKA_CONSUMER_GROUP_ID": product_image_asset_kafka_consumer_group_id,
    #                                                     "TEMPLATE_KAFKA_INPUT_TOPIC": product_image_asset_kafka_input_topic,
    #                                                     "TEMPLATE_INPUT_OFFSET_FILE": product_image_asset_offset_file,
    #                                                     "TEMPLATE_OUTPUT_BASE_PATH": product_image_asset_output_path + curr_day_path
    #                                                 })

    # product_online_purchasable_flink_extract = \
    #     flink_job_helper.create_submit_job_operator(task_id="trigger_product_online_purchasable_flink_extract",
    #                                                 pod_name="product-live-on-site-airflow-flink-job-trigger",
    #                                                 flink_deployment=product_live_on_site_flink_deployment,
    #                                                 flink_service_url=product_live_on_site_flink_service_url,
    #                                                 patch_template_file=product_live_on_site_flink_patch_template_file,
    #                                                 env_vars={
    #                                                     "TEMPLATE_RESTART_NONCE": str(random.randint(1, 10)),
    #                                                     "TEMPLATE_KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
    #                                                     "TEMPLATE_SCHEMA_REGISTRY_URL": kafka_schema_registry_url,
    #                                                     "TEMPLATE_KAFKA_CONSUMER_GROUP_ID": product_live_on_site_kafka_consumer_group_id,
    #                                                     "TEMPLATE_KAFKA_INPUT_TOPIC": product_live_on_site_kafka_input_topic,
    #                                                     "TEMPLATE_INPUT_OFFSET_FILE": product_live_on_site_offset_file,
    #                                                     "TEMPLATE_OUTPUT_BASE_PATH": product_live_on_site_output_path + curr_day_path
    #                                                 })

    # # Step 5 - Monitor the metamorph jobs submitted in the previous steps
    # product_image_asset_flink_status = \
    #     flink_job_helper.create_check_job_operator(prev_task_id="trigger_product_image_asset_flink_extract",
    #                                                task_id="monitor_product_image_asset_flink_extract",
    #                                                pod_name="product-image-asset-airflow-flink-job-monitor",
    #                                                flink_service_url=product_image_asset_flink_service_url)

    # product_online_purchasable_flink_status = \
    #     flink_job_helper.create_check_job_operator(prev_task_id="trigger_product_online_purchasable_flink_extract",
    #                                                task_id="monitor_product_online_purchasable_flink_extract",
    #                                                pod_name="product-live-on-site-airflow-flink-job-monitor",
    #                                                flink_service_url=product_live_on_site_flink_service_url)

    # Step 7 - ETL Batch process to set the ACTIVE_LOAD_IND to Y/N
    start_sellability_load = get_elt_control_load(
        "start_sellability_load", hook, subject_area_name, "START"
    )
    end_sellability_load = get_elt_control_load(
        "end_sellability_load", hook, subject_area_name, "END"
    )

    # Step 8 - Task to get control table dates
    cntrl_tbl_dt_ext = get_cntrl_tbl_dates("cntrl_tbl_dt_ext", subject_area_name, hook)

    # # Step 9 - Copies all the sql files from s3 to ec2 to work with teradata
    # # copy_sellability_sqls_from_s3_to_ec2 = copy_s3_to_ec2("copy_sellability_sqls_from_s3_to_ec2",
    # #                                                       s3_sql_path, ec2_sql_path)

    # # # Step 10 - Check S3 _SUCCESS Files to confirm the availability of extracts
    # # productOnlinePurchasableItemS3Check = s3_check_success_file('productOnlinePurchasableItemS3Check',
    # #                                                             product_live_on_site_output_path + curr_day_path)
    # # productImageAssetS3Check = s3_check_success_file('productImageAssetS3Check',
    # #                                                  product_image_asset_output_path + curr_day_path)

    # # Step 11 - TPT Load of extract files into TD Landing tables
    product_online_purchasable_GCS_to_BQ = dataproc_gcs_to_bq('product_online_purchasable_GCS_to_BQ',DAG_NAME,config.get(env,'dataset_name'),config.get(env,'online_table_name'),gcp_conn,gcs_bucket+product_live_on_site_output_path+curr_day_path,dag)

    product_online_purchasable_s3_to_gcs = call_s3_to_gcs('product_online_purchasable_s3_to_gcs',aws_conn_id,gcp_conn,s3_bucket,product_live_on_site_output_path+curr_day_path,gcs_bucket,dag)  

    product_image_asset_GCS_to_BQ = dataproc_gcs_to_bq('product_image_asset_GCS_to_BQ',DAG_NAME,config.get(env,'dataset_name'),config.get(env,'img_table_name'),gcp_conn,gcs_bucket+product_image_asset_output_path+curr_day_path,dag)

    product_image_asset_s3_to_gcs = call_s3_to_gcs('product_image_asset_s3_to_gcs',aws_conn_id,gcp_conn,s3_bucket,product_image_asset_output_path+curr_day_path,gcs_bucket,dag)  

    # Step 12 - Dummy tasks to mark the completion of Teradata landing and dimension table load
    product_landing_tbl_load_complete = dummy_task(
        "product_landing_table_load_complete", dag
    )
    product_dim_tbl_load_complete = dummy_task(
        "product_dimension_table_load_complete", dag
    )

    load_product_online_purchasable_item_dimension = call_bqinsert_operator(dag,'product_online_purchasable_item_dimension_table',os.path.join(
            sql_path,
            "PRODUCT_ONLINE_PURCHASABLE_ITEM_LDG_2_PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM.sql"),project_id,sql_config_dict,region,gcp_conn)


    # Step 13 - TD Script for stage to dimension table load
    # PRODUCT_ONLINE_PURCHASABLE_ITEM_LDG_2_PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM = open(
    #     os.path.join(
    #         sql_path,
    #         "PRODUCT_ONLINE_PURCHASABLE_ITEM_LDG_2_PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM.sql",
    #     ),
    #     "r",
    # ).read()
    # load_product_online_purchasable_item_dimension = BigQueryInsertJobOperator(
    #     task_id="product_online_purchasable_item_dimension_table",
    #     configuration={
    #         "query": {
    #             "query": PRODUCT_ONLINE_PURCHASABLE_ITEM_LDG_2_PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM,
    #             "useLegacySql": False,
    #         }
    #     },
    #     project_id=project_id,
    #     gcp_conn_id=gcp_conn,
    #     params=sql_config_dict,
    #     location=region,
    #     dag=dag,
    # )

    load_product_image_asset_dimension = call_bqinsert_operator(dag,'product_image_asset_dimension_table',os.path.join(sql_path, "PRODUCT_IMAGE_ASSET_LDG_2_PRODUCT_IMAGE_ASSET_DIM.sql"),project_id,sql_config_dict,region,gcp_conn)

    # PRODUCT_IMAGE_ASSET_LDG_2_PRODUCT_IMAGE_ASSET_DIM = open(
    #     os.path.join(sql_path, "PRODUCT_IMAGE_ASSET_LDG_2_PRODUCT_IMAGE_ASSET_DIM.sql"),
    #     "r",
    # ).read()
    # load_product_image_asset_dimension = BigQueryInsertJobOperator(
    #     task_id="product_image_asset_dimension_table",
    #     configuration={
    #         "query": {
    #             "query": PRODUCT_IMAGE_ASSET_LDG_2_PRODUCT_IMAGE_ASSET_DIM,
    #             "useLegacySql": False,
    #         }
    #     },
    #     project_id=project_id,
    #     gcp_conn_id=gcp_conn,
    #     params=sql_config_dict,
    #     location=region,
    #     dag=dag,
    # )

    # Dependency between tasks
    creds_setup >> creds_setup_aws >> start_sellability_load >> backup_offset_files
    backup_offset_files >> cntrl_tbl_dt_ext
    cntrl_tbl_dt_ext >> [
        product_online_purchasable_s3_to_gcs,
        product_image_asset_s3_to_gcs,
    ]
    
    product_online_purchasable_s3_to_gcs >> product_online_purchasable_GCS_to_BQ
    product_image_asset_s3_to_gcs >> product_image_asset_GCS_to_BQ

    [
        product_online_purchasable_GCS_to_BQ,
        product_image_asset_GCS_to_BQ,
    ] >> product_landing_tbl_load_complete

    product_landing_tbl_load_complete >> [
        load_product_online_purchasable_item_dimension,
        load_product_image_asset_dimension,
    ]

    (
        [
            load_product_online_purchasable_item_dimension,
            load_product_image_asset_dimension,
        ]
        >> product_dim_tbl_load_complete
        >> end_sellability_load
    )
