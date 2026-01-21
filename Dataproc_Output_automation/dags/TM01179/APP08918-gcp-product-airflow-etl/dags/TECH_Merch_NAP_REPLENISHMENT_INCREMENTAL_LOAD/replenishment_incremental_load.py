# from airflow.models.taskinstance import task_instance
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from datetime import datetime
from datetime import timedelta
from nordstrom.utils.cloud_creds import cloud_creds
from nordstrom.utils.setup_module import setup_module
from os import path
from pathlib import Path
import configparser
import logging
import os
import re
import sys

file_path = os.path.join(
    Path(os.path.dirname(os.path.abspath(__file__))).parent,
    "../modules/common/packages/__init__.py",
)
# setup_module(module_name="ds_product_common", file_path=file_path)
# from bigquery_util import get_batch_load_status, get_query_results_into_xcom
# from servicenow_util import invoke_servicenow_api

# from ds_product_common.s3_util import check_s3_object_availability
# from ds_product_common.sqs_msg_processor_util_nsk import sqs_file_processor
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
    get_query_results_into_xcom,
)

# fetch environment from OS variable
# env = os.environ["ENVIRONMENT"]
env = "development"

# Fetch the data from config file
config = configparser.ConfigParser()
Current_path = os.path.split(__file__)[0]
sys.path.append(Current_path)
# Airflow variables
config.read(
    os.path.join(
        Current_path,
        "../../configs/TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/nap_merch_replenishment.cfg",
    )
)
owner = config.get(env, "dag_owner")
# email = config.get(env, 'dag_alert_email_addr')
retry_limit = int(config.get(env, "retries"))
retry_delay = int(config.getint(env, "retry_delay"))
schedule = config.get(env, "dag_schedule_interval")
# EC2 Path variables
# s3_sql_path = config.get(env, 'sql_path')
current_path = os.path.split(__file__)[0]
sql_path = os.path.join(current_path, "../../sql/")
# s3bucket_date_format = (
#     "year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/"
#     "hour={{ ts_nodash[9:11]}}/"
# )

s3bucket_date_format = "year=2024/month=10/day=08/hour=10/"
rp_ec2_sql_path = config.get(env, "ec2_sql_path") + "rp/"
subject_area_name = config.get(env, "subject_area_name")
s3_file_path = config.get(env, "s3_file_path")
aws_conn_id = config.get(env, "aws_conn_id")
gcs_bucket = config.get(env, "gcs_bucket")
s3_connection_id = config.get(env, "s3_connection_id")
s3_bucket = config.get(env, "s3_bucket")

# File paths
rp_date_set_s3_out_path = (
    config.get(env, "rp_date_set_s3_out_path") + s3bucket_date_format
)
inv_deployment_rule_s3_out_path = (
    config.get(env, "inv_deployment_rule_s3_out_path") + s3bucket_date_format
)
item_warehouse_ranged_s3_out_path = (
    config.get(env, "item_warehouse_ranged_s3_out_path") + s3bucket_date_format
)
rp_setting_s3_out_path = (
    config.get(env, "rp_setting_s3_out_path") + s3bucket_date_format
)
rp_eligibility_s3_out_path = (
    config.get(env, "rp_eligibility_s3_out_path") + s3bucket_date_format
)

rp_date_set_s3_out_path_prefix = config.get(env, "rp_date_set_s3_out_path_prefix")
inv_deployment_rule_s3_out_path_prefix = config.get(
    env, "inv_deployment_rule_s3_out_path_prefix"
)
item_warehouse_ranged_s3_out_path_prefix = config.get(
    env, "item_warehouse_ranged_s3_out_path_prefix"
)

rp_setting_s3_out_path_prefix = config.get(env, "rp_setting_s3_out_path_prefix")
rp_eligibility_s3_out_path_prefix = config.get(env, "rp_eligibility_s3_out_path_prefix")
rp_supply_dateset_s3_out_path_prefix = config.get(
    env, "sqs_s3_out_path_rp_supply_dateset_prefix"
)


# SQS queues
rp_date_set_queue_url = config.get(env, "rp_date_set_queue_url")
inv_deployment_rule_queue_url = config.get(env, "inv_deployment_rule_queue_url")
item_warehouse_ranged_queue_url = config.get(env, "item_warehouse_ranged_queue_url")
rp_setting_queue_url = config.get(env, "rp_setting_queue_url")
rp_eligibility_queue_url = config.get(env, "rp_eligibility_queue_url")

# File identifiers
rp_date_set_file_identifier = config.get(env, "rp_date_set_file_identifier")
inv_deployment_rule_file_identifier = config.get(
    env, "inv_deployment_rule_file_identifier"
)
item_warehouse_ranged_file_identifier = config.get(
    env, "item_warehouse_ranged_file_identifier"
)
rp_setting_file_identifier = config.get(env, "rp_setting_file_identifier")
rp_eligibility_file_identifier = config.get(env, "rp_eligibility_file_identifier")

# Replenishment On & Off Supply Variables
rp_supply_dateset_queue_url = config.get(env, "rp_supply_dateset_queue_url")
rp_supply_dateset_file_identifier = config.get(env, "rp_supply_dateset_file_identifier")
sqs_s3_out_path_rp_supply_dateset = (
    config.get(env, "sqs_s3_out_path_rp_supply_dateset") + s3bucket_date_format
)

# Service now incident configs
sn_incident_severity = config.get(env, "sn_incident_severity")
project_id = config.get(env, "project_id")
gcp_conn = config.get(env, "gcp_conn")
region = config.get(env, "region")
sql_dict = dict(config["db"])
gcs_root_bucket_name = config.get(env, "gcs_root_bucket_name")

bq_bucket = config.get(env, "bq_bucket")
service_account_email = config.get(env, "service_account_email")
gcs_bucket_name = config.get(env, "gcs_bucket")
s3_bucket = config.get(env, "s3_bucket")

# RP OSOS Timeline iterations variables
rp_osos_iteration_sql_text = "SELECT CASE WHEN MAX_ITERATIONS > 1 THEN MAX_ITERATIONS ELSE 1 END AS MAX_ITERATIONS \
							  FROM (SELECT MAX(RECORDVERSION) AS MAX_ITERATIONS \
							  FROM `<DBENV>_NAP_BASE_VWS.RP_SKU_LOC_DATE_LDG` ) TMP;"

# sed_text = "\nsed -i 's/<DB_OSOS_ITERATIONS>/{{task_instance.xcom_pull(key='rp_osos_timeline_iterations',\
# 			 task_ids='rp_osos_timeline_iterations',include_prior_dates=True)}}/g'"


def pull_and_append_value_to_sqldict(task_id, sql_dict, sql_path, hook, **kwargs):
    xcom_pulled_value = kwargs["ti"].xcom_pull(
        task_ids="rp_osos_timeline_iterations", key=task_id
    )
    if xcom_pulled_value is not None:
        sql_dict = {**sql_dict, "DB_OSOS_ITERATIONS": xcom_pulled_value}
    else:
        sql_dict = {**sql_dict, "DB_OSOS_ITERATIONS": 1}
    rp_sku_loc_date_opr_to_rp_sku_loc_timeline_dim = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SKU_LOC_DATE_OPR_2_RP_SKU_LOC_TIMELINE_DIM.sql",
        ),
        "r",
    ).read()
    rp_sku_loc_date_opr_to_rp_sku_loc_timeline_dim = (
        rp_sku_loc_date_opr_to_rp_sku_loc_timeline_dim.replace("{{", "{")
        .replace("}}", "}")
        .replace("params.", "")
    )
    rp_sku_loc_date_opr_to_rp_sku_loc_timeline_dim = (
        rp_sku_loc_date_opr_to_rp_sku_loc_timeline_dim.format(**sql_dict)
    )
    result_list = hook.run(rp_sku_loc_date_opr_to_rp_sku_loc_timeline_dim)
    logging.info(
        f"Result for the above Query RP_SKU_LOC_DATE_OPR_2_RP_SKU_LOC_TIMELINE_DIM.sql {result_list}"
    )


# method to invoke the callback function to create service now incident
def create_servicenow_incident(context):
    invoke_servicenow_api(context, sn_incident_severity)


default_args = {
    "owner": owner,
    "start_date": datetime(2020, 5, 7),
    #'email'              : email.split(','),
    #'email_on_failure'   : False if env == 'development' else True,
    #'email_on_retry'     : False,
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
        nauth_conn_id=config.get(env, "nauth_conn_id"),
        cloud_conn_id=config.get(env, "aws_conn_id"),
        aws_iam_role_arn=aws_iam_role_arn,
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()

# Initializing BQ hook
hook = BigQueryHook(gcp_conn_id=gcp_conn, location=region, use_legacy_sql=False)

with DAG(
    "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
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
    # Pushes xcom value as 'Y' for key REPLENISHMENT_LOAD
    start_load = get_batch_load_status(
        "start_replenishment_load", subject_area_name, "START"
    )

    # osos_refresh_sqls = copy_s3_to_ec2_with_modification("refresh_osos_sqls", s3_sql_path, rp_ec2_sql_path, sed_text)
    pull_osos_iteration_task = PythonOperator(
        task_id="pull_osos_iteration_task",
        python_callable=pull_and_append_value_to_sqldict,
        op_kwargs={
            "task_id": "rp_osos_timeline_iterations",
            "sql_dict": sql_dict,
            "sql_path": sql_path,
            "hook": hook,
        },
        provide_context=True,
        dag=dag,
    )
    # Step 3 - Task for processing messages from AWS SQS queue. If the queue has any Selling Rights or Status files
    # then the xcom value will be updated as 'File received' or 'File Not Received'
    # rp_date_set_sqs_processing = sqs_file_processor("rp_date_set_sqs_processing", rp_date_set_queue_url,
    # 												   rp_date_set_file_identifier,
    # 												   rp_date_set_s3_out_path)
    rp_date_set_sqs_processing = DummyOperator(
        task_id="rp_date_set_sqs_processing", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # inv_deployment_rule_sqs_processing = sqs_file_processor("inv_deployment_rule_sqs_processing",
    # 														inv_deployment_rule_queue_url,
    # 												   inv_deployment_rule_file_identifier,
    # 												   inv_deployment_rule_s3_out_path)
    inv_deployment_rule_sqs_processing = DummyOperator(
        task_id="inv_deployment_rule_sqs_processing", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # item_warehouse_ranged_sqs_processing = sqs_file_processor("item_warehouse_ranged_sqs_processing",
    # 														  item_warehouse_ranged_queue_url,
    # 														  item_warehouse_ranged_file_identifier,
    # 														  item_warehouse_ranged_s3_out_path)
    item_warehouse_ranged_sqs_processing = DummyOperator(
        task_id="item_warehouse_ranged_sqs_processing", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # rp_setting_sqs_processing = sqs_file_processor("rp_setting_sqs_processing", rp_setting_queue_url,
    # 											   rp_setting_file_identifier,
    # 											   rp_setting_s3_out_path)
    rp_setting_sqs_processing = DummyOperator(
        task_id="rp_setting_sqs_processing", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # rp_eligibility_sqs_processing = sqs_file_processor("rp_eligibility_sqs_processing", rp_eligibility_queue_url,
    # 												   rp_eligibility_file_identifier,
    # 												   rp_eligibility_s3_out_path)
    rp_eligibility_sqs_processing = DummyOperator(
        task_id="rp_eligibility_sqs_processing", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # rp_supply_dateset_sqs_processing = sqs_file_processor("rp_supply_dateset_sqs_processing",
    # 													  rp_supply_dateset_queue_url,
    # 													  rp_supply_dateset_file_identifier,
    # 													  sqs_s3_out_path_rp_supply_dateset)
    rp_supply_dateset_sqs_processing = DummyOperator(
        task_id="rp_supply_dateset_sqs_processing", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # Step 4 - Check if the queue processing task found any files in the queue, if not skips the Teradata load
    # rp_date_set_check_file_availability = check_s3_object_availability('rp_date_set_check_file_availability',
    # 																   rp_date_set_s3_out_path,
    # 																   'rp_date_set_skip_load',
    # 																   'rp_date_set_s3_to_landing')
    rp_date_set_check_file_availability = DummyOperator(
        task_id="rp_date_set_check_file_availability", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # inv_deployment_rule_check_file_availability = check_s3_object_availability('inv_deployment_rule_check_file_availability'
    # 																		  ,inv_deployment_rule_s3_out_path,
    # 																		  'inv_deployment_rule_skip_load',
    # 																		  'inv_deployment_rule_s3_to_landing')
    inv_deployment_rule_check_file_availability = DummyOperator(
        task_id="inv_deployment_rule_check_file_availability", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # item_warehouse_ranged_check_file_availability = check_s3_object_availability('item_warehouse_ranged_check_file_availability',
    # 																			 item_warehouse_ranged_s3_out_path,
    # 																			'item_warehouse_ranged_skip_load',
    # 																			'item_warehouse_ranged_s3_to_landing')
    item_warehouse_ranged_check_file_availability = DummyOperator(
        task_id="item_warehouse_ranged_check_file_availability", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # rp_setting_check_file_availability = check_s3_object_availability('rp_setting_check_file_availability',
    # 																  rp_setting_s3_out_path,
    # 																 'rp_setting_skip_load',
    # 																 'rp_setting_s3_to_landing')
    rp_setting_check_file_availability = DummyOperator(
        task_id="rp_setting_check_file_availability", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # rp_eligibility_check_file_availability = check_s3_object_availability('rp_eligibility_check_file_availability',
    # 																	  rp_eligibility_s3_out_path,
    # 																	 'rp_eligibility_skip_load',
    # 																	 'rp_eligibility_s3_to_landing')
    rp_eligibility_check_file_availability = DummyOperator(
        task_id="rp_eligibility_check_file_availability", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # rp_supply_dateset_check_file_availability = check_s3_object_availability(
    # 	'rp_supply_dateset_check_file_availability',
    # 	sqs_s3_out_path_rp_supply_dateset,
    # 	'rp_supply_dateset_skip_load',
    # 	'rp_supply_dateset_s3_to_landing')
    rp_supply_dateset_check_file_availability = DummyOperator(
        task_id="rp_supply_dateset_check_file_availability", dag=dag
    )  # MAKE AS A DUMMY OPERATOR

    # Task to transfer file from S3 to GCS

    rp_date_set_s3_to_gcs = call_s3_to_gcs(
        "rp_date_set_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        rp_date_set_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    inv_deployment_rule_s3_to_gcs = call_s3_to_gcs(
        "inv_deployment_rule_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        inv_deployment_rule_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    item_warehouse_ranged_s3_to_gcs = call_s3_to_gcs(
        "item_warehouse_ranged_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        item_warehouse_ranged_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    rp_setting_s3_to_gcs = call_s3_to_gcs(
        "rp_setting_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        rp_setting_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    rp_eligibility_s3_to_gcs = call_s3_to_gcs(
        "rp_eligibility_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        rp_eligibility_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    rp_supply_dateset_s3_to_gcs = call_s3_to_gcs(
        "rp_supply_dateset_s3_to_gcs",
        aws_conn_id,
        gcp_conn,
        s3_bucket,
        rp_supply_dateset_s3_out_path_prefix + s3bucket_date_format,
        gcs_bucket,
        dag,
    )

    # Define the GCS to BigQuery task
    rp_date_set_gcs_to_landing = dataproc_gcs_to_bq(
        "rp_date_set_gcs_to_landing",
        "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "rp_date_set_table"),
        gcp_conn,
        gcs_bucket + rp_date_set_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    inv_deployment_rule_gcs_to_landing = dataproc_gcs_to_bq(
        "inv_deployment_rule_gcs_to_landing",
        "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "inv_deployment_rule_table"),
        gcp_conn,
        gcs_bucket + inv_deployment_rule_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    # Define the GCS to BigQuery task
    item_warehouse_ranged_gcs_to_landing = dataproc_gcs_to_bq(
        "item_warehouse_ranged_gcs_to_landing",
        "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "item_warehouse_ranged_table"),
        gcp_conn,
        gcs_bucket + item_warehouse_ranged_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    rp_setting_gcs_to_landing = dataproc_gcs_to_bq(
        "rp_setting_gcs_to_landing",
        "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "rp_setting_table"),
        gcp_conn,
        gcs_bucket + rp_setting_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    rp_supply_dateset_gcs_to_landing = dataproc_gcs_to_bq(
        "rp_supply_dateset_gcs_to_landing",
        "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "rp_supply_dateset_tables"),
        gcp_conn,
        gcs_bucket + rp_supply_dateset_s3_out_path_prefix+ s3bucket_date_format,
        dag,
    )

    rp_eligibility_gcs_to_landing = dataproc_gcs_to_bq(
        "rp_eligibility_gcs_to_landing",
        "gcp_" + owner + "_REPLENISHMENT_INCREMENTAL_LOAD",
        config.get(env, "dataset_name"),
        config.get(env, "rp_eligibility_tables"),
        gcp_conn,
        gcs_bucket + rp_eligibility_s3_out_path_prefix + s3bucket_date_format,
        dag,
    )

    rp_date_set_landing_to_opr = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SKU_LOC_DATE_LDG_2_RP_SKU_LOC_DATE_OPR.sql",
        ),
        "r",
    ).read()
    rp_date_set_landing_to_opr = BigQueryInsertJobOperator(
        task_id="rp_date_set_landing_to_opr",
        configuration={
            "query": {"query": rp_date_set_landing_to_opr, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    inv_deployment_rule_landing_to_opr = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/INV_DEPLOYMENT_RULE_LDG_2_INV_DEPLOYMENT_RULE_OPR.sql",
        ),
        "r",
    ).read()
    inv_deployment_rule_landing_to_opr = BigQueryInsertJobOperator(
        task_id="inv_deployment_rule_landing_to_opr",
        configuration={
            "query": {
                "query": inv_deployment_rule_landing_to_opr,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_setting_landing_to_opr = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SETTING_LDG_2_RP_SETTING_DTL_OPR.sql",
        ),
        "r",
    ).read()
    rp_setting_landing_to_opr = BigQueryInsertJobOperator(
        task_id="rp_setting_landing_to_opr",
        configuration={
            "query": {"query": rp_setting_landing_to_opr, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_supply_dateset_landing_to_opr = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SKU_LOC_SUPPLY_DATE_LDG_2_RP_SKU_LOC_SUPPLY_DATE_OPR.sql",
        ),
        "r",
    ).read()
    rp_supply_dateset_landing_to_opr = BigQueryInsertJobOperator(
        task_id="rp_supply_dateset_landing_to_opr",
        configuration={
            "query": {"query": rp_supply_dateset_landing_to_opr, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_sku_loc_date_opr_to_rp_sku_loc_date_dim = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SKU_LOC_DATE_OPR_2_RP_SKU_LOC_DATE_DIM.sql",
        ),
        "r",
    ).read()
    rp_sku_loc_date_opr_to_rp_sku_loc_date_dim = BigQueryInsertJobOperator(
        task_id="rp_sku_loc_date_opr_to_rp_sku_loc_date_dim",
        configuration={
            "query": {
                "query": rp_sku_loc_date_opr_to_rp_sku_loc_date_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    inv_deployment_rule_opr_to_inv_deployment_rule_dim = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/INV_DEPLOYMENT_RULE_OPR_2_INV_DEPLOYMENT_RULE_DIM.sql",
        ),
        "r",
    ).read()
    inv_deployment_rule_opr_to_inv_deployment_rule_dim = BigQueryInsertJobOperator(
        task_id="inv_deployment_rule_opr_to_inv_deployment_rule_dim",
        configuration={
            "query": {
                "query": inv_deployment_rule_opr_to_inv_deployment_rule_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    item_warehouse_ranged_landing_to_item_warehouse_ranged_dim = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/ITEM_WAREHOUSE_RANGED_LDG_2_ITEM_WAREHOUSE_RANGED_DIM.sql",
        ),
        "r",
    ).read()
    item_warehouse_ranged_landing_to_item_warehouse_ranged_dim = (
        BigQueryInsertJobOperator(
            task_id="item_warehouse_ranged_landing_to_item_warehouse_ranged_dim",
            configuration={
                "query": {
                    "query": item_warehouse_ranged_landing_to_item_warehouse_ranged_dim,
                    "useLegacySql": False,
                }
            },
            project_id=project_id,
            gcp_conn_id=gcp_conn,
            params=sql_dict,
            location=region,
            dag=dag,
        )
    )

    rp_setting_dtl_opr_to_rp_setting_dtl_dim = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SETTING_DTL_OPR_2_RP_SETTING_DTL_DIM.sql",
        ),
        "r",
    ).read()
    rp_setting_dtl_opr_to_rp_setting_dtl_dim = BigQueryInsertJobOperator(
        task_id="rp_setting_dtl_opr_to_rp_setting_dtl_dim",
        configuration={
            "query": {
                "query": rp_setting_dtl_opr_to_rp_setting_dtl_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_eligibility_landing_to_rp_eligibility_dim = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_ITEM_ELIGIBILITY_LDG_2_RP_ITEM_ELIGIBILITY_DIM.sql",
        ),
        "r",
    ).read()
    rp_eligibility_landing_to_rp_eligibility_dim = BigQueryInsertJobOperator(
        task_id="rp_eligibility_landing_to_rp_eligibility_dim",
        configuration={
            "query": {
                "query": rp_eligibility_landing_to_rp_eligibility_dim,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_supply_dateset_landing_to_dimension = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_SKU_LOC_SUPPLY_DATE_OPR_2_RP_SKU_LOC_SUPPLY_DATE_DIM.sql",
        ),
        "r",
    ).read()
    rp_supply_dateset_landing_to_dimension = BigQueryInsertJobOperator(
        task_id="rp_supply_dateset_landing_to_dimension",
        configuration={
            "query": {
                "query": rp_supply_dateset_landing_to_dimension,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    # Step 8 - Task which pushes xcom value as 'Y' for key REPLENISHMENT_LOAD which
    # indicates the current replenishment load is successful
    end_load = get_batch_load_status("end_replenishment_load", subject_area_name, "END")

    # Step 9 - Dummy task if no files found by the sqs file processor task
    rp_date_set_skip_load = DummyOperator(task_id="rp_date_set_skip_load")
    inv_deployment_rule_skip_load = DummyOperator(
        task_id="inv_deployment_rule_skip_load"
    )
    item_warehouse_ranged_skip_load = DummyOperator(
        task_id="item_warehouse_ranged_skip_load"
    )
    rp_setting_skip_load = DummyOperator(task_id="rp_setting_skip_load")
    rp_eligibility_skip_load = DummyOperator(task_id="rp_eligibility_skip_load")
    rp_supply_dateset_skip_load = DummyOperator(task_id="rp_supply_dateset_skip_load")

    update_replenishment_batch_id = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_INCREMENTAL_ELT_LKP_UPDATE.sql",
        ),
        "r",
    ).read()
    update_replenishment_batch_id = BigQueryInsertJobOperator(
        task_id="update_replenishment_batch_id",
        configuration={
            "query": {"query": update_replenishment_batch_id, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    update_reserved_stock_batch_id = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RS_INCREMENTAL_ELT_LKP_UPDATE.sql",
        ),
        "r",
    ).read()
    update_reserved_stock_batch_id = BigQueryInsertJobOperator(
        task_id="update_reserved_stock_batch_id",
        configuration={
            "query": {"query": update_reserved_stock_batch_id, "useLegacySql": False}
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_incremental_changes_to_rp_indicator_dtl_ldg = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_INDICATOR_INCREMENTAL_CHANGES.sql",
        ),
        "r",
    ).read()
    rp_incremental_changes_to_rp_indicator_dtl_ldg = BigQueryInsertJobOperator(
        task_id="rp_incremental_changes_to_rp_indicator_dtl_ldg",
        configuration={
            "query": {
                "query": rp_incremental_changes_to_rp_indicator_dtl_ldg,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_indicator_dtl_ldg_to_rp_indicator_dtl_xref = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_INDICATOR_DTL_LDG_2_RP_INDICATOR_DTL_XREF.sql",
        ),
        "r",
    ).read()
    rp_indicator_dtl_ldg_to_rp_indicator_dtl_xref = BigQueryInsertJobOperator(
        task_id="rp_indicator_dtl_ldg_to_rp_indicator_dtl_xref",
        configuration={
            "query": {
                "query": rp_indicator_dtl_ldg_to_rp_indicator_dtl_xref,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    rp_indicator_dtl_xref_to_rp_indicator_xref = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RP_INDICATOR_DTL_XREF_2_RP_INDICATOR_XREF.sql",
        ),
        "r",
    ).read()
    rp_indicator_dtl_xref_to_rp_indicator_xref = BigQueryInsertJobOperator(
        task_id="rp_indicator_dtl_xref_to_rp_indicator_xref",
        configuration={
            "query": {
                "query": rp_indicator_dtl_xref_to_rp_indicator_xref,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    reserved_stock_changes_to_rp_indicator_xref = open(
        os.path.join(
            sql_path,
            "TECH_Merch_NAP_REPLENISHMENT_INCREMENTAL_LOAD/RS_CHANGES_2_RP_INDICATOR_XREF.sql",
        ),
        "r",
    ).read()
    reserved_stock_changes_to_rp_indicator_xref = BigQueryInsertJobOperator(
        task_id="reserved_stock_changes_to_rp_indicator_xref",
        configuration={
            "query": {
                "query": reserved_stock_changes_to_rp_indicator_xref,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_dict,
        location=region,
        dag=dag,
    )

    # Step 12 - Get sql results into XCOM
    get_osos_iteration = get_query_results_into_xcom(
        "rp_osos_timeline_iterations", rp_osos_iteration_sql_text, hook
    )

    # Step 0, End DAG
    rp_view_end_load = DummyOperator(task_id="rp_view_end_load", dag=dag)

    # Task dependency
    creds_setup >> creds_setup_aws >> start_load
    (start_load >> rp_date_set_sqs_processing >> rp_date_set_check_file_availability)
    (start_load >> inv_deployment_rule_sqs_processing >> inv_deployment_rule_check_file_availability)
    (start_load >>
        item_warehouse_ranged_sqs_processing
        >> item_warehouse_ranged_check_file_availability
    )
    (start_load >> rp_setting_sqs_processing >> rp_setting_check_file_availability)
    (start_load >> rp_eligibility_sqs_processing >> rp_eligibility_check_file_availability)
    (start_load >> rp_supply_dateset_sqs_processing >> rp_supply_dateset_check_file_availability)

    (
        rp_date_set_check_file_availability
        >> rp_date_set_s3_to_gcs
        >> rp_date_set_gcs_to_landing
        >> rp_date_set_landing_to_opr
        >> rp_sku_loc_date_opr_to_rp_sku_loc_date_dim
        >> get_osos_iteration
        >> pull_osos_iteration_task
        >> end_load
    )
    (
        inv_deployment_rule_check_file_availability
        >> inv_deployment_rule_s3_to_gcs
        >> inv_deployment_rule_gcs_to_landing
        >> inv_deployment_rule_landing_to_opr
        >> inv_deployment_rule_opr_to_inv_deployment_rule_dim
        >> end_load
    )
    (
        item_warehouse_ranged_check_file_availability
        >> item_warehouse_ranged_s3_to_gcs
        >> item_warehouse_ranged_gcs_to_landing
        >> item_warehouse_ranged_landing_to_item_warehouse_ranged_dim
        >> end_load
    )
    (
        rp_setting_check_file_availability
        >> rp_setting_s3_to_gcs
        >> rp_setting_gcs_to_landing
        >> rp_setting_landing_to_opr
        >> rp_setting_dtl_opr_to_rp_setting_dtl_dim
        >> end_load
    )
    (
        rp_eligibility_check_file_availability
        >> rp_eligibility_s3_to_gcs
        >> rp_eligibility_gcs_to_landing
        >> rp_eligibility_landing_to_rp_eligibility_dim
        >> end_load
    )

    rp_date_set_check_file_availability >> rp_date_set_skip_load >> end_load
    (
        inv_deployment_rule_check_file_availability
        >> inv_deployment_rule_skip_load
        >> end_load
    )
    (
        item_warehouse_ranged_check_file_availability
        >> item_warehouse_ranged_skip_load
        >> end_load
    )
    rp_setting_check_file_availability >> rp_setting_skip_load >> end_load
    rp_eligibility_check_file_availability >> rp_eligibility_skip_load >> end_load

    (
        rp_supply_dateset_check_file_availability
        >> rp_supply_dateset_s3_to_gcs
        >> rp_supply_dateset_gcs_to_landing
    )
    (
        rp_supply_dateset_gcs_to_landing
        >> rp_supply_dateset_landing_to_opr
        >> rp_supply_dateset_landing_to_dimension
        >> end_load
    )
    rp_supply_dateset_check_file_availability >> rp_supply_dateset_skip_load >> end_load

    (
        end_load
        >> update_replenishment_batch_id
        >> rp_incremental_changes_to_rp_indicator_dtl_ldg
        >> rp_indicator_dtl_ldg_to_rp_indicator_dtl_xref
        >> rp_indicator_dtl_xref_to_rp_indicator_xref
        >> update_reserved_stock_batch_id
        >> reserved_stock_changes_to_rp_indicator_xref
        >> rp_view_end_load
    )
