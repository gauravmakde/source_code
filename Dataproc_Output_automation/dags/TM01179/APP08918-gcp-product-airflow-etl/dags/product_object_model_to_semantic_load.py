import os
import configparser
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from nordstrom.utils.setup_module import setup_module
from pathlib import Path

file_path = os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent,
                         'common/utils/ds_product_common/__init__.py')

setup_module(module_name="ds_product_common", file_path=file_path)

from ds_product_common.BigQuery_util import (
    get_product_elt_control_load)

from ds_product_common.BigQuery_util import gcs_copy_files
# from ds_product_common.servicenow_util import invoke_servicenow_api
from nordstrom.utils.cloud_creds import cloud_creds
import logging

# env = 'prod'
env = os.environ['ENVIRONMENT']
abspath = os.path.dirname(os.path.abspath(__file__))

# Fetch the data from config files
config = configparser.ConfigParser()
gcp_access_config = configparser.ConfigParser()
path = os.path.split(__file__)[0]

#gcp access configs
gcp_access_config.read(os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent, 'common/configs/gcp_access.cfg'))
gcp_conn = gcp_access_config.get(env, 'gcp_conn')
nauth_conn_id = gcp_access_config.get(env, 'nauth_conn_id')
bq_service_account_email = gcp_access_config.get(env, 'bq_service_account_email')
service_account_email = gcp_access_config.get(env, 'service_account_email')
project_id = gcp_access_config.get(env, 'project_id')
location = gcp_access_config.get(env, 'location')
dataplex_project_id = gcp_access_config.get(env, 'dataplex_project_id')
source_bucket = gcp_access_config.get(env, 'source_bucket')
source_object = gcp_access_config.get(env, 'source_object')
destination_bucket = gcp_access_config.get(env, 'destination_bucket')
destination_object = gcp_access_config.get(env, 'destination_object')

config.read(os.path.join(path, 'configs/product_object_model_to_semantic_load.cfg'))
source_bucket = gcp_access_config.get(env, 'source_bucket')
source_object = gcp_access_config.get(env, 'source_object')
destination_bucket = gcp_access_config.get(env, 'destination_bucket')
destination_object = gcp_access_config.get(env, 'destination_object')

owner = config.get(env, 'dag_owner')
email = config.get(env, 'dag_alert_email_addr')
spark_apps_path = config.get(env, 'spark_apps_path')
metamorph_base_path = config.get(env, 'metamorph_base_path')
metamorph_config = metamorph_base_path + 'config/'
metamorph_class_name = config.get(env, 'metamorph_class_name')
metamorph_jar = config.get(env, 'metamorph_jar')
schedule = config.get(env, 'dag_schedule_interval')
emr_cluster_conn_id = config.get(env, 'emr_cluster_connection_id')
livy_conn_id = config.get(env, 'livy_connection_id')
retries = config.getint(env, 'retries')
retry_delay = config.getint(env, 'retry_delay')
avro_base_path = config.get(env, 'avro_base_path')
teradata_environment = config.get(env, 'teradata_environment')
subnet_url = config.get(env, 'subnet_url')
config_path = config.get(env, 'config_path')

parameters = {'dbenv': teradata_environment, 'dataplex_project_id': dataplex_project_id}

# Spark Config
spark_timeout_sec = int(config.get(env, 'spark_timeout_sec'))
poke_interval_sec = int(config.get(env, 'poke_interval_sec'))
executor_cores = int(config.get(env, 'executor_cores'))
executor_memory = config.get(env, 'executor_memory')
executor_count = int(config.get(env, 'executor_count'))
max_executor_count = int(config.get(env, 'max_executor_count'))
executor_allocation_ratio = float(config.get(env, 'executor_allocation_ratio'))

# Teradata configs
# s3_sql_path = config.get(env, 'sql_path')
# ec2_sql_path = config.get(env, 'ec2_sql_path') + 'product_object_model/'
subject_area_name = config.get(env, 'subject_area_name')

yesterday_date = datetime.datetime.now() + timedelta(days=-1)
Dyear = str(yesterday_date.strftime("%Y"))
Dmonth = str(yesterday_date.strftime("%m"))
Dday = str(yesterday_date.strftime("%d"))
Dhour = str(yesterday_date.strftime("%H"))

DAG_NAME = 'gcp_PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1'
CLUSTER_TYPE = config.get(env, 'cluster_type')  # persistent cluster
CLUSTER_TEMPLATE = config.get(env, 'cluster_template')

# Service now incident configs
sn_incident_severity = config.get(env, 'sn_incident_severity')

# metamorph_config paths
metamorph_offset_path = config.get(env, 'metamorph_offset_path')
metamorph_offset_backup_path = config.get(env, 'metamorph_offset_backup_path')
metamorph_out_path = config.get(env, 'metamorph_out_path')
metamorph_data_path = 's3://' + metamorph_out_path + '/data/'
curr_day_path = '/year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/hour=' + Dhour + '/'
metamorph_data_suffix = '/incremental' + curr_day_path
metamorph_manifest_path = metamorph_base_path + 'manifest/'


def get_batch_id(dag_id, task_id):
    current_datetime = datetime.datetime.today()

    if len(dag_id) <= 22:
        dag_id = dag_id.replace('_', '-').rstrip('-')
        ln_task_id = 45 - len(dag_id)
        task_id = task_id[-ln_task_id:].replace('_', '-').strip('-')
    elif len(task_id) <= 22:
        task_id = task_id.replace('_', '-').strip('-')
        ln_dag_id = 45 - len(task_id)
        dag_id = dag_id[:ln_dag_id].replace('_', '-').rstrip('-')
    else:
        dag_id = dag_id[:23].replace('_', '-').rstrip('-')
        task_id = task_id[-22:].replace('_', '-').strip('-')

    date = current_datetime.strftime('%Y%m%d%H%M%S')
    return f'''{dag_id.lower()}--{task_id.lower()}--{date.lower()}'''


# def create_servicenow_incident(context):
#     """
#        This function triggers servicenow api
#          Parameters : context, severity
#          Returns    : operator
#     """
#     invoke_servicenow_api(context, sn_incident_severity)


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=gcp_access_config.get(env, 'nauth_conn_id'),
        cloud_conn_id=gcp_access_config.get(env, 'gcp_conn'),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")




def invokeMetamorph(task_id, DAG_NAME, project_id, location,
                    metamorph_class_name,
                    config_path,executor_memory,
                    executor_cores,config_file_name,  dag,
                    year=None, month=None, day=None, hour=None):
    session_name = get_batch_id(dag_id=DAG_NAME, task_id=task_id)
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=project_id,
        region=location,
        gcp_conn_id=gcp_conn,
        batch={
            "runtime_config": {"version": "1.1",
                               "properties": {"spark.executor.memory": "{}".format(executor_memory),
                                              "spark.executor.cores": "{}".format(executor_cores),
                                              'spark.hadoop.fs.s3a.fast.upload': 'true',
                                              'spark.dynamicAllocation.enabled': 'true',
                                              'spark.coalesce.trigger': 'true',
                                              'spark.sql.caseSensitive': 'false',
                                              'spark.driver.extraJavaOptions': f'-Dyear={year} -Dmonth={month} -Dday={day} -Dhour={hour}',
                                              "spark.sql.avro.datetimeRebaseModeInRead": "LEGACY",
                                              "spark.bigquery.viewsEnabled": "true"
                                              }
                               },
            "spark_batch": {
                "main_class": f'{metamorph_class_name}',  # app_class_name,
                "jar_file_uris": [metamorph_jar],
                "file_uris": [f'{config_path}/{config_file_name}.json', f'{config_path}/{config_file_name}.conf'],
                "args": [
                    f'{config_file_name}.conf',
                    ]
            },
            "environment_config": {
                "execution_config": {
                    "service_account": bq_service_account_email,
                    "subnetwork_uri": subnet_url,
                },
            }
        },
        batch_id=session_name,
        dag=dag)


default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 3, 1),
    # 'email': email.split(','),
    # 'email_on_failure': False if env == 'development' else True,
    'email_on_retry': False,
    'sla': timedelta(minutes=210),
    # 'on_failure_callback': create_servicenow_incident
}

with DAG(DAG_NAME, default_args=default_args, schedule_interval=None, catchup=False,
         user_defined_macros=parameters) as dag:
    
    
    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args = [service_account_email],
        dag=dag
    )

    backup_offset_files = gcs_copy_files("backup_offset_files", source_bucket, source_object+'product/', destination_bucket,
                                         destination_object+'product/', gcp_conn_id=gcp_conn, dag=dag)

    load_product_choice_dimension = BigQueryInsertJobOperator(
        task_id="product_choice_dimension_table",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_CHOICE_LDG_2_PRODUCT_CHOICE_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_style_dimension = BigQueryInsertJobOperator(
        task_id="product_style_dimension_table",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_STYLE_LDG_2_PRODUCT_STYLE_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_sku_dimension = BigQueryInsertJobOperator(
        task_id="product_sku_dimension_table",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_SKU_LDG_2_PRODUCT_SKU_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    selling_control_start_batch = BigQueryInsertJobOperator(
        task_id="selling_control_start_batch",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_ITEM_SELLING_RIGHTS_ELT_CTL_START.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    product_selling_control_xref_to_sku_dim_pre_load = BigQueryInsertJobOperator(
        task_id="product_selling_control_xref_to_sku_dim_pre_load",
        configuration={
            "query": {
                "query": open(
                    os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_ITEM_SELLING_RIGHTS_XREF_2_PRODUCT_SKU_DIM_PRE_LOAD.sql'),
                    "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    product_selling_control_xref_to_sku_dim_post_load = BigQueryInsertJobOperator(
        task_id="product_selling_control_xref_to_sku_dim_post_load",
        configuration={
            "query": {
                "query": open(
                    os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_ITEM_SELLING_RIGHTS_XREF_2_PRODUCT_SKU_DIM_POST_LOAD.sql'),
                    "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    selling_control_end_batch = BigQueryInsertJobOperator(
        task_id="selling_control_end_batch",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_ITEM_SELLING_RIGHTS_ELT_CTL_END.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_sku_reprocessing_sync = BigQueryInsertJobOperator(
        task_id="product_sku_reprocessing_sync",
        configuration={
            "query": {
                "query": open(
                    os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_SKU_REPROCESSING.sql'),
                    "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_upc_dimension = BigQueryInsertJobOperator(
        task_id="product_upc_dimension_table",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_UPC_DIM_LDG_2_PRODUCT_UPC_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_casepack_sku_dimension = BigQueryInsertJobOperator(
        task_id="load_product_casepack_sku_dimension",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/CASEPACK_SKU_LDG_2_PRODUCT_SKU_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_casepack_upc_dimension = BigQueryInsertJobOperator(
        task_id="load_product_casepack_upc_dimension",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/CASEPACK_UPC_LDG_2_PRODUCT_UPC_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_casepack_style_dimension = BigQueryInsertJobOperator(
        task_id="load_product_casepack_style_dimension",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/CASEPACK_STYLE_LDG_2_PRODUCT_STYLE_DIM.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_product_casepack_sku_xref_dimension = BigQueryInsertJobOperator(
        task_id="load_product_casepack_sku_xref_dimension",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/CASEPACK_SKU_XREF_LDG_2_CASEPACK_SKU_XREF.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    dq_timeliness_start = BigQueryInsertJobOperator(
        task_id="dq_timeliness_start",
        configuration={
            "query": {
                "query": open(
                    os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_NPS_TIMELINESS_START.sql'),
                    "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    dq_timeliness_end = BigQueryInsertJobOperator(
        task_id="dq_timeliness_end",
        configuration={
            "query": {
                "query": open(
                    os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_NPS_TIMELINESS_END.sql'),
                    "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    product_epm_rms_style_xref_load = BigQueryInsertJobOperator(
        task_id="product_epm_rms_style_xref_load",
        configuration={
            "query": {
                "query": open(os.path.join(abspath,
                                           '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_STYLE_RMS_DIM_VW_2_PRODUCT_EPM_RMS_STYLE_XREF.sql'),
                              "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag,
    )

    load_start_product = get_product_elt_control_load("START_PRODUCT_LOAD", subject_area_name, 'START',
                                                      gcp_region=location, gcp_connection=gcp_conn,
                                                      gcp_project_id=project_id)

    load_end_product = get_product_elt_control_load("END_PRODUCT_LOAD", subject_area_name, 'END', gcp_region=location,
                                                    gcp_connection=gcp_conn,
                                                    gcp_project_id=project_id)

    ProductChoiceMetamorphExtract = invokeMetamorph('triggerProductChoiceMetamorphExtract', DAG_NAME, project_id, location,
                                                    metamorph_class_name, config_path, executor_memory, executor_cores,
                                                    'product-choice', dag,
                                                    Dyear, Dmonth, Dday, Dhour)

    ProductStyleMetamorphExtract = invokeMetamorph('triggerProductStyleMetamorphExtract', DAG_NAME, project_id, location,
                                                   metamorph_class_name, config_path, executor_memory, executor_cores,
                                                   'product-style', dag,
                                                   Dyear, Dmonth, Dday, Dhour)

    ProductSkuMetamorphExtract = invokeMetamorph('triggerProductSkuMetamorphExtract', DAG_NAME, project_id, location,
                                                 metamorph_class_name, config_path, executor_memory, executor_cores,
                                                 'product-sku-upc', dag,
                                                 Dyear, Dmonth, Dday, Dhour)

    productCasePackMetamorphExtract = invokeMetamorph('triggerProductCasePackMetamorphExtract', DAG_NAME, project_id, location,
                                                      metamorph_class_name, config_path, executor_memory,
                                                      executor_cores, 'product-casepack-v2', dag,
                                                      Dyear, Dmonth, Dday, Dhour)
    



    product_dimension_table_load_complete = DummyOperator(task_id="product_dimension_table_load_complete",
                                                          trigger_rule="none_failed", dag=dag)

    product_sku_lkup_load = BigQueryInsertJobOperator(
        task_id="product_sku_lkup_load",
        configuration={
            "query": {
                "query": open(os.path.join(abspath, '../sql/PRODUCT_OBJECT_MODEL_TO_SEMANTIC_LOAD_V1/PRODUCT_SKU_DIM_2_PRODUCT_SKU_LKUP.sql'), "r").read(),
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn,
        dag=dag
    )


    creds_setup >> load_start_product
    load_start_product >> ProductChoiceMetamorphExtract >> dq_timeliness_start
    load_start_product >> ProductStyleMetamorphExtract >> dq_timeliness_start
    load_start_product >> ProductSkuMetamorphExtract >> dq_timeliness_start
    load_start_product >> productCasePackMetamorphExtract >> dq_timeliness_start

    dq_timeliness_start >> load_product_choice_dimension >> selling_control_start_batch
    selling_control_start_batch >> product_selling_control_xref_to_sku_dim_pre_load >> load_product_sku_dimension >> product_selling_control_xref_to_sku_dim_post_load >> selling_control_end_batch
    selling_control_end_batch >> load_product_sku_reprocessing_sync >> product_dimension_table_load_complete
    # dq_timeliness_start >> load_product_upc_dimension >> product_dimension_table_load_complete
    dq_timeliness_start >> load_product_casepack_style_dimension >> load_product_style_dimension >> selling_control_start_batch
    load_product_sku_reprocessing_sync >> product_sku_lkup_load >> product_epm_rms_style_xref_load >> product_dimension_table_load_complete
    dq_timeliness_start >> load_product_casepack_upc_dimension >> load_product_upc_dimension >> product_dimension_table_load_complete
    dq_timeliness_start >> load_product_casepack_sku_dimension >> product_dimension_table_load_complete
    # dq_timeliness_start >> load_product_casepack_style_dimension >> product_dimension_table_load_complete
    dq_timeliness_start >> load_product_casepack_sku_xref_dimension >> product_dimension_table_load_complete
   
    product_dimension_table_load_complete >> dq_timeliness_end >> load_end_product >> backup_offset_files
