import os
import configparser
from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import PythonOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from nordstrom.utils.setup_module import setup_module

file_path = os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent,
                         '../common/utils/ds_product_common/__init__.py')

setup_module(module_name="ds_product_common", file_path=file_path)
from ds_product_common.BigQuery_util import (
    get_bteq_load,
    gcs_copy_files,
    get_elt_control_load)
 

env = os.environ['ENVIRONMENT']
# env = 'prod'

# Fetch the data from config file
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
config.read(os.path.join(path, '../configs/vendor_brand_object_semantic_layer.cfg'))
# logging.info(config)
# Airflow variables
owner = config.get(env, 'dag_owner')
email = config.get(env, 'dag_alert_email_addr')
spark_apps_path = config.get(env, 'spark_apps_path')
# metamorph_config_product = s3://tf-nonprod-nap-product-persistent/apps/metamorph_config/config/
metamorph_config = config.get(env, 'metamorph_config')
metamorph_class_name = config.get(env, 'metamorph_class_name')
metamorph_jar = config.get(env, 'metamorph_jar')

# metamorph_offset_backup_path = config.get(env, 'metamorph_offset_backup_path')
schedule = config.get(env, 'dag_schedule_interval')
emr_cluster_conn_id = config.get(env, 'emr_cluster_connection_id')
livy_conn_id = config.get(env, 'livy_connection_id')
teradata_environment = config.get(env, 'teradata_environment')
executor_count = int(config.get(env, 'executor_count'))
max_executor_count = int(config.get(env, 'max_executor_count'))
executor_allocation_ratio = float(config.get(env, 'executor_allocation_ratio'))
subnet_url = config.get(env, 'subnet_url')

gcp_access_config = configparser.ConfigParser()
path = os.path.split(__file__)[0]

# gcp access configs
gcp_access_config.read(
    os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent, '../common/configs/gcp_access.cfg'))
conn_id = gcp_access_config.get(env, 'gcp_conn')
nauth_conn_id = gcp_access_config.get(env, 'nauth_conn_id')
bq_service_account_email = gcp_access_config.get(env, 'bq_service_account_email')
service_account_email = gcp_access_config.get(env, 'service_account_email')
project_id = gcp_access_config.get(env, 'project_id')
source_bucket = gcp_access_config.get(env, 'source_bucket')
source_object = gcp_access_config.get(env, 'source_object')
destination_bucket = gcp_access_config.get(env, 'destination_bucket')
destination_object = gcp_access_config.get(env, 'destination_object')
location = gcp_access_config.get(env, 'location')
dataplex_project_id = gcp_access_config.get(env, 'dataplex_project_id')

# Fetch data for the store_traffic Metamorph extracts
vendor_brand_xref_config = config.get(env, 'vendor_brand_xref_config')
vendor_brand_config = config.get(env, 'vendor_brand_config')
vendor_label_config = config.get(env, 'vendor_label_config')
vendor_payto_config = config.get(env, 'vendor_payto_config')
vendor_payto_relationship_config = config.get(env, 'vendor_payto_relationship_config')
vendor_partner_config = config.get(env, 'vendor_partner_config')
vendor_orderfrom_config = config.get(env, 'vendor_orderfrom_config')
vendor_orderfrom_paymentterms_xref_config = config.get(env, 'vendor_orderfrom_paymentterms_xref_config')

# Spark Config
SPARK_TIMEOUT_SEC = int(config.get(env, 'spark_timeout_sec'))
POKE_INTERVAL_SEC = int(config.get(env, 'poke_interval_sec'))
EXECUTOR_CORES = int(config.get(env, 'executor_cores'))
EXECUTOR_MEMORY = config.get(env, 'executor_memory')

# Setting up arguments
yesterday_date = datetime.datetime.now() + timedelta(days=-1)
Dyear = str(yesterday_date.strftime("%Y"))
Dmonth = str(yesterday_date.strftime("%m"))
Dday = str(yesterday_date.strftime("%d"))
Dhour = str(yesterday_date.strftime("%H"))

DAG_NAME = 'gcp_VENDOR_BRAND_METAMORPH'
CLUSTER_TYPE = config.get(env, 'cluster_type')  # persistent cluster
CLUSTER_TEMPLATE = config.get(env, 'cluster_template')

vendor_brand_xref_subject_area = config.get(env, 'vendor_brand_xref_subject_area')
vendor_label_subject_area = config.get(env, 'vendor_label_subject_area')
vendor_brand_subject_area = config.get(env, 'vendor_brand_subject_area')
vendor_payto_subject_area = config.get(env, 'vendor_payto_subject_area')
vendor_payto_relationship_subject_area = config.get(env, 'vendor_payto_relationship_subject_area')
vendor_partner_subject_area = config.get(env, 'vendor_partner_subject_area')
vendor_orderfrom_subject_area = config.get(env, 'vendor_orderfrom_subject_area')
vendor_orderfrom_paymentterms_xref_subject_area = config.get(env, 'vendor_orderfrom_paymentterms_xref_subject_area')
vendor_orderfrom_market_subject_area = config.get(env, 'vendor_orderfrom_market_subject_area')
vendor_orderfrom_postaladdress_xref_subject_area = config.get(env, 'vendor_orderfrom_postaladdress_xref_subject_area')
vendor_relationship_orderfrom_subject_area = config.get(env, 'vendor_relationship_orderfrom_subject_area')
vendor_relationship_payto_subject_area = config.get(env, 'vendor_relationship_payto_subject_area')
vendor_relationship_partner_subject_area = config.get(env, 'vendor_relationship_partner_subject_area')
vendor_site_orderfrom_subject_area = config.get(env, 'vendor_site_orderfrom_subject_area')
vendor_site_payto_subject_area = config.get(env, 'vendor_site_payto_subject_area')
config_path = config.get(env, 'config_path')

# metamorph_config data output prefix path
metamorph_vendor_brand_xref_data_output_file_prefix_path = config.get(env,
                                                                      'metamorph_vendor_brand_xref_data_output_file_prefix_path')
metamorph_vendor_label_data_output_file_prefix_path = config.get(env,
                                                                 'metamorph_vendor_label_data_output_file_prefix_path')
metamorph_vendor_brand_data_output_file_prefix_path = config.get(env,
                                                                 'metamorph_vendor_brand_data_output_file_prefix_path')
metamorph_vendor_payto_data_output_file_prefix_path = config.get(env,
                                                                 'metamorph_vendor_payto_data_output_file_prefix_path')
metamorph_vendor_payto_relationship_data_output_file_prefix_path = config.get(env,
                                                                              'metamorph_vendor_payto_relationship_data_output_file_prefix_path')
metamorph_vendor_orderfrom_data_output_file_prefix_path = config.get(env,
                                                                     'metamorph_vendor_orderfrom_data_output_file_prefix_path')
metamorph_vendor_partner_data_output_file_prefix_path = config.get(env,
                                                                   'metamorph_vendor_partner_data_output_file_prefix_path')
metamorph_vendor_market_data_output_file_prefix_path = config.get(env,
                                                                  'metamorph_vendor_market_data_output_file_prefix_path')
metamorph_vendor_paymentterms_xref_data_output_file_prefix_path = config.get(env,
                                                                             'metamorph_vendor_paymentterms_xref_data_output_file_prefix_path')
metamorph_vendor_postaladdress_xref_data_output_file_prefix_path = config.get(env,
                                                                              'metamorph_vendor_postaladdress_xref_data_output_file_prefix_path')
metamorph_vendor_relationship_orderfrom_data_output_file_prefix_path = config.get(env,
                                                                                  'metamorph_vendor_relationship_orderfrom_data_output_file_prefix_path')
metamorph_vendor_relationship_payto_data_output_file_prefix_path = config.get(env,
                                                                              'metamorph_vendor_relationship_payto_data_output_file_prefix_path')
metamorph_vendor_relationship_partner_data_output_file_prefix_path = config.get(env,
                                                                                'metamorph_vendor_relationship_partner_data_output_file_prefix_path')
metamorph_vendor_site_orderfrom_data_output_file_prefix_path = config.get(env,
                                                                          'metamorph_vendor_site_orderfrom_data_output_file_prefix_path')
metamorph_vendor_site_payto_data_output_file_prefix_path = config.get(env,
                                                                      'metamorph_vendor_site_payto_data_output_file_prefix_path')

# metamorph_config data output prefix path with date partition
metamorph_vendor_brand_xref_data_output_file_path_with_date_partition = metamorph_vendor_brand_xref_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_label_data_output_file_path_with_date_partition = metamorph_vendor_label_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_brand_data_output_file_path_with_date_partition = metamorph_vendor_brand_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_payto_data_output_file_path_with_date_partition = metamorph_vendor_payto_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_payto_relationship_data_output_file_path_with_date_partition = metamorph_vendor_payto_relationship_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_orderfrom_data_output_file_path_with_date_partition = metamorph_vendor_orderfrom_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_partner_data_output_file_path_with_date_partition = metamorph_vendor_partner_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_market_data_output_file_path_with_date_partition = metamorph_vendor_market_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_paymentterms_xref_data_output_file_path_with_date_partition = metamorph_vendor_paymentterms_xref_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_postaladdress_xref_data_output_file_path_with_date_partition = metamorph_vendor_postaladdress_xref_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_relationship_orderfrom_data_output_file_path_with_date_partition = metamorph_vendor_relationship_orderfrom_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_relationship_payto_data_output_file_path_with_date_partition = metamorph_vendor_relationship_payto_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_relationship_partner_data_output_file_path_with_date_partition = metamorph_vendor_relationship_partner_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_site_orderfrom_data_output_file_path_with_date_partition = metamorph_vendor_site_orderfrom_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'
metamorph_vendor_site_payto_data_output_file_path_with_date_partition = metamorph_vendor_site_payto_data_output_file_prefix_path + 'year=' + Dyear + '/month=' + Dmonth + '/day=' + Dday + '/'

# Service now incident configs
sn_incident_severity = config.get(env, 'sn_incident_severity')


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
    return f'''{dag_id.lower()}--{task_id.lower()}--{date.lower()}'''.lower()


# def create_servicenow_incident(context):
#     invoke_servicenow_api(context, sn_incident_severity)


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=gcp_access_config.get(env, 'nauth_conn_id'),
        cloud_conn_id=gcp_access_config.get(env, 'gcp_conn'),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")


CURRENT_TIMESTAMP = datetime.datetime.now()



def invokeMetamorph(task_id, DAG_NAME, project_id, location,
                    metamorph_class_name,
                    config_file_name,
                    executor_cores, executor_memory, dag,
                    year=None, month=None, day=None, hour=None):
    session_name = get_batch_id(dag_id=DAG_NAME, task_id=task_id)
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=project_id,
        region=location,
        gcp_conn_id=conn_id,
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
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': email.split(','),
    # 'email_on_failure': False if env == 'development' else True,
    # 'email_on_retry': False,
    'sla': timedelta(minutes=180),
    # 'on_failure_callback': create_servicenow_incident
}
parameters = {'DBENV': teradata_environment}
# parameters = {'DBENV' : 'PRD'} 
with DAG(DAG_NAME, default_args=default_args, schedule_interval=schedule, catchup=False,
         user_defined_macros=parameters) as dag:
    
    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args = [service_account_email],
        dag=dag
    )

    backup_offset_files = gcs_copy_files('backup_offset_files', source_bucket, source_object+'vendor/', destination_bucket,
                                         destination_object+'vendor/',
                                         dag,
                                         conn_id)  # use above config variables in this function and gcs_connection_id
    # # Step 4 - Triggering the metamorph_config scripts for extracting the objects

    vendorBrandXrefMetamorphExtract = invokeMetamorph('triggerVendorBrandXrefMetamorphExtract', DAG_NAME, project_id,
                                                      location,
                                                      metamorph_class_name,
                                                      vendor_brand_xref_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
                                                      dag, Dyear, Dmonth, Dday, Dhour)

    vendorLabelMetamorphExtract = invokeMetamorph('triggerVendorLabelMetamorphExtract', DAG_NAME, project_id, location,
                                                  metamorph_class_name,
                                                  vendor_label_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
                                                  dag, Dyear, Dmonth, Dday, Dhour)

    vendorBrandMetamorphExtract = invokeMetamorph('triggerVendorBrandMetamorphExtract', DAG_NAME, project_id, location,
                                                  metamorph_class_name,
                                                  vendor_brand_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
                                                  dag, Dyear, Dmonth, Dday, Dhour)

    vendorPaytoMetamorphExtract = invokeMetamorph('triggerVendorPaytoMetamorphExtract', DAG_NAME, project_id, location,
                                                  metamorph_class_name,
                                                  vendor_payto_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
                                                  dag, Dyear, Dmonth, Dday, Dhour)

    vendorPaytoRelationshipMetamorphExtract = invokeMetamorph('triggerVendorPaytoRelationshipMetamorphExtract',
                                                              DAG_NAME, project_id, location,
                                                              metamorph_class_name,
                                                              vendor_payto_relationship_config, EXECUTOR_CORES,
                                                              EXECUTOR_MEMORY,
                                                              dag, Dyear, Dmonth, Dday, Dhour)

    vendorOrderfromMetamorphExtract = invokeMetamorph('triggerVendorOrderfromMetamorphExtract', DAG_NAME, project_id,
                                                      location,
                                                      metamorph_class_name,
                                                      vendor_orderfrom_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
                                                      dag, Dyear, Dmonth, Dday, Dhour)

    vendorPartnerMetamorphExtract = invokeMetamorph('triggerVendorPartnerMetamorphExtract', DAG_NAME, project_id,
                                                    location,
                                                    metamorph_class_name,
                                                    vendor_partner_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
                                                    dag, Dyear, Dmonth, Dday, Dhour)

    vendorOrderfromPaymenttermsXrefMetamorphExtract = invokeMetamorph(
        'triggerVendorOrderfromPaymenttermsXrefMetamorphExtract', DAG_NAME, project_id, location,
        metamorph_class_name,
        vendor_orderfrom_paymentterms_xref_config, EXECUTOR_CORES, EXECUTOR_MEMORY,
        dag, Dyear, Dmonth, Dday, Dhour)

    # # Step 6 - ETL Batch process to set the ACTIVE_LOAD_IND to Y/N
    # project_id = 'cf-nordstrom'
    vendor_brand_xref_start_load = get_elt_control_load('VENDOR_BRAND_XREF_BATCH_START', vendor_brand_xref_subject_area,
                                                        'START', dataplex_project_id, gcp_region=location,
                                                        gcp_connection=conn_id, dag=dag)
    vendor_brand_xref_end_load = get_elt_control_load('VENDOR_BRAND_XREF_BATCH_END', vendor_brand_xref_subject_area,
                                                      'END', project_id, gcp_region=location,
                                                      gcp_connection=conn_id, dag=dag)

    vendor_label_start_load = get_elt_control_load('VENDOR_LABEL_BATCH_START', vendor_label_subject_area, 'START',
                                                   dataplex_project_id, gcp_region=location,
                                                   gcp_connection=conn_id, dag=dag)
    vendor_label_end_load = get_elt_control_load('VENDOR_LABEL_BATCH_END', vendor_label_subject_area, 'END', dataplex_project_id,
                                                 gcp_region=location,
                                                 gcp_connection=conn_id, dag=dag)

    vendor_brand_start_load = get_elt_control_load('VENDOR_BRAND_BATCH_START', vendor_brand_subject_area, 'START',
                                                   dataplex_project_id, gcp_region=location,
                                                   gcp_connection=conn_id, dag=dag)
    vendor_brand_end_load = get_elt_control_load('VENDOR_BRAND_BATCH_END', vendor_brand_subject_area, 'END', dataplex_project_id,
                                                 gcp_region=location,
                                                 gcp_connection=conn_id, dag=dag)

    vendor_payto_start_load = get_elt_control_load('VENDOR_PAYTO_BATCH_START', vendor_payto_subject_area, 'START',
                                                   dataplex_project_id, gcp_region=location,
                                                   gcp_connection=conn_id, dag=dag)
    vendor_payto_end_load = get_elt_control_load('VENDOR_PAYTO_BATCH_END', vendor_payto_subject_area, 'END', project_id,
                                                 gcp_region=location,
                                                 gcp_connection=conn_id, dag=dag)

    vendor_payto_relationship_start_load = get_elt_control_load('VENDOR_PAYTO_RELATIONSHIP_BATCH_START',
                                                                vendor_payto_relationship_subject_area, 'START',
                                                                dataplex_project_id, gcp_region=location,
                                                                gcp_connection=conn_id, dag=dag)
    vendor_payto_relationship_end_load = get_elt_control_load('VENDOR_PAYTO_RELATIONSHIP_BATCH_END',
                                                              vendor_payto_relationship_subject_area, 'END', dataplex_project_id,
                                                              gcp_region=location,
                                                              gcp_connection=conn_id, dag=dag)

    vendor_orderfrom_start_load = get_elt_control_load('VENDOR_ORDERFROM_BATCH_START', vendor_orderfrom_subject_area,
                                                       'START', dataplex_project_id, gcp_region=location,
                                                       gcp_connection=conn_id, dag=dag)
    vendor_orderfrom_end_load = get_elt_control_load('VENDOR_ORDERFROM_BATCH_END', vendor_orderfrom_subject_area, 'END',
                                                     dataplex_project_id, gcp_region=location,
                                                     gcp_connection=conn_id, dag=dag)

    vendor_partner_start_load = get_elt_control_load('VENDOR_PARTNER_BATCH_START', vendor_partner_subject_area, 'START',
                                                     dataplex_project_id, gcp_region=location,
                                                     gcp_connection=conn_id, dag=dag)
    vendor_partner_end_load = get_elt_control_load('VENDOR_PARTNER_BATCH_END', vendor_partner_subject_area, 'END',
                                                   dataplex_project_id, gcp_region=location,
                                                   gcp_connection=conn_id, dag=dag)

    vendor_orderfrom_paymentterms_start_load = get_elt_control_load('VENDOR_ORDERFROM_PAYMENTTERMS_BATCH_START',
                                                                    vendor_orderfrom_paymentterms_xref_subject_area,
                                                                    'START', dataplex_project_id, gcp_region=location,
                                                                    gcp_connection=conn_id, dag=dag)
    vendor_orderfrom_paymentterms_end_load = get_elt_control_load('VENDOR_ORDERFROM_PAYMENTTERMS_BATCH_END',
                                                                  vendor_orderfrom_paymentterms_xref_subject_area,
                                                                  'END', dataplex_project_id, gcp_region=location,
                                                                  gcp_connection=conn_id, dag=dag)

    vendor_orderfrom_market_start_load = get_elt_control_load('VENDOR_ORDERFROM_MARKET_BATCH_START',
                                                              vendor_orderfrom_market_subject_area, 'START', dataplex_project_id,
                                                              gcp_region=location,
                                                              gcp_connection=conn_id, dag=dag)
    vendor_orderfrom_market_end_load = get_elt_control_load('VENDOR_ORDERFROM_MARKET_BATCH_END',
                                                            vendor_orderfrom_market_subject_area, 'END', dataplex_project_id,
                                                            gcp_region=location,
                                                            gcp_connection=conn_id, dag=dag)

    vendor_orderfrom_postaladdress_xref_start_load = get_elt_control_load('VENDOR_ORDERFROM_POSTALADDRESS_BATCH_START',
                                                                          vendor_orderfrom_postaladdress_xref_subject_area,
                                                                          'START', dataplex_project_id, gcp_region=location,
                                                                          gcp_connection=conn_id, dag=dag)
    vendor_orderfrom_postaladdress_xref_end_load = get_elt_control_load('VENDOR_ORDERFROM_POSTALADDRESS_BATCH_END',
                                                                        vendor_orderfrom_postaladdress_xref_subject_area,
                                                                        'END', dataplex_project_id, gcp_region=location,
                                                                        gcp_connection=conn_id, dag=dag)

    vendor_relationship_orderfrom_start_load = get_elt_control_load('VENDOR_RELATIONSHIP_ORDERFROM_BATCH_START',
                                                                    vendor_relationship_orderfrom_subject_area, 'START',
                                                                    dataplex_project_id, gcp_region=location,
                                                                    gcp_connection=conn_id, dag=dag)
    vendor_relationship_orderfrom_end_load = get_elt_control_load('VENDOR_RELATIONSHIP_ORDERFROM_BATCH_END',
                                                                  vendor_relationship_orderfrom_subject_area, 'END',
                                                                  dataplex_project_id, gcp_region=location,
                                                                  gcp_connection=conn_id, dag=dag)

    vendor_relationship_payto_start_load = get_elt_control_load('VENDOR_RELATIONSHIP_PAYTO_BATCH_START',
                                                                vendor_relationship_payto_subject_area, 'START',
                                                                dataplex_project_id, gcp_region=location,
                                                                gcp_connection=conn_id, dag=dag)
    vendor_relationship_payto_end_load = get_elt_control_load('VENDOR_RELATIONSHIP_PAYTO_BATCH_END',
                                                              vendor_relationship_payto_subject_area, 'END', dataplex_project_id,
                                                              gcp_region=location,
                                                              gcp_connection=conn_id, dag=dag)

    vendor_relationship_partner_start_load = get_elt_control_load('VENDOR_RELATIONSHIP_PARTNER_BATCH_START',
                                                                  vendor_relationship_partner_subject_area, 'START',
                                                                  dataplex_project_id, gcp_region=location,
                                                                  gcp_connection=conn_id, dag=dag)
    vendor_relationship_partner_end_load = get_elt_control_load('VENDOR_RELATIONSHIP_PARTNER_BATCH_END',
                                                                vendor_relationship_partner_subject_area, 'END',
                                                                dataplex_project_id, gcp_region=location,
                                                                gcp_connection=conn_id, dag=dag)

    vendor_site_orderfrom_start_load = get_elt_control_load('VENDOR_SITE_ORDERFROM_BATCH_START',
                                                            vendor_site_orderfrom_subject_area, 'START', dataplex_project_id,
                                                            gcp_region=location,
                                                            gcp_connection=conn_id, dag=dag)
    vendor_site_orderfrom_end_load = get_elt_control_load('VENDOR_SITE_ORDERFROM_BATCH_END',
                                                          vendor_site_orderfrom_subject_area, 'END', dataplex_project_id,
                                                          gcp_region=location,
                                                          gcp_connection=conn_id, dag=dag)

    vendor_site_payto_start_load = get_elt_control_load('VENDOR_SITE_PAYTO_BATCH_START',
                                                        vendor_site_payto_subject_area, 'START', dataplex_project_id,
                                                        gcp_region=location,
                                                        gcp_connection=conn_id, dag=dag)
    vendor_site_payto_end_load = get_elt_control_load('VENDOR_SITE_PAYTO_BATCH_END',
                                                      vendor_site_payto_subject_area, 'END', dataplex_project_id,
                                                      gcp_region=location,
                                                      gcp_connection=conn_id, dag=dag)

    start_vendor_load = DummyOperator(task_id='START_VENDOR_LOAD', dag=dag)

    # Step 11 - TD Script for stage to dimension table load
    load_vendor_brand_xref_dimension = get_bteq_load('vendor_brand_xref_dimension_table',
                                                     "sql/VENDOR_BRAND_METAMORPH/VENDOR_BRAND_XREF_LDG_2_VENDOR_BRAND_XREF.sql",
                                                     dag, conn_id)  # add gcp_conn_id at the time of delivery
    load_vendor_label_dimension = get_bteq_load('load_vendor_label_dimension',
                                                "sql/VENDOR_BRAND_METAMORPH/VENDOR_LABEL_DIM_LDG_2_VENDOR_LABEL_DIM.sql", dag, conn_id)
    load_vendor_brand_dimension = get_bteq_load('vendor_brand_dimension_table',
                                                "sql/VENDOR_BRAND_METAMORPH/VENDOR_BRAND_DIM_LDG_2_VENDOR_BRAND_DIM.sql", dag, conn_id)
    load_vendor_payto_dimension = get_bteq_load('vendor_patyo_dimension_table',
                                                "sql/VENDOR_BRAND_METAMORPH/VENDOR_PAYTO_LDG_2_VENDOR_DIM.sql", dag, conn_id)
    load_vendor_payto_relationship_dimension = get_bteq_load('vendor_patyo_relationship_dimension_table',
                                                             "sql/VENDOR_BRAND_METAMORPH/VENDOR_PAYTO_RELATIONSHIP_LDG_2_VENDOR_RELATIONSHIP_DIM.sql",
                                                             dag, conn_id)
    load_vendor_orderfrom_dimension = get_bteq_load('vendor_orderfrom_dimension_table',
                                                    "sql/VENDOR_BRAND_METAMORPH/VENDOR_ORDER_FROM_LDG_2_VENDOR_DIM.sql", dag, conn_id)
    load_vendor_partner_dimension = get_bteq_load('vendor_partner_dimension_table',
                                                  "sql/VENDOR_BRAND_METAMORPH/VENDOR_PARTNER_LDG_2_VENDOR_DIM.sql", dag, conn_id)
    load_vendor_orderfrom_market_dimension = get_bteq_load('load_vendor_orderfrom_market_dimension',
                                                           "sql/VENDOR_BRAND_METAMORPH/VENDOR_ORDERFROM_MARKET_LDG_2_VENDOR_ORDERFROM_MARKET_DIM.sql",
                                                           dag, conn_id)
    load_vendor_orderfrom_paymentterms_xref_dimension = get_bteq_load(
        'load_vendor_orderfrom_paymentterms_xref_dimension',
        "sql/VENDOR_BRAND_METAMORPH/VENDOR_ORDERFROM_PAYMENTTERMS_XREF_LDG_2_VENDOR_ORDERFROM_PAYMENTTERMS_XREF.sql", dag, conn_id)
    load_vendor_orderfrom_postaladdress_xref_dimension = get_bteq_load(
        'load_vendor_orderfrom_postaladdress_xref_dimension',
        "sql/VENDOR_BRAND_METAMORPH/VENDOR_ORDERFROM_POSTALADDRESS_XREF_LDG_2_VENDOR_ORDERFROM_POSTALADDRESS_XREF.sql", dag, conn_id)
    load_vendor_relationship_order_from_dimension = get_bteq_load('load_vendor_relationship_order_from_dimension',
                                                                  "sql/VENDOR_BRAND_METAMORPH/VENDOR_RELATIONSHIP_ORDER_FROM_LDG_2_VENDOR_RELATIONSHIP_DIM.sql",
                                                                  dag, conn_id)
    load_vendor_relationship_pay_to_dimension = get_bteq_load('load_vendor_relationship_pay_to_dimension',
                                                              "sql/VENDOR_BRAND_METAMORPH/VENDOR_RELATIONSHIP_PAY_TO_LDG_2_VENDOR_RELATIONSHIP_DIM.sql",
                                                              dag, conn_id)
    load_vendor_relationship_partner_dimension = get_bteq_load('load_vendor_relationship_patrtner_dimension',
                                                               "sql/VENDOR_BRAND_METAMORPH/VENDOR_RELATIONSHIP_PARTNER_LDG_2_VENDOR_RELATIONSHIP_DIM.sql",
                                                               dag, conn_id)
    load_vendor_site_order_from_dimension = get_bteq_load('load_vendor_site_order_from_dimension',
                                                          "sql/VENDOR_BRAND_METAMORPH/VENDOR_SITE_ORDER_FROM_LDG_2_VENDOR_SITE_DIM.sql", dag,
                                                          conn_id)
    load_vendor_site_pay_to_dimension = get_bteq_load('load_vendor_site_pay_to_dimension',
                                                      "sql/VENDOR_BRAND_METAMORPH/VENDOR_SITE_PAY_TO_LDG_2_VENDOR_SITE_DIM.sql", dag, conn_id)

    # Step 13 - Dummy tasks to mark the completion of Teradata landing and dimension table load
    end_vendor_load = DummyOperator(task_id='END_VENDOR_LOAD', dag=dag)

creds_setup >> start_vendor_load >> backup_offset_files >> vendorBrandXrefMetamorphExtract

vendorBrandXrefMetamorphExtract >> vendor_brand_xref_start_load >> load_vendor_brand_xref_dimension >> vendor_brand_xref_end_load

start_vendor_load >> backup_offset_files >> vendorLabelMetamorphExtract

vendorLabelMetamorphExtract >> vendor_label_start_load >> load_vendor_label_dimension >> vendor_label_end_load

start_vendor_load >> backup_offset_files >> vendorBrandMetamorphExtract

vendorBrandMetamorphExtract >> vendor_brand_start_load >> load_vendor_brand_dimension >> vendor_brand_end_load

start_vendor_load >> backup_offset_files >> vendorPaytoMetamorphExtract

vendorPaytoMetamorphExtract >> vendor_payto_start_load >> load_vendor_payto_dimension >> vendor_payto_end_load

vendorPaytoMetamorphExtract >> vendor_relationship_payto_start_load >> load_vendor_relationship_pay_to_dimension >> vendor_relationship_payto_end_load

vendorPaytoMetamorphExtract >> vendor_site_payto_start_load >> load_vendor_site_pay_to_dimension >> vendor_site_payto_end_load

start_vendor_load >> backup_offset_files >> vendorPaytoRelationshipMetamorphExtract

vendorPaytoRelationshipMetamorphExtract >> vendor_payto_relationship_start_load >> load_vendor_payto_relationship_dimension >> vendor_payto_relationship_end_load

start_vendor_load >> backup_offset_files >> vendorOrderfromMetamorphExtract

vendorOrderfromMetamorphExtract >> vendor_orderfrom_start_load >> load_vendor_orderfrom_dimension >> vendor_orderfrom_end_load

vendorOrderfromMetamorphExtract >> vendor_orderfrom_market_start_load >> load_vendor_orderfrom_market_dimension >> vendor_orderfrom_market_end_load

vendorOrderfromMetamorphExtract >> vendor_orderfrom_postaladdress_xref_start_load >> load_vendor_orderfrom_postaladdress_xref_dimension >> vendor_orderfrom_postaladdress_xref_end_load

vendorOrderfromMetamorphExtract >> vendor_relationship_orderfrom_start_load >> load_vendor_relationship_order_from_dimension >> vendor_relationship_orderfrom_end_load

vendorOrderfromMetamorphExtract >> vendor_site_orderfrom_start_load >> load_vendor_site_order_from_dimension >> vendor_site_orderfrom_end_load

start_vendor_load >> backup_offset_files >> vendorPartnerMetamorphExtract

vendorPartnerMetamorphExtract >> vendor_partner_start_load >> load_vendor_partner_dimension >> vendor_partner_end_load

vendorPartnerMetamorphExtract >> vendor_relationship_partner_start_load >> load_vendor_relationship_partner_dimension >> vendor_relationship_partner_end_load

start_vendor_load >> backup_offset_files >> vendorOrderfromPaymenttermsXrefMetamorphExtract

vendorOrderfromPaymenttermsXrefMetamorphExtract >> vendor_orderfrom_paymentterms_start_load >> load_vendor_orderfrom_paymentterms_xref_dimension >> vendor_orderfrom_paymentterms_end_load

(vendor_label_end_load, vendor_brand_end_load, vendor_brand_xref_end_load, vendor_site_payto_end_load,
 vendor_orderfrom_end_load, vendor_orderfrom_market_end_load, vendor_orderfrom_postaladdress_xref_end_load,
 vendor_relationship_orderfrom_end_load, vendor_site_orderfrom_end_load, vendor_partner_end_load,
 vendor_relationship_partner_end_load, vendor_orderfrom_paymentterms_end_load, vendor_payto_relationship_end_load,
 vendor_payto_end_load, vendor_relationship_payto_end_load) >> end_vendor_load
