# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        639d12b3fbee02e40d033c255408322336422583
# CI pipeline URL:      https://git.jwn.app/TM00989/APP04216-gcp-das-marketing-insights/-/pipelines/7431246
# CI commit timestamp:  2024-11-05T17:33:47+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os
import configparser
import yaml
from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from nordstrom.operators.operator import LivyOperator
from nordstrom.sensors.sensor import LivySensor

from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP04216-gcp-das-marketing-insights')[0] + 'APP04216-gcp-das-marketing-insights/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_loyalty_member_consistency_check_6761_DAS_MARKETING_das_marketing_insights'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = config.get('framework_setup', 'airflow_newrelic_api_key_name')

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican
from modules.metrics.main_common import main
                

sys.path.append(path + '/../')
from modules.utils.src.branching import loyalty_member_missing_ids_notification_branching
from airflow.operators.python import BranchPythonOperator

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(60)) if int(60) > 1 else None

delta_core_jar = config.get('dataproc', 'delta_core_jar')
etl_jar_version = config.get('dataproc', 'etl_jar_version')
metastore_service_path = config.get('dataproc', 'metastore_service_path')
spark_jar_path = config.get('dataproc', 'spark_jar_path')
user_config_path = config.get('dataproc', 'user_config_path')

def get_batch_id(dag_id, task_id):   
    
    current_datetime = datetime.today()       
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
    return f'''{dag_id}--{task_id}--{date}'''
    
def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id = config.get('dag', 'nauth_conn_id'),
        cloud_conn_id = config.get('dag', 'gcp_conn'),
        service_account_email = service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def pelican_check():
    return Pelican.validate(dag_name=dag_id, env=airflow_environment)

class PelicanJobSensor(BaseSensorOperator):
    def poke(self, context):
        return Pelican.check_job_status(context, dag_name=dag.dag_id, env=airflow_environment)

def toggle_pelican_validator(pelican_flag,dag):
    if pelican_flag == True:
        pelican_sensor_task = PelicanJobSensor(
            task_id='pelican_job_sensor',
            poke_interval=300,  # check every 5 minutes
            timeout=4500,  # timeout after 75 minutes
            mode='poke'
        )

        return pelican_sensor_task
    else:
        dummy_sensor = DummyOperator(task_id="dummy_sensor",dag=dag)

        return dummy_sensor

def toggle_pelican_sensor(pelican_flag,dag):
    if pelican_flag == True:
        pelican_validation = PythonOperator(
            task_id="pelican_validation",
            python_callable=pelican_check,
            dag=dag
        )

        return pelican_validation
    else:
        dummy_validation = DummyOperator(task_id="dummy_validation",dag=dag)
        
        return dummy_validation    
              
default_args = {
        
    'retries': 0 if config_env == 'development' else 3,
    'description': 'loyalty_member_consistency_check_6761_DAS_MARKETING_das_marketing_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}

with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval = cron,
        max_active_runs=set_max_active_runs,
        template_searchpath=[sql_path],
        concurrency=set_concurrency,
         ) as dag:

    project_id = config.get('dag', 'gcp_project_id')
    service_account_email = config.get('dag', 'service_account_email')
    region = config.get('dag', 'region')
    gcp_conn = config.get('dag', 'gcp_conn')
    subnet_url = config.get('dag', 'subnet_url')
    pelican_flag = config.getboolean('dag', 'pelican_flag')
    env = os.getenv('ENVIRONMENT')

    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args = [service_account_email],
    )  
    
    pelican_validator = toggle_pelican_validator(pelican_flag,dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag,dag)

    #Please add the consumer_group_name,topic_name from kafka json file and remove if kafka is not present 
    fetch_batch_run_id_task = FetchKafkaBatchRunIdOperator(
        task_id = 'fetch_batch_run_id_task',
        gcp_project_id = project_id,
        gcp_connection_id = gcp_conn,
        gcp_region = region,
        topic_name = 'humanresources-job-changed-avro',
        consumer_group_name = 'humanresources-job-changed-avro_hr_job_events_load_2656_napstore_insights_hr_job_events_load_0_stg_table',
        offsets_table = f"`{dataplex_project_id}.onehop_etl_app_db.kafka_consumer_offset_batch_details`",
        source_table = f"`{dataplex_project_id}.onehop_etl_app_db.source_kafka_consumer_offset_batch_details`",
        default_latest_run_id = int((datetime.today() - timedelta(days=1)).strftime('%Y%m%d000000')),
        dag = dag
    )

session_name=get_batch_id(dag_id=dag_id,task_id="refresh_om_table_attach_partitions")   
    refresh_om_table_attach_partitions = DataprocCreateBatchOperator(
            task_id = "refresh_om_table_attach_partitions",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.sql.autoBroadcastJoinThreshold': '-1', 'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_loyalty_member_consistency_check_6761_DAS_MARKETING_das_marketing_insights_refresh_om_table_attach_partitions.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="refresh_dlq_table_attach_partitions")   
    refresh_dlq_table_attach_partitions = DataprocCreateBatchOperator(
            task_id = "refresh_dlq_table_attach_partitions",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.sql.autoBroadcastJoinThreshold': '-1', 'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_loyalty_member_consistency_check_6761_DAS_MARKETING_das_marketing_insights_refresh_dlq_table_attach_partitions.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="compare_om_and_dlq_tables_job")   
    compare_om_and_dlq_tables_job = DataprocCreateBatchOperator(
            task_id = "compare_om_and_dlq_tables_job",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.sql.autoBroadcastJoinThreshold': '-1', 'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_loyalty_member_consistency_check_6761_DAS_MARKETING_das_marketing_insights_compare_om_and_dlq_tables_job.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

check_missing_ids_presence_branching_operator = BranchPythonOperator(
    task_id='check_missing_ids_presence_branching',
    python_callable=loyalty_member_missing_ids_notification_branching.check_missing_ids_presence)

    launch_process_notification_copy_list_of_ids_to_incremental_folder = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='k8s_connection',
    namespace='app04216',
    task_id='process_notification_copy_list_of_ids_to_incremental_folder',
    job_name='process-notification-copy-list-of-ids-to-incremental-folder-loy',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app04216/tm00989-app04216-das-marketing-insights/prod/python/utils:1.3.22',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python/utils/src/bulk_s3_copy.py', '--s3_source_bucket_env_name', 'S3_MKTG_BUCKET', '--s3_target_bucket_env_name', 'S3_MKTG_BUCKET', '--source_to_target_path_pair', "{'source': 'missing_ids/loyalty_member/missing_create_ids/', 'target': 'missing_ids/loyalty_member/incremental/year=<year>/month=<month>/day=<day>/'}"],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'S3_MKTG_BUCKET', 'value': 'tf-das-marketing-prod-teradata-stage'}, {'name': 'S3_MMM_BUCKET', 'value': 'prod-mmm-marketing-mix-model'}, {'name': 'S3_OPTIMINE_BUCKET', 'value': 'prod-mft-optimine'}, {'name': 'S3_ICON_LOUNGE_BUCKET', 'value': 'prod-icon-lounge'}, {'name': 'ENV', 'value': 'production'}, {'name': 'TD_ENV', 'value': 'PRD'}, {'name': 'TD_HOST', 'value': 'tdnapprod.nordstrom.net'}, {'name': 'TD_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/teradata/teradta_db_credentials'}, {'name': 'NR_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/newrelic/newrelic_ingest_key'}, {'name': 'MAIL_SA_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/service_account/sa_credentials'}, {'name': 'VAULT_PATH', 'value': 'vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::699296113972:role/k8s/tf-k8s-controlled-role'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_MARKETING', 'customer_project_name': 'das_marketing_insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='app04216-das-mktg-sa',
    nsk_cluster='nsk-curry-prod')

launch_process_notification_send_notification_to_slack = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='k8s_connection',
    namespace='app04216',
    task_id='process_notification_send_notification_to_slack',
    job_name='process-notification-send-notification-to-slack-loyalty-member',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app04216/tm00989-app04216-das-marketing-insights/prod/python/utils:1.3.18',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python/utils/src/send_email.py', '--vault_host_env_var', 'VAULT_PATH', '--vault_port_env_var', 'VAULT_PORT', '--k8s_vault_role_id_env_var', 'VAULT_ROLE_ID', '--k8s_vault_secret_id_env_var', 'VAULT_SECRET', '--vault_email_key_env_var', 'MAIL_SA_VAULT_PATH', '--vault_email_value_user_key', 'user', '--vault_email_value_password_key', 'password', '--mail_subject', 'Missing CustomerEnrolled Event IDs notification', '--mail_body', 'Missing CustomerEnrolled Events detected! The CSV file with IDs could be found in the following folder: s3://<bucket>/missing_ids/loyalty_member/incremental/year=<year>/month=<month>/day=<day>', '--from_address', 'das_marketing_isf_mb_auto@nordstrom.com', '--to_addresses', '<to_addresses>', '--replace_mapping', '{{"year": "{year}", "month": "{month}", "day": "{day}", "bucket": "{bucket}", "to_addresses": "{to_addresses}"}}'.format(bucket=eval('{"test": "mock","nonprod": "tf-das-marketing-nonprod-teradata-stage", "prod": "tf-das-marketing-prod-teradata-stage"}[env]'),year=eval('str(datetime.now().strftime("%Y"))'),month=eval('str(datetime.now().strftime("%m"))'),day=eval('str(datetime.now().strftime("%d"))'),to_addresses=eval('{"test": "mock","nonprod": "test-ooo-notification-aaaamfdfmbl3eawwoaybm5atpa@nordstrom.slack.com", "prod": "das-pod-crashing-loya-aaaaiak743vndt4xz4g5l2wzqe@nordstrom.slack.com"}[env]'))],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'S3_MKTG_BUCKET', 'value': 'tf-das-marketing-prod-teradata-stage'}, {'name': 'S3_MMM_BUCKET', 'value': 'prod-mmm-marketing-mix-model'}, {'name': 'S3_OPTIMINE_BUCKET', 'value': 'prod-mft-optimine'}, {'name': 'S3_ICON_LOUNGE_BUCKET', 'value': 'prod-icon-lounge'}, {'name': 'ENV', 'value': 'production'}, {'name': 'TD_ENV', 'value': 'PRD'}, {'name': 'TD_HOST', 'value': 'tdnapprod.nordstrom.net'}, {'name': 'TD_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/teradata/teradta_db_credentials'}, {'name': 'NR_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/newrelic/newrelic_ingest_key'}, {'name': 'MAIL_SA_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/service_account/sa_credentials'}, {'name': 'VAULT_PATH', 'value': 'vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::699296113972:role/k8s/tf-k8s-controlled-role'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_MARKETING', 'customer_project_name': 'das_marketing_insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='app04216-das-mktg-sa',
    nsk_cluster='nsk-curry-prod')

launch_end_dummy = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='k8s_connection',
    namespace='app04216',
    task_id='end_dummy',
    job_name='end-dummy-loyalty-member-consistency-check-6761-das-marketing-d',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app04216/tm00989-app04216-das-marketing-insights/prod/python/utils:1.3.22',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python/utils/src/dummy.py', '?trigger_operator?'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'S3_MKTG_BUCKET', 'value': 'tf-das-marketing-prod-teradata-stage'}, {'name': 'S3_MMM_BUCKET', 'value': 'prod-mmm-marketing-mix-model'}, {'name': 'S3_OPTIMINE_BUCKET', 'value': 'prod-mft-optimine'}, {'name': 'S3_ICON_LOUNGE_BUCKET', 'value': 'prod-icon-lounge'}, {'name': 'ENV', 'value': 'production'}, {'name': 'TD_ENV', 'value': 'PRD'}, {'name': 'TD_HOST', 'value': 'tdnapprod.nordstrom.net'}, {'name': 'TD_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/teradata/teradta_db_credentials'}, {'name': 'NR_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/newrelic/newrelic_ingest_key'}, {'name': 'MAIL_SA_VAULT_PATH', 'value': 'data/application/APP04216/6761/das_marketing/das-marketing-insights/prod/service_account/sa_credentials'}, {'name': 'VAULT_PATH', 'value': 'vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::699296113972:role/k8s/tf-k8s-controlled-role'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_MARKETING', 'customer_project_name': 'das_marketing_insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='app04216-das-mktg-sa',
    nsk_cluster='nsk-curry-prod')

creds_setup >> fetch_batch_run_id_task >>[compare_om_and_dlq_tables_job, refresh_dlq_table_attach_partitions, refresh_om_table_attach_partitions]
    refresh_om_table_attach_partitions >>[compare_om_and_dlq_tables_job]
    refresh_dlq_table_attach_partitions >>[compare_om_and_dlq_tables_job]
    compare_om_and_dlq_tables_job >>[compare_om_and_dlq_tables_job_sensor, compare_om_and_dlq_tables_job_sensor][check_missing_ids_presence_branching_operator]
    check_missing_ids_presence_branching_operator >>[launch_end_dummy, launch_process_notification_copy_list_of_ids_to_incremental_folder]
    launch_process_notification_copy_list_of_ids_to_incremental_folder >>[launch_process_notification_send_notification_to_slack]
    launch_process_notification_send_notification_to_slack >>    launch_end_dummy >> pelican_validator >> pelican_sensor