# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        015bfa273ca3dee41fce56043614dda24e661cc1
# CI pipeline URL:      https://git.jwn.app/TM00156/app00757-nvms/APP09584-nvms-airflow/APP09584-gcp-insight-framework-pipeline/-/pipelines/8101162
# CI commit timestamp:  2025-01-09T15:44:30+00:00
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
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP09584-gcp-insight-framework-pipeline')[0] + 'APP09584-gcp-insight-framework-pipeline/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_nap_inactive_vndr_extract_25532_NVMS_NVMS_Airflow'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = config.get('framework_setup', 'airflow_newrelic_api_key_name')

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican
from modules.metrics.main_common import main
                

start_date = pendulum.datetime(2022, 2, 24, tz="US/Pacific")
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

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
    'description': 'nap_inactive_vndr_extract_25532_NVMS_NVMS_Airflow DAG Description',
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

    tpt_jdbc_export_JOB_0_1_EXPORT_INACTIVE_VENDORS_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_0_1_EXPORT_INACTIVE_VENDORS_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_0_1_EXPORT_INACTIVE_VENDORS_export_tbl = SSHOperator(
    task_id='tpt_JOB_0_1_EXPORT_INACTIVE_VENDORS_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://nvms-isf-etl-store-prod/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/25532/NVMS/NVMS_Airflow/sql/nap_inactive_vndr_extract_25532_NVMS_NVMS_Airflow_JOB_0_1_nap_inactive_vendor_extract.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://nvms-extracts-tpt-export-prod/vendor-reports/inactive_vendor_status/inactive_vendors.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_USR_VWS_NVMS_INACTIVE_VENDOR_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    launch_JOB_0_2_INACTIVATE_VENDOR_STATUS = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NVMS_K8S_GUMBO_CONN_PROD',
    namespace='app09584',
    task_id='JOB_0_2_INACTIVATE_VENDOR_STATUS',
    job_name='job-0-2-inactivate-vendor-status-nap-inactive-vndr-extract-546',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app09584/tm00156-app00757-nvms-app09584-nvms-airflow-app09584-insight-fr/prod/python_modules/inactive_vendor:0.0.20',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python_modules/inactive_vendor/main.py'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'EXEC_ENV', 'value': 'GITLAB'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'EXECUTE_COMMAND', 'value': 'python /app/jobs/executor.py'}, {'name': 'tpt_bucket_name', 'value': 'nvms-extracts-tpt-export-prod'}, {'name': 'da_base_url', 'value': 'https://sharedingress-nsk-gumbo-prod-us-west-2.nordstromaws.app/app00757/nvms-da/da'}, {'name': 'K8S_NAMESPACE', 'value': 'APP09584'}, {'name': 'USER_ID', 'value': 'inactivationAutomation'}, {'name': 'VENDOR_GLOBAL_KEY', 'valueFrom': {'secretKeyRef': {'key': 'VENDOR_GLOBAL_KEY', 'name': 'nvms-airflow-prod'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::372359261305:role/nsk/app09584-service-account-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'NVMS', 'customer_project_name': 'NVMS_Airflow'}},
    startup_timeout=300,
    retries=3,
    service_account_name='nvms-airflow-prod',
    nsk_cluster='nsk-gumbo-nonprod')

creds_setup >> fetch_batch_run_id_task >> tpt_jdbc_export_JOB_0_1_EXPORT_INACTIVE_VENDORS_control_tbl >>[tpt_JOB_0_1_EXPORT_INACTIVE_VENDORS_export_tbl]
    tpt_JOB_0_1_EXPORT_INACTIVE_VENDORS_export_tbl >>[launch_JOB_0_2_INACTIVATE_VENDOR_STATUS]
    launch_JOB_0_2_INACTIVATE_VENDOR_STATUS >> pelican_validator >> pelican_sensor