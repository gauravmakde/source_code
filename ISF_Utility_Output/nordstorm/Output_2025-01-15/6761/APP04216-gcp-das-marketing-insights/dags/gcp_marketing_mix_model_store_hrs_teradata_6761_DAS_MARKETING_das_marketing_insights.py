# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        fea9b3aae50de8ebb3a72cb17923c7d2f7b91e42
# CI pipeline URL:      https://git.jwn.app/TM00989/APP04216-gcp-das-marketing-insights/-/pipelines/7465607
# CI commit timestamp:  2024-11-08T04:14:26+00:00
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

from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor

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

dag_id = 'gcp_marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = config.get('framework_setup', 'airflow_newrelic_api_key_name')

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican
from modules.metrics.main_common import main
                

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(90)) if int(90) > 1 else None

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
    'description': 'marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}

def default_query_function_1_days():
    dag_run = get_current_context()["dag_run"]
    start_date = dag_run.start_date - timedelta(days=1)
    return {
        'start_date_gte': start_date.isoformat()
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

    launch_other_appId_upstream_dependencies_sensor_0 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_0',
    conn_id='okta_connection_app02432',
    external_dag_id='gcp_hr_outoforder_worker_data_daily_load_2656_napstore_insights',
    date_query_fn=default_query_function_1_days,
    timeout=7200,
    retries=0,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'])

    launch_other_appId_upstream_dependencies_sensor_1 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_1',
    conn_id='okta_connection_app02432',
    external_dag_id='gcp_hr_worker_load_v1_2656_napstore_insights',
    date_query_fn=default_query_function_1_days,
    timeout=7200,
    retries=0,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'])

    launch_other_appId_upstream_dependencies_sensor_2 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_2',
    conn_id='okta_connection_app02432',
    external_dag_id='gcp_hr_outoforder_worker_data_daily_load_2656_napstore_insights',
    date_query_fn=default_query_function_1_days,
    timeout=7200,
    retries=0,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'])

    launch_other_appId_upstream_dependencies_sensor_3 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_3',
    conn_id='okta_connection_app02432',
    external_dag_id='gcp_hr_job_events_load_2656_napstore_insights',
    date_query_fn=default_query_function_1_days,
    timeout=7200,
    retries=0,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'])

    launch_other_appId_upstream_dependencies_sensor_4 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_4',
    conn_id='okta_connection_app02432',
    external_dag_id='gcp_hr_timeblock_load_2656_napstore_insights',
    date_query_fn=default_query_function_1_days,
    timeout=7200,
    retries=0,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'])

    
    marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights_marketing_mix_model_store_hrs_marketing_mix_model_store_hrs_teradata = open(
    os.path.join(sql_path, 'marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights_marketing_mix_model_store_hrs_marketing_mix_model_store_hrs_teradata.sql'),
    "r").read()

    launch_marketing_mix_model_store_hrs_teradata_job = BigQueryInsertJobOperator(
        task_id = "marketing_mix_model_store_hrs_teradata_job",
        configuration = 
        {
            "query": 
            {
                "query": marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights_marketing_mix_model_store_hrs_marketing_mix_model_store_hrs_teradata,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )

tpt_jdbc_export_marketing_mix_model_store_hrs_job_export_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_marketing_mix_model_store_hrs_job_export_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_marketing_mix_model_store_hrs_job_export_export_tbl = SSHOperator(
    task_id='tpt_marketing_mix_model_store_hrs_job_export_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-nap-prod-mkt-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6761/DAS_MARKETING/das_marketing_insights/sql/marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights_marketing_mix_model_store_hrs_marketing_mix_model_store_hrs_tpt_export.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_MMM_BATCH -p \"\$tdwallet(SCH_NAP_MMM_BATCH_PWD)\"  -t s3://prod-mmm-marketing-mix-model/Nordstrom/Outbound/delta_load/weekly_refresh/year={year}/month={month}/day={day}/mmm_store_hrs_export_{year}_{month}_{day}.csv -a N -d , -c 1 -q N -f '' -j {database_env}_MMM_STORE_HRS_LDG_JOB".format(database_env=eval('{"nonprod": "PROTO_DSA_AI_DB","test": "PROTO_DSA_AI_DB", "prod": "PRD_NAP_DSA_AI_STG"}[env]'),year=eval('str(datetime.now().strftime("%Y"))'),month=eval('str(datetime.now().strftime("%m"))'),day=eval('str(datetime.now().strftime("%d"))')),
    timeout=1800,
    ssh_conn_id="teradata_ssh_connection_to_ec2_isf")

    creds_setup >> fetch_batch_run_id_task >> launch_other_appId_upstream_dependencies_sensor_0 >>[launch_marketing_mix_model_store_hrs_teradata_job]
    launch_other_appId_upstream_dependencies_sensor_1 >>[launch_marketing_mix_model_store_hrs_teradata_job]
    launch_other_appId_upstream_dependencies_sensor_2 >>[launch_marketing_mix_model_store_hrs_teradata_job]
    launch_other_appId_upstream_dependencies_sensor_3 >>[launch_marketing_mix_model_store_hrs_teradata_job]
    launch_other_appId_upstream_dependencies_sensor_4 >>[launch_marketing_mix_model_store_hrs_teradata_job]
    launch_marketing_mix_model_store_hrs_teradata_job >>[tpt_jdbc_export_marketing_mix_model_store_hrs_job_export_control_tbl]
    tpt_jdbc_export_marketing_mix_model_store_hrs_job_export_control_tbl >>[tpt_marketing_mix_model_store_hrs_job_export_export_tbl] >> pelican_validator >> pelican_sensor