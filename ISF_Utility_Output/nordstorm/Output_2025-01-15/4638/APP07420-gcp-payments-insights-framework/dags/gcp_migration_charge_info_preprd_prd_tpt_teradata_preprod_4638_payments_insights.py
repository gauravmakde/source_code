# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        20d627573a3de4fe423c6691cdf281f130030d2d
# CI pipeline URL:      https://git.jwn.app/TM01164/nap-payments/APP07420-gcp-payments-insights-framework/-/pipelines/7239485
# CI commit timestamp:  2024-10-16T12:42:22+00:00
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

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP07420-gcp-payments-insights-framework')[0] + 'APP07420-gcp-payments-insights-framework/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_migration_charge_info_preprd_prd_tpt_teradata_preprod_4638_payments_insights'
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
        
    'retries': 0 if config_env == 'development' else 5,
    'description': 'migration_charge_info_preprd_prd_tpt_teradata_preprod_4638_payments_insights DAG Description',
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

    tpt_jdbc_export_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)

    tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_export_tbl = SSHOperator(
    task_id='tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PREPROD_NAP_UTL -s s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/migration_charge_info_preprd_prd_tpt_teradata_preprod_4638_payments_insights_migrate_chargeback_info_data_preprd_to_prd_stage_chargeback_information_preprod_tpt_export_preprod.sql -h tdnapprodcop1.nordstrom.net -l 'N' -u SCH_NAP_CUSTPAYMNT_BATCH_PREPROD -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PREPROD_PWD)\"  -t s3://tf-nap-prod-chargeback-migration/chargeback_information_preprod_tpt_export/{partition}/{filename}.csv -a N -d , -c 1 -q N -f '' -j PREPROD_NAP_UTL_payment_chargeback_information_fact_JOB -r TD-UTILITIES-EC2".format(partition=eval('datetime.today().strftime("YEAR=%Y/MONTH=%m/DAY=%d")'),filename=eval('"chargeback_information_preprod_tpt_export_" + str(int(round(datetime.now().timestamp())))')),
    timeout=1800,
    ssh_conn_id="TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT",
    retries=2)

    tpt_jdbc_insert_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)

    tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_load_tbl = SSHOperator(
    task_id='tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_PYMNT_FRAUD_UTL -j PAYMENT_CHARGEBACK_INFORMATION_LDG_LOAD -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_PYMNT_FRAUD_BATCH -p \"\$tdwallet(SCH_NAP_PYMNT_FRAUD_BATCH_PWD)\" -f s3://tf-nap-prod-chargeback-migration/chargeback_information_preprod_tpt_export/{partition}/*csv -o UPDATE -r TD-UTILITIES-EC2".format(partition=eval('datetime.today().strftime("YEAR=%Y/MONTH=%m/DAY=%d")')),
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT'),
    retries=2)

    
        
    teradata_teradata_migration_charge_info_preprd_prd_tpt_teradata_preprod_4638_payments_insights_migrate_chargeback_info_data_preprd_to_prd_stage_chargeback_information_insert_from_stg_to_fct_teradata_preprod = open( os.path.join(sql_path, 'teradata/teradata/migration_charge_info_preprd_prd_tpt_teradata_preprod_4638_payments_insights_migrate_chargeback_info_data_preprd_to_prd_stage_chargeback_information_insert_from_stg_to_fct_teradata_preprod.sql'),"r").read()

    launch_migrate_chargeback_info_data_preprd_to_prd_stage_insert_from_stg_to_fct_teradata_stage_job = BigQueryInsertJobOperator(
        task_id = "migrate_chargeback_info_data_preprd_to_prd_stage_insert_from_stg_to_fct_teradata_stage_job",
        configuration = 
        {
            "query": 
            {
                "query": teradata_teradata_migration_charge_info_preprd_prd_tpt_teradata_preprod_4638_payments_insights_migrate_chargeback_info_data_preprd_to_prd_stage_chargeback_information_insert_from_stg_to_fct_teradata_preprod,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

creds_setup >> fetch_batch_run_id_task >> tpt_jdbc_export_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_control_tbl >>[tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_export_tbl]
    tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_teradata_to_s3_tpt_stage_job_export_tbl >>[tpt_jdbc_insert_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_control_tbl]
    tpt_jdbc_insert_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_control_tbl >>[tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_load_tbl]
    tpt_migrate_chargeback_info_data_preprd_to_prd_stage_load_from_s3_to_teradata_tpt_stage_job_load_tbl >>[launch_migrate_chargeback_info_data_preprd_to_prd_stage_insert_from_stg_to_fct_teradata_stage_job]
    launch_migrate_chargeback_info_data_preprd_to_prd_stage_insert_from_stg_to_fct_teradata_stage_job >> pelican_validator >> pelican_sensor