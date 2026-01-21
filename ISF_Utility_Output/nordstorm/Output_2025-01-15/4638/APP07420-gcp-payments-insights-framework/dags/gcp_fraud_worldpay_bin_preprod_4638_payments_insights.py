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

from nordstrom.operators.operator import LivyOperator
from nordstrom.sensors.sensor import LivySensor

from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable

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

dag_id = 'gcp_fraud_worldpay_bin_preprod_4638_payments_insights'
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
        
    'retries': 0 if config_env == 'development' else 5,
    'description': 'fraud_worldpay_bin_preprod_4638_payments_insights DAG Description',
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

    tpt_jdbc_insert_read_s3_write_td_stage_s3_csv_to_td_stg_job_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_read_s3_write_td_stage_s3_csv_to_td_stg_job_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)

    tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl = SSHOperator(
    task_id='tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PREPROD_NAP_PYMNT_FRAUD_UTL -j WORLDPAY_BIN_LDG_TPT_ENGINE_JOB_PREPROD -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_PYMNT_FRAUD_BATCH_PREPROD -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PWD)\" -f s3://tf-nap-preprod-master-merchant-bin-files/raw/* -o UPDATE -r TD-UTILITIES-EC2",
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT'),
    retries=2)

    
        
    teradata_teradata_fraud_worldpay_bin_preprod_4638_payments_insights_read_s3_write_td_stage_fraud_worldpay_bin_dim_preprod = open( os.path.join(sql_path, 'teradata/teradata/fraud_worldpay_bin_preprod_4638_payments_insights_read_s3_write_td_stage_fraud_worldpay_bin_dim_preprod.sql'),"r").read()

    launch_read_s3_write_td_stage_td_stg_to_td_dim_job = BigQueryInsertJobOperator(
        task_id = "read_s3_write_td_stage_td_stg_to_td_dim_job",
        configuration = 
        {
            "query": 
            {
                "query": teradata_teradata_fraud_worldpay_bin_preprod_4638_payments_insights_read_s3_write_td_stage_fraud_worldpay_bin_dim_preprod,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

session_name=get_batch_id(dag_id=dag_id,task_id="read_td_write_td_stage_td_dim_to_td_stg_job")   
    read_td_write_td_stage_td_dim_to_td_stg_job = DataprocCreateBatchOperator(
            task_id = "read_td_write_td_stage_td_dim_to_td_stg_job",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_td_dim_to_td_stg_job.json",
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

teradata_teradata_fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_fraud_worldpay_rollup_dim_preprod = open( os.path.join(sql_path, 'teradata/teradata/fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_fraud_worldpay_rollup_dim_preprod.sql'),"r").read()

    launch_read_td_write_td_stage_td_stg_to_td_dim_job = BigQueryInsertJobOperator(
        task_id = "read_td_write_td_stage_td_stg_to_td_dim_job",
        configuration = 
        {
            "query": 
            {
                "query": teradata_teradata_fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_fraud_worldpay_rollup_dim_preprod,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_s3_write_s3_stage_s3_raw_to_s3_processed_job',
    job_name='read-s3-write-s3-stage-s3-raw-to-s3-processed-job-fraud-worldpa',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app07420/tm01164-nap-payments-APP07420-gcp-payments-insights-framework/prod/python/fraud_worldpay_bin:1.1.0',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python/fraud_worldpay_bin/main.py'],
    resources={'limits': {'cpu': 1, 'memory': '1Gi' },'requests': {'cpu': 1, 'memory': '1Gi'}},
    common_envs=[],
    task_envs=[{'name': 'RESET_S3_BUCKET', 'value': 'tf-nap-prod-reset-landing'}, {'name': 'FRAUD_WORLDPAY_BIN_BUCKET', 'value': 'tf-nap-prod-master-merchant-bin-files'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')

creds_setup >> fetch_batch_run_id_task >> tpt_jdbc_insert_read_s3_write_td_stage_s3_csv_to_td_stg_job_control_tbl >>[tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl]
    tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl >>[launch_read_s3_write_td_stage_td_stg_to_td_dim_job]
    launch_read_s3_write_td_stage_td_stg_to_td_dim_job >>[read_td_write_td_stage_td_dim_to_td_stg_job]
    read_td_write_td_stage_td_dim_to_td_stg_job >>[launch_read_td_write_td_stage_td_stg_to_td_dim_job]
    launch_read_td_write_td_stage_td_stg_to_td_dim_job >>[launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job]
    launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job >> pelican_validator >> pelican_sensor