# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        32f98f68c883a0655ec80f99633f250401296d2c
# CI pipeline URL:      https://git.jwn.app/TM01189/etl/APP02560-gcp-insights-v2/-/pipelines/7238619
# CI commit timestamp:  2024-10-16T10:37:12+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import sys
import pendulum
import os,configparser
import logging
from os import  path
from nordstrom.utils.cloud_creds import cloud_creds
from datetime import datetime, timedelta
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
# from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# from airflow.models.dagrun import DagRun



# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP02560-gcp-insights-v2')[0] + 'APP02560-gcp-insights-v2/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_prev_day_oldf_batch_completed_17421_DAS_SC_OUTBOUND_APP02560_insights_v2'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 


from modules.pelican import Pelican
                

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 1
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1)>1 else None

    
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
    'description': 'prev_day_oldf_batch_completed_17421_DAS_SC_OUTBOUND_APP02560_insights_v2 DAG Description',
    'retry_delay': timedelta(minutes=10),
    # 'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    # 'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    # 'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
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
    concurrency=set_concurrency ) as dag:

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

      

    
        
    batch_start = open( os.path.join(sql_path, 'prev_day_oldf_batch_completed/batch_start.sql'),"r").read()

    launch_main_job_1_td_batch_start = BigQueryInsertJobOperator(
        task_id = "main_job_1_td_batch_start",
        configuration = 
        {
            "query": 
            {
                "query": batch_start,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

   
    # launch_main_job_2_check_oldf_batch_status = launch_k8s_api_job_operator(
    # dag=dag,
    # connection_id='TECH_ISF_NAP_SC_OUTBOUND_K8S_GUMBO_CONN_PROD',
    # namespace='app02560',
    # task_id='main_job_2_check_oldf_batch_status',
    # job_name='main-job-2-check-oldf-batch-status-prev-day-oldf-batch-complete',
    # macros=None,
    # container_image='artifactory.nordstrom.com/docker/app02560/tm01189-etl-app02560-insights-v2/prod/python/sql_sensor:0.0.1',
    # container_command=['python', '/home/nonroot/python/sql_sensor/src/main.py', '--sql', "SELECT case when max(BATCH_DATE)=CURRENT_DATE then 1 else 0 end  FROM {DBENV}_NAP_BASE_VWS.ETL_BATCH_INFO  WHERE INTERFACE_CODE = 'ORDER_LINE_DETAIL_FACT' and EXTRACT(HOUR FROM DW_SYS_END_TMSTP)>=1", '--time_out', '10800', '--poke_interval', '300', '--default_database', '{DBENV}_NAP_BASE_VWS'],
    # resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    # common_envs=[],
    # task_envs=[{'name': 'ENV', 'value': 'production'}, {'name': 'TD_ENV', 'value': 'PRD'}, {'name': 'TD_HOST', 'value': 'tdnapprod.nordstrom.net'}, {'name': 'TD_VAULT_PATH', 'value': 'data/application/APP02560/shared/prod/teradata/nap/service_account'}, {'name': 'NR_VAULT_PATH', 'value': 'data/application/APP02560/shared/prod/newrelic/ingest_license_key'}, {'name': 'BD_VAULT_PATH', 'value': 'data/application/APP02560/shared/prod/bigdata/service_account'}, {'name': 'PRESTO_HOST', 'value': 'bigdata-starburst-presto.prod.bigdata.vip.nordstrom.com'}, {'name': 'PRESTO_CATALOG', 'value': 'hiveacp'}, {'name': 'VAULT_PATH', 'value': 'prod-vault.vault.vip.nordstrom.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'S3_BUCKET', 'value': 'napoutbound-prod-common-etl'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    # metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::851414313645:role/app02560NSKRole'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP02560_insights_v2'}},
    # startup_timeout=300,
    # retries=0,
    # service_account_name='isf-pipeline-sa',
    # nsk_cluster='nsk-gumbo-prod')


# Define the SQL query to check old batch status
    batch_status_query = """
    SELECT 
        CASE 
            WHEN MAX(BATCH_DATE) = CURRENT_DATE THEN 1 
            ELSE 0 
        END AS batch_status
    FROM 
        `{{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_INFO`
    WHERE 
        INTERFACE_CODE = 'ORDER_LINE_DETAIL_FACT'
        AND EXTRACT(HOUR FROM DW_SYS_END_TMSTP) >= 1;
"""

# Define the BigQueryInsertJobOperator
    launch_main_job_2_check_oldf_batch_status = BigQueryInsertJobOperator(
    task_id='main_job_2_check_oldf_batch_status',
    configuration={
        "query": {
            "query": batch_status_query,
            "useLegacySql": False
        }
    },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
           
   

    
        
    save_timeliness_metric = open( os.path.join(sql_path, 'prev_day_oldf_batch_completed/prev_day_oldf_batch_completed_17421_DAS_SC_OUTBOUND_APP02560_insights_v2_main_save_timeliness_metric.sql'),"r").read()

    launch_main_job_3_save_timeliness_metric = BigQueryInsertJobOperator(
        task_id = "main_job_3_save_timeliness_metric",
        configuration = 
        {
            "query": 
            {
                "query": save_timeliness_metric,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

        
        
    batch_end = open( os.path.join(sql_path, 'prev_day_oldf_batch_completed/batch_end.sql'),"r").read()

    launch_main_job_4_td_batch_end = BigQueryInsertJobOperator(
        task_id = "main_job_4_td_batch_end",
        configuration = 
        {
            "query": 
            {
                "query": batch_end,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

    
    creds_setup >> launch_main_job_1_td_batch_start >>[launch_main_job_2_check_oldf_batch_status]
    launch_main_job_2_check_oldf_batch_status >>[launch_main_job_3_save_timeliness_metric]
    launch_main_job_3_save_timeliness_metric >>[launch_main_job_4_td_batch_end]
    launch_main_job_4_td_batch_end >> pelican_validator >> pelican_sensor