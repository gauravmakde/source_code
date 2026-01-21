# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        42fd7be63b807cfe6854dbaed5eb6a7298825eda
# CI pipeline URL:      https://git.jwn.app/TM01007/APP07324-Analytics-Insights/-/pipelines/6723120
# CI commit timestamp:  2024-08-14T21:47:51+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os,configparser
from datetime import datetime, timedelta
from os import  path
import sys
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator
from nordstrom.utils.cloud_creds import cloud_creds

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# Airflow variables
# airflow_environment = os.environ.get('ENVIRONMENT', 'local')
airflow_environment='nonprod'
root_path = path.dirname(__file__).split('app09392-onix-testing')[
                0] + 'app09392-onix-testing/'
dags_path = os.path.abspath(os.path.dirname(__file__))
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['development','nonprod'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
# sql_config_dict = dict(config['db'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)


start_date = pendulum.datetime(2024, 2, 27, 17, 0)
cron = '0 17 * * *'
dag_id = 'gcp_digital_sales_transactions_11521_ACE_ENG'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

def get_batch_id(dag_id,task_id):  
   current_datetime = datetime.today()  

   if len(dag_id)<=22:
      dag_id = dag_id.replace('_','-').rstrip('-')
      ln_task_id = 45 - len(dag_id)
      task_id = task_id[-ln_task_id:].replace('_','-').strip('-') 
   elif len(task_id)<=22:
      task_id = task_id.replace('_','-').strip('-')
      ln_dag_id = 45 - len(task_id)
      dag_id = dag_id[:ln_dag_id].replace('_','-').rstrip('-')    
   else:
     dag_id = dag_id[:23].replace('_','-').rstrip('-') 
     task_id = task_id[-22:].replace('_','-').strip('-') 

   date = current_datetime.strftime('%Y%m%d%H%M%S')
   return f'''{dag_id}--{task_id}--{date}'''

def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get('dag', 'nauth_conn_id'),
        cloud_conn_id=config.get('dag', 'gcp_conn'),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

default_args = {
        
    'retries': 3,
    'description': 'digital_sales_transactions_11521_ACE_ENG DAG Description',
    'retry_delay': timedelta(minutes=10),
    # 'email' : 'cf_nordstrom@datametica.com' if conf.get("webserver","instance_name") == 'Datametica Composer Environment' else ['dsa-app08742-session-path-expanded-events-data-pipelines@nordstrom.pagerduty.com', 'soren.stime1@nordstrom.com'],
    # 'email_on_failure': True,
    'email_on_retry': False,
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval = None if config.get("dag","cron") == 'none' else cron,
    max_active_runs=set_max_active_runs,
    concurrency=set_concurrency)as dag:

    project_id = config.get('dag', 'gcp_project_id')
    service_account_email = config.get('dag', 'service_account_email')
    dataproc_sa= config.get('dataproc','dataproc_sa')
    region = config.get('dag', 'region')
    gcp_conn = config.get('dag', 'gcp_conn')
    subnet_url = config.get('dag', 'subnet_url')
    env = os.getenv('ENVIRONMENT')

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )

    

     

   
    
    session_name=get_batch_id(dag_id=dag_id,task_id="run_hive_hive_digital_sales_transactions_sql_job_0")
    livy_run_hive_hive_digital_sales_transactions_sql_job_0 = DataprocCreateBatchOperator(
        task_id="run_hive_hive_digital_sales_transactions_sql_job_0",
        project_id=project_id,
        region=region,
        gcp_conn_id=gcp_conn,
#       op_args={ "version":"1.1"},
        batch={
            "runtime_config": {"version": "1.1",
                               "properties":{'spark.airflow.run_id': '{{ ts_nodash }}'} },
            "spark_batch": {
                "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                "jar_file_uris": [config.get('dataproc', 'spark_jar_path') + "/delta-core_2.12-2.2.0.jar",
                                  config.get('dataproc', 'spark_jar_path') + "/uber-onehop-etl-pipeline-2.2.1.jar"],
                "args": [
                    config.get('dataproc', 'user_config_path') + '/argument_digital_sales_transactions_11521_ACE_ENG_run_hive_hive_digital_sales_transactions_sql_job_0.json',
                    # '--aws_user_role_external_id', Variable.get('aws_role_externalid')
                ],

            },
            "environment_config": {
                "execution_config": {
                    "service_account": dataproc_sa,
                    "subnetwork_uri": subnet_url,
                },
                # "peripherals_config": {
                #     "metastore_service": config.get('dataproc', 'metastore_service_path')
                # }
            }
        },
        batch_id=session_name,
        dag=dag

)


    creds_setup >> livy_run_hive_hive_digital_sales_transactions_sql_job_0