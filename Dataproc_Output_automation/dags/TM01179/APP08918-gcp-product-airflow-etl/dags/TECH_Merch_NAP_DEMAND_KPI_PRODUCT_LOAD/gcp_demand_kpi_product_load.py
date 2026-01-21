from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from nordstrom.utils.cloud_creds import cloud_creds
from nordstrom.utils.setup_module import setup_module
from os import  path
from pathlib import Path
import configparser
import logging
import os
import sys

file_path=os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent,'../modules/common/packages/__init__.py')
setup_module(module_name="ds_product_common", file_path=file_path)

from ds_product_common.servicenow_util import invoke_servicenow_api

# fetch environment from OS variable

env = 'nonprod'  # os.environ['ENVIRONMENT']

# Fetch the data from config file
config = configparser.ConfigParser()
root_path = path.dirname(__file__).split('APP08918-gcp-product-airflow-etl')[
                0] + 'APP08918-gcp-product-airflow-etl/'
config.read(os.path.join(root_path, 'configs/TECH_Merch_NAP_DEMAND_KPI_PRODUCT_LOAD/nap_demand_kpi.cfg'))
owner = config.get(env, 'dag_owner')
email = 'None'  # config.get(env, 'dag_alert_email_addr')
retry_limit = config.get(env, 'retries')
retry_delay = config.getint(env, 'retry_delay')

# bigquery configs
sql_path = root_path + 'sql/TECH_Merch_NAP_DEMAND_KPI_PRODUCT_LOAD/'

# Service now incident configs
sn_incident_severity = config.get(env, 'sn_incident_severity')

# Airflow variables
schedule = None  # config.get(env, 'dag_schedule_interval')

# external_dag_task variables
vendor_dag_id = config.get(env, 'vendor_dag_id')
vendor_dag_end_task_id = config.get(env, 'vendor_dag_end_task_id')
product_dag_id = config.get(env, 'product_dag_id')
product_dag_end_task_id = config.get(env, 'product_dag_end_task_id')

# gcp variables

project_id = config.get(env, 'gcp_project_id')
service_account_email = config.get(env, 'service_account_email')
region = config.get(env, 'region')
gcp_conn = config.get(env,'gcp_conn')

# dag_id
dag_id = 'gcp_' + owner + '_DEMAND_KPI_PRODUCT_LOAD'

parameters={'dbenv':'dev','gcp_project_id':project_id }

# method to invoke the callback function to create service now incident
def create_servicenow_incident(context):
    invoke_servicenow_api(context, sn_incident_severity)


# This function return the most recent execution date for the dependent DAG.
def get_most_recent_dag_run(execution_date, task, **kwargs):
    dag_runs = DagRun.find(dag_id=task.external_dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date


def sense_external_task_completion(task_id, external_dag_id, external_task_name):
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_name,
        poke_interval=120,
        execution_date_fn=get_most_recent_dag_run,
        timeout=timedelta(days=1).total_seconds(),
        dag=dag)


def call_bqinsert_operator(dag,task_id,sql_file_location,project_id,location,gcp_conn_id=None,parameters=None):
    with open(sql_file_location,'r') as file1:
            sql_string = file1.read()

    return BigQueryInsertJobOperator(dag = dag,
        
        task_id=task_id,
        configuration={
            "query": {
                "query": sql_string,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id = gcp_conn_id,
        location=location
    )

def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get(env, 'nauth_conn_id'),
        cloud_conn_id=config.get(env, 'gcp_conn'),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()


default_args = {
    'owner': owner,
    'start_date': datetime(2021, 3, 1),
    'email': email.split(','),
    'email_on_failure': False,  # if env == 'development' else True,
    'email_on_retry': False,
    'retries': retry_limit,
    'retry_delay': timedelta(seconds=retry_delay),
    'on_failure_callback': create_servicenow_incident
}

with DAG(dag_id, default_args=default_args, schedule_interval=schedule, catchup=False) as dag:
    #setting Credentials
    creds_setup = PythonOperator(task_id="setup_creds",python_callable=setup_creds,op_args=[service_account_email])


    # Step 1 - Dummy task for the begin task
    load_start = DummyOperator(task_id='load_start')


    # Step 3 - External task sensor for checking Vendor completion
    # vendor_dimension_load_check = sense_external_task_completion('vendor_dimension_load_check',
    #                                                              vendor_dag_id,
    #                                                              vendor_dag_end_task_id)
                                                                 
    # Step 4 - External task sensor for checking Product completion
    # product_dimension_load_check = sense_external_task_completion('product_dimension_load_check',
    #                                                               product_dag_id,
    #                                                               product_dag_end_task_id)

    # Step 5 - Invoke Demand-KPI product detail dimension table 
    demand_product_detail_dim_load = call_bqinsert_operator(dag,'demand_product_detail_dim_load',
                                                   sql_path + 'DEMAND_PRODUCT_DETAIL_DIM.sql',project_id,region,gcp_conn,parameters)
    # Step 6 - Dummy task for the end task
    load_end = DummyOperator(task_id='load_end')

 
    # Task dependency
    creds_setup >> load_start >> demand_product_detail_dim_load >> load_end