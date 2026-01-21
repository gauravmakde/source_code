# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        14af3294fe4d7f1170033dcccb307b299ce51642
# CI pipeline URL:      https://git.jwn.app/TM00593/das_merch/APP04070-gcp-MFP_kafka_to_TD_IF/-/pipelines/7038235
# CI commit timestamp:  2024-09-23T10:52:04+00:00
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
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging


from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor


# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP04070-gcp-MFP_kafka_to_TD_IF')[0] + 'APP04070-gcp-MFP_kafka_to_TD_IF/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_sales_amount_10976_tech_nap_merch'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)
print("module_path :--------------",module_path) 

from modules.pelican import Pelican

from modules.sensor_utils_modules.sensor_utils import get_dag_run_date_range


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
        
    'retries': 0 if config_env == 'development' else 2,
    'description': 'sales_amount_10976_tech_nap_merch DAG Description',
    'retry_delay': timedelta(minutes=15),
    'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    # 'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    # 'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    'email_on_failure': False,
    'email_on_retry': False,
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


    launch_other_appId_upstream_dependencies_sensor_0 = ApiDagRunLatestSensor(
        dag=dag,
        task_id='check_other_appId_upstream_dependencies_0',
        conn_id='okta_conn_to_APP04070_team',
        external_dag_id='gcp_jwn_demand_lifecycle_load_10976_tech_nap_merch',
        date_query_fn=get_dag_run_date_range(),
        timeout=3600,
        retries=3,
        poke_interval=60)

    launch_other_appId_upstream_dependencies_sensor_1 = ApiDagRunLatestSensor(
        dag=dag,
        task_id='check_other_appId_upstream_dependencies_1',
        conn_id='okta_conn_to_APP08983_team',
        external_dag_id='gcp_ascp_rms_all_inventory_stock_quantity_load_v1_16780_TECH_SC_NAP_insights',
        date_query_fn=get_dag_run_date_range(),
        timeout=3600,
        retries=3,
        poke_interval=60)

    
        
    inventory_sales_insights_etl_batch_dt_start_updt = open( os.path.join(sql_path, 'inventory_sales_insights/etl_batch_dt_start_updt.sql'),"r").read()

    launch_execute_elt_control_start_update = BigQueryInsertJobOperator(
        task_id = "execute_elt_control_start_update",
        configuration = 
        {
            "query": 
            {
                "query": inventory_sales_insights_etl_batch_dt_start_updt,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )    
        
    inventory_sales_insights_delete_metrics = open( os.path.join(sql_path, 'inventory_sales_insights/delete_metrics.sql'),"r").read()

    launch_execute_delete_metrics = BigQueryInsertJobOperator(
        task_id = "execute_delete_metrics",
        configuration = 
        {
            "query": 
            {
                "query": inventory_sales_insights_delete_metrics,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
    
        
    inventory_sales_insights_load_inventory = open( os.path.join(sql_path, 'inventory_sales_insights/load_inventory.sql'),"r").read()

    launch_execute_load_inventory = BigQueryInsertJobOperator(
        task_id = "execute_load_inventory",
        configuration = 
        {
            "query": 
            {
                "query": inventory_sales_insights_load_inventory,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
    
        
    inventory_sales_insights_load_sales = open( os.path.join(sql_path, 'inventory_sales_insights/load_sales.sql'),"r").read()

    launch_execute_load_sales = BigQueryInsertJobOperator(
        task_id = "execute_load_sales",
        configuration = 
        {
            "query": 
            {
                "query": inventory_sales_insights_load_sales,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
        
    inventory_sales_insights_clear_clearance_markdown_hierarchy_dim = open( os.path.join(sql_path, 'inventory_sales_insights/clear_clearance_markdown_hierarchy_dim.sql'),"r").read()

    launch_execute_clear_clearance_hierarchy_dim = BigQueryInsertJobOperator(
        task_id = "execute_clear_clearance_hierarchy_dim",
        configuration = 
        {
            "query": 
            {
                "query": inventory_sales_insights_clear_clearance_markdown_hierarchy_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
        
    inventory_sales_insights_load_clearance_markdown_hierarchy_dim = open( os.path.join(sql_path, 'inventory_sales_insights/load_clearance_markdown_hierarchy_dim.sql'),"r").read()

    launch_execute_load_clearance_hierarchy_dim = BigQueryInsertJobOperator(
        task_id = "execute_load_clearance_hierarchy_dim",
        configuration = 
        {
            "query": 
            {
                "query": inventory_sales_insights_load_clearance_markdown_hierarchy_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

    creds_setup >> launch_other_appId_upstream_dependencies_sensor_0 >>[launch_execute_elt_control_start_update]
    creds_setup >> launch_other_appId_upstream_dependencies_sensor_1 >>[launch_execute_elt_control_start_update]
    launch_execute_elt_control_start_update >>[launch_execute_delete_metrics]
    launch_execute_delete_metrics >>[launch_execute_load_inventory]
    launch_execute_load_inventory >>[launch_execute_load_sales]
    launch_execute_load_sales >>[launch_execute_clear_clearance_hierarchy_dim]
    launch_execute_clear_clearance_hierarchy_dim >>[launch_execute_load_clearance_hierarchy_dim]
    launch_execute_load_clearance_hierarchy_dim >> pelican_validator >> pelican_sensor