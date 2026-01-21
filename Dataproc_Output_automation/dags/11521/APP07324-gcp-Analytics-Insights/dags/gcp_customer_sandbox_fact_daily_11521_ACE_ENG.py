# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        f4ceb236e834760fcadbdbd01832fa141062be7c
# CI pipeline URL:      https://git.jwn.app/TM01007/APP07324-gcp-Analytics-Insights/-/pipelines/7430782
# CI commit timestamp:  2024-11-05T17:08:43+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os,configparser
from datetime import datetime, timedelta
from os import  path
import sys
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.models.dagrun import DagRun

from airflow.sensors.external_task import ExternalTaskSensor

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP07324-gcp-Analytics-Insights')[0] + 'APP07324-gcp-Analytics-Insights/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_customer_sandbox_fact_daily_11521_ACE_ENG'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.pelican import Pelican
                

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(180)) if int(180)>1 else None

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
        
    'retries': 0 if config_env == 'development' else 0,
    'description': 'customer_sandbox_fact_daily_11521_ACE_ENG DAG Description',
    'retry_delay': timedelta(minutes=10),
    # 'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    # 'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    # 'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    # 'sla': dag_sla

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


    def get_most_recent_dag_run(execution_date, task, **kwargs):
        dag_runs = DagRun.find(dag_id=task.external_dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        if dag_runs:
            return dag_runs[0].execution_date

    launch_check_external_upstream_dependencies_sensor_0 = ExternalTaskSensor(
    dag=dag,
    task_id='check_external_upstream_dependencies_0',
    external_dag_id='gcp_cco_tables_11521_ACE_ENG',
    timeout=7200,
    execution_delta=timedelta(minutes=210),
    retries=0)

    
        
    customer_sandbox_daily_customer_store_distance_buckets = open( os.path.join(sql_path, 'customer_sandbox_daily/customer_store_distance_buckets.sql'),"r").read()

    launch_customer_store_distance_buckets_build = BigQueryInsertJobOperator(
        task_id = "customer_store_distance_buckets_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_customer_store_distance_buckets,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
        
    customer_sandbox_daily_reporting_year_shopped_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/reporting_year_shopped_lkp.sql'),"r").read()

    launch_reporting_year_shopped_lkp_build = BigQueryInsertJobOperator(
        task_id = "reporting_year_shopped_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_reporting_year_shopped_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

        
    customer_sandbox_daily_cust_tenure_bucket_years_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_tenure_bucket_years_lkp.sql'),"r").read()

    launch_cust_tenure_bucket_years_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_tenure_bucket_years_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_tenure_bucket_years_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


    
        
    customer_sandbox_daily_cust_jwn_trip_bucket_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_jwn_trip_bucket_lkp.sql'),"r").read()

    launch_cust_jwn_trip_bucket_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_jwn_trip_bucket_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_jwn_trip_bucket_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
   
        
    customer_sandbox_daily_cust_jwn_net_spend_bucket_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_jwn_net_spend_bucket_lkp.sql'),"r").read()

    launch_cust_jwn_net_spend_bucket_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_jwn_net_spend_bucket_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_jwn_net_spend_bucket_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
        
    customer_sandbox_daily_cust_age_group_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_age_group_lkp.sql'),"r").read()

    launch_cust_age_group_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_age_group_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_age_group_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


        
    customer_sandbox_daily_cust_channel_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_channel_lkp.sql'),"r").read()

    launch_cust_channel_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_channel_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_channel_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

        
    customer_sandbox_daily_cust_channel_combo_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_channel_combo_lkp.sql'),"r").read()

    launch_cust_channel_combo_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_channel_combo_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_channel_combo_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
    
        
    customer_sandbox_daily_cust_loyalty_type_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_loyalty_type_lkp.sql'),"r").read()

    launch_cust_loyalty_type_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_loyalty_type_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_loyalty_type_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               
    
        
    customer_sandbox_daily_cust_loyalty_level_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_loyalty_level_lkp.sql'),"r").read()

    launch_cust_loyalty_level_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_loyalty_level_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_loyalty_level_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

        
    customer_sandbox_daily_cust_nms_market_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_nms_market_lkp.sql'),"r").read()

    launch_cust_nms_market_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_nms_market_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_nms_market_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

    
        
    customer_sandbox_daily_cust_dma_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_dma_lkp.sql'),"r").read()

    launch_cust_dma_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_dma_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_dma_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


        
    customer_sandbox_daily_rack_sol_dist_bucket_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/rack_sol_dist_bucket_lkp.sql'),"r").read()

    launch_rack_sol_dist_bucket_lkp_build = BigQueryInsertJobOperator(
        task_id = "rack_sol_dist_bucket_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_rack_sol_dist_bucket_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


    
        
    customer_sandbox_daily_nord_sol_dist_bucket_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/nord_sol_dist_bucket_lkp.sql'),"r").read()

    launch_nord_sol_dist_bucket_lkp_build = BigQueryInsertJobOperator(
        task_id = "nord_sol_dist_bucket_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_nord_sol_dist_bucket_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

        
    customer_sandbox_daily_closest_store_banner_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/closest_store_banner_lkp.sql'),"r").read()

    launch_closest_store_banner_lkp_build = BigQueryInsertJobOperator(
        task_id = "closest_store_banner_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_closest_store_banner_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

    
        
    customer_sandbox_daily_closest_sol_dist_bucket_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/closest_sol_dist_bucket_lkp.sql'),"r").read()

    launch_closest_sol_dist_bucket_lkp_build = BigQueryInsertJobOperator(
        task_id = "closest_sol_dist_bucket_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_closest_sol_dist_bucket_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

        
    customer_sandbox_daily_item_delivery_method_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/item_delivery_method_lkp.sql'),"r").read()

    launch_item_delivery_method_lkp_build = BigQueryInsertJobOperator(
        task_id = "item_delivery_method_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_item_delivery_method_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

   
        
    customer_sandbox_daily_cust_chan_buyer_flow_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/cust_chan_buyer_flow_lkp.sql'),"r").read()

    launch_cust_chan_buyer_flow_lkp_build = BigQueryInsertJobOperator(
        task_id = "cust_chan_buyer_flow_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_cust_chan_buyer_flow_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


    
        
    customer_sandbox_daily_audience_engagement_cohort_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/audience_engagement_cohort_lkp.sql'),"r").read()

    launch_audience_engagement_cohort_lkp_build = BigQueryInsertJobOperator(
        task_id = "audience_engagement_cohort_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_audience_engagement_cohort_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


        
    customer_sandbox_daily_store_lkp = open( os.path.join(sql_path, 'customer_sandbox_daily/store_lkp.sql'),"r").read()

    launch_store_lkp_build = BigQueryInsertJobOperator(
        task_id = "store_lkp_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_store_lkp,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


    
        
    customer_sandbox_daily_customer_sandbox_fact = open( os.path.join(sql_path, 'customer_sandbox_daily/customer_sandbox_fact.sql'),"r").read()

    launch_customer_sandbox_fact_build = BigQueryInsertJobOperator(
        task_id = "customer_sandbox_fact_build",
        configuration = 
        {
            "query": 
            {
                "query": customer_sandbox_daily_customer_sandbox_fact,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

    creds_setup >>  launch_check_external_upstream_dependencies_sensor_0 >> [launch_audience_engagement_cohort_lkp_build, launch_cust_age_group_lkp_build, launch_cust_chan_buyer_flow_lkp_build, launch_cust_channel_combo_lkp_build, launch_cust_channel_lkp_build, launch_cust_dma_lkp_build, launch_cust_jwn_net_spend_bucket_lkp_build, launch_cust_jwn_trip_bucket_lkp_build, launch_cust_loyalty_level_lkp_build, launch_cust_loyalty_type_lkp_build, launch_cust_nms_market_lkp_build, launch_cust_tenure_bucket_years_lkp_build, launch_customer_store_distance_buckets_build, launch_item_delivery_method_lkp_build, launch_reporting_year_shopped_lkp_build, launch_store_lkp_build]
    launch_customer_store_distance_buckets_build >> [launch_closest_sol_dist_bucket_lkp_build, launch_closest_store_banner_lkp_build, launch_nord_sol_dist_bucket_lkp_build, launch_rack_sol_dist_bucket_lkp_build]
    launch_reporting_year_shopped_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_tenure_bucket_years_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_jwn_trip_bucket_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_jwn_net_spend_bucket_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_age_group_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_channel_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_channel_combo_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_loyalty_type_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_loyalty_level_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_nms_market_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_dma_lkp_build >> launch_customer_sandbox_fact_build
    launch_rack_sol_dist_bucket_lkp_build >> launch_customer_sandbox_fact_build
    launch_nord_sol_dist_bucket_lkp_build >> launch_customer_sandbox_fact_build
    launch_closest_store_banner_lkp_build >> launch_customer_sandbox_fact_build
    launch_closest_sol_dist_bucket_lkp_build >> launch_customer_sandbox_fact_build
    launch_item_delivery_method_lkp_build >> launch_customer_sandbox_fact_build
    launch_cust_chan_buyer_flow_lkp_build >> launch_customer_sandbox_fact_build
    launch_audience_engagement_cohort_lkp_build >> launch_customer_sandbox_fact_build
    launch_store_lkp_build >> launch_customer_sandbox_fact_build
    launch_customer_sandbox_fact_build >> pelican_validator >> pelican_sensor