# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        9e225085788dc38f5291eaa75c2887e7317f1675
# CI pipeline URL:      https://git.jwn.app/TM00787/app02432-store-analytics/APP02432-gcp-napstore-insights/-/pipelines/7257999
# CI commit timestamp:  2024-10-17T19:01:26+00:00
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
root_path = path.dirname(__file__).split('APP02432-gcp-napstore-insights')[0] + 'APP02432-gcp-napstore-insights/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_hr_outoforder_worker_data_daily_load_2656_napstore_insights'
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
from modules import branch_hire_cancelled_stage
from modules import branch_employee_stage
from modules import branch_hired_stage
from modules import branch_job_stage
from modules import branch_line_position_stage
from modules import branch_work_contact_stage
from modules import branch_org_stage
from airflow.operators.python import BranchPythonOperator

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1)>1 else None

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
    'description': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights DAG Description',
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

session_name=get_batch_id(dag_id=dag_id,task_id="branch1_dummy_task")   
    branch1_dummy_task = DataprocCreateBatchOperator(
            task_id = "branch1_dummy_task",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch1_dummy_task.json",
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

session_name=get_batch_id(dag_id=dag_id,task_id="branch2_dummy_task")   
    branch2_dummy_task = DataprocCreateBatchOperator(
            task_id = "branch2_dummy_task",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch2_dummy_task.json",
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

session_name=get_batch_id(dag_id=dag_id,task_id="branch3_dummy_task")   
    branch3_dummy_task = DataprocCreateBatchOperator(
            task_id = "branch3_dummy_task",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch3_dummy_task.json",
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

stage_branching_employee_stage_branching_job1_operator = BranchPythonOperator(
    task_id='stage_branching_employee_stage_branching_job1',
    python_callable=branch_employee_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="employee_details_stage_load_0_employee_details_stg_tables")   
    employee_details_stage_load_0_employee_details_stg_tables = DataprocCreateBatchOperator(
            task_id = "employee_details_stage_load_0_employee_details_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_load_0_employee_details_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_hr_employee_status_details_load_dim = open(
    os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_hr_employee_status_details_load_dim.sql'),
    "r").read()

    launch_employee_details_stage_load_1_employee_details_dim_tables = BigQueryInsertJobOperator(
        task_id = "employee_details_stage_load_1_employee_details_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_hr_employee_status_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )

stage_branching_org_stage_branching_job2_operator = BranchPythonOperator(
    task_id='stage_branching_org_stage_branching_job2',
    python_callable=branch_org_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="org_details_stage_load_2_org_details_stg_tables")   
    org_details_stage_load_2_org_details_stg_tables = DataprocCreateBatchOperator(
            task_id = "org_details_stage_load_2_org_details_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_load_2_org_details_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_hr_org_details_load_dim = open(
    os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_hr_org_details_load_dim.sql'),
    "r").read()

    launch_org_details_stage_load_3_org_details_dim_tables = BigQueryInsertJobOperator(
        task_id = "org_details_stage_load_3_org_details_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_hr_org_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )

stage_branching_job_stage_branching_job3_operator = BranchPythonOperator(
    task_id='stage_branching_job_stage_branching_job3',
    python_callable=branch_job_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="job_details_stage_load_4_job_details_stg_tables")   
    job_details_stage_load_4_job_details_stg_tables = DataprocCreateBatchOperator(
            task_id = "job_details_stage_load_4_job_details_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_load_4_job_details_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim.sql,apps_hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_org_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_line_position_details_load_dim = open( os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim.sql,apps/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_org_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_line_position_details_load_dim.sql'),"r").read()

    launch_job_details_stage_load_5_job_details_dim_tables = BigQueryInsertJobOperator(
        task_id = "job_details_stage_load_5_job_details_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim.sql,apps_hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_org_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_line_position_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

stage_branching_line_position_stage_branching_job4_operator = BranchPythonOperator(
    task_id='stage_branching_line_position_stage_branching_job4',
    python_callable=branch_line_position_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="line_position_details_stage_load_6_lineposition_details_stg_tables")   
    line_position_details_stage_load_6_lineposition_details_stg_tables = DataprocCreateBatchOperator(
            task_id = "line_position_details_stage_load_6_lineposition_details_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_load_6_lineposition_details_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_hr_line_position_details_load_dim = open(
    os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_hr_line_position_details_load_dim.sql'),
    "r").read()

    launch_line_position_details_stage_load_7_lineposition_details_dim_tables = BigQueryInsertJobOperator(
        task_id = "line_position_details_stage_load_7_lineposition_details_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_hr_line_position_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )

stage_branching_work_contact_stage_branching_job5_operator = BranchPythonOperator(
    task_id='stage_branching_work_contact_stage_branching_job5',
    python_callable=branch_work_contact_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="work_contact_details_stage_load_8_workcontact_details_stg_tables")   
    work_contact_details_stage_load_8_workcontact_details_stg_tables = DataprocCreateBatchOperator(
            task_id = "work_contact_details_stage_load_8_workcontact_details_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_load_8_workcontact_details_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_hr_work_contact_details_load_dim = open(
    os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_hr_work_contact_details_load_dim.sql'),
    "r").read()

    launch_work_contact_details_stage_load_9_workcontact_details_dim_tables = BigQueryInsertJobOperator(
        task_id = "work_contact_details_stage_load_9_workcontact_details_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_hr_work_contact_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )

stage_branching_hire_details_stage_branching_job6_operator = BranchPythonOperator(
    task_id='stage_branching_hire_details_stage_branching_job6',
    python_callable=branch_hired_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="hired_details_stage_load_hired_details_a_stg_tables")   
    hired_details_stage_load_hired_details_a_stg_tables = DataprocCreateBatchOperator(
            task_id = "hired_details_stage_load_hired_details_a_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_load_hired_details_a_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim.sql,apps_hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_org_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_job_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_line_position_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_work_contact_details_load_dim = open( os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim.sql,apps/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_org_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_job_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_line_position_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_work_contact_details_load_dim.sql'),"r").read()

    launch_hired_details_stage_load_hired_details_b_dim_tables = BigQueryInsertJobOperator(
        task_id = "hired_details_stage_load_hired_details_b_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim.sql,apps_hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_org_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_job_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_line_position_details_load_dim,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_work_contact_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

stage_branching_hire_cancelled_details_stage_branching_job7_operator = BranchPythonOperator(
    task_id='stage_branching_hire_cancelled_details_stage_branching_job7',
    python_callable=branch_hire_cancelled_stage.callable_func_name)

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables")   
    hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables = DataprocCreateBatchOperator(
            task_id = "hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables.json",
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

hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_hr_hire_cancelled_details_load_dim = open(
    os.path.join(sql_path, 'hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_hr_hire_cancelled_details_load_dim.sql'),
    "r").read()

    launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables = BigQueryInsertJobOperator(
        task_id = "hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables",
        configuration = 
        {
            "query": 
            {
                "query": hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_hr_hire_cancelled_details_load_dim,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )

session_name=get_batch_id(dag_id=dag_id,task_id="stage_final_dummy_task")   
    stage_final_dummy_task = DataprocCreateBatchOperator(
            task_id = "stage_final_dummy_task",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_stage_final_dummy_task.json",
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

creds_setup >> fetch_batch_run_id_task >>[branch1_dummy_task, branch2_dummy_task, branch3_dummy_task, employee_details_stage_load_0_employee_details_stg_tables, hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables, hired_details_stage_load_hired_details_a_stg_tables, job_details_stage_load_4_job_details_stg_tables, line_position_details_stage_load_6_lineposition_details_stg_tables, org_details_stage_load_2_org_details_stg_tables, stage_final_dummy_task, work_contact_details_stage_load_8_workcontact_details_stg_tables]
    branch1_dummy_task >>[stage_branching_employee_stage_branching_job1_operator, stage_branching_org_stage_branching_job2_operator]
    branch2_dummy_task >>[stage_branching_job_stage_branching_job3_operator, stage_branching_line_position_stage_branching_job4_operator]
    branch3_dummy_task >>[stage_branching_hire_cancelled_details_stage_branching_job7_operator, stage_branching_hire_details_stage_branching_job6_operator, stage_branching_work_contact_stage_branching_job5_operator]
    stage_branching_employee_stage_branching_job1_operator >>[branch2_dummy_task, employee_details_stage_load_0_employee_details_stg_tables]
    employee_details_stage_load_0_employee_details_stg_tables >>[launch_employee_details_stage_load_1_employee_details_dim_tables]
    launch_employee_details_stage_load_1_employee_details_dim_tables >>    stage_branching_org_stage_branching_job2_operator >>[branch2_dummy_task, org_details_stage_load_2_org_details_stg_tables]
    org_details_stage_load_2_org_details_stg_tables >>[launch_org_details_stage_load_3_org_details_dim_tables]
    launch_org_details_stage_load_3_org_details_dim_tables >>    stage_branching_job_stage_branching_job3_operator >>[branch3_dummy_task, job_details_stage_load_4_job_details_stg_tables]
    job_details_stage_load_4_job_details_stg_tables >>[launch_job_details_stage_load_5_job_details_dim_tables]
    launch_job_details_stage_load_5_job_details_dim_tables >>    stage_branching_line_position_stage_branching_job4_operator >>[branch3_dummy_task, line_position_details_stage_load_6_lineposition_details_stg_tables]
    line_position_details_stage_load_6_lineposition_details_stg_tables >>[launch_line_position_details_stage_load_7_lineposition_details_dim_tables]
    launch_line_position_details_stage_load_7_lineposition_details_dim_tables >>    stage_branching_work_contact_stage_branching_job5_operator >>[stage_final_dummy_task, work_contact_details_stage_load_8_workcontact_details_stg_tables]
    work_contact_details_stage_load_8_workcontact_details_stg_tables >>[launch_work_contact_details_stage_load_9_workcontact_details_dim_tables]
    launch_work_contact_details_stage_load_9_workcontact_details_dim_tables >>    stage_branching_hire_details_stage_branching_job6_operator >>[hired_details_stage_load_hired_details_a_stg_tables, stage_final_dummy_task]
    hired_details_stage_load_hired_details_a_stg_tables >>[launch_hired_details_stage_load_hired_details_b_dim_tables]
    launch_hired_details_stage_load_hired_details_b_dim_tables >>    stage_branching_hire_cancelled_details_stage_branching_job7_operator >>[hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables, stage_final_dummy_task]
    hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables >>[launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables]
    launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables >>    stage_final_dummy_task >> pelican_validator >> pelican_sensor