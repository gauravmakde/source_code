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
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.models.dagrun import DagRun

from airflow.sensors.external_task import ExternalTaskSensor

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP07575-gcp-isf-airflow')[0] + 'APP07575-gcp-isf-airflow/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_isf_order_cancel_17393_customer_das_customer'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 


from modules.pelican import Pelican
from modules.metrics.main_common import main
                
start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
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
    'description': 'isf_order_cancel_17393_customer_das_customer DAG Description',
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


    def get_most_recent_dag_run(execution_date, task, **kwargs):
        dag_runs = DagRun.find(dag_id=task.external_dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        if dag_runs:
            return dag_runs[0].execution_date

    launch_check_external_upstream_dependencies_sensor_0 = ExternalTaskSensor(
    dag=dag,
    task_id='check_external_upstream_dependencies_0',
    external_dag_id='gcp_DW_CUSTOMER_DIM',
    timeout=7200,
    external_task_id='ANALYTICAL_DIM_LD',
    execution_delta=timedelta(minutes=120))

    
    order_cancel_order_cancel_elt_control_activate = open( os.path.join(sql_path, 'order_cancel/order_cancel_elt_control_activate.sql'),"r").read()

    launch_order_cancel_elt_control_activate = BigQueryInsertJobOperator(
        task_id = "order_cancel_elt_control_activate",
        configuration = 
        {
            "query": 
            {
                "query": order_cancel_order_cancel_elt_control_activate,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

    order_cancel_order_cancel_fact_ld = open( os.path.join(sql_path, 'order_cancel/order_cancel_fact_ld.sql'),"r").read()

    launch_order_cancel_fact_load = BigQueryInsertJobOperator(
        task_id = "order_cancel_fact_load",
        configuration = 
        {
            "query": 
            {
                "query": order_cancel_order_cancel_fact_ld,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

    order_cancel_order_cancel_fact_acp_id_update = open( os.path.join(sql_path, 'order_cancel/order_cancel_fact_acp_id_update.sql'),"r").read()

    launch_order_cancel_fact_acp_id_update = BigQueryInsertJobOperator(
        task_id = "order_cancel_fact_acp_id_update",
        configuration = 
        {
            "query": 
            {
                "query": order_cancel_order_cancel_fact_acp_id_update,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

    launch_order_cancel_send_newrelic_metric=PythonOperator(
        task_id='order_cancel_send_newrelic_metric',
        python_callable=main,
        op_kwargs={
        'newrelic_connection' : config.get('framework_setup', 'newrelic_connection'),
        'hook' : BigQueryHook(gcp_conn_id=gcp_conn,location=region, use_legacy_sql=False),
        'bigquery_environment' : sql_config_dict['dbenv'],
        'metrics_yaml_file' : os.path.join(module_path,'yaml/DUMMY_COMMON_METRICS.yaml'),
                    } ) 

    order_cancel_order_cancel_elt_control_deactivate = open( os.path.join(sql_path, 'order_cancel/order_cancel_elt_control_deactivate.sql'),"r").read()

    launch_order_cancel_elt_control_deactivate = BigQueryInsertJobOperator(
        task_id = "order_cancel_elt_control_deactivate",
        configuration = 
        {
            "query": 
            {
                "query": order_cancel_order_cancel_elt_control_deactivate,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

    creds_setup >> launch_check_external_upstream_dependencies_sensor_0 >>[launch_order_cancel_elt_control_activate]
    launch_order_cancel_elt_control_activate >>[launch_order_cancel_fact_load]
    launch_order_cancel_fact_load >>[launch_order_cancel_fact_acp_id_update]
    launch_order_cancel_fact_acp_id_update >>[launch_order_cancel_send_newrelic_metric]
    launch_order_cancel_send_newrelic_metric >>[launch_order_cancel_elt_control_deactivate]
    launch_order_cancel_elt_control_deactivate >> pelican_validator >> pelican_sensor