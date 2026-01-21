# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        9abab657b849d9392975402a32c34550e62618ae
# CI pipeline URL:      https://git.jwn.app/TM01478/app08983-gcp-isf-nsk-airflow-dags/-/pipelines/7194774
# CI commit timestamp:  2024-10-10T16:25:13+00:00
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
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging

# from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP08983-gcp-isf-nsk-airflow-dags')[0] + 'APP08983-gcp-isf-nsk-airflow-dags/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_phase_zero_tableau_refresh_16780_TECH_SC_NAP_insights'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)

from modules.pelican import Pelican
from modules.date_functions import upstream_dependency

start_date = pendulum.datetime(2022, 2, 24)
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(120)) if int(120) > 1 else None

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
    'description': 'phase_zero_tableau_refresh_16780_TECH_SC_NAP_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email':None,  # if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    'email_on_failure':False,  # config.getboolean(dag_id, 'email_on_failure'),
    'email_on_retry':False,  # config.getboolean(dag_id, 'email_on_retry'),
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

    launch_other_appId_upstream_dependencies_sensor_0 = DummyOperator(task_id='check_other_appId_upstream_dependencies_0')

    # ApiDagRunLatestSensor(
    # dag=dag,
    # task_id='check_other_appId_upstream_dependencies_0',
    # conn_id='okta_conn_app08983',
    # external_dag_id='gcp_phase_zero_td_16780_TECH_SC_NAP_insights',
    # date_query_fn=upstream_dependency.hours12_query_function,
    # timeout=3600,
    # retries=1,
    # poke_interval=120)

    launch_main_tableau_refresh = DummyOperator(task_id='TECH_ISF_NAP_SCINV_K8S_CONN_PROD')

    # launch_k8s_api_job_operator(
    # dag=dag,
    # connection_id='TECH_ISF_NAP_SCINV_K8S_CONN_PROD',
    # namespace='app08983',
    # task_id='main_tableau_refresh',
    # job_name='main-tableau-refresh-phase-zero-tableau-refresh-16780-tech-sc-n',
    # macros=None,
    # container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/tableau-agent:0.1.6',
    # container_command=['python', '/home/nonroot/agent/main.py', '--source_type', 'datasource', '--site', 'Merchandising', '--datasource_name', 'PO_INBOUND_TABLEAU_EXTRACT_FACT (T2DL_DAS_Phase_Zero.PO_INBOUND_TABLEAU_EXTRACT_FACT) (T2DL_DAS_Phase_Zero)', '--vault_path', 'data/application/APP08983/prod/db/tableau/onehop/tableau_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200'],
    # resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    # common_envs=[],
    # task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local.'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP08983'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP08983-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'phase_zero_tableau_refresh_16780_TECH_SC_NAP_insights'}],
    # metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::560772504729:role/APP08983_NAP_SCINV_Prod_NSK'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'TECH_SC_NAP', 'customer_project_name': 'insights'}},
    # startup_timeout=300,
    # retries=3,
    # service_account_name='app08983-isf-prod',
    # nsk_cluster='nsk-gumbo-prod')

    creds_setup >> launch_other_appId_upstream_dependencies_sensor_0 >>[launch_main_tableau_refresh]
    launch_main_tableau_refresh >> pelican_validator >> pelican_sensor