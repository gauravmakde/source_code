# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        fa35953ba7eea04d644fc6fbc3d83aba045afc2c
# CI pipeline URL:      https://git.jwn.app/TM01164/nap-payments/app07420-payments-insights-framework/-/pipelines/7066897
# CI commit timestamp:  2024-09-25T17:43:10+00:00
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
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook


env = os.environ.get('ENVIRONMENT', 'local')
# Fetch the data from config file
python_module_path = path.dirname(__file__).split('app08314-isf-airflow-dags')[0] + 'app08314-isf-airflow-dags/common_libraries/'
python_root_module_path = path.dirname(__file__).split('app08314-isf-airflow-dags')[0] + 'app08314-isf-airflow-dags/'
sql_path = python_root_module_path + '4638/payments/insights/sql/'
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(python_root_module_path)
sys.path.append(python_module_path)
path = os.path.split(__file__)[0]

from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status


start_date = pendulum.datetime(2022, 2, 24)
cron = None
dag_id = 'settlement_result_hashkey_backfill_teradata_4638_payments_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 3,
    'description': 'settlement_result_hashkey_backfill_teradata_4638_payments_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['TECH_NAP_CREDIT_PAYMENTS_DL@nordstrom.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}


with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=cron,
        max_active_runs=set_max_active_runs,
        template_searchpath=[sql_path],
        concurrency=set_concurrency,
        ) as dag:

    launch_settlement_result_hashkey_backfill_teradata_stage_0_job_insert_update = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='settlement_result_hashkey_backfill_teradata_stage_0_job_insert_update',
    job_name='settlement-result-hashkey-backfill-teradata-stage-0-job-insert',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/settlement_result_hashkey_backfill_teradata_4638_payments_insights_settlement_result_hashkey_backfill_teradata_stage_0_settlement_result_hashkey_backfill_teradata_fct.sql', '--bucket', 'tf-nap-prod-credit-payments-isf-airflow', '--executor', 'teradata', '--host', 'tdnapprodcop1.nordstrom.net', '--database', 'PRD_NAP_FCT', '--vault_path', 'data/application/APP07420/4638/payments/insights/prod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local.'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'app07420'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-app07420-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'settlement_result_hashkey_backfill_teradata_4638_payments_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')


    launch_settlement_result_hashkey_backfill_teradata_stage_0_job_insert_update_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='settlement_result_hashkey_backfill_teradata_stage_0_job_insert_update_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='settlement-result-hashkey-backfill-teradata-stage-0-job-insert',
    nsk_cluster='nsk-gumbo-prod')


    launch_settlement_result_hashkey_backfill_teradata_stage_0_job_insert_update >> [launch_settlement_result_hashkey_backfill_teradata_stage_0_job_insert_update_sensor]

