# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        9e225085788dc38f5291eaa75c2887e7317f1675
# CI pipeline URL:      https://git.jwn.app/TM00787/app02432-store-analytics/APP02432-napstore-insights/-/pipelines/7257999
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
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook


env = os.environ.get('ENVIRONMENT', 'local')
os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID'
# Fetch the data from config file
python_module_path=path.dirname(__file__).split('app02432-isf-airflow-nsk-dag-repo')[0]+'app02432-isf-airflow-nsk-dag-repo/common_libraries/'
python_root_module_path=path.dirname(__file__).split('app02432-isf-airflow-nsk-dag-repo')[0]+'app02432-isf-airflow-nsk-dag-repo/'
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(python_root_module_path)
sys.path.append(python_module_path)
path = os.path.split(__file__)[0]

from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status


start_date = pendulum.datetime(2022, 2, 24)
cron = '0 9 * * 1-5'
dag_id = 'store_cluster_xref_load_2656_napstore_insights'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

default_args = {
    'retries': 3,
    'description': 'store_cluster_xref_load_2656_napstore_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['nap-customer-selling@nordstrom.pagerduty.com'],
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
    concurrency=set_concurrency) as dag:
    

    launch_load_ldg_to_dim_0_start_batch = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_0_start_batch',
    job_name='load-ldg-to-dim-0-start-batch-store-cluster-xref-load-2656-naps',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/store_cluster_xref_load_2656_napstore_insights_load_store_cluster_xref_elt_control_start_updt.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'store_cluster_xref_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_0_start_batch_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_0_start_batch_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='load-ldg-to-dim-0-start-batch-store-cluster-xref-load-2656-naps',
    nsk_cluster='nsk-miso-prod')


    tpt_jdbc_insert_load_csv_to_ldg_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_load_csv_to_ldg_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')


    tpt_load_csv_to_ldg_load_tbl = SSHOperator(
    task_id='tpt_load_csv_to_ldg_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_UTL -j PRD_NAP_STG_STORE_CLUSTER_XREF_LDG_JOB -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_STOR_BATCH -p \"\$tdwallet(SCH_NAP_STOR_BATCH_PWD)\" -f s3://nap-prod-store-clusters-data-from-mft/JWN_AssortmentClusters.csv -o LOAD -z",
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='napstore_teradata_ssh_conn_id'))


    launch_load_ldg_to_dim_0_audit_check = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_0_audit_check',
    job_name='load-ldg-to-dim-0-audit-check-store-cluster-xref-load-2656-naps',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/store_cluster_xref_load_2656_napstore_insights_load_store_cluster_xref_audit_check.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'store_cluster_xref_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_0_audit_check_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_0_audit_check_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='load-ldg-to-dim-0-audit-check-store-cluster-xref-load-2656-naps',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_1_run_sql = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_1_run_sql',
    job_name='load-ldg-to-dim-1-run-sql-store-cluster-xref-load-2656-napstore',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/store_cluster_xref_load_2656_napstore_insights_load_store_cluster_xref_ldg_to_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'store_cluster_xref_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_1_run_sql_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_1_run_sql_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='load-ldg-to-dim-1-run-sql-store-cluster-xref-load-2656-napstore',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_2_end_batch = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_2_end_batch',
    job_name='load-ldg-to-dim-2-end-batch-store-cluster-xref-load-2656-napsto',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/store_cluster_xref_load_2656_napstore_insights_load_store_cluster_xref_elt_control_end_updt.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'store_cluster_xref_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_2_end_batch_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='load_ldg_to_dim_2_end_batch_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='load-ldg-to-dim-2-end-batch-store-cluster-xref-load-2656-napsto',
    nsk_cluster='nsk-miso-prod')


    launch_load_ldg_to_dim_0_start_batch >> [launch_load_ldg_to_dim_0_start_batch_sensor]
    launch_load_ldg_to_dim_0_start_batch_sensor >> [tpt_jdbc_insert_load_csv_to_ldg_control_tbl]
    tpt_jdbc_insert_load_csv_to_ldg_control_tbl >> [tpt_load_csv_to_ldg_load_tbl]
    tpt_load_csv_to_ldg_load_tbl >> [launch_load_ldg_to_dim_0_audit_check]
    launch_load_ldg_to_dim_0_audit_check >> [launch_load_ldg_to_dim_0_audit_check_sensor]
    launch_load_ldg_to_dim_0_audit_check_sensor >> [launch_load_ldg_to_dim_1_run_sql]
    launch_load_ldg_to_dim_1_run_sql >> [launch_load_ldg_to_dim_1_run_sql_sensor]
    launch_load_ldg_to_dim_1_run_sql_sensor >> [launch_load_ldg_to_dim_2_end_batch]
    launch_load_ldg_to_dim_2_end_batch >> [launch_load_ldg_to_dim_2_end_batch_sensor]

