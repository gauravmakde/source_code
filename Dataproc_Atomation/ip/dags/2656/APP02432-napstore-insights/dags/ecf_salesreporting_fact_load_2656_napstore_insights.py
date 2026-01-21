# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        1effc4c9851f89ec95a1e4a434ce11daeb14e52a
# CI pipeline URL:      https://git.jwn.app/TM00787/app02432-store-analytics/APP02432-napstore-insights/-/pipelines/6728624
# CI commit timestamp:  2024-08-15T13:53:29+00:00
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
from nordstrom.operators.livy_operator import LivyOperator
from nordstrom.sensors.livy_sensor import LivySensor
from nordstrom.subdags.ephemeral_cluster  import EtlClusterSubdag, get_cluster_connection_id
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable


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
cron = '0 17 * * *'
dag_id = 'ecf_salesreporting_fact_load_2656_napstore_insights'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

default_args = {
    'retries': 3,
    'description': 'ecf_salesreporting_fact_load_2656_napstore_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['TECH_DAS_Customer_Selling@nordstrom.com', 'nap-customer-selling@nordstrom.pagerduty.com'],
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
    

    launch_m_load_fact = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='m_load_fact',
    job_name='m-load-fact-ecf-salesreporting-fact-load-2656-napstore-insights',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/ecf_salesreporting/ecf_salesreporting_fact_load_2656_napstore_insights_m_ecf_salesreporting_load_fact.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'ecf_salesreporting_fact_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_m_load_fact_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='m_load_fact_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='m-load-fact-ecf-salesreporting-fact-load-2656-napstore-insights',
    nsk_cluster='nsk-miso-prod')


    create_persistent_cluster_nsp_etl = SubDagOperator(
    task_id='create_persistent_cluster_nsp_etl',
    subdag=EtlClusterSubdag(
    parent_dag_name="ecf_salesreporting_fact_load_2656_napstore_insights",
    child_dag_name="create_persistent_cluster_nsp_etl",
    cluster_conn_id="TECH_ISF_NAP_STORE_TEAM_NSP_ETL_CLUSTER_PROD",
    cluster_template="nsp-etl",
    default_args=default_args))


    livy_m_load_postgres = LivyOperator(
    task_id='m_load_postgres',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_ecf_salesreporting_fact_load_2656_napstore_insights_m_load_postgres.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='ecf_salesreporting_fact_load_2656_napstore_insights_m_load_postgres_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_m_load_postgres_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='m_load_postgres_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='m_load_postgres') }}")

    launch_m_load_fact >> [launch_m_load_fact_sensor]
    launch_m_load_fact_sensor >> [create_persistent_cluster_nsp_etl]
    create_persistent_cluster_nsp_etl >> [livy_m_load_postgres]
    livy_m_load_postgres >> [livy_m_load_postgres_sensor]

