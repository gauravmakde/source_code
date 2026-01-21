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
cron = '56 03 * * *'
dag_id = 'store_dim_delta_load_2656_napstore_insights'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

default_args = {
    'retries': 3,
    'description': 'store_dim_delta_load_2656_napstore_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['nap_store_experience_analytics@nordstrom.pagerduty.com'],
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
    

    create_persistent_cluster_nsp_etl = SubDagOperator(
    task_id='create_persistent_cluster_nsp_etl',
    subdag=EtlClusterSubdag(
    parent_dag_name="store_dim_delta_load_2656_napstore_insights",
    child_dag_name="create_persistent_cluster_nsp_etl",
    cluster_conn_id="TECH_ISF_NAP_STORE_TEAM_NSP_ETL_CLUSTER_PROD",
    cluster_template="nsp-etl",
    default_args=default_args))


    livy_store_dim_delta_job_1_convert = LivyOperator(
    task_id='store_dim_delta_job_1_convert',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_store_dim_delta_load_2656_napstore_insights_store_dim_delta_job_1_convert.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='store_dim_delta_load_2656_napstore_insights_store_dim_delta_job_1_convert_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_store_dim_delta_job_1_convert_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='store_dim_delta_job_1_convert_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='store_dim_delta_job_1_convert') }}")

    livy_store_dim_delta_job_2_merge = LivyOperator(
    task_id='store_dim_delta_job_2_merge',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_store_dim_delta_load_2656_napstore_insights_store_dim_delta_job_2_merge.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='store_dim_delta_load_2656_napstore_insights_store_dim_delta_job_2_merge_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_store_dim_delta_job_2_merge_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='store_dim_delta_job_2_merge_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='store_dim_delta_job_2_merge') }}")

    create_persistent_cluster_nsp_etl >> [livy_store_dim_delta_job_1_convert]
    livy_store_dim_delta_job_1_convert >> [livy_store_dim_delta_job_1_convert_sensor]
    livy_store_dim_delta_job_1_convert_sensor >> [livy_store_dim_delta_job_2_merge]
    livy_store_dim_delta_job_2_merge >> [livy_store_dim_delta_job_2_merge_sensor]

