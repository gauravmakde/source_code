# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        5c2a4557c84c4af28c1bfccb2db0b4b187f82462
# CI pipeline URL:      https://git.jwn.app/TM01164/nap-credit/APP06788-credit-insights-framework/-/pipelines/7299882
# CI commit timestamp:  2024-10-22T21:04:42+00:00
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
from nordstrom.operators.livy_operator import LivyOperator
from nordstrom.sensors.livy_sensor import LivySensor
from nordstrom.subdags.ephemeral_cluster  import EtlClusterSubdag, get_cluster_connection_id
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable


env = os.environ.get('ENVIRONMENT', 'local')
# Fetch the data from config file
python_module_path = path.dirname(__file__).split('app08314-isf-airflow-dags')[0] + 'app08314-isf-airflow-dags/common_libraries/'
python_root_module_path = path.dirname(__file__).split('app08314-isf-airflow-dags')[0] + 'app08314-isf-airflow-dags/'
sql_path = python_root_module_path + '3678/credit/insights/sql/'
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(python_root_module_path)
sys.path.append(python_module_path)
path = os.path.split(__file__)[0]

from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status


start_date = pendulum.datetime(2022, 2, 24)
cron = '0 13 * * *'
dag_id = 'credit_connectors_s3_parquet_3678_credit_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 3,
    'description': 'credit_connectors_s3_parquet_3678_credit_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['credit-and-payments-o-aaaae5j73ev6b7jl2a6dakc5vm@nordstrom.slack.com'],
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

    create_persistent_cluster_napcp_etl = SubDagOperator(
    task_id='create_persistent_cluster_napcp_etl',
    subdag=EtlClusterSubdag(
    parent_dag_name="credit_connectors_s3_parquet_3678_credit_insights",
    child_dag_name="create_persistent_cluster_napcp_etl",
    cluster_conn_id="TECH_ISF_NAP_CREDIT_PAYMENTS_ETL_CLUSTER_V2_PROD",
    cluster_template="napcp-etl",
    default_args=default_args))


    livy_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job = LivyOperator(
    task_id='credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/3678/credit/insights/user_config/argument_credit_connectors_s3_parquet_3678_credit_insights_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='credit_connectors_s3_parquet_3678_credit_insights_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240', 'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


    livy_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job') }}")

    create_persistent_cluster_napcp_etl >> [livy_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job]
    livy_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job >> [livy_credit_connectors_s3_parquet_stage_0_kafka_to_s3_parquet_job_sensor]

