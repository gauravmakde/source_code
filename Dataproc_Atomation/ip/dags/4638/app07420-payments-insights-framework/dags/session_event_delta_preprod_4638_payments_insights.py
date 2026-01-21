# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        20d627573a3de4fe423c6691cdf281f130030d2d
# CI pipeline URL:      https://git.jwn.app/TM01164/nap-payments/app07420-payments-insights-framework/-/pipelines/7239485
# CI commit timestamp:  2024-10-16T12:42:22+00:00
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
sql_path = python_root_module_path + '4638/payments/insights/sql/'
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(python_root_module_path)
sys.path.append(python_module_path)
path = os.path.split(__file__)[0]

from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status


start_date = pendulum.datetime(2022, 2, 24)
cron = '0 */4 * * *'
dag_id = 'session_event_delta_preprod_4638_payments_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 5,
    'description': 'session_event_delta_preprod_4638_payments_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['credit-and-payments-p-aaaak7svy2d5ilbjwirhkdnqxa@nordstrom.slack.com'],
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
    parent_dag_name="session_event_delta_preprod_4638_payments_insights",
    child_dag_name="create_persistent_cluster_napcp_etl",
    cluster_conn_id="TECH_ISF_NAP_CREDIT_PAYMENTS_ETL_CLUSTER_V2_PROD",
    cluster_template="napcp-etl",
    default_args=default_args))


    livy_session_event_batch_start_delta_session_event_batch_start_delta_job = LivyOperator(
    task_id='session_event_batch_start_delta_session_event_batch_start_delta_job',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_session_event_batch_start_delta_session_event_batch_start_delta_job.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_session_event_batch_start_delta_session_event_batch_start_delta_job_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_session_event_batch_start_delta_session_event_batch_start_delta_job_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='session_event_batch_start_delta_session_event_batch_start_delta_job_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='session_event_batch_start_delta_session_event_batch_start_delta_job') }}")

    livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job = LivyOperator(
    task_id='session_obj_flatten_delta_session_kafka_to_s3_delta_job',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_session_obj_flatten_delta_session_kafka_to_s3_delta_job.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_session_obj_flatten_delta_session_kafka_to_s3_delta_job_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='session_obj_flatten_delta_session_kafka_to_s3_delta_job_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='session_obj_flatten_delta_session_kafka_to_s3_delta_job') }}")

    livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta = LivyOperator(
    task_id='customer_activity_added_to_bag_session_delta_added_to_bag_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_added_to_bag_session_delta_added_to_bag_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_added_to_bag_session_delta_added_to_bag_session_delta') }}")

    livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta = LivyOperator(
    task_id='customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta') }}")

    livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta = LivyOperator(
    task_id='customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta') }}")

    livy_customer_activity_arrived_session_delta_arrived_session_delta = LivyOperator(
    task_id='customer_activity_arrived_session_delta_arrived_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_arrived_session_delta_arrived_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_arrived_session_delta_arrived_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_arrived_session_delta_arrived_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_arrived_session_delta_arrived_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_arrived_session_delta_arrived_session_delta') }}")

    livy_customer_activity_authenticated_session_delta_authenticated_session_delta = LivyOperator(
    task_id='customer_activity_authenticated_session_delta_authenticated_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_authenticated_session_delta_authenticated_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_authenticated_session_delta_authenticated_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_authenticated_session_delta_authenticated_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_authenticated_session_delta_authenticated_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_authenticated_session_delta_authenticated_session_delta') }}")

    livy_customer_activity_engaged_session_delta_engaged_session_delta = LivyOperator(
    task_id='customer_activity_engaged_session_delta_engaged_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_engaged_session_delta_engaged_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_engaged_session_delta_engaged_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_engaged_session_delta_engaged_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_engaged_session_delta_engaged_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_engaged_session_delta_engaged_session_delta') }}")

    livy_customer_activity_identified_session_delta_identified_session_delta = LivyOperator(
    task_id='customer_activity_identified_session_delta_identified_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_identified_session_delta_identified_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_identified_session_delta_identified_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_identified_session_delta_identified_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_identified_session_delta_identified_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_identified_session_delta_identified_session_delta') }}")

    livy_customer_activity_impressed_session_delta_impressed_session_delta = LivyOperator(
    task_id='customer_activity_impressed_session_delta_impressed_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_impressed_session_delta_impressed_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_impressed_session_delta_impressed_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_impressed_session_delta_impressed_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_impressed_session_delta_impressed_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_impressed_session_delta_impressed_session_delta') }}")

    livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta = LivyOperator(
    task_id='customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta') }}")

    livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta = LivyOperator(
    task_id='customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta') }}")

    livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta = LivyOperator(
    task_id='customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta') }}")

    livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta = LivyOperator(
    task_id='customer_activity_searchperformed_session_delta_searchperformed_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_searchperformed_session_delta_searchperformed_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_searchperformed_session_delta_searchperformed_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_searchperformed_session_delta_searchperformed_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_searchperformed_session_delta_searchperformed_session_delta') }}")

    livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta = LivyOperator(
    task_id='customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta') }}")

    livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta = LivyOperator(
    task_id='customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta') }}")

    livy_session_event_batch_end_delta_session_event_batch_end_delta_job = LivyOperator(
    task_id='session_event_batch_end_delta_session_event_batch_end_delta_job',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_session_event_delta_preprod_4638_payments_insights_session_event_batch_end_delta_session_event_batch_end_delta_job.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='session_event_delta_preprod_4638_payments_insights_session_event_batch_end_delta_session_event_batch_end_delta_job_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_session_event_batch_end_delta_session_event_batch_end_delta_job_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='session_event_batch_end_delta_session_event_batch_end_delta_job_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='session_event_batch_end_delta_session_event_batch_end_delta_job') }}")

    create_persistent_cluster_napcp_etl >> [livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta, livy_customer_activity_arrived_session_delta_arrived_session_delta, livy_customer_activity_authenticated_session_delta_authenticated_session_delta, livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta, livy_customer_activity_engaged_session_delta_engaged_session_delta, livy_customer_activity_identified_session_delta_identified_session_delta, livy_customer_activity_impressed_session_delta_impressed_session_delta, livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta, livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta, livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta, livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta, livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta, livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta, livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta, livy_session_event_batch_end_delta_session_event_batch_end_delta_job, livy_session_event_batch_start_delta_session_event_batch_start_delta_job, livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job]
    livy_session_event_batch_start_delta_session_event_batch_start_delta_job >> [livy_session_event_batch_start_delta_session_event_batch_start_delta_job_sensor]
    livy_session_event_batch_start_delta_session_event_batch_start_delta_job_sensor >> [livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job]
    livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job >> [livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job_sensor]
    livy_session_obj_flatten_delta_session_kafka_to_s3_delta_job_sensor >> [livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta, livy_customer_activity_arrived_session_delta_arrived_session_delta, livy_customer_activity_authenticated_session_delta_authenticated_session_delta, livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta, livy_customer_activity_engaged_session_delta_engaged_session_delta, livy_customer_activity_identified_session_delta_identified_session_delta, livy_customer_activity_impressed_session_delta_impressed_session_delta, livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta, livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta, livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta, livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta, livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta, livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta, livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta]
    livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta >> [livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta_sensor]
    livy_customer_activity_added_to_bag_session_delta_added_to_bag_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta >> [livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta_sensor]
    livy_customer_activity_removed_from_bag_session_delta_removed_from_bag_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta >> [livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta_sensor]
    livy_customer_activity_ordersubmitted_session_delta_ordersubmitted_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_arrived_session_delta_arrived_session_delta >> [livy_customer_activity_arrived_session_delta_arrived_session_delta_sensor]
    livy_customer_activity_arrived_session_delta_arrived_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_authenticated_session_delta_authenticated_session_delta >> [livy_customer_activity_authenticated_session_delta_authenticated_session_delta_sensor]
    livy_customer_activity_authenticated_session_delta_authenticated_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_engaged_session_delta_engaged_session_delta >> [livy_customer_activity_engaged_session_delta_engaged_session_delta_sensor]
    livy_customer_activity_engaged_session_delta_engaged_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_identified_session_delta_identified_session_delta >> [livy_customer_activity_identified_session_delta_identified_session_delta_sensor]
    livy_customer_activity_identified_session_delta_identified_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_impressed_session_delta_impressed_session_delta >> [livy_customer_activity_impressed_session_delta_impressed_session_delta_sensor]
    livy_customer_activity_impressed_session_delta_impressed_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta >> [livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta_sensor]
    livy_customer_activity_product_detail_viewed_session_delta_product_detail_viewed_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta >> [livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta_sensor]
    livy_customer_activity_product_summary_collection_viewed_session_delta_product_summary_collection_viewed_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta >> [livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta_sensor]
    livy_customer_activity_product_summary_selected_session_delta_product_summary_selected_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta >> [livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta_sensor]
    livy_customer_activity_searchperformed_session_delta_searchperformed_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta >> [livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta_sensor]
    livy_customer_activity_browse_search_results_generated_session_delta_browse_search_results_generated_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta >> [livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta_sensor]
    livy_customer_activity_keyword_search_results_generated_session_delta_keyword_search_results_generated_session_delta_sensor >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job]
    livy_session_event_batch_end_delta_session_event_batch_end_delta_job >> [livy_session_event_batch_end_delta_session_event_batch_end_delta_job_sensor]

