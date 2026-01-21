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
cron = '0 10 * * 3'
dag_id = 'fraud_worldpay_bin_preprod_4638_payments_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 5,
    'description': 'fraud_worldpay_bin_preprod_4638_payments_insights DAG Description',
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

    tpt_jdbc_insert_read_s3_write_td_stage_s3_csv_to_td_stg_job_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_read_s3_write_td_stage_s3_csv_to_td_stg_job_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)


    tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl = SSHOperator(
    task_id='tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PREPROD_NAP_PYMNT_FRAUD_UTL -j WORLDPAY_BIN_LDG_TPT_ENGINE_JOB_PREPROD -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_PYMNT_FRAUD_BATCH_PREPROD -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PWD)\" -f s3://tf-nap-preprod-master-merchant-bin-files/raw/* -o UPDATE -r TD-UTILITIES-EC2",
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT'),
    retries=2)


    launch_read_s3_write_td_stage_td_stg_to_td_dim_job = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_s3_write_td_stage_td_stg_to_td_dim_job',
    job_name='read-s3-write-td-stage-td-stg-to-td-dim-job-fraud-worldpay-bin',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/fraud_worldpay_bin_preprod_4638_payments_insights_read_s3_write_td_stage_fraud_worldpay_bin_dim_preprod.sql', '--bucket', 'tf-nap-prod-credit-payments-isf-airflow', '--executor', 'teradata', '--host', 'tdnapprodcop1.nordstrom.net', '--database', 'PRD_NAP_PYMNT_FRAUD_BASE_VWS', '--vault_path', 'data/application/APP07420/4638/payments/insights/prod/teradata/payment_fraud/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local.'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'app07420'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-app07420-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'fraud_worldpay_bin_preprod_4638_payments_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')


    launch_read_s3_write_td_stage_td_stg_to_td_dim_job_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_s3_write_td_stage_td_stg_to_td_dim_job_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='read-s3-write-td-stage-td-stg-to-td-dim-job-fraud-worldpay-bin',
    nsk_cluster='nsk-gumbo-prod')


    create_persistent_cluster_napcp_etl = SubDagOperator(
    task_id='create_persistent_cluster_napcp_etl',
    subdag=EtlClusterSubdag(
    parent_dag_name="fraud_worldpay_bin_preprod_4638_payments_insights",
    child_dag_name="create_persistent_cluster_napcp_etl",
    cluster_conn_id="TECH_ISF_NAP_CREDIT_PAYMENTS_ETL_CLUSTER_V2_PROD",
    cluster_template="napcp-etl",
    default_args=default_args))


    livy_read_td_write_td_stage_td_dim_to_td_stg_job = LivyOperator(
    task_id='read_td_write_td_stage_td_dim_to_td_stg_job',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_td_dim_to_td_stg_job.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_td_dim_to_td_stg_job_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_read_td_write_td_stage_td_dim_to_td_stg_job_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='read_td_write_td_stage_td_dim_to_td_stg_job_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='read_td_write_td_stage_td_dim_to_td_stg_job') }}")

    launch_read_td_write_td_stage_td_stg_to_td_dim_job = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_td_write_td_stage_td_stg_to_td_dim_job',
    job_name='read-td-write-td-stage-td-stg-to-td-dim-job-fraud-worldpay-bin',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/fraud_worldpay_bin_preprod_4638_payments_insights_read_td_write_td_stage_fraud_worldpay_rollup_dim_preprod.sql', '--bucket', 'tf-nap-prod-credit-payments-isf-airflow', '--executor', 'teradata', '--host', 'tdnapprodcop1.nordstrom.net', '--database', 'PRD_NAP_PYMNT_FRAUD_BASE_VWS', '--vault_path', 'data/application/APP07420/4638/payments/insights/prod/teradata/payment_fraud/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local.'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'app07420'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-app07420-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'fraud_worldpay_bin_preprod_4638_payments_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')


    launch_read_td_write_td_stage_td_stg_to_td_dim_job_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_td_write_td_stage_td_stg_to_td_dim_job_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='read-td-write-td-stage-td-stg-to-td-dim-job-fraud-worldpay-bin',
    nsk_cluster='nsk-gumbo-prod')


    launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_s3_write_s3_stage_s3_raw_to_s3_processed_job',
    job_name='read-s3-write-s3-stage-s3-raw-to-s3-processed-job-fraud-worldpa',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app07420/tm01164-nap-payments-app07420-payments-insights-framework/prod/python/fraud_worldpay_bin:1.1.0',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python/fraud_worldpay_bin/main.py'],
    resources={'limits': {'cpu': 1, 'memory': '1Gi' },'requests': {'cpu': 1, 'memory': '1Gi'}},
    common_envs=[],
    task_envs=[{'name': 'RESET_S3_BUCKET', 'value': 'tf-nap-prod-reset-landing'}, {'name': 'FRAUD_WORLDPAY_BIN_BUCKET', 'value': 'tf-nap-prod-master-merchant-bin-files'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')


    launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='read_s3_write_s3_stage_s3_raw_to_s3_processed_job_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='read-s3-write-s3-stage-s3-raw-to-s3-processed-job-fraud-worldpa',
    nsk_cluster='nsk-gumbo-prod')


    tpt_jdbc_insert_read_s3_write_td_stage_s3_csv_to_td_stg_job_control_tbl >> [tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl]
    tpt_read_s3_write_td_stage_s3_csv_to_td_stg_job_load_tbl >> [launch_read_s3_write_td_stage_td_stg_to_td_dim_job]
    launch_read_s3_write_td_stage_td_stg_to_td_dim_job >> [launch_read_s3_write_td_stage_td_stg_to_td_dim_job_sensor]
    launch_read_s3_write_td_stage_td_stg_to_td_dim_job_sensor >> [create_persistent_cluster_napcp_etl]
    create_persistent_cluster_napcp_etl >> [livy_read_td_write_td_stage_td_dim_to_td_stg_job]
    livy_read_td_write_td_stage_td_dim_to_td_stg_job >> [livy_read_td_write_td_stage_td_dim_to_td_stg_job_sensor]
    livy_read_td_write_td_stage_td_dim_to_td_stg_job_sensor >> [launch_read_td_write_td_stage_td_stg_to_td_dim_job]
    launch_read_td_write_td_stage_td_stg_to_td_dim_job >> [launch_read_td_write_td_stage_td_stg_to_td_dim_job_sensor]
    launch_read_td_write_td_stage_td_stg_to_td_dim_job_sensor >> [launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job]
    launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job >> [launch_read_s3_write_s3_stage_s3_raw_to_s3_processed_job_sensor]

