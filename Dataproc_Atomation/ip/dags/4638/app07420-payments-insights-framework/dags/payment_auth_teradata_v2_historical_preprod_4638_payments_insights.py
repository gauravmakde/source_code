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
cron = None
dag_id = 'payment_auth_teradata_v2_historical_preprod_4638_payments_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 5,
    'description': 'payment_auth_teradata_v2_historical_preprod_4638_payments_insights DAG Description',
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
    parent_dag_name="payment_auth_teradata_v2_historical_preprod_4638_payments_insights",
    child_dag_name="create_persistent_cluster_napcp_etl",
    cluster_conn_id="TECH_ISF_NAP_CREDIT_PAYMENTS_ETL_CLUSTER_V2_PROD",
    cluster_template="napcp-etl",
    default_args=default_args))


    livy_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job = LivyOperator(
    task_id='payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job',
    app_file="s3a://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/user_config/argument_payment_auth_teradata_v2_historical_preprod_4638_payments_insights_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='payment_auth_teradata_v2_historical_preprod_4638_payments_insights_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.parquet.writeLegacyFormat': 'true', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_LIVY_V2_PROD',
    task_id='payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job') }}")

    tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)


    tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_load_tbl = SSHOperator(
    task_id='tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PREPROD_NAP_UTL -j PAYMENT_AUTHORIZATION_HDR_V2_LDG_TPT_ENGINE_JOB_PREPROD -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_CUSTPAYMNT_BATCH_PREPROD -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PREPROD_PWD)\" -f s3://tf-nap-preprod-payments-object-model-presentation/data/csv/customer-payment-authorization-analytical-avro-preprod/PAYMENT_AUTHORIZATION_HDR_V2_LDG/*.csv -o UPDATE -r TD-UTILITIES-EC2",
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT'),
    retries=2)


    tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)


    tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_load_tbl = SSHOperator(
    task_id='tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PREPROD_NAP_UTL -j PAYMENT_AUTHORIZATION_TENDER_V2_LDG_TPT_ENGINE_JOB_PREPROD -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_CUSTPAYMNT_BATCH_PREPROD -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PREPROD_PWD)\" -f s3://tf-nap-preprod-payments-object-model-presentation/data/csv/customer-payment-authorization-analytical-avro-preprod/PAYMENT_AUTHORIZATION_TENDER_V2_LDG/*.csv -o UPDATE -r TD-UTILITIES-EC2",
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT'),
    retries=2)


    launch_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job',
    job_name='payment-auth-teradata-hdr-stg-to-teradata-hdr-fct-stage-teradat',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/payment_auth_teradata_v2_historical_preprod_4638_payments_insights_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_v2_preprod.sql', '--bucket', 'tf-nap-prod-credit-payments-isf-airflow', '--executor', 'teradata', '--host', 'tdnapprodcop1.nordstrom.net', '--database', 'PREPROD_NAP_FCT', '--vault_path', 'data/application/APP07420/4638/payments/insights/preprod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local.'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'app07420'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-app07420-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'payment_auth_teradata_v2_historical_preprod_4638_payments_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')


    launch_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='payment-auth-teradata-hdr-stg-to-teradata-hdr-fct-stage-teradat',
    nsk_cluster='nsk-gumbo-prod')


    launch_payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job',
    job_name='payment-auth-teradata-tender-stg-to-teradata-tender-fct-stage-t',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/payment_auth_teradata_v2_historical_preprod_4638_payments_insights_payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_payment_auth_teradata_tender_stg_to_teradata_tender_fct_v2_preprod.sql', '--bucket', 'tf-nap-prod-credit-payments-isf-airflow', '--executor', 'teradata', '--host', 'tdnapprodcop1.nordstrom.net', '--database', 'PREPROD_NAP_FCT', '--vault_path', 'data/application/APP07420/4638/payments/insights/preprod/teradata/teradata_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local.'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'app07420'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-app07420-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'payment_auth_teradata_v2_historical_preprod_4638_payments_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::594668836985:role/k8s/tf-nap-prod-payments-k8s'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'payments', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='isf-pipeline-sa',
    nsk_cluster='nsk-gumbo-prod')


    launch_payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAP_PAYMENTS_K8S_NSK_CONN_V2_PROD',
    namespace='app07420',
    task_id='payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='payment-auth-teradata-tender-stg-to-teradata-tender-fct-stage-t',
    nsk_cluster='nsk-gumbo-prod')


    create_persistent_cluster_napcp_etl >> [livy_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job]
    livy_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job >> [livy_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job_sensor]
    livy_payment_auth_kafka_to_teradata_stg_historical_stage_hive_to_s3_csv_job_sensor >> [tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_control_tbl]
    tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_control_tbl >> [tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_load_tbl]
    tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_hdr_job_load_tbl >> [tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_control_tbl]
    tpt_jdbc_insert_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_control_tbl >> [tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_load_tbl]
    tpt_payment_auth_kafka_to_teradata_stg_historical_stage_s3_csv_to_teradata_tender_job_load_tbl >> [launch_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job, launch_payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job]
    launch_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job >> [launch_payment_auth_teradata_hdr_stg_to_teradata_hdr_fct_stage_teradata_hdr_stg_to_teradata_hdr_fct_job_sensor]
    launch_payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job >> [launch_payment_auth_teradata_tender_stg_to_teradata_tender_fct_stage_teradata_tender_stg_to_teradata_tender_fct_job_sensor]

