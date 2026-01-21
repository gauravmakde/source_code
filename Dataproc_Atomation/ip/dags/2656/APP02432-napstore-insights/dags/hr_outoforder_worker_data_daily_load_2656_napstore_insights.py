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

sys.path.append(path + '/../')
from modules import branch_hire_cancelled_stage
from modules import branch_employee_stage
from modules import branch_hired_stage
from modules import branch_job_stage
from modules import branch_line_position_stage
from modules import branch_work_contact_stage
from modules import branch_org_stage
from airflow.operators.python import BranchPythonOperator

start_date = pendulum.datetime(2022, 2, 24)
cron = '0 9 * * *'
dag_id = 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

default_args = {
    'retries': 3,
    'description': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights DAG Description',
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
    

    create_persistent_cluster_nsp_etl = SubDagOperator(
    task_id='create_persistent_cluster_nsp_etl',
    subdag=EtlClusterSubdag(
    parent_dag_name="hr_outoforder_worker_data_daily_load_2656_napstore_insights",
    child_dag_name="create_persistent_cluster_nsp_etl",
    cluster_conn_id="TECH_ISF_NAP_STORE_TEAM_NSP_ETL_CLUSTER_PROD",
    cluster_template="nsp-etl",
    default_args=default_args))


    livy_branch1_dummy_task = LivyOperator(
    task_id='branch1_dummy_task',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch1_dummy_task.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch1_dummy_task_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_branch1_dummy_task_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='branch1_dummy_task_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='branch1_dummy_task') }}")

    livy_branch2_dummy_task = LivyOperator(
    task_id='branch2_dummy_task',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch2_dummy_task.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch2_dummy_task_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_branch2_dummy_task_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='branch2_dummy_task_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='branch2_dummy_task') }}")

    livy_branch3_dummy_task = LivyOperator(
    task_id='branch3_dummy_task',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch3_dummy_task.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_branch3_dummy_task_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_branch3_dummy_task_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='branch3_dummy_task_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='branch3_dummy_task') }}")

    stage_branching_employee_stage_branching_job1_operator = BranchPythonOperator(
    task_id='stage_branching_employee_stage_branching_job1',
    python_callable=branch_employee_stage.callable_func_name)


    livy_employee_details_stage_load_0_employee_details_stg_tables = LivyOperator(
    task_id='employee_details_stage_load_0_employee_details_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_load_0_employee_details_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_load_0_employee_details_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_employee_details_stage_load_0_employee_details_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='employee_details_stage_load_0_employee_details_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='employee_details_stage_load_0_employee_details_stg_tables') }}")

    launch_employee_details_stage_load_1_employee_details_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='employee_details_stage_load_1_employee_details_dim_tables',
    job_name='employee-details-stage-load-1-employee-details-dim-tables-hr-ou',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_employee_details_stage_hr_employee_status_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_employee_details_stage_load_1_employee_details_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='employee_details_stage_load_1_employee_details_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='employee-details-stage-load-1-employee-details-dim-tables-hr-ou',
    nsk_cluster='nsk-miso-prod')


    stage_branching_org_stage_branching_job2_operator = BranchPythonOperator(
    task_id='stage_branching_org_stage_branching_job2',
    python_callable=branch_org_stage.callable_func_name)


    livy_org_details_stage_load_2_org_details_stg_tables = LivyOperator(
    task_id='org_details_stage_load_2_org_details_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_load_2_org_details_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_load_2_org_details_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_org_details_stage_load_2_org_details_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='org_details_stage_load_2_org_details_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='org_details_stage_load_2_org_details_stg_tables') }}")

    launch_org_details_stage_load_3_org_details_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='org_details_stage_load_3_org_details_dim_tables',
    job_name='org-details-stage-load-3-org-details-dim-tables-hr-outoforder-w',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_org_details_stage_hr_org_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_org_details_stage_load_3_org_details_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='org_details_stage_load_3_org_details_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='org-details-stage-load-3-org-details-dim-tables-hr-outoforder-w',
    nsk_cluster='nsk-miso-prod')


    stage_branching_job_stage_branching_job3_operator = BranchPythonOperator(
    task_id='stage_branching_job_stage_branching_job3',
    python_callable=branch_job_stage.callable_func_name)


    livy_job_details_stage_load_4_job_details_stg_tables = LivyOperator(
    task_id='job_details_stage_load_4_job_details_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_load_4_job_details_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_load_4_job_details_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_job_details_stage_load_4_job_details_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='job_details_stage_load_4_job_details_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='job_details_stage_load_4_job_details_stg_tables') }}")

    launch_job_details_stage_load_5_job_details_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='job_details_stage_load_5_job_details_dim_tables',
    job_name='job-details-stage-load-5-job-details-dim-tables-hr-outoforder-w',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_job_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_org_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_job_details_stage_hr_line_position_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_job_details_stage_load_5_job_details_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='job_details_stage_load_5_job_details_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='job-details-stage-load-5-job-details-dim-tables-hr-outoforder-w',
    nsk_cluster='nsk-miso-prod')


    stage_branching_line_position_stage_branching_job4_operator = BranchPythonOperator(
    task_id='stage_branching_line_position_stage_branching_job4',
    python_callable=branch_line_position_stage.callable_func_name)


    livy_line_position_details_stage_load_6_lineposition_details_stg_tables = LivyOperator(
    task_id='line_position_details_stage_load_6_lineposition_details_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_load_6_lineposition_details_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_load_6_lineposition_details_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_line_position_details_stage_load_6_lineposition_details_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='line_position_details_stage_load_6_lineposition_details_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='line_position_details_stage_load_6_lineposition_details_stg_tables') }}")

    launch_line_position_details_stage_load_7_lineposition_details_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='line_position_details_stage_load_7_lineposition_details_dim_tables',
    job_name='line-position-details-stage-load-7-lineposition-details-dim-tab',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_line_position_details_stage_hr_line_position_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_line_position_details_stage_load_7_lineposition_details_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='line_position_details_stage_load_7_lineposition_details_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='line-position-details-stage-load-7-lineposition-details-dim-tab',
    nsk_cluster='nsk-miso-prod')


    stage_branching_work_contact_stage_branching_job5_operator = BranchPythonOperator(
    task_id='stage_branching_work_contact_stage_branching_job5',
    python_callable=branch_work_contact_stage.callable_func_name)


    livy_work_contact_details_stage_load_8_workcontact_details_stg_tables = LivyOperator(
    task_id='work_contact_details_stage_load_8_workcontact_details_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_load_8_workcontact_details_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_load_8_workcontact_details_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_work_contact_details_stage_load_8_workcontact_details_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='work_contact_details_stage_load_8_workcontact_details_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='work_contact_details_stage_load_8_workcontact_details_stg_tables') }}")

    launch_work_contact_details_stage_load_9_workcontact_details_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='work_contact_details_stage_load_9_workcontact_details_dim_tables',
    job_name='work-contact-details-stage-load-9-workcontact-details-dim-table',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_work_contact_details_stage_hr_work_contact_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_work_contact_details_stage_load_9_workcontact_details_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='work_contact_details_stage_load_9_workcontact_details_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='work-contact-details-stage-load-9-workcontact-details-dim-table',
    nsk_cluster='nsk-miso-prod')


    stage_branching_hire_details_stage_branching_job6_operator = BranchPythonOperator(
    task_id='stage_branching_hire_details_stage_branching_job6',
    python_callable=branch_hired_stage.callable_func_name)


    livy_hired_details_stage_load_hired_details_a_stg_tables = LivyOperator(
    task_id='hired_details_stage_load_hired_details_a_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_load_hired_details_a_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_load_hired_details_a_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_hired_details_stage_load_hired_details_a_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='hired_details_stage_load_hired_details_a_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='hired_details_stage_load_hired_details_a_stg_tables') }}")

    launch_hired_details_stage_load_hired_details_b_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='hired_details_stage_load_hired_details_b_dim_tables',
    job_name='hired-details-stage-load-hired-details-b-dim-tables-hr-outoford',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_employee_status_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_org_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_job_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_line_position_details_load_dim.sql,apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hired_details_stage_hr_work_contact_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_hired_details_stage_load_hired_details_b_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='hired_details_stage_load_hired_details_b_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='hired-details-stage-load-hired-details-b-dim-tables-hr-outoford',
    nsk_cluster='nsk-miso-prod')


    stage_branching_hire_cancelled_details_stage_branching_job7_operator = BranchPythonOperator(
    task_id='stage_branching_hire_cancelled_details_stage_branching_job7',
    python_callable=branch_hire_cancelled_stage.callable_func_name)


    livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables = LivyOperator(
    task_id='hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables') }}")

    launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables',
    job_name='hire-cancelled-details-stage-load-10-worker-hire-cancelled-dim',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/sql/hr_outoforder_worker_data_daily_load_2656_napstore_insights_hire_cancelled_details_stage_hr_hire_cancelled_details_load_dim.sql', '--bucket', 'tf-nap-prod-airflow-nsk-insights', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_HR_STG', '--vault_path', 'data/application/APP02432/2656/napstore/APP02432-napstore-insights/prod/teradata/teradata_hr_db_credentials', '--vault_host', 'https://vpce-01f9458aff654a035-ng0ubact.vpce-svc-00627e41a40fb1e3c.us-west-2.vpce.amazonaws.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP02432'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'onehop-insights-framework-APP02432-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'hr_outoforder_worker_data_daily_load_2656_napstore_insights'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::122990593628:role/nap-hr-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'napstore', 'customer_project_name': 'insights'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstore-insights',
    nsk_cluster='nsk-miso-prod')


    launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORE_INSIGHTS_CONN_PROD',
    namespace='app02432',
    task_id='hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='hire-cancelled-details-stage-load-10-worker-hire-cancelled-dim',
    nsk_cluster='nsk-miso-prod')


    livy_stage_final_dummy_task = LivyOperator(
    task_id='stage_final_dummy_task',
    app_file="s3a://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.0.1.jar",
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://tf-nap-prod-airflow-nsk-insights/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/2656/napstore/insights/user_config/argument_hr_outoforder_worker_data_daily_load_2656_napstore_insights_stage_final_dummy_task.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='15g',
    driver_cores=2,
    executor_memory='12g',
    executor_cores=5,
    session_name='hr_outoforder_worker_data_daily_load_2656_napstore_insights_stage_final_dummy_task_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_stage_final_dummy_task_sensor = LivySensor(
    conn_id='TECH_ISF_NAP_STORE_TEAM_NSP_ETL_LIVY_PROD',
    task_id='stage_final_dummy_task_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='stage_final_dummy_task') }}")

    create_persistent_cluster_nsp_etl >> [livy_branch1_dummy_task, livy_branch2_dummy_task, livy_branch3_dummy_task, livy_employee_details_stage_load_0_employee_details_stg_tables, livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables, livy_hired_details_stage_load_hired_details_a_stg_tables, livy_job_details_stage_load_4_job_details_stg_tables, livy_line_position_details_stage_load_6_lineposition_details_stg_tables, livy_org_details_stage_load_2_org_details_stg_tables, livy_stage_final_dummy_task, livy_work_contact_details_stage_load_8_workcontact_details_stg_tables]
    livy_branch1_dummy_task >> [livy_branch1_dummy_task_sensor]
    livy_branch1_dummy_task_sensor >> [stage_branching_employee_stage_branching_job1_operator, stage_branching_org_stage_branching_job2_operator]
    livy_branch2_dummy_task >> [livy_branch2_dummy_task_sensor]
    livy_branch2_dummy_task_sensor >> [stage_branching_job_stage_branching_job3_operator, stage_branching_line_position_stage_branching_job4_operator]
    livy_branch3_dummy_task >> [livy_branch3_dummy_task_sensor]
    livy_branch3_dummy_task_sensor >> [stage_branching_hire_cancelled_details_stage_branching_job7_operator, stage_branching_hire_details_stage_branching_job6_operator, stage_branching_work_contact_stage_branching_job5_operator]
    stage_branching_employee_stage_branching_job1_operator >> [livy_branch2_dummy_task, livy_employee_details_stage_load_0_employee_details_stg_tables]
    livy_employee_details_stage_load_0_employee_details_stg_tables >> [livy_employee_details_stage_load_0_employee_details_stg_tables_sensor]
    livy_employee_details_stage_load_0_employee_details_stg_tables_sensor >> [launch_employee_details_stage_load_1_employee_details_dim_tables]
    launch_employee_details_stage_load_1_employee_details_dim_tables >> [launch_employee_details_stage_load_1_employee_details_dim_tables_sensor]
    stage_branching_org_stage_branching_job2_operator >> [livy_branch2_dummy_task, livy_org_details_stage_load_2_org_details_stg_tables]
    livy_org_details_stage_load_2_org_details_stg_tables >> [livy_org_details_stage_load_2_org_details_stg_tables_sensor]
    livy_org_details_stage_load_2_org_details_stg_tables_sensor >> [launch_org_details_stage_load_3_org_details_dim_tables]
    launch_org_details_stage_load_3_org_details_dim_tables >> [launch_org_details_stage_load_3_org_details_dim_tables_sensor]
    stage_branching_job_stage_branching_job3_operator >> [livy_branch3_dummy_task, livy_job_details_stage_load_4_job_details_stg_tables]
    livy_job_details_stage_load_4_job_details_stg_tables >> [livy_job_details_stage_load_4_job_details_stg_tables_sensor]
    livy_job_details_stage_load_4_job_details_stg_tables_sensor >> [launch_job_details_stage_load_5_job_details_dim_tables]
    launch_job_details_stage_load_5_job_details_dim_tables >> [launch_job_details_stage_load_5_job_details_dim_tables_sensor]
    stage_branching_line_position_stage_branching_job4_operator >> [livy_branch3_dummy_task, livy_line_position_details_stage_load_6_lineposition_details_stg_tables]
    livy_line_position_details_stage_load_6_lineposition_details_stg_tables >> [livy_line_position_details_stage_load_6_lineposition_details_stg_tables_sensor]
    livy_line_position_details_stage_load_6_lineposition_details_stg_tables_sensor >> [launch_line_position_details_stage_load_7_lineposition_details_dim_tables]
    launch_line_position_details_stage_load_7_lineposition_details_dim_tables >> [launch_line_position_details_stage_load_7_lineposition_details_dim_tables_sensor]
    stage_branching_work_contact_stage_branching_job5_operator >> [livy_stage_final_dummy_task, livy_work_contact_details_stage_load_8_workcontact_details_stg_tables]
    livy_work_contact_details_stage_load_8_workcontact_details_stg_tables >> [livy_work_contact_details_stage_load_8_workcontact_details_stg_tables_sensor]
    livy_work_contact_details_stage_load_8_workcontact_details_stg_tables_sensor >> [launch_work_contact_details_stage_load_9_workcontact_details_dim_tables]
    launch_work_contact_details_stage_load_9_workcontact_details_dim_tables >> [launch_work_contact_details_stage_load_9_workcontact_details_dim_tables_sensor]
    stage_branching_hire_details_stage_branching_job6_operator >> [livy_hired_details_stage_load_hired_details_a_stg_tables, livy_stage_final_dummy_task]
    livy_hired_details_stage_load_hired_details_a_stg_tables >> [livy_hired_details_stage_load_hired_details_a_stg_tables_sensor]
    livy_hired_details_stage_load_hired_details_a_stg_tables_sensor >> [launch_hired_details_stage_load_hired_details_b_dim_tables]
    launch_hired_details_stage_load_hired_details_b_dim_tables >> [launch_hired_details_stage_load_hired_details_b_dim_tables_sensor]
    stage_branching_hire_cancelled_details_stage_branching_job7_operator >> [livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables, livy_stage_final_dummy_task]
    livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables >> [livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables_sensor]
    livy_hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables_sensor >> [launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables]
    launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables >> [launch_hire_cancelled_details_stage_load_10_worker_hire_cancelled_dim_tables_sensor]
    livy_stage_final_dummy_task >> [livy_stage_final_dummy_task_sensor]

