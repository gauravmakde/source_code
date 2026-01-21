# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        d9b8f16ac401d26734a9d3ba72d748402b955b80
# CI pipeline URL:      https://git.jwn.app/TM01189/app08649-store-inventory/app08649-insights-v2/-/pipelines/7192970
# CI commit timestamp:  2024-10-10T13:08:19+00:00
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
from nordstrom.operators.livy_operator import LivyOperator
from nordstrom.sensors.livy_sensor import LivySensor
from nordstrom.subdags.ephemeral_cluster  import EtlClusterSubdag, get_cluster_connection_id
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable


env = os.environ.get('ENVIRONMENT', 'local')
os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_SC_OUTBOUND_NEWRELIC_CONNECTION_ID_PROD'
# Fetch the data from config file
python_module_path=path.dirname(__file__).split('app08649-insights-v2-airflow-dags')[0]+'app08649-insights-v2-airflow-dags/common_libraries/'
python_root_module_path=path.dirname(__file__).split('app08649-insights-v2-airflow-dags')[0]+'app08649-insights-v2-airflow-dags/'
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(python_root_module_path)
sys.path.append(python_module_path)
path = os.path.split(__file__)[0]

from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status


start_date = pendulum.datetime(2022, 2, 24)
cron = '15 8 * * *'
dag_id = 'inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

default_args = {
    'retries': 3,
    'description': 'inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2 DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['nap-sco-high-pri@nordstrom.pagerduty.com'],
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
    

    launch_main_9_job_1_td_batch_start = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_1_td_batch_start',
    job_name='main-9-job-1-td-batch-start-inventory-store-carton-audited-1761',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/17610/DAS_SC_OUTBOUND/APP08649_insights_v2/sql/inventory_store_carton_audited/inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2_main_9_batch_start.sql', '--bucket', 'napstoreinvtry-prod-common-etl', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_FCT', '--vault_path', 'data/application/APP08649/shared/prod/teradata/nap/service_account', '--vault_host', 'https://prod-vault.vault.vip.nordstrom.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP08649'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'APP08649-insights-v2-APP08649-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::483375294780:role/napstorinvtry-k8s-controlled-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP08649_insights_v2'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstorinvtry-insights',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_1_td_batch_start_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_1_td_batch_start_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='main-9-job-1-td-batch-start-inventory-store-carton-audited-1761',
    nsk_cluster='nsk-gumbo-prod')


    create_persistent_cluster_scoisf_etl = SubDagOperator(
    task_id='create_persistent_cluster_scoisf_etl',
    subdag=EtlClusterSubdag(
    parent_dag_name="inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2",
    child_dag_name="create_persistent_cluster_scoisf_etl",
    cluster_conn_id="TECH_ISF_NAPSTORINVTRY_ETL_CLUSTER_PROD",
    cluster_template="scoisf-etl",
    default_args=default_args))


    livy_main_9_job_2_load_kafka_to_db_stg = LivyOperator(
    task_id='main_9_job_2_load_kafka_to_db_stg',
    app_file="s3a://napstoreinvtry-prod-common-etl/apps/etl_framework/prod/jars/uber-onehop-etl-pipeline-2.1.0.jar",
    conn_id='TECH_ISF_NAPSTORINVTRY_LIVY_PROD',
    class_name="com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
    app_args=['@s3://napstoreinvtry-prod-common-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/17610/DAS_SC_OUTBOUND/APP08649_insights_v2/user_config/argument_inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2_main_9_job_2_load_kafka_to_db_stg.json', '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
    driver_memory='12g',
    driver_cores=2,
    executor_memory='9g',
    executor_cores=5,
    session_name='inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2_main_9_job_2_load_kafka_to_db_stg_'+CURRENT_TIMESTAMP.strftime('%Y%m%d_%H%M%S'),
    spark_conf={'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id': '{{ ts_nodash }}', 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '30', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'})


    livy_main_9_job_2_load_kafka_to_db_stg_sensor = LivySensor(
    conn_id='TECH_ISF_NAPSTORINVTRY_LIVY_PROD',
    task_id='main_9_job_2_load_kafka_to_db_stg_sensor',
    batch_id="{{ task_instance.xcom_pull(task_ids='main_9_job_2_load_kafka_to_db_stg') }}")

    launch_main_9_job_3_load_stg_to_fct_table = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_3_load_stg_to_fct_table',
    job_name='main-9-job-3-load-stg-to-fct-table-inventory-store-carton-audit',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/17610/DAS_SC_OUTBOUND/APP08649_insights_v2/sql/inventory_store_carton_audited/inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2_main_9_td_stg_to_fact.sql', '--bucket', 'napstoreinvtry-prod-common-etl', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_FCT', '--vault_path', 'data/application/APP08649/shared/prod/teradata/nap/service_account', '--vault_host', 'https://prod-vault.vault.vip.nordstrom.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP08649'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'APP08649-insights-v2-APP08649-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::483375294780:role/napstorinvtry-k8s-controlled-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP08649_insights_v2'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstorinvtry-insights',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_3_load_stg_to_fct_table_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_3_load_stg_to_fct_table_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='main-9-job-3-load-stg-to-fct-table-inventory-store-carton-audit',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_4_teradata_fact_dqc = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_4_teradata_fact_dqc',
    job_name='main-9-job-4-teradata-fact-dqc-inventory-store-carton-audited-1',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/17610/DAS_SC_OUTBOUND/APP08649_insights_v2/sql/inventory_store_carton_audited/inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2_main_9_td_fact_dqc.sql', '--bucket', 'napstoreinvtry-prod-common-etl', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_FCT', '--vault_path', 'data/application/APP08649/shared/prod/teradata/nap/service_account', '--vault_host', 'https://prod-vault.vault.vip.nordstrom.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP08649'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'APP08649-insights-v2-APP08649-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::483375294780:role/napstorinvtry-k8s-controlled-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP08649_insights_v2'}},
    startup_timeout=300,
    retries=0,
    service_account_name='napstorinvtry-insights',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_4_teradata_fact_dqc_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_4_teradata_fact_dqc_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='main-9-job-4-teradata-fact-dqc-inventory-store-carton-audited-1',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_5_td_batch_end = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_5_td_batch_end',
    job_name='main-9-job-5-td-batch-end-inventory-store-carton-audited-17610',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08499/insights-framework/etl-executor:0.4.1',
    container_command=['python', '/home/nonroot/etl_executor/main.py', '--sql_files', 'apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/17610/DAS_SC_OUTBOUND/APP08649_insights_v2/sql/inventory_store_carton_audited/inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2_main_9_batch_end.sql', '--bucket', 'napstoreinvtry-prod-common-etl', '--executor', 'teradata', '--host', 'tdnapprod.nordstrom.net', '--database', 'PRD_NAP_FCT', '--vault_path', 'data/application/APP08649/shared/prod/teradata/nap/service_account', '--vault_host', 'https://prod-vault.vault.vip.nordstrom.com:8200', '--mount_point', 'nordsecrets'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'STATSD_HOST', 'value': 'gostatsd.kube-system.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'APP_ID', 'value': 'APP08649'}, {'name': 'NEW_RELIC_APP_NAME', 'value': 'APP08649-insights-v2-APP08649-PROD'}, {'name': 'ENV', 'value': 'PROD'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}, {'name': 'DAG_ID', 'value': 'inventory_store_carton_audited_17610_DAS_SC_OUTBOUND_APP08649_insights_v2'}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::483375294780:role/napstorinvtry-k8s-controlled-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP08649_insights_v2'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstorinvtry-insights',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_5_td_batch_end_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_9_job_5_td_batch_end_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='main-9-job-5-td-batch-end-inventory-store-carton-audited-17610',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_9_job_1_td_batch_start >> [launch_main_9_job_1_td_batch_start_sensor]
    launch_main_9_job_1_td_batch_start_sensor >> [create_persistent_cluster_scoisf_etl]
    create_persistent_cluster_scoisf_etl >> [livy_main_9_job_2_load_kafka_to_db_stg]
    livy_main_9_job_2_load_kafka_to_db_stg >> [livy_main_9_job_2_load_kafka_to_db_stg_sensor]
    livy_main_9_job_2_load_kafka_to_db_stg_sensor >> [launch_main_9_job_3_load_stg_to_fct_table]
    launch_main_9_job_3_load_stg_to_fct_table >> [launch_main_9_job_3_load_stg_to_fct_table_sensor]
    launch_main_9_job_3_load_stg_to_fct_table_sensor >> [launch_main_9_job_4_teradata_fact_dqc]
    launch_main_9_job_4_teradata_fact_dqc >> [launch_main_9_job_4_teradata_fact_dqc_sensor]
    launch_main_9_job_4_teradata_fact_dqc_sensor >> [launch_main_9_job_5_td_batch_end]
    launch_main_9_job_5_td_batch_end >> [launch_main_9_job_5_td_batch_end_sensor]

