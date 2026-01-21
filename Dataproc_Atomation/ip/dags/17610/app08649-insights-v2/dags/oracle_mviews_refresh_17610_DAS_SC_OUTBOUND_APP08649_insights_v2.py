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
cron = '0 13 * * *'
dag_id = 'oracle_mviews_refresh_17610_DAS_SC_OUTBOUND_APP08649_insights_v2'
paused_upon_creation=True
set_max_active_runs=1
set_concurrency=10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict={"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla =  timedelta(minutes=int(-1))  if int(-1)>1 else None

default_args = {
    'retries': 3,
    'description': 'oracle_mviews_refresh_17610_DAS_SC_OUTBOUND_APP08649_insights_v2 DAG Description',
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
    

    launch_main_mviews_refresh = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_mviews_refresh',
    job_name='main-mviews-refresh-oracle-mviews-refresh-17610-das-sc-outbound',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08649/tm01189-app08649-store-inventory-app08649-insights-v2/prod/python/oracle_query_executor:1.0.5',
    container_command=['python', '/home/nonroot/python/oracle_query_executor/src/main.py', '--port', '1522', '--sql', "BEGIN DBMS_MVIEW.REFRESH(list => 'SCH_SIM16_NRD.MV_CUP_STORE_SUMMARY_SIM_NAP,SCH_SIM16_NRD.MV_CUP_STORE_EXPOSURE_SIM_NAP', nested => true); END;"],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'ENV', 'value': 'production'}, {'name': 'TD_ENV', 'value': 'PRD'}, {'name': 'TD_HOST', 'value': 'tdnapprod.nordstrom.net'}, {'name': 'TD_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/teradata/nap/service_account'}, {'name': 'NR_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/newrelic/ingest_license_key'}, {'name': 'BD_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/bigdata/service_account'}, {'name': 'ORA_SIM_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/oracle/sim16/service_account'}, {'name': 'ORA_SIM_HOST', 'value': 'y0319p11875.nordstrom.net'}, {'name': 'ORA_SIM_DB', 'value': 'DP1008'}, {'name': 'PRESTO_HOST', 'value': 'acp-starburst-presto.prod.bigdata.vip.nordstrom.com'}, {'name': 'PRESTO_CATALOG', 'value': 'hiveacp'}, {'name': 'VAULT_PATH', 'value': 'prod-vault.vault.vip.nordstrom.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'S3_BUCKET', 'value': 'napstoreinvtry-prod-common-etl'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::483375294780:role/napstorinvtry-k8s-controlled-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP08649_insights_v2'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstorinvtry-insights',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_mviews_refresh_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_mviews_refresh_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='main-mviews-refresh-oracle-mviews-refresh-17610-das-sc-outbound',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_mviews_dqc = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_mviews_dqc',
    job_name='main-mviews-dqc-oracle-mviews-refresh-17610-das-sc-outbound-app',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app08649/tm01189-app08649-store-inventory-app08649-insights-v2/prod/python/oracle_query_executor:1.0.5',
    container_command=['python', '/home/nonroot/python/oracle_query_executor/src/main.py', '--port', '1522', '--sql', "DECLARE V_ROW_COUNT NUMBER; TYPE STR_LIST_TYPE IS TABLE OF VARCHAR2(32); V_MVIEWS_LIST STR_LIST_TYPE; BEGIN V_MVIEWS_LIST := STR_LIST_TYPE('MV_STORE_ITEM_SELL_NONSELL','MV_CUP_STORE_EXPOSURE_SIM_NAP','MV_CUP_STORE_SUMMARY_SIM_NAP'); FOR INDX IN V_MVIEWS_LIST.FIRST..V_MVIEWS_LIST.LAST LOOP SELECT NUM_ROWS INTO V_ROW_COUNT FROM ALL_TABLES WHERE TABLE_NAME = V_MVIEWS_LIST(INDX) AND OWNER = 'SCH_SIM16_NRD'; IF V_ROW_COUNT = 0 THEN RAISE_APPLICATION_ERROR(-20001, 'SCH_SIM16_NRD.'||V_MVIEWS_LIST(INDX)||' DOES NOT HAVE DATA'); END IF; END LOOP; END;"],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'ENV', 'value': 'production'}, {'name': 'TD_ENV', 'value': 'PRD'}, {'name': 'TD_HOST', 'value': 'tdnapprod.nordstrom.net'}, {'name': 'TD_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/teradata/nap/service_account'}, {'name': 'NR_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/newrelic/ingest_license_key'}, {'name': 'BD_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/bigdata/service_account'}, {'name': 'ORA_SIM_VAULT_PATH', 'value': 'data/application/APP08649/shared/prod/oracle/sim16/service_account'}, {'name': 'ORA_SIM_HOST', 'value': 'y0319p11875.nordstrom.net'}, {'name': 'ORA_SIM_DB', 'value': 'DP1008'}, {'name': 'PRESTO_HOST', 'value': 'acp-starburst-presto.prod.bigdata.vip.nordstrom.com'}, {'name': 'PRESTO_CATALOG', 'value': 'hiveacp'}, {'name': 'VAULT_PATH', 'value': 'prod-vault.vault.vip.nordstrom.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'S3_BUCKET', 'value': 'napstoreinvtry-prod-common-etl'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::483375294780:role/napstorinvtry-k8s-controlled-prod'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'DAS_SC_OUTBOUND', 'customer_project_name': 'APP08649_insights_v2'}},
    startup_timeout=300,
    retries=3,
    service_account_name='napstorinvtry-insights',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_mviews_dqc_sensor = monitor_k8s_api_job_status(
    dag=dag,
    connection_id='TECH_ISF_NAPSTORINVTRY_K8S_GUMBO_CONN_PROD',
    namespace='app08649',
    task_id='main_mviews_dqc_sensor',
    poke_interval=100,
    time_out=1800,
    job_name='main-mviews-dqc-oracle-mviews-refresh-17610-das-sc-outbound-app',
    nsk_cluster='nsk-gumbo-prod')


    launch_main_mviews_refresh >> [launch_main_mviews_refresh_sensor]
    launch_main_mviews_refresh_sensor >> [launch_main_mviews_dqc]
    launch_main_mviews_dqc >> [launch_main_mviews_dqc_sensor]

