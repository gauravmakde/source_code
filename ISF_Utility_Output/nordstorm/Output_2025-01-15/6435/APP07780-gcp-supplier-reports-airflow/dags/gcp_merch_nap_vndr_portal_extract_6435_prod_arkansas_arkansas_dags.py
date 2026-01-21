# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        071f18a46ffec42014e89f2ca3202fae1c309e33
# CI pipeline URL:      https://git.jwn.app/TM00156/app01396-supplier-reports/APP07780-gcp-supplier-reports-airflow/-/pipelines/8086607
# CI commit timestamp:  2025-01-08T15:44:37+00:00
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
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from nordstrom.operators.operator import LivyOperator
from nordstrom.sensors.sensor import LivySensor

from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP07780-gcp-supplier-reports-airflow')[0] + 'APP07780-gcp-supplier-reports-airflow/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = config.get('framework_setup', 'airflow_newrelic_api_key_name')

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican
from modules.metrics.main_common import main
                

start_date = pendulum.datetime(2022, 2, 24, tz="US/Pacific")
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

delta_core_jar = config.get('dataproc', 'delta_core_jar')
etl_jar_version = config.get('dataproc', 'etl_jar_version')
metastore_service_path = config.get('dataproc', 'metastore_service_path')
spark_jar_path = config.get('dataproc', 'spark_jar_path')
user_config_path = config.get('dataproc', 'user_config_path')

def get_batch_id(dag_id, task_id):   
    
    current_datetime = datetime.today()       
    if len(dag_id) <= 22:
        dag_id = dag_id.replace('_', '-').rstrip('-')
        ln_task_id = 45 - len(dag_id)
        task_id = task_id[-ln_task_id:].replace('_', '-').strip('-')
    elif len(task_id) <= 22:
        task_id = task_id.replace('_', '-').strip('-')
        ln_dag_id = 45 - len(task_id)
        dag_id = dag_id[:ln_dag_id].replace('_', '-').rstrip('-')
    else:
        dag_id = dag_id[:23].replace('_', '-').rstrip('-')
        task_id = task_id[-22:].replace('_', '-').strip('-')

    date = current_datetime.strftime('%Y%m%d%H%M%S')
    return f'''{dag_id}--{task_id}--{date}'''
    
def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id = config.get('dag', 'nauth_conn_id'),
        cloud_conn_id = config.get('dag', 'gcp_conn'),
        service_account_email = service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def pelican_check():
    return Pelican.validate(dag_name=dag_id, env=airflow_environment)

class PelicanJobSensor(BaseSensorOperator):
    def poke(self, context):
        return Pelican.check_job_status(context, dag_name=dag.dag_id, env=airflow_environment)

def toggle_pelican_validator(pelican_flag,dag):
    if pelican_flag == True:
        pelican_sensor_task = PelicanJobSensor(
            task_id='pelican_job_sensor',
            poke_interval=300,  # check every 5 minutes
            timeout=4500,  # timeout after 75 minutes
            mode='poke'
        )

        return pelican_sensor_task
    else:
        dummy_sensor = DummyOperator(task_id="dummy_sensor",dag=dag)

        return dummy_sensor

def toggle_pelican_sensor(pelican_flag,dag):
    if pelican_flag == True:
        pelican_validation = PythonOperator(
            task_id="pelican_validation",
            python_callable=pelican_check,
            dag=dag
        )

        return pelican_validation
    else:
        dummy_validation = DummyOperator(task_id="dummy_validation",dag=dag)
        
        return dummy_validation    
              
default_args = {
        
    'retries': 0 if config_env == 'development' else 3,
    'description': 'merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}

def default_query_function_8_days():
    dag_run = get_current_context()["dag_run"]
    start_date = dag_run.start_date - timedelta(days=8)
    return {
        'start_date_gte': start_date.isoformat()
    }

with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval = cron,
        max_active_runs=set_max_active_runs,
        template_searchpath=[sql_path],
        concurrency=set_concurrency,
         ) as dag:

    project_id = config.get('dag', 'gcp_project_id')
    service_account_email = config.get('dag', 'service_account_email')
    region = config.get('dag', 'region')
    gcp_conn = config.get('dag', 'gcp_conn')
    subnet_url = config.get('dag', 'subnet_url')
    pelican_flag = config.getboolean('dag', 'pelican_flag')
    env = os.getenv('ENVIRONMENT')

    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args = [service_account_email],
    )  
    
    pelican_validator = toggle_pelican_validator(pelican_flag,dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag,dag)

    #Please add the consumer_group_name,topic_name from kafka json file and remove if kafka is not present 
    fetch_batch_run_id_task = FetchKafkaBatchRunIdOperator(
        task_id = 'fetch_batch_run_id_task',
        gcp_project_id = project_id,
        gcp_connection_id = gcp_conn,
        gcp_region = region,
        topic_name = 'humanresources-job-changed-avro',
        consumer_group_name = 'humanresources-job-changed-avro_hr_job_events_load_2656_napstore_insights_hr_job_events_load_0_stg_table',
        offsets_table = f"`{dataplex_project_id}.onehop_etl_app_db.kafka_consumer_offset_batch_details`",
        source_table = f"`{dataplex_project_id}.onehop_etl_app_db.source_kafka_consumer_offset_batch_details`",
        default_latest_run_id = int((datetime.today() - timedelta(days=1)).strftime('%Y%m%d000000')),
        dag = dag
    )

    launch_other_appId_upstream_dependencies_sensor_0 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_0',
    conn_id='okta-token-connection',
    external_dag_id='gcp_merch_nap_mri_transaction_fact_to_week_aggregate_load_10976_tech_nap_merch',
    date_query_fn=default_query_function_8_days,
    timeout=180,
    retries=0,
    poke_interval=60)

    launch_other_appId_upstream_dependencies_sensor_1 = ApiDagRunLatestSensor(
    dag=dag,
    task_id='check_other_appId_upstream_dependencies_1',
    conn_id='okta-token-connection',
    external_dag_id='gcp_merch_nap_mri_transaction_fact_to_day_aggregate_wkly_load_10976_tech_nap_merch',
    date_query_fn=default_query_function_8_days,
    timeout=180,
    retries=0,
    poke_interval=60)

    tpt_jdbc_export_JOB_0_1_EXPORT_WK_RNG_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_0_1_EXPORT_WK_RNG_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_0_1_EXPORT_WK_RNG_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_0_1_EXPORT_WK_RNG_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_0_1_wr_vendor_portal_dim_wk_rng.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/week_range/week_range.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_0_2_READ_WK_RNG_DATA")   
    JOB_0_2_READ_WK_RNG_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_0_2_READ_WK_RNG_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_0_2_READ_WK_RNG_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

pause_idle_termination_arkansas_etl_JOB_0_2_READ_WK_RNG_DATA = SimpleHttpOperator(
    task_id='pause_idle_termination_arkansas_etl_JOB_0_2_READ_WK_RNG_DATA',
    endpoint='/persistent/cluster/arkansas-etl?createdBy=merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags&safeToDelete=NO',
    method='PUT',
    http_conn_id="TECH_ISF_TEAM_ARKANSAS_ETL_CLUSTER_PROD"
    )

    tpt_jdbc_export_JOB_1_1_EXPORT_DEPT_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_1_EXPORT_DEPT_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_1_EXPORT_DEPT_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_1_EXPORT_DEPT_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_1_wr_vendor_portal_dim_department.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_dept/vndr_portal_dept.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_2_EXPORT_LOC_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_2_EXPORT_LOC_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_2_EXPORT_LOC_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_2_EXPORT_LOC_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_2_wr_vendor_portal_dim_location.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_loc/vndr_portal_loc.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_3_EXPORT_SUPP_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_3_EXPORT_SUPP_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_3_EXPORT_SUPP_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_3_EXPORT_SUPP_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_3_wr_vendor_portal_dim_supplier.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_supp/vndr_portal_supp.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_4_EXPORT_SKU_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_4_EXPORT_SKU_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_4_EXPORT_SKU_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_4_EXPORT_SKU_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_4_wr_vendor_portal_dim_sku.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_sku/vndr_portal_sku.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_5_EXPORT_STYLE_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_5_EXPORT_STYLE_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_5_EXPORT_STYLE_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_5_EXPORT_STYLE_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_5_wr_vendor_portal_dim_style.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_style/vndr_portal_style.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_6_EXPORT_VPN_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_6_EXPORT_VPN_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_6_EXPORT_VPN_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_6_EXPORT_VPN_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_6_wr_vendor_portal_rpt_vpn.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_vpn/vndr_portal_vpn.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_MERCH_TRANSACTION_STYLE_STORE_WEEK_AGG_FACT_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_7_EXPORT_DEPT_LOC_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_7_EXPORT_DEPT_LOC_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_7_EXPORT_DEPT_LOC_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_7_EXPORT_DEPT_LOC_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_7_wr_vendor_portal_rpt_dep_loc.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_dept_loc/vndr_portal_dept_loc.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_8_EXPORT_COLOR_SIZE_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_8_EXPORT_COLOR_SIZE_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_8_EXPORT_COLOR_SIZE_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_8_EXPORT_COLOR_SIZE_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_8_wr_vendor_portal_rpt_color_size.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_color_size/vndr_portal_color_size.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_1_9_EXPORT_TY_LY_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_1_9_EXPORT_TY_LY_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_1_9_EXPORT_TY_LY_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_1_9_EXPORT_TY_LY_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_1_9_wr_vendor_portal_rpt_ty_ly.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_dept_loc_ty_ly/vndr_portal_dept_loc_ty_ly.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_1_READ_DEPT_DATA")   
    JOB_2_1_READ_DEPT_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_1_READ_DEPT_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_1_READ_DEPT_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_2_READ_LOC_DATA")   
    JOB_2_2_READ_LOC_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_2_READ_LOC_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_2_READ_LOC_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_3_READ_SUPP_DATA")   
    JOB_2_3_READ_SUPP_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_3_READ_SUPP_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_3_READ_SUPP_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_4_READ_SKU_DATA")   
    JOB_2_4_READ_SKU_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_4_READ_SKU_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_4_READ_SKU_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_5_READ_STYLE_DATA")   
    JOB_2_5_READ_STYLE_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_5_READ_STYLE_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_5_READ_STYLE_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_6_READ_VPN_DATA")   
    JOB_2_6_READ_VPN_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_6_READ_VPN_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_6_READ_VPN_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_7_READ_DEPT_LOC_DATA")   
    JOB_2_7_READ_DEPT_LOC_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_7_READ_DEPT_LOC_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_7_READ_DEPT_LOC_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_8_READ_COLOR_SIZE_DATA")   
    JOB_2_8_READ_COLOR_SIZE_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_8_READ_COLOR_SIZE_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_8_READ_COLOR_SIZE_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

session_name=get_batch_id(dag_id=dag_id,task_id="JOB_2_9_READ_TY_LY_DATA")   
    JOB_2_9_READ_TY_LY_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_2_9_READ_TY_LY_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_2_9_READ_TY_LY_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

tpt_jdbc_export_JOB_3_6_EXPORT_VPN_HIST_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_3_6_EXPORT_VPN_HIST_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_3_6_EXPORT_VPN_HIST_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_3_6_EXPORT_VPN_HIST_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_3_6_wr_vendor_portal_rpt_vpn_hist.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_vpn_hst/vndr_portal_vpn_hst.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_3_7_wr_vendor_portal_rpt_dep_loc_hist.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_dept_loc_hst/vndr_portal_dept_loc_hst.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_3_8_wr_vendor_portal_rpt_color_size_hist.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_color_size_hst/vndr_portal_color_size_hst.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    tpt_jdbc_export_JOB_3_9_EXPORT_TY_LY_HIST_DATA_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_JOB_3_9_EXPORT_TY_LY_HIST_DATA_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    tpt_JOB_3_9_EXPORT_TY_LY_HIST_DATA_export_tbl = SSHOperator(
    task_id='tpt_JOB_3_9_EXPORT_TY_LY_HIST_DATA_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-vndr-prod-onehop-etl/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/6435/prod_arkansas/arkansas_dags/sql/merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_3_9_wr_vendor_portal_rpt_ty_ly_hist.sql -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SUP_EXTRACT -p \"\$tdwallet(SCH_NAP_SUP_EXTRACT_PWD)\"  -t s3://vndr-extracts-tpt-export-prod/vendor-portal-teradata-prod-v1/vendor_portal/bkup/vndr_portal_dept_loc_ty_ly_hst/vndr_portal_dept_loc_ty_ly_hst.csv -a 'Y' -d 01 -c 1 -q N -f '' -j PRD_NAP_BASE_VWS_merch_transaction_sbclass_store_week_agg_fact_JOB",
    timeout=1800,
    ssh_conn_id="TECH_ISF_TEAM_ARKANSAS_TDUTIL_SERVER_S3EXT_PROD")

    
            
        session_name=get_batch_id(dag_id=dag_id,task_id="JOB_4_0_DEL_BKUP_DATA")   
    JOB_4_0_DEL_BKUP_DATA = DataprocCreateBatchOperator(
            task_id = "JOB_4_0_DEL_BKUP_DATA",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.yarn.maxAppAttempts': '1', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}", 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '5', 'spark.dynamicAllocation.maxExecutors': '80', 'spark.dynamicAllocation.initialExecutors': '10', 'spark.dynamicAllocation.executorIdleTimeout': '240'}},
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_merch_nap_vndr_portal_extract_6435_prod_arkansas_arkansas_dags_JOB_4_0_DEL_BKUP_DATA.json",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    },
                    "environment_config": 
                                        {
                                        "execution_config": 
                                                        {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                      #  "peripherals_config": {"metastore_service": metastore_service_path}
                                        }
            },
            batch_id = session_name,
            dag = dag

)

launch_JOB_5_0_python_executor = launch_k8s_api_job_operator(
    dag=dag,
    connection_id='TECH_ISF_TEAM_ARKANSAS_K8S_BEET_CONN_PROD',
    namespace='app07780',
    task_id='JOB_5_0_python_executor',
    job_name='job-5-0-python-executor-merch-nap-vndr-portal-extract-6435-f37',
    macros=None,
    container_image='artifactory.nordstrom.com/docker/app07780/tm00156-app01396-supplier-reports-app07780-supplier-reports-air/prod/python_modules/reporting_dataload_automation_trigger:0.1.3',
    container_command=['/home/nonroot/docker-entrypoint.sh', 'python_modules/reporting_dataload_automation_trigger/main.py'],
    resources={'limits': {'cpu': 5, 'memory': '6Gi' },'requests': {'cpu': 3, 'memory': '2Gi'}},
    common_envs=[],
    task_envs=[{'name': 'TIER', 'value': '3'}, {'name': 'EXEC_ENV', 'value': 'GITLAB'}, {'name': 'STATSD_HOST', 'value': 'statsd.k8s-newrelic.svc.cluster.local'}, {'name': 'STATSD_PORT', 'value': '8128'}, {'name': 'EXECUTE_COMMAND', 'value': 'python /app/jobs/executor.py'}, {'name': 'VAULT_PATH', 'value': 'prod-vault.vault.vip.nordstrom.com'}, {'name': 'VAULT_PORT', 'value': '8200'}, {'name': 'K8S_NAMESPACE', 'value': 'APP07780'}, {'name': 'rep_dataload_instance_id', 'value': 'i-021a7b8f148a4fe4e'}, {'name': 'VAULT_ROLE_ID', 'valueFrom': {'secretKeyRef': {'key': 'roleid', 'name': 'vault'}}}, {'name': 'VAULT_SECRET', 'valueFrom': {'secretKeyRef': {'key': 'secret', 'name': 'vault'}}}],
    metadata={'annotations': {'kube2iam.beta.nordstrom.net/role': 'arn:aws:iam::372359261305:role/app07780-airflow-nsk-serviceaccount-role'}, 'labels': {'nap_domain': 'onehop-etl', 'nap_project': 'insights-framework', 'customer_team_name': 'prod_arkansas', 'customer_project_name': 'arkansas_dags'}},
    startup_timeout=300,
    retries=3,
    service_account_name='app07780-airflow-nsk-serviceaccount',
    nsk_cluster='nsk-beet-prod')

creds_setup >> fetch_batch_run_id_task >> launch_other_appId_upstream_dependencies_sensor_0 >>[tpt_jdbc_export_JOB_0_1_EXPORT_WK_RNG_DATA_control_tbl]
    launch_other_appId_upstream_dependencies_sensor_1 >>[tpt_jdbc_export_JOB_0_1_EXPORT_WK_RNG_DATA_control_tbl]
    tpt_jdbc_export_JOB_0_1_EXPORT_WK_RNG_DATA_control_tbl >>[tpt_JOB_0_1_EXPORT_WK_RNG_DATA_export_tbl]
    tpt_JOB_0_1_EXPORT_WK_RNG_DATA_export_tbl >>[JOB_0_2_READ_WK_RNG_DATA, JOB_2_1_READ_DEPT_DATA, JOB_2_2_READ_LOC_DATA, JOB_2_3_READ_SUPP_DATA, JOB_2_4_READ_SKU_DATA, JOB_2_5_READ_STYLE_DATA, JOB_2_6_READ_VPN_DATA, JOB_2_7_READ_DEPT_LOC_DATA, JOB_2_8_READ_COLOR_SIZE_DATA, JOB_2_9_READ_TY_LY_DATA, JOB_4_0_DEL_BKUP_DATA]
    JOB_0_2_READ_WK_RNG_DATA >>[pause_idle_termination_arkansas_etl_JOB_0_2_READ_WK_RNG_DATA]
    pause_idle_termination_arkansas_etl_JOB_0_2_READ_WK_RNG_DATA >>[tpt_jdbc_export_JOB_1_1_EXPORT_DEPT_DATA_control_tbl, tpt_jdbc_export_JOB_1_2_EXPORT_LOC_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_1_EXPORT_DEPT_DATA_control_tbl >>[tpt_JOB_1_1_EXPORT_DEPT_DATA_export_tbl]
    tpt_JOB_1_1_EXPORT_DEPT_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_3_EXPORT_SUPP_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_2_EXPORT_LOC_DATA_control_tbl >>[tpt_JOB_1_2_EXPORT_LOC_DATA_export_tbl]
    tpt_JOB_1_2_EXPORT_LOC_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_4_EXPORT_SKU_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_3_EXPORT_SUPP_DATA_control_tbl >>[tpt_JOB_1_3_EXPORT_SUPP_DATA_export_tbl]
    tpt_JOB_1_3_EXPORT_SUPP_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_5_EXPORT_STYLE_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_4_EXPORT_SKU_DATA_control_tbl >>[tpt_JOB_1_4_EXPORT_SKU_DATA_export_tbl]
    tpt_JOB_1_4_EXPORT_SKU_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_6_EXPORT_VPN_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_5_EXPORT_STYLE_DATA_control_tbl >>[tpt_JOB_1_5_EXPORT_STYLE_DATA_export_tbl]
    tpt_JOB_1_5_EXPORT_STYLE_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_7_EXPORT_DEPT_LOC_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_6_EXPORT_VPN_DATA_control_tbl >>[tpt_JOB_1_6_EXPORT_VPN_DATA_export_tbl]
    tpt_JOB_1_6_EXPORT_VPN_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_8_EXPORT_COLOR_SIZE_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_7_EXPORT_DEPT_LOC_DATA_control_tbl >>[tpt_JOB_1_7_EXPORT_DEPT_LOC_DATA_export_tbl]
    tpt_JOB_1_7_EXPORT_DEPT_LOC_DATA_export_tbl >>[tpt_jdbc_export_JOB_1_9_EXPORT_TY_LY_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_8_EXPORT_COLOR_SIZE_DATA_control_tbl >>[tpt_JOB_1_8_EXPORT_COLOR_SIZE_DATA_export_tbl]
    tpt_JOB_1_8_EXPORT_COLOR_SIZE_DATA_export_tbl >>[tpt_jdbc_export_JOB_3_6_EXPORT_VPN_HIST_DATA_control_tbl]
    tpt_jdbc_export_JOB_1_9_EXPORT_TY_LY_DATA_control_tbl >>[tpt_JOB_1_9_EXPORT_TY_LY_DATA_export_tbl]
    tpt_JOB_1_9_EXPORT_TY_LY_DATA_export_tbl >>[tpt_jdbc_export_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_control_tbl]
    JOB_2_1_READ_DEPT_DATA >>[JOB_2_2_READ_LOC_DATA, JOB_2_3_READ_SUPP_DATA]
    JOB_2_2_READ_LOC_DATA >>[JOB_2_4_READ_SKU_DATA]
    JOB_2_3_READ_SUPP_DATA >>[JOB_2_5_READ_STYLE_DATA]
    JOB_2_4_READ_SKU_DATA >>[JOB_2_6_READ_VPN_DATA]
    JOB_2_5_READ_STYLE_DATA >>[JOB_2_7_READ_DEPT_LOC_DATA]
    JOB_2_6_READ_VPN_DATA >>[JOB_2_8_READ_COLOR_SIZE_DATA]
    JOB_2_7_READ_DEPT_LOC_DATA >>[JOB_2_9_READ_TY_LY_DATA]
    JOB_2_8_READ_COLOR_SIZE_DATA >>[JOB_4_0_DEL_BKUP_DATA]
    JOB_2_9_READ_TY_LY_DATA >>[JOB_4_0_DEL_BKUP_DATA]
    tpt_jdbc_export_JOB_3_6_EXPORT_VPN_HIST_DATA_control_tbl >>[tpt_JOB_3_6_EXPORT_VPN_HIST_DATA_export_tbl]
    tpt_JOB_3_6_EXPORT_VPN_HIST_DATA_export_tbl >>[tpt_jdbc_export_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_control_tbl]
    tpt_jdbc_export_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_control_tbl >>[tpt_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_export_tbl]
    tpt_JOB_3_7_EXPORT_DEPT_LOC_HIST_DATA_export_tbl >>[tpt_jdbc_export_JOB_3_9_EXPORT_TY_LY_HIST_DATA_control_tbl]
    tpt_jdbc_export_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_control_tbl >>[tpt_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_export_tbl]
    tpt_JOB_3_8_EXPORT_COLOR_SIZE_HIST_DATA_export_tbl >>[JOB_2_1_READ_DEPT_DATA]
    tpt_jdbc_export_JOB_3_9_EXPORT_TY_LY_HIST_DATA_control_tbl >>[tpt_JOB_3_9_EXPORT_TY_LY_HIST_DATA_export_tbl]
    tpt_JOB_3_9_EXPORT_TY_LY_HIST_DATA_export_tbl >>[JOB_2_1_READ_DEPT_DATA]
    JOB_4_0_DEL_BKUP_DATA >>[launch_JOB_5_0_python_executor]
    launch_JOB_5_0_python_executor >> pelican_validator >> pelican_sensor