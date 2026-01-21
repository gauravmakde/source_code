# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        e56289031cac5b0ff6d0f69dc3841146a1416441
# CI pipeline URL:      https://git.jwn.app/TM00593/das_merch/APP04070-gcp-MFP_kafka_to_TD_IF/-/pipelines/6430191
# CI commit timestamp:  2024-07-04T06:27:04+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os,configparser
from datetime import datetime, timedelta
from os import  path
import sys
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP04070-gcp-MFP_kafka_to_TD_IF')[0] + 'APP04070-gcp-MFP_kafka_to_TD_IF/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_project_mfp_eowsl_kafka_to_td_tpt_10976_tech_nap_merch'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican
                

start_date = pendulum.datetime(2022, 2, 24, tz="US/Pacific")
cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1)>1 else None

delta_core_jar = config.get('dataproc', 'delta_core_jar')
etl_jar_version = config.get('dataproc', 'etl_jar_version')
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
    return f'''{dag_id}--{task_id}--{date}'''.lower()
    
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
        
    'retries': 0 if config_env == 'development' else 2,
    'description': 'project_mfp_eowsl_kafka_to_td_tpt_10976_tech_nap_merch DAG Description',
    'retry_delay': timedelta(minutes=15),
    # 'email':None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),
    # 'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),
    # 'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    # 'sla': dag_sla

}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval = cron,
    max_active_runs=set_max_active_runs,
    concurrency=set_concurrency ) as dag:

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
        topic_name = 'inventory-end-of-week-stock-ledger-analytical-avro',
        consumer_group_name = 'onix-inventory-end-of-week-stock-ledger-analytical-avro_project_mfp_eowsl_kafka_to_td_tpt_10976_tech_nap_merch_read_eowsl_from_kafka_and_write_to_td_job_4',
        offsets_table = f"`{dataplex_project_id}.onehop_etl_app_db.kafka_consumer_offset_batch_details`",
        # source_table = f"`{dataplex_project_id}.onehop_etl_app_db.source_kafka_consumer_offset_batch_details`",
        default_latest_run_id = int((datetime.today() - timedelta(days=1)).strftime('%Y%m%d000000')),
        dag = dag
    )

    session_name=get_batch_id(dag_id=dag_id,task_id="read_eowsl_from_kafka_and_write_to_td_job_4")   
    read_eowsl_from_kafka_and_write_to_td_job_4 = DataprocCreateBatchOperator(
            task_id = "read_eowsl_from_kafka_and_write_to_td_job_4",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
            batch=
            {    
                "runtime_config": {"version": "1.1","properties": {
                        "spark.sql.legacy.avro.datetimeRebaseModeInWrite": "LEGACY",
                        "spark.speculation": "false",
                        "spark.airflow.run_id": "{{ ti.xcom_pull(task_ids = 'fetch_batch_run_id_task')}}",
                        "spark.sql.avro.datetimeRebaseModeInRead": "LEGACY",
                        "spark.sql.parquet.int96AsTimestamp": "false",
                        "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
                        "spark.sql.parquet.binaryAsString": "true",
                },
            },
                "spark_batch":  
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline.jar"],
                    "args": [f"{user_config_path}/argument_project_mfp_eowsl_kafka_to_td_tpt_10976_tech_nap_merch_read_eowsl_from_kafka_and_write_to_td_job_4.json",
                    #'--aws_user_role_external_id', Variable.get('aws_role_externalid')
                        ],
                    },
                    "environment_config": {
                                        "execution_config": {
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        },
                                        }
            },
            batch_id = session_name,
            dag = dag

)

    tpt_jdbc_insert_read_eowsl_from_kafka_and_write_to_td_job_5_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_read_eowsl_from_kafka_and_write_to_td_job_5_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"')

    # tpt_read_eowsl_from_kafka_and_write_to_td_job_5_load_tbl = SSHOperator(
    # task_id='tpt_read_eowsl_from_kafka_and_write_to_td_job_5_load_tbl',
    # command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_UTL -j PRD_NAP_STG_MERCH_STOCKLEDGER_WEEK_LDG_JOB -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_MERCH_BATCH -p \"\$tdwallet(SCH_NAP_MERCH_BATCH_PWD)\" -f s3://nap-das-merch-tpt-prod-v1/eowsl/*/*/*/* -o UPDATE",
    # timeout=1800,
    # ssh_conn_id="TECH_ISF_NAP_MERCH_MFP_SECURITY_TDUTIL_SERVER_S3EXT_PROD")

    
    read_eowsl_from_teradata_and_write_to_teradata = open(
    os.path.join(sql_path, 'read_eowsl_from_teradata_and_write_to_teradata.sql'),
    "r").read()

    launch_read_eowsl_from_kafka_and_write_to_td_job_6 = BigQueryInsertJobOperator(
        task_id = "read_eowsl_from_kafka_and_write_to_td_job_6",
        configuration = 
        {
            "query": 
            {
                "query": read_eowsl_from_teradata_and_write_to_teradata,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )



    creds_setup >> fetch_batch_run_id_task >>[read_eowsl_from_kafka_and_write_to_td_job_4]
    read_eowsl_from_kafka_and_write_to_td_job_4 >>[tpt_jdbc_insert_read_eowsl_from_kafka_and_write_to_td_job_5_control_tbl]
    tpt_jdbc_insert_read_eowsl_from_kafka_and_write_to_td_job_5_control_tbl >>[launch_read_eowsl_from_kafka_and_write_to_td_job_6]
    launch_read_eowsl_from_kafka_and_write_to_td_job_6 >> pelican_validator >> pelican_sensor