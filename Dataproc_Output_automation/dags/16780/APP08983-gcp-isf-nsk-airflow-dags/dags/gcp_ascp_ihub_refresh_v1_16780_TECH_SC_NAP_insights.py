

import pendulum
import os
import configparser
import yaml
from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator
from nordstrom.utils.cloud_creds import cloud_creds
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
# Airflow variables
# airflow_environment = os.environ.get('ENVIRONMENT', 'local')
airflow_environment = 'nonprod'
root_path = path.dirname(__file__).split('APP08983-gcp-isf-nsk-airflow-dags')[0] + 'APP08983-gcp-isf-nsk-airflow-dags/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = ("development" if airflow_environment in ["nonprod", "development"] else "production")
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'gcp_ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator                
from modules.pelican import Pelican
from modules.metrics.main_common import main

start_date = pendulum.datetime(2022, 2, 24, tz = "US/Pacific")
cron = None if config.get(dag_id, 'cron').lower() == 'none' else config.get(dag_id, 'cron')
dataplex_project_id = config.get('dag', 'dataplex_project_id')
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes = int(-1)) if int(-1) > 1 else None

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
    return Pelican.validate(dag_name = dag_id, env = airflow_environment)


class PelicanJobSensor(BaseSensorOperator):
    def poke(self, context):
        return Pelican.check_job_status(
            context, dag_name = dag.dag_id, env = airflow_environment
        )


def toggle_pelican_validator(pelican_flag, dag):
    if pelican_flag == True:
        pelican_sensor_task = PelicanJobSensor(
            task_id = "pelican_job_sensor",
            poke_interval = 300,  # check every 5 minutes
            timeout = 4500,  # timeout after 75 minutes
            mode = "poke",
        )

        return pelican_sensor_task
    else:
        dummy_sensor = DummyOperator(task_id = "dummy_sensor", dag = dag)

        return dummy_sensor


def toggle_pelican_sensor(pelican_flag, dag):
    if pelican_flag == True:
        pelican_validation = PythonOperator(
            task_id = "pelican_validation", python_callable = pelican_check, dag = dag
        )

        return pelican_validation
    else:
        dummy_validation = DummyOperator(task_id = "dummy_validation", dag = dag)

        return dummy_validation

               
default_args = {
        
    'retries': 0 if config_env == 'development' else 3,
    'description': 'ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights DAG Description',
    'retry_delay': timedelta(minutes = 10),
    # "email": (
    #     None
    #     if config.get(dag_id, "email").lower() == "none"
    #     else config.get(dag_id, "email")
    # ),
    # "email_on_failure": config.getboolean(dag_id, "email_on_failure"),
    # "email_on_retry": config.getboolean(dag_id, "email_on_retry"),
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}

with DAG(
        dag_id = dag_id,
        default_args = default_args,
        schedule_interval = cron,
        max_active_runs = set_max_active_runs,
        template_searchpath=[sql_path],
        concurrency = set_concurrency,
         ) as dag:

    project_id = config.get('dag', 'gcp_project_id')
    service_account_email = config.get('dag', 'service_account_email')
    region = config.get('dag', 'region')
    gcp_conn = config.get('dag', 'gcp_conn')
    subnet_url = config.get('dag', 'subnet_url')
    pelican_flag = config.getboolean("dag", "pelican_flag")
    env = os.getenv('ENVIRONMENT')

    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args = [service_account_email],
    )  

    pelican_validator = toggle_pelican_validator(pelican_flag, dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag, dag)

    
    
    # Please add the consumer_group_name,topic_name from kafka json file and remove if kafka is not present 
    fetch_batch_run_id_task = FetchKafkaBatchRunIdOperator(
        task_id = 'fetch_batch_run_id_task',
        gcp_project_id = project_id,
        gcp_connection_id = gcp_conn,
        gcp_region = region,
        topic_name = 'ascp-inventory-stock-quantity-eis-object-model-avro',
        consumer_group_name = 'onix1-ascp-inventory-stock-quantity-eis-object-model-avro_ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_kafka_load_kafka',
        offsets_table = "`jwn-nap-user1-nonprod-zmnb.onehop_etl_app_db.kafka_consumer_offset_batch_details`",
        # source_table = f"`{{dataplex_project_id}}.onehop_etl_app_db.source_kafka_consumer_offset_batch_details`",
        default_latest_run_id = int((datetime.today() - timedelta(days=1)).strftime('%Y%m%d000000')),
        dag = dag
    )

    
        
    common_sql_start_batch = open( os.path.join(sql_path, 'common_sql/start_batch.sql'),"r").read()

    launch_start_batch_load_start_batch = BigQueryInsertJobOperator(
        task_id = "start_batch_load_start_batch",
        configuration = 
        {
            "query": 
            {
                "query": common_sql_start_batch,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )

    
        
    ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_metrics_load_metrics = open( os.path.join(sql_path, 'common_sql/load_metrics.sql'),"r").read()

    launch_metrics_load_metrics = BigQueryInsertJobOperator(
        task_id = "metrics_load_metrics",
        configuration = 
        {
            "query": 
            {
                "query": ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_metrics_load_metrics,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               

    
    session_name=get_batch_id(dag_id=dag_id,task_id="kafka_load_kafka")   
    kafka_load_kafka = DataprocCreateBatchOperator(
            task_id = "kafka_load_kafka",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
            # op_args = { "version":"1.1"},
            batch=
            {    
                "runtime_config": {"version": "1.1","properties":{ 'spark.sql.legacy.avro.datetimeRebaseModeInWrite': 'LEGACY', 'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY', 'spark.speculation': 'false', 'spark.airflow.run_id':"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}"
            }
        },    
                "spark_batch": 
                    {
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{spark_jar_path}/{delta_core_jar}",f"{spark_jar_path}/uber-onehop-etl-pipeline-{etl_jar_version}.jar"],
                    "args": [f"{user_config_path}/argument_ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_kafka_load_kafka.json",
                    # '--aws_user_role_external_id', Variable.get('aws_role_externalid')
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


        
    ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_kafka_start_cntl_track_kafka_start = open( os.path.join(sql_path, 'common_sql/track_kafka_start.sql'),"r").read()

    launch_kafka_start_cntl_track_kafka_start = BigQueryInsertJobOperator(
        task_id = "kafka_start_cntl_track_kafka_start",
        configuration = 
        {
            "query": 
            {
                "query": ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_kafka_start_cntl_track_kafka_start,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
        
    ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_kafka_end_cntl_track_kafka_end = open( os.path.join(sql_path, 'common_sql/track_kafka_end.sql'),"r").read()

    launch_kafka_end_cntl_track_kafka_end = BigQueryInsertJobOperator(
        task_id = "kafka_end_cntl_track_kafka_end",
        configuration = 
        {
            "query": 
            {
                "query": ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_kafka_end_cntl_track_kafka_end,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )


    # tpt_jdbc_insert_tpt_load_tpt_control_tbl = BashOperator(
    # task_id = 'tpt_jdbc_insert_tpt_load_tpt_control_tbl',
    # bash_command = f'echo "Skipping Control Table Entry"')

    # tpt_tpt_load_tpt_load_tbl = SSHOperator(
    # task_id = 'tpt_tpt_load_tpt_load_tbl',
    # command="/db/bigquery/bin/tpt_load.sh  -e PRD_NAP_UTL -j PRD_NAP_STG_INVENTORY_STOCK_QUANTITY_PHYSICAL_LDG_JOB -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_SCINV_BATCH_PRD -p \"\$tdwallet(SCH_NAP_SCINV_BATCH_PRD_PWD)\" -f s3://tf-nap-scinv-prod-ascp-extract-csv/data/inventory_stock_quantity_physical_ldg_table_csv/*.csv.gz -o UPDATE",
    # timeout = 1800,
    # ssh_conn_id = "TECH_ISF_NAP_SCINV_TDUTIL_SERVER_S3EXT_PROD")

    
        
    ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_teradata_inventory_stock_quantity_physical_fact_load = open( os.path.join(sql_path, 'ascp_ihub_refresh_v1/inventory_stock_quantity_physical_fact_load.sql'),"r").read()

    launch_bigquery_load_bigquery = BigQueryInsertJobOperator(
        task_id = "bigquery_load_bigquery",
        configuration = 
        {
            "query": 
            {
                "query": ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_teradata_inventory_stock_quantity_physical_fact_load,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
    # launch_send_new_relic_metric_send_newrelic_metric = DummyOperator(task_id='launch_send_new_relic_metric_send_newrelic_metric')
    launch_send_new_relic_metric_send_newrelic_metric = PythonOperator(
        task_id="send_new_relic_metric_send_newrelic_metric",
        python_callable=main,
        op_kwargs={
            "newrelic_connection": config.get("dag", "newrelic_connection"),
            "hook": BigQueryHook(
                gcp_conn_id=gcp_conn, location=region, use_legacy_sql=False
            ),
            "bigquery_environment": sql_config_dict["dbenv"],
            "metrics_yaml_file": os.path.join(
                module_path, "yaml/ASCP_COMMON_METRICS.yaml"
            ),
            'isf_dag_name': sql_config_dict['dag_name'],
            'tpt_job_name': sql_config_dict['tpt_job_name'],
            'subject_area_name': sql_config_dict['subject_area'],
            'ldg_table_name': sql_config_dict['ldg_table_name']
        },
    )
                
        
    ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_end_batch_end_batch = open( os.path.join(sql_path, 'common_sql/end_batch.sql'),"r").read()

    launch_end_batch_load_end_batch = BigQueryInsertJobOperator(
        task_id = "end_batch_load_end_batch",
        configuration = 
        {
            "query": 
            {
                "query": ascp_ihub_refresh_v1_16780_TECH_SC_NAP_insights_end_batch_end_batch,
                "useLegacySql": False
            }
        },
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               


    creds_setup >> fetch_batch_run_id_task >> launch_start_batch_load_start_batch >>[launch_metrics_load_metrics]
    launch_metrics_load_metrics >>[launch_kafka_start_cntl_track_kafka_start, kafka_load_kafka]
    kafka_load_kafka >>[launch_kafka_end_cntl_track_kafka_end]
    launch_kafka_start_cntl_track_kafka_start >> launch_kafka_end_cntl_track_kafka_end >> [launch_bigquery_load_bigquery]
    launch_bigquery_load_bigquery >>[launch_send_new_relic_metric_send_newrelic_metric]
    launch_send_new_relic_metric_send_newrelic_metric >>[launch_end_batch_load_end_batch]
    launch_end_batch_load_end_batch >> pelican_validator >> pelican_sensor
