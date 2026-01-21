# common function modules that will be used in all the dags
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.jdbc_hook import JdbcHook
import configparser
import logging
import os
# from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook
from pathlib import Path
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# fetch environment from OS variable
import sys
# from google.cloud import bigquery
# from google.oauth2 import service_account
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import GoogleAPIError
env = os.environ['ENVIRONMENT']
# env = 'prod'
# Read the config files
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
sys.path.append(path)
# Airflow variables
config.read(os.path.join(Path(path).parent.parent, 'configs/bigquery.cfg'))

# Get the Teradata connection details based on the environment
bq_env = config.get(env, 'bigquery_environment')
control_db_name = config.get(env, 'control_db_name')
teradata_db_host = config.get(env, 'teradata_db_host')
teradata_user = config.get(env, 'teradata_user')
teradata_pwd = config.get(env, 'teradata_pwd')
ssh_connection_id = config.get(env, 'ssh_connection_id')
td_connection_audit = config.get(env, 'td_connection_audit')
gcp_project_id = config.get(env, 'gcp_project_id')


parameters = {'DBENV': bq_env, 'dbenv': bq_env,'gcp_project_id':gcp_project_id,'bq_project_id':gcp_project_id,'project_id':gcp_project_id,'db_env': bq_env}
# Preprod variables
teradata_environment_preprod = config.get(env, 'teradata_environment_preprod')
control_db_name_preprod = config.get(env, 'control_db_name_preprod')
teradata_user_preprod = config.get(env, 'teradata_user_preprod')
teradata_pwd_preprod = config.get(env, 'teradata_pwd_preprod')
ssh_connection_id_preprod = config.get(env, 'ssh_connection_id_preprod')
sql_path = config.get(env, 'sql_path')
customer_dim_init_sql_prefix = config.get(env, 'customer_dim_init_sql_prefix')
# service_account_file_path = os.path.join(path, config.get(env, 'service_account'))


# Initialize a BigQuery client with the credentials
# credentials = service_account.Credentials.from_service_account_file(service_account_file_path)
# client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def etl_control_load(subject_area, task_type, gcp_connection, gcp_project_id, gcp_region):
    hook = BigQueryHook(gcp_conn_id=gcp_connection)
    client = hook.get_client(project_id=gcp_project_id, location=gcp_region)

    sql = ''
    if task_type == 'START':
        sql = f"CALL {gcp_project_id}.{bq_env}_nap_utl.ELT_CONTROL_{task_type}_LOAD('{subject_area}')"

    elif task_type == 'END':
        sql = f"CALL {gcp_project_id}.{bq_env}_nap_utl.ELT_CONTROL_{task_type}_LOAD('{subject_area}')"

    query_job = client.query(sql)
    logging.info(f'Result from big_query:{query_job}')


# function to call etl_control_load from a python operator
def get_elt_control_load(task_id, subject_area, task_type, project_id, gcp_region, gcp_connection, dag):
    return PythonOperator(task_id=task_id,
                          python_callable=etl_control_load,
                          op_kwargs={'task_type': task_type, 'subject_area': subject_area,
                                     'gcp_connection': gcp_connection, 'gcp_project_id': project_id,
                                     'gcp_region': gcp_region},
                          retries=3, dag=dag)


def gcs_copy_files(task_id, source_bucket, source_object, destination_bucket, destination_object, dag, gcp_conn_id):
    return GCSToGCSOperator(
        task_id=task_id,
        source_bucket=source_bucket,
        source_object=source_object,
        destination_bucket=destination_bucket,
        destination_object=destination_object,
        gcp_conn_id=gcp_conn_id,
        dag=dag,
        # project_id='cf-nordstrom'
    )


def get_bteq_load(task_id, query, dag, gcp_conn_id):  # add gcp_connection id
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": open(os.path.join(os.path.dirname(os.path.abspath(__file__)) + customer_dim_init_sql_prefix, query), "r").read(),
                "useLegacySql": False,
            }
        },
        dag=dag,
        params=parameters,
        # project_id='cf-nordstrom',
        gcp_conn_id=gcp_conn_id,
    )
def product_etl_control_load(subject_area, task_type, gcp_connection, gcp_project_id, gcp_region):
    hook = BigQueryHook(gcp_conn_id=gcp_connection)
    client = hook.get_client(project_id=gcp_project_id, location=gcp_region)
    sql =''
    if task_type == 'START':
        sql = "EXEC {}_NAP_UTL.PRODUCT_ELT_CONTROL_{}_LOAD('{}')".format(bq_env, task_type, subject_area)


    elif task_type == 'END':
        sql = "EXEC {}_NAP_UTL.ELT_CONTROL_{}_LOAD('{}')".format(bq_env, task_type, subject_area)

    try:
        query_job = client.query(sql)
        logging.info(f'Result from BigQuery: {query_job}')
    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        if query_job is None:
            raise Exception('no records found')
    logging.info(f'Result from big_query:{query_job}')

# function to call product_etl_control_load from a python operator
def get_product_elt_control_load(task_id, subject_area, task_type, gcp_connection, gcp_project_id, gcp_region):
    return PythonOperator(task_id=task_id,
                          python_callable=product_etl_control_load,
                          op_kwargs={'task_type': task_type, 'subject_area': subject_area,
                                     'gcp_connection': gcp_connection, 'gcp_project_id': gcp_project_id,
                                     'gcp_region': gcp_region},
                          retries=3)