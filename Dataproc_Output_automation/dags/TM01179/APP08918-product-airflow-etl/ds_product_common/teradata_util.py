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
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = 'nonprod'

# Read the config files
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
config.read(os.path.join(Path(path).parent, 'configs/teradata.cfg'))

# Get the Teradata connection details based on the environment
teradata_environment = config.get(env, 'teradata_environment')
control_db_name = config.get(env, 'control_db_name')
teradata_db_host = config.get(env, 'teradata_db_host')
teradata_user = config.get(env, 'teradata_user')
teradata_pwd = config.get(env, 'teradata_pwd')
ssh_connection_id = config.get(env, 'ssh_connection_id')
td_connection_audit = config.get(env,'td_connection_audit')
project_id = config.get(env, 'project_id')
gcp_conn_id = config.get(env, 'gcp_connection')
location = config.get(env, 'location')

# Preprod variables
teradata_environment_preprod = config.get(env, 'teradata_environment_preprod')
control_db_name_preprod = config.get(env, 'control_db_name_preprod')
teradata_user_preprod = config.get(env, 'teradata_user_preprod')
teradata_pwd_preprod = config.get(env, 'teradata_pwd_preprod')
ssh_connection_id_preprod = config.get(env, 'ssh_connection_id_preprod')


def product_etl_control_load(subject_area, task_type, hook):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id,use_legacy_sql=False,location=location)
    if task_type == 'START':
        sql = "CALL `{}.{}_nap_utl.product_elt_control_{}_load`('{}')".format(project_id.lower(),teradata_environment.lower(),task_type.lower(),subject_area)
        query_job = hook.run(sql)
        logging.info(f'Result from BigQuery: {query_job}')

    elif task_type == 'END':
        sql = "CALL `{}.{}_nap_utl.elt_control_{}_load`('{}')".format(project_id.lower(),teradata_environment.lower(),task_type.lower(),subject_area)
        query_job = hook.run(sql)
        logging.info(f'Result from BigQuery: {query_job}')


# function to call product_etl_control_load from a python operator
def get_product_elt_control_load(task_id, subject_area, task_type, hook, dag):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id,use_legacy_sql=False,location=location)
    return PythonOperator(task_id=task_id,
                          python_callable=product_etl_control_load,
                          op_kwargs={'task_type': task_type, 'subject_area': subject_area, 'hook':hook, 'dag':dag},
                          retries=3)


                                              
                       

def get_cntrl_tbl_output(subject_area, hook, **kwargs):
    """
    This function executes SQLS on <DBENV>_NAP_UTL.ELT_CONTROL to get the control dates
    parameters : subject_area
    return: Control_dates
    """
    ti = kwargs['ti']
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id,use_legacy_sql=False,location=location)
    # jdbc_hook = JdbcHook(jdbc_conn_id=td_connection_audit)
    sql = """
    SELECT 
        CAST(EXTRACT_FROM_TMSTP AS STRING) AS Extract_Begin_Date, 
        CAST(EXTRACT_TO_TMSTP AS STRING) AS Extract_End_Date, 
        CAST(CURR_BATCH_DATE AS STRING) AS CURR_BATCH_DATE 
        FROM {}.{}_nap_utl.elt_control  
        WHERE Subject_Area_Nm='{}' """.format(project_id.lower(),teradata_environment.lower(), subject_area)

    # result = jdbc_hook.get_first(sql)
    # client=bigquery.Client(project=project_id)
    query_job = hook.get_first(sql)
    result = [ list(row) for row in query_job]
    
    logging.info(f"Result from BQ: {result}")
    ti.xcom_push(key='bq_query_result', value=result)
    
    if result is None:
        raise Exception('No records found for the subject area - {}'.format(subject_area))
    else:
        # Assuming result has the form: [[Extract_Begin_Date, Extract_End_Date, CURR_BATCH_DATE]]
        cntrl_dates = result[0][0] + "_" + result[0][1] + "_" + result[0][2]
        logging.info(cntrl_dates)
        return cntrl_dates


def get_cntrl_tbl_dates(task_id, subject_area, hook, dag):
    """
    This function gets the control table dates from <DBENV>_NAP_UTL.ELT_CONTROL
    parameters : Task_id, subject_area
    return: Dates in XCOM
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id,use_legacy_sql=False,location=location)
    return PythonOperator(task_id=task_id,
                          python_callable=get_cntrl_tbl_output,
                          op_kwargs={'subject_area': subject_area, 'hook':hook, 'dag':dag},
                          do_xcom_push=True,
                          retries=3)
