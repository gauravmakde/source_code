# common function modules that will be used in all the dags
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.hooks.jdbc_hook import JdbcHook
from google.api_core.exceptions import GoogleAPIError
from datetime import timedelta, datetime
import configparser
import logging
import os
from pathlib import Path
from os import path

# fetch environment from OS variable
# env = 'nonprod'
# env = os.environ['ENVIRONMENT']
env = 'development'


# Read the config files
config = configparser.ConfigParser()
# path = os.path.split(__file__)[0]
# config.read(os.path.join(Path(path).parent, 'modules/common/configs/bigquery.cfg'))
path = path.dirname(__file__).split('APP08918-gcp-product-airflow-etl')[
                0] + 'APP08918-gcp-product-airflow-etl/'
config.read(os.path.join(path, 'modules/common/configs/bigquery.cfg'))

# Get the Teradata connection details based on the environment
bigquery_environment = config.get(env, 'bigquery_environment')
control_db_name = config.get(env, 'control_db_name')
teradata_db_host = config.get(env, 'teradata_db_host')
teradata_user = config.get(env, 'teradata_user')
teradata_pwd = config.get(env, 'teradata_pwd')
ssh_connection_id = config.get(env, 'ssh_connection_id')
td_connection_audit = config.get(env,'td_connection_audit')

# Preprod variables
bigquery_environment_preprod = config.get(env, 'bigquery_environment_preprod')
control_db_name_preprod = config.get(env, 'control_db_name_preprod')
teradata_user_preprod = config.get(env, 'teradata_user_preprod')
teradata_pwd_preprod = config.get(env, 'teradata_pwd_preprod')
ssh_connection_id_preprod = config.get(env, 'ssh_connection_id_preprod')
service_account_email = config.get(env, "service_account_email")
subnet_url = config.get(env, "subnet_url")
project_id = config.get(env, "gcp_project_id")
pyspark_path = config.get(env, "pyspark_path")
region = config.get(env, "region")
gcp_conn_id=config.get(env, 'gcp_conn')

# Initializing BQ hook
def get_batch_id(dag_id, task_id):

    current_datetime = datetime.today()
    if len(dag_id) <= 22:
        dag_id = dag_id.replace("_", "-").rstrip("-")
        ln_task_id = 45 - len(dag_id)
        task_id = task_id[-ln_task_id:].replace("_", "-").strip("-")
    elif len(task_id) <= 22:
        task_id = task_id.replace("_", "-").strip("-")
        ln_dag_id = 45 - len(task_id)
        dag_id = dag_id[:ln_dag_id].replace("_", "-").rstrip("-")
    else:
        dag_id = dag_id[:23].replace("_", "-").rstrip("-")
        task_id = task_id[-22:].replace("_", "-").strip("-")

    date = current_datetime.strftime("%Y%m%d%H%M%S")
    return f"""{dag_id}--{task_id}--{date}""".lower()





def call_bqinsert_operator(dag,task_id,sql_file_location,project_id,parameters,location,gcp_conn_id):
    with open(sql_file_location,'r') as file1:
            sql_string = file1.read()

    return BigQueryInsertJobOperator(dag = dag,
        
        task_id=task_id,
        configuration={
            "query": {
                "query": sql_string,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id = gcp_conn_id,
        location=location
    )



def call_s3_to_gcs(task_id,aws_conn_id,gcp_conn_id,s3_bucket,s3_prefix,dest_gcs,dag):
    return S3ToGCSOperator(
            task_id=task_id,
            aws_conn_id=aws_conn_id,
            bucket=s3_bucket,
            prefix=s3_prefix,
            gcp_conn_id=gcp_conn_id,
            dest_gcs=dest_gcs,
            replace=False,
            dag=dag,
    )

def dataproc_gcs_to_bq(task_id,dag_name,dataset_name,table_name,gcp_conn,csv_file_path,dag):
    session_name = get_batch_id(dag_id=f"gcp_{dag_name}", task_id=task_id)
    return DataprocCreateBatchOperator(
        task_id=task_id,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": pyspark_path,
                "args": [
                    f"{project_id}",
                    dataset_name,table_name,
                    f"{csv_file_path}*"
                ],
            },
            "environment_config": {
                "execution_config": {
                    "service_account": service_account_email,
                    "subnetwork_uri": subnet_url,
                }
            },
        },
        region=region,
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        batch_id=session_name,
        dag=dag
    )






def etl_control_load(hook,subject_area,task_type):
    # jdbc_hook = JdbcHook(jdbc_conn_id=td_connection_audit)
    if task_type == 'START':
        sql = "CALL `{}`.{}_NAP_UTL.ELT_CONTROL_{}_LOAD('{}')".format(project_id,bigquery_environment, task_type.upper(), subject_area)
        try:
            result = hook.get_records(sql=sql)
            logging.info(f'''The output is : {result}''')
        except GoogleAPIError as e:
           print(f"An error occurred: {e}")
        if result is None:
            raise Exception('no records found')

    elif task_type == 'END':
        sql = "CALL `{}`.{}_NAP_UTL.ELT_CONTROL_{}_LOAD('{}')".format(project_id,bigquery_environment, task_type.upper(), subject_area)
        try:
            result = hook.get_records(sql=sql)
            logging.info(f'''The output is : {result}''')
        except GoogleAPIError as e:
            print(f"An error occurred: {e}")
            if result is None:
                raise Exception('no records found')



# function to call etl_control_load from a python operator
def get_elt_control_load(task_id,hook,subject_area,task_type):
    return PythonOperator(task_id = task_id,
                          python_callable=etl_control_load,
                          op_kwargs={'task_type': task_type,'hook':hook, 'subject_area': subject_area},
                          retries=3)

def product_etl_control_load(subject_area, task_type, hook):
    # jdbc_hook = JdbcHook(jdbc_conn_id=td_connection_audit)

    if task_type == 'START':
        sql = "CALL `{}`.{}_NAP_UTL.PRODUCT_ELT_CONTROL_{}_LOAD('{}')".format(project_id,bigquery_environment, task_type.upper(), subject_area)
        result = hook.run(sql)

    elif task_type == 'END':
        sql = "CALL `{}`.{}_NAP_UTL.ELT_CONTROL_{}_LOAD('{}')".format(project_id,bigquery_environment, task_type.upper(), subject_area)
        result = hook.run(sql)


# function to call product_etl_control_load from a python operator
def get_product_elt_control_load(task_id, hook, subject_area, task_type):
    print(f"Received args: {task_id}, {hook}, {subject_area}, {task_type}")
    return PythonOperator(task_id=task_id,
                          python_callable=product_etl_control_load,
                          op_kwargs={'subject_area': subject_area,'task_type': task_type, 'hook':hook},
                          retries=3)

def etl_batch_load_status(subject_area, task_type, **kwargs):
    """
    This function prevents parallel execution by checking the batch_status in XCOM
    of the relevant subject area .
    parameters : subject_Area , Task_type
    return : XCOM.
    """
    ti = kwargs['ti']
    key_name = 'ETL_BATCH_PROGRESS_' + subject_area
    # Get the status of the subject area
    batch_status = ti.xcom_pull(key=key_name, include_prior_dates=True)


# Batch_status set manually for testing.

    batch_status = 'N'
    logging.info(f'The current batch status for {key_name} is {batch_status}.')

    # for the first time execution, set the status flag to N
    if batch_status is None:
        batch_status = 'N'

    # If a load is in progress and task type is START, then raise exception and fail
    if task_type == 'START' and batch_status == 'Y':
        raise Exception(f'ETL batch is in progress for {subject_area}')

    # Set the ETL batch process flag based on the value of task_type.
    if task_type == 'START':
        batch_status = 'Y'
    else:
        batch_status = 'N'

    # Push the batch status to XCOM
    ti.xcom_push(key=key_name, value=batch_status)
    logging.info(f'Value of the batch status set for {key_name} is {batch_status}.')


def get_batch_load_status(task_id, subject_area, task_type):
    """
    This function call etl_batch_load_status from a python operator
    parameters : Task_id, Subject_Area , Task_type
    return : None.
    """
    return PythonOperator(task_id=task_id,
                          python_callable=etl_batch_load_status,
                          op_kwargs={'subject_area': subject_area , 'task_type': task_type},
                          provide_context=True,
                          trigger_rule="none_failed",)


def get_cntrl_tbl_output(subject_area,hook):
    """
    This function executes SQLS on <DBENV>_NAP_UTL.ELT_CONTROL to get the control dates
    parameters : subject_area
    return: Control_dates
    """

    sql = " SELECT \
    CAST(EXTRACT_FROM_TMSTP AS STRING) AS Extract_Begin_Date, \
    CAST(EXTRACT_TO_TMSTP AS STRING) AS Extract_End_Date, \
    CAST(CURR_BATCH_DATE AS STRING) AS CURR_BATCH_DATE \
FROM `{}_NAP_UTL.ELT_CONTROL` \
WHERE Subject_Area_Nm = '{}'".format(bigquery_environment, subject_area)

    result = hook.get_first(sql)
    if result is None:
        raise Exception('No records found for the subject area - {}'.format(subject_area))
    else:
        cntrl_dates = result[0] + "_" + result[1] + "_" + result[2]
        logging.info(cntrl_dates)
        return cntrl_dates
    
    # query_job = hook.get_records(sql)
    # result = [ list(row) for row in query_job]
    
    # logging.info(f"Result from BQ: {result}")
       
    # if len(result) < 1:
    #     raise Exception('No records found')
    # else:
    #     logging.info(f'Result from BigQuery: {result[0]}')
    #     cntrl_dates = result[0][0] + "_" + result[0][1] + "_" + result[0][2]
    #     logging.info(cntrl_dates)
    #     return cntrl_dates

def get_cntrl_tbl_dates(task_id, subject_area,hook):
    """
    This function gets the control table dates from <DBENV>_NAP_UTL.ELT_CONTROL
    parameters : Task_id, subject_area
    return: Dates in XCOM
    """
    return PythonOperator(task_id=task_id,
                          python_callable=get_cntrl_tbl_output,
                          op_kwargs={'subject_area': subject_area,'hook':hook},
                          do_xcom_push=True,
                          retries=3)
def get_first_rec_into_xcom(task_id, query_txt, hook, **kwargs):
    """
    This function executes SQLS and return result to XCOM
    parameters : subject_area
    return: query result (No results will default XCOM to 0)
    """
    # jdbc_hook = JdbcHook(jdbc_conn_id=td_connection_audit)
    sql= query_txt.replace("<DBENV>", bigquery_environment)
    logging.info(f'Query executed : {sql}')
    result_list = hook.get_first(sql)
    
    if result_list is None:
        kwargs['ti'].xcom_push(key=task_id, value=0)
        logging.info(f'No results for the query.')
    else:
        result = result_list[0]
        kwargs['ti'].xcom_push(key=task_id, value=result)
        logging.info(f'Result pushed to XCom for {task_id}: {result}')
        return result

def get_query_results_into_xcom(task_id, query_txt, hook):
    """
    This function gets the first record of the executed query into XCOM
    parameters : Task_id, query_txt
    return: results in XCOM
    """
    return PythonOperator(task_id=task_id,
                          python_callable=get_first_rec_into_xcom,
                          op_kwargs={'task_id' : task_id,
                                     'query_txt': query_txt,
                                     'hook': hook},
                          do_xcom_push=True,
                          provide_context=True,
                          retries=3)
