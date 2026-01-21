import logging
from datetime import date
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

logging.basicConfig(level='INFO')
dag_run = """
select id, dag_id, state, run_id, external_trigger, start_date, end_date, date(execution_date) as execution_date
from dag_run where date(execution_date) BETWEEN TO_DATE('2023-01-01', 'YYYY-MM-DD') and current_date;
""" # ALL DAG Run info for last 7 days

pg_conn_id = "TECH_ACE_ENGINEERING_AIRFLOW_DAG_STATS" # same across Dev and Prod

def get_stats_csv():
    try:
        db_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    except:
        raise Exception("Error on conn_id")
    
    dr_result = db_hook.get_pandas_df(dag_run)
    
    # Make TPT-ready
    dr_result.to_csv('dr_techex.csv', index=False, sep = """""", header= False) # use TPT separator for easy TPT load

    # hacky logic to determine the executing environment - not really a good way to do it otherwise...
    try:
        bucket = 'acedev-etl'
        s3_inst = S3Hook(aws_conn_id='TECH_ACE_ENGINEERING_SQS_CONN_DEV') #as-de-nonprod-role IAM role w/ attached S3 policy to acedev-etl
        s3_inst.load_file(filename='dr_techex.csv', key='tpt_load/techex_dag_run/hist_result.csv', bucket_name=bucket, replace=True)
    except S3UploadFailedError:
        bucket = 'ace-etl'
        s3_inst = S3Hook(aws_conn_id='TECH_ACE_ENGINEERING_SQS_CONN_PROD') #as-de-prod-role IAM role w/ attached S3 policy to ace-etl
        s3_inst.load_file(filename='dr_techex.csv', key='tpt_load/techex_dag_run/hist_result.csv', bucket_name=bucket, replace=True)
    # if result is None:
    #     raise Exception('No records found')


def callable_func_name(*arg, **kwargs):
    get_stats_csv()
    if 1 == 1:  
        return {task1}
    else:
        return {task2}