import logging
from datetime import date
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

logging.basicConfig(level='INFO')
dag_run = """
select * from dag_run where dag_id like '%11521_ACE%'
and date(execution_date) BETWEEN current_date-7 and current_date;
""" # DAG Run info for last 7 days

task_instance = """
select * from task_instance where dag_id like '%11521_ACE%'
and date(execution_date) BETWEEN current_date-7 and current_date;
""" # Task instance info for last 7 days

dag = """
select * from dag where owners = 'TECH_ACE_ENGINEERING';
""" # all DAG's - including inactive/paused ones - good for DDL. Kill 'n' fill.

sla_miss = """
select * from sla_miss where dag_id like '%11521_ACE%'
and date(execution_date) BETWEEN current_date-7 and current_date;
""" # SLA misses from last 7 days 

pg_conn_id = "TECH_ACE_ENGINEERING_AIRFLOW_DAG_STATS" # same across Dev and Prod

def get_stats_csv():
    try:
        db_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    except:
        raise Exception("Error on conn_id")
    
    dr_result = db_hook.get_pandas_df(dag_run)
    ti_result = db_hook.get_pandas_df(task_instance)
    dag_result = db_hook.get_pandas_df(dag)
    sla_result = db_hook.get_pandas_df(sla_miss)

    dr_result.to_csv('dr_result.csv', index=False) 
    ti_result.to_csv('ti_result.csv', index=False) 
    dag_result.to_csv('dag_result.csv', index=False) 
    sla_result.to_csv('sla_result.csv', index=False) 

    # hacky logic to determine the executing environment - not really a good way to do it otherwise...
    try:
        bucket = 'acedev-etl'
        s3_inst = S3Hook(aws_conn_id='TECH_ACE_ENGINEERING_SQS_CONN_DEV') #as-de-nonprod-role IAM role w/ attached S3 policy to acedev-etl
        s3_inst.load_file(filename='dr_result.csv', key='dsae_airflow_stats/dr_result.csv', bucket_name=bucket, replace=True)
        s3_inst.load_file(filename='ti_result.csv', key='dsae_airflow_stats/ti_result.csv', bucket_name=bucket, replace=True)
        s3_inst.load_file(filename='dag_result.csv', key='dsae_airflow_stats/dag_result.csv', bucket_name=bucket, replace=True)
        s3_inst.load_file(filename='sla_result.csv', key='dsae_airflow_stats/sla_result.csv', bucket_name=bucket, replace=True)
    except S3UploadFailedError:
        bucket = 'ace-etl'
        s3_inst = S3Hook(aws_conn_id='TECH_ACE_ENGINEERING_SQS_CONN_PROD') #as-de-prod-role IAM role w/ attached S3 policy to ace-etl
        s3_inst.load_file(filename='dr_result.csv', key='dsae_airflow_stats/dr_result.csv', bucket_name=bucket, replace=True)
        s3_inst.load_file(filename='ti_result.csv', key='dsae_airflow_stats/ti_result.csv', bucket_name=bucket, replace=True)
        s3_inst.load_file(filename='dag_result.csv', key='dsae_airflow_stats/dag_result.csv', bucket_name=bucket, replace=True)
        s3_inst.load_file(filename='sla_result.csv', key='dsae_airflow_stats/sla_result.csv', bucket_name=bucket, replace=True)
    # if result is None:
    #     raise Exception('No records found')


def callable_func_name(*arg, **kwargs):
    get_stats_csv()
    if 1 == 1:  
        return {task1}
    else:
        return {task2}