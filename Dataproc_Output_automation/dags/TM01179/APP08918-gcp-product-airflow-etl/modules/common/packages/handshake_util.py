# common function modules that will be used in all the dags
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.hooks.S3_hook import S3Hook
import configparser
import json
import logging
from pathlib import Path
from nordstrom.utils.aws_creds import aws_creds
import os
from time import sleep
from os import path


# fetch environment from OS variable
env = 'nonprod'

# Read the config files
config = configparser.ConfigParser()
path = path.dirname(__file__).split('APP08918-gcp-product-airflow-etl')[
                0] + 'APP08918-gcp-product-airflow-etl/'
config.read(os.path.join(path, 'modules/common/configs/handshake.cfg'))


# service now configs
nauth_conn_id = config.get(env, 'nauth_conn_id')
aws_conn_id = config.get(env, 'aws_conn_id')
gitlab_conn_id = config.get(env,'gitlab_conn_id')

def setup_aws():
	"""
	This function refreshes the aws credentials by running aws_creds using NAuth.
	The triggers the corresponding dag.
		Parameters : None
		Returns    : None
	"""
	@aws_creds(nauth_conn_id=nauth_conn_id, aws_conn_id=aws_conn_id, gitlab_conn_id=gitlab_conn_id)
	def set_up_credential():
		logging.info("AWS connection is refreshed!!")
	set_up_credential()


def create_handshake_files(s3_bucket_name, s3_bucket_key, handshake_execution_dt, **kwargs):
	"""
	This function checks if the s3Key being passed exists or not
		Parameters : s3_bucket_name, s3_bucket_key, poll_timeout_min, poke_interval_sec
		Returns    : None
	"""
	setup_aws()
	s3 = S3Hook("TECH_Merch_NAP_aws_conn")
	s3_file_key = s3_bucket_key + 'execution_dt=' + handshake_execution_dt + '/SUCCESS.txt'
	s3.get_conn().put_object(Bucket=s3_bucket_name, Key=s3_file_key)
	file_created = 's3://' + s3_bucket_name + '/' + s3_file_key
	logging.info(f'Handshake file created : {file_created}')


def s3_create_handshake_files(task_id, s3_bucket_name, s3_bucket_key, handshake_execution_dt):
	"""
	This function creates a handshake file
	Parameters : TaskId, s3_bucket_name, s3_bucket_key
	Returns    : task
	"""
	return PythonOperator(
			task_id=task_id,
			python_callable=create_handshake_files,
			op_kwargs={'s3_bucket_name': s3_bucket_name,
					   's3_bucket_key': s3_bucket_key,
					   'handshake_execution_dt' : handshake_execution_dt},
			provide_context=True,
			retries=0
	)
