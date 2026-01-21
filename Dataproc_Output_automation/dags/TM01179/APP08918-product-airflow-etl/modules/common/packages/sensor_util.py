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
from nordstrom.utils.cloud_creds import cloud_creds

# fetch environment from OS variable
env = os.environ['ENVIRONMENT']

SUCCESS_FILE_NAME = '_SUCCESS'

# Read the config files
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
config.read(os.path.join(Path(path).parent, 'configs/sensors.cfg'))

# service now configs
nauth_conn_id = config.get(env, 'nauth_conn_id')
aws_conn_id = config.get(env, 'aws_conn_id')
gitlab_conn_id = config.get(env,'gitlab_conn_id')

# Folder format
s3bucket_date_format = '/year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/' \
                       'hour={{ ts_nodash[9:11]}}/min={{ ts_nodash[11:13]}}/'


def setup_aws():
	"""
	This function refreshes the aws credentials by running aws_creds using NAuth.
	The triggers the corresponding dag.
		Parameters : None
		Returns    : None
	"""
	@cloud_creds(
        nauth_conn_id="nauth-connection-nonprod",
        cloud_conn_id="aws_default",
        aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer",
    )
	def set_up_credential():
		logging.info("AWS connection is refreshed!!")
	set_up_credential()


def check_s3_keys(s3_bucket_name, s3_bucket_key, poll_timeout_min, poke_interval_sec, **kwargs):
	"""
	This function checks if the s3Key being passed exists or not
		Parameters : s3_bucket_name, s3_bucket_key, poll_timeout_min, poke_interval_sec
		Returns    : None
	"""
	more_iterations = True
	total_number_of_iteration = 0
	logging.info(f'S3 Bucket being polled:- S3BucketName: {s3_bucket_name} S3BucketKey :{s3_bucket_key}')
	while more_iterations:
		setup_aws()
		s3 = S3Hook(aws_conn_id)
		obj_status = s3.check_for_key(bucket_name=s3_bucket_name, key=s3_bucket_key)
		if obj_status:
			logging.info(f'File Found: S3BucketName: {s3_bucket_name} S3BucketKey :{s3_bucket_key} ')
			more_iterations = False
		if more_iterations:
			total_number_of_iteration = total_number_of_iteration + 1
			logging.info(f'Poll for  S3BucketName: {s3_bucket_name} S3BucketKey :{s3_bucket_key} '
						 f'- Iteration Count -{total_number_of_iteration}')
			sleep(poke_interval_sec)
		if total_number_of_iteration > int(poll_timeout_min):
			raise Exception(f'File not received S3BucketName: {s3_bucket_name} S3BucketKey :{s3_bucket_key}')


def s3_check_keys(task_id, s3_bucket_name, s3_bucket_key, poll_timeout_min, poke_interval_sec):
	"""
	This function checks if the s3Key being passed exists or not
	Parameters : TaskId, s3_bucket_name, s3_bucket_key, poll_timeout_min, poke_interval_sec
	Returns    : task
	"""
	return PythonOperator(
			task_id=task_id,
			python_callable=check_s3_keys,
			op_kwargs={'s3_bucket_name': s3_bucket_name, 's3_bucket_key': s3_bucket_key,
					   'poll_timeout_min': poll_timeout_min, 'poke_interval_sec': poke_interval_sec},
			provide_context=True,
			retries=0
	)


def check_s3_key(s3key):
    """
       This function checks if the s3Key being passed exists ot not
         Parameters : Context
         Returns    : message or raise error
    """
    setup_aws()
    hook = S3Hook(aws_conn_id)
    batch_already_run = hook.check_for_key(s3key)
    if batch_already_run:
        return f'File is present in the path: {s3key}'
    else:
        raise Exception(f'File NOT already present in the path: {s3key}')


def s3_check_key(task_id, s3key):
    """
       This function calls check_s3_key for checking if an S3 File exists or not
         Parameters : S3Key
         Returns    : operator
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=check_s3_key,
        provide_context=True,
        op_kwargs={'s3key': s3key},
        retries=3
    )


def s3_check_success_file(task_id, s3_root_key):
    """
       This function calls s3_check_key for checking if an _SUCCESS S3 File exists or not
         Parameters : s3_root_key
         Returns    : operator
    """
    return s3_check_key(task_id, f'{s3_root_key}{SUCCESS_FILE_NAME}')


def s3_file_check(task_id, s3_bucket_name, s3_file_key):
    """
    This function checks if the s3Key being passed exists or not
    Parameters : s3BucketName, s3FileKey
    Returns    : operator
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=check_s3_keys,
        op_kwargs={'s3_bucket_name': s3_bucket_name, 's3_bucket_key': s3_file_key,
                   'poll_timeout_min': 180, 'poke_interval_sec': 5},
        provide_context=True,
        retries=0
    )
