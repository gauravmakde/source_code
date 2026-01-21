import os
import configparser
from nordstrom.utils.setup_module import setup_module
from pathlib import Path

file_path = os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent, 'common/packages/__init__.py')
setup_module(module_name="ds_product_common", file_path=file_path)
from airflow.operators.python import PythonOperator
from nordstrom.utils.aws_creds import aws_creds
import logging
from airflow.hooks.S3_hook import S3Hook

#fetch environment from OS variable
env = os.environ['ENVIRONMENT']

# Fetch the data from config file
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
config.read(os.path.join(Path(path).parent, 'configs/s3.cfg'))
# AWS configs
nauth_conn_id = config.get(env, 'nauth_conn_id')
default_aws_conn_id = config.get(env, 'aws_conn_id')
gitlab_conn_id = config.get(env,'gitlab_conn_id')

# EC2 Path variables
default_s3bucket_date_format = 'year={{ ds_nodash[:4]}}/month={{ ds_nodash[4:6]}}/day={{ ds_nodash[6:8]}}/hour={{ ts_nodash[9:11]}}/'

def setup_aws(aws_conn_id):
    """
    This function refreshes the aws credentials by running aws_creds using NAuth
        Parameters : None
        Returns    : None
    """
    @aws_creds(nauth_conn_id="nauth-connection-nonprod",cloud_conn_id="aws_default",aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer")
    def set_up_credential():
        logging.info("AWS connection is refreshed!!")
    set_up_credential()


def get_s3_client(aws_conn_id=default_aws_conn_id):
    """
    This function return boto3.client (session.py)
    Parameters : aws_conn_id
    Returns    : S3 boto3.client
    """
    setup_aws(aws_conn_id)
    s3 = S3Hook(aws_conn_id)
    return s3.get_conn()


def __copy_files_recursively(s3_hook, source_s3_path, target_s3_path, s3bucket_date_format, acl_policy):
    """
    This private function copies files recursively between the source_s3_path and target_s3_path (same account)
    Parameters : s3_hook, source_s3_path, target_s3_path, s3bucket_date_format, acl_policy
    """
    source_bucket_name, source_prefix = s3_hook.parse_s3_url(source_s3_path)
    out_s3path = target_s3_path + s3bucket_date_format
    dest_bucket_name, dest_prefix = s3_hook.parse_s3_url(out_s3path)
    logging.info(f'source_s3_path: {source_s3_path}, target_s3_path: {out_s3path}')

    s3_client = s3_hook.get_conn()
    for source_key in list_files(s3_client, source_bucket_name, source_prefix):
        dest_key = dest_prefix + source_key[len(source_prefix):]
        s3_client.copy_object(CopySource={'Bucket': source_bucket_name, 'Key': source_key},
                              Bucket=dest_bucket_name,
                              Key=dest_key,
                              ACL=acl_policy)
        logging.info(f'copied from s3://{source_bucket_name}/{source_key} to s3://{dest_bucket_name}/{dest_key}')


def list_files(s3_client, s3_bucket, prefix):
    """
    This function yields all keys in an S3 bucket
    Parameters : S3 boto3.client, s3_bucket, prefix
    Returns    : object keys
    """
    params = {'Bucket': s3_bucket, 'Prefix': prefix}

    while True:
        objects = s3_client.list_objects_v2(**params)
        for obj in objects.get('Contents', []):
            yield obj['Key']
        try:
            params['ContinuationToken'] = objects['NextContinuationToken']
        except KeyError:
            break


def copy_files(source_s3_path, target_s3_path, s3bucket_date_format, aws_conn_id, acl_policy):
    """
    This function copies files between the source_s3_path and target_s3_path (same account)
    Parameters : TaskId, source_s3_path, target_s3_path
    Returns    : task
    """
    setup_aws(aws_conn_id)
    s3 = S3Hook(aws_conn_id)
    __copy_files_recursively(s3, source_s3_path, target_s3_path, s3bucket_date_format, acl_policy)


def copy_files_several_paths(paths, s3bucket_date_format, aws_conn_id, acl_policy):
    """
    This function copies files between the source_s3_path and target_s3_path (same account) provided by list of tuple
    Parameters : paths [(source_s3_path1, target_s3_path1),...,(source_s3_pathN, target_s3_pathN)]
    Returns    : task
    """
    setup_aws(aws_conn_id)
    s3 = S3Hook(aws_conn_id)
    for source_s3_path, target_s3_path in paths:
        __copy_files_recursively(s3, source_s3_path, target_s3_path, s3bucket_date_format, acl_policy)


def copy_file_by_key(source_bucket, source_key, target_bucket, target_key, acl_policy, aws_conn_id):
    """
    This function copies a file between the source_bucket and target_bucket (same account)
    Parameters : source_bucket, source_key, target_bucket, target_key, acl_policy, aws_conn_id
    Returns    : task
    """
    setup_aws(aws_conn_id)
    s3 = S3Hook(aws_conn_id)

    logging.info("source file: " + source_bucket + "/" + source_key)
    logging.info("target file: " + target_bucket + "/" + target_key)

    s3.copy_object(
        source_bucket_name=source_bucket,
        source_bucket_key=source_key,
        dest_bucket_name=target_bucket,
        dest_bucket_key=target_key,
        acl_policy=acl_policy
    )


def s3_copy_files(task_id, source_s3_path, target_s3_path, s3bucket_date_format=default_s3bucket_date_format, aws_conn_id=default_aws_conn_id, acl_policy='bucket-owner-full-control'):
    """
    This function copies files within the same account
    Parameters : TaskId, source_s3_path, target_s3_path
    Returns    : task
    """
    return PythonOperator(task_id=task_id,
                          python_callable=copy_files,
                          op_kwargs={'source_s3_path': source_s3_path,
                                     'target_s3_path': target_s3_path,
                                     's3bucket_date_format': s3bucket_date_format,
                                     'aws_conn_id': aws_conn_id,
                                     'acl_policy': acl_policy},
                          provide_context=True,
                          retries=3)


def s3_copy_files_several_paths(task_id, paths, s3bucket_date_format=default_s3bucket_date_format, aws_conn_id=default_aws_conn_id, acl_policy='bucket-owner-full-control'):
    """
    This function copies files within the same account between the source_s3_path and target_s3_path (same account) provided by list of tuple
    Parameters : TaskId, paths [(source_s3_path1, target_s3_path1),...,(source_s3_pathN, target_s3_pathN)]
    Returns    : task
    """
    return PythonOperator(task_id=task_id,
                          python_callable=copy_files_several_paths,
                          op_kwargs={'paths': paths,
                                     's3bucket_date_format': s3bucket_date_format,
                                     'aws_conn_id': aws_conn_id,
                                     'acl_policy': acl_policy},
                          provide_context=True,
                          retries=3)


def s3_copy_file_by_key(task_id, source_bucket, source_key, target_bucket, target_key, aws_conn_id=default_aws_conn_id, acl_policy='bucket-owner-full-control'):
    """
    This function copies files within the same account
    Parameters : TaskId, source_s3_path, target_s3_path
    Returns    : task
    """
    return PythonOperator(task_id=task_id, python_callable=copy_file_by_key,
                          op_kwargs={'source_bucket': source_bucket,
                                     'source_key': source_key,
                                     'target_bucket': target_bucket,
                                     'target_key': target_key,
                                     'aws_conn_id': aws_conn_id,
                                     'acl_policy': acl_policy},
                          provide_context=True,
                          retries=3)
