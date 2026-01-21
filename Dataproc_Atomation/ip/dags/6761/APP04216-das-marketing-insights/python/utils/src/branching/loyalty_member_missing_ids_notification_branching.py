import logging
import os
import time

from airflow.hooks.S3_hook import S3Hook
from nordstrom.utils.aws_creds import aws_creds

__NUMBER_OF_RETRIES = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30
__bucket_mapping = {
    'nonprod': 'tf-das-marketing-nonprod-teradata-stage',
    'prod': 'tf-das-marketing-prod-teradata-stage'
}


def __setup_aws():
    @aws_creds(
        nauth_conn_id="nauth_connection",
        aws_conn_id="aws_s3_connection"
    )
    def set_up_credential():
        return S3Hook("aws_s3_connection")

    return set_up_credential()


# Checks if there is a file with missing IDs and if this file has some data
# Returns task1_name to process this data or task2_name to skip notification process
def check_missing_ids_presence(*args, **kwargs):
    env = os.environ['ENVIRONMENT']
    bucket = __bucket_mapping[env]
    prefix = 'missing_ids/loyalty_member/missing_create_ids/'
    s3hook = __setup_aws()

    result = __s3_list_objects(s3hook, bucket=bucket, prefix=prefix)
    logging.info(f'List of object : {result}')
    if result:
        data_prefixes = [item for item in result if '_SUCCESS' not in item]
        if len(data_prefixes) > 0:
            data_prefix = data_prefixes[0]
            file_content = __get_s3_file(s3hook, bucket, data_prefix)
            if len(file_content) > 0:
                return {task1_name} if len(result) > 1 else {task2_name}
    return {task2_name}


def __s3_list_objects(s3hook, bucket, prefix):
    for i in range(__NUMBER_OF_RETRIES):
        try:
            result = s3hook.list_keys(bucket, prefix)
            return result
        except Exception as e:
            logging.info(f'Failed to access path. Try {i + 1}/{__NUMBER_OF_RETRIES}')
            if (i + 1) == __NUMBER_OF_RETRIES:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def __get_s3_file(s3hook, bucket, prefix):
    for i in range(__NUMBER_OF_RETRIES):
        try:
            return s3hook.read_key(key=prefix, bucket_name=bucket)
        except Exception as e:
            logging.info(f'Failed to access path. Try {i + 1}/{__NUMBER_OF_RETRIES}')
            if (i + 1) == __NUMBER_OF_RETRIES:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)
