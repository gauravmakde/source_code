import logging
import os
import time

from airflow.hooks.S3_hook import S3Hook
from nordstrom.utils.aws_creds import aws_creds

__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS = 5
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


def gc_attributes_import_branching_callable(*args, **kwargs):
    env = os.environ['ENVIRONMENT']
    bucket = __bucket_mapping[env]
    prefix = 'data/gift_card_static_import/stg/gift_card_rpt_attributes/'

    result = __s3_list_objects(bucket=bucket, prefix=prefix)
    logging.info(f'List of object : {result}')
    if result:
        filtered_contents = list(filter(lambda elem: not (elem.endswith('/')), result))
        print(bool(filtered_contents))
        return {task1_name} if bool(filtered_contents) else {task2_name}

    return {task2_name}


def __s3_list_objects(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            s3hook = __setup_aws()
            result = s3hook.list_keys(bucket,prefix)
            # result = boto3.client('s3').list_objects_v2(**{'Bucket': bucket, 'Prefix': prefix})
            return result
        except Exception as e:
            logging.info(f'Failed to access path. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)