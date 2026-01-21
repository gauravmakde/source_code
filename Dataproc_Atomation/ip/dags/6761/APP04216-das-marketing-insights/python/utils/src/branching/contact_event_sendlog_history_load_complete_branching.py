import json
import logging
import os
import time

import boto3

__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30
__bucket_mapping = {
    'development': 'tf-das-marketing-nonprod-teradata-stage',
    'production': 'tf-das-marketing-prod-teradata-stage'
}


def __s3_load_object(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            return boto3.client('s3').get_object(Bucket=bucket, Key=prefix)['Body'].read()
        except Exception as e:
            logging.warning(f'Failed to load data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def contact_event_sendlog_history_is_load_completed(*args, **kwargs):
    env = os.environ['ENVIRONMENT']
    bucket = __bucket_mapping[env]
    execution_plan_key = 'manifest/contact_event_sendlog/execution_plan.json'

    execution_plan = json.loads(__s3_load_object(bucket=bucket, prefix=execution_plan_key))
    current_part = execution_plan["current"]
    parts = list(filter(lambda key: key.startswith('part_'), execution_plan.keys()))

    logging.info(f'Current part is {current_part}')
    logging.info(f'All execution parts: {parts}')

    return {task1_name} if parts.index(current_part) != len(parts) - 1 else {task2_name}
