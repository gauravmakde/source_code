import logging
import os
import time

import boto3

__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30


def clean_path(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket)
            for ob in bucket.objects.filter(Prefix=prefix):
                if not (ob.meta.data['Key'].endswith('/') and ob.meta.data['Size'] == 0):
                    logging.info(ob.delete())
            break
        except Exception as e:
            logging.warning(f'Failed to clean data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)


def s3_load_object(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            return boto3.client('s3').get_object(Bucket=bucket, Key=prefix)['Body'].read()
        except Exception as e:
            logging.warning(f'Failed to load data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def s3_put_object(bucket, key, body):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            return boto3.client('s3').put_object(
                Bucket=bucket,
                Key=key,
                Body=body
            )
        except Exception as e:
            logging.warning(f'Failed to put object. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def copy_objects(source_bucket, source_prefix, target_bucket, target_prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            s3 = boto3.resource('s3')
            t_bucket = s3.Bucket(target_bucket)
            s_bucket = s3.Bucket(source_bucket)

            for obj in s_bucket.objects.filter(Prefix=source_prefix):
                target_key = obj.key.replace(source_prefix, target_prefix)
                t_bucket.Object(target_key).copy(
                    {
                        'Bucket': source_bucket,
                        'Key': obj.key
                    }
                )
                logging.info(f"{source_bucket}/{obj.key} ---> {target_bucket}/{target_key}")

            break
        except Exception as e:
            logging.warning(f'Failed to copy data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)
