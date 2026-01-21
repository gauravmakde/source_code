import argparse
import logging
import os
import time

import boto3

__NUMBER_OF_TRIES_TO_DELETE_OBJECTS = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30


def __get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--s3_source_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_source_prefix_format", required=True, type=str)

    return parser.parse_args()


def get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)


def clean_path(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_DELETE_OBJECTS):
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket)
            for ob in bucket.objects.filter(Prefix=prefix):
                print(ob.delete())
            break
        except Exception as e:
            print(f'Failed to clean data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_DELETE_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_DELETE_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def main():
    args = __get_arguments()

    source_bucket = get_env_variable(args.s3_source_bucket_env_name)
    print(source_bucket)

    source_prefix = args.s3_source_prefix_format
    print(source_prefix)
    logging.info(f'Source path: 3://{source_bucket}/{source_prefix}')

    clean_path(bucket=source_bucket, prefix=source_prefix)

if __name__ == '__main__':
    logger = logging.getLogger()
    logging.info(f"Deletion start")
    logger.setLevel(logging.INFO)
    main()
