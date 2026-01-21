import argparse
import logging
import os
import time

import boto3

__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30
__sensor_mode_handlers = {
    'bool_value': lambda result, path: result,
    'fail_on_exist': lambda result, path: result if not result else __raise_error(f'Provided path: ({path}) is not empty.'),
    'fail_on_absent': lambda result, path: result if result else __raise_error(f'Provided path: ({path}) is empty.')
}


def __raise_error(msg):
    raise ValueError(msg)


def __get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--s3_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_prefix", required=True, type=str)
    parser.add_argument("--sensor_mode", default="bool_value", help="bool_value - default/fail_on_exist/fail_on_absent")
    parser.add_argument("--ignore_folders", action="store_true", default=False, required=False)

    return parser.parse_args()


def __get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)


def __verify_mode(mode):
    if mode not in __sensor_mode_handlers.keys():
        raise ValueError(f"Invalid s3 sensor mode: {mode}. Valid values: {list(__sensor_mode_handlers.keys())}")


def main():
    args = __get_arguments()
    __verify_mode(args.sensor_mode)

    bucket = __get_env_variable(args.s3_bucket_env_name)

    result = is_objects_stored(
        bucket=bucket,
        prefix=args.s3_prefix,
        ignore_folders=args.ignore_folders
    )

    return __sensor_mode_handlers[args.sensor_mode](result, f's3://{bucket}/{args.s3_prefix}')


def is_objects_stored(bucket, prefix, ignore_folders=False):
    result = __s3_list_objects(bucket=bucket, prefix=prefix)

    if ignore_folders and 'Contents' in result:
        filtered_contents = list(filter(lambda elem: not (elem['Key'].endswith('/') and elem['Size'] == 0), result['Contents']))
        return bool(filtered_contents)

    return 'Contents' in result


def __s3_list_objects(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            result = boto3.client('s3').list_objects_v2(**{'Bucket': bucket, 'Prefix': prefix})
            return result
        except Exception as e:
            logging.info(f'Failed to access path. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
