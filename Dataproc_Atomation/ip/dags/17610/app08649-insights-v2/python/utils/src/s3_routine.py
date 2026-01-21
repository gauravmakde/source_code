import argparse
import logging
import os
import time
from datetime import datetime, timedelta

import boto3

__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30


def __get_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--routine_name", required=True, type=str)
  
    parser.add_argument("--source_weeks_delta", required=False, default=0, type=int)
    parser.add_argument("--source_days_delta", required=False, default=0, type=int)
    parser.add_argument("--source_hours_delta", required=False, default=0, type=int)
    parser.add_argument("--source_minutes_delta", required=False, default=0, type=int)
    parser.add_argument("--source_seconds_delta", required=False, default=0, type=int)
    parser.add_argument("--source_milliseconds_delta", required=False, default=0, type=int)
    parser.add_argument("--source_microseconds_delta", required=False, default=0, type=int)

    parser.add_argument("--target_weeks_delta", required=False, default=0, type=int)
    parser.add_argument("--target_days_delta", required=False, default=0, type=int)
    parser.add_argument("--target_hours_delta", required=False, default=0, type=int)
    parser.add_argument("--target_minutes_delta", required=False, default=0, type=int)
    parser.add_argument("--target_seconds_delta", required=False, default=0, type=int)
    parser.add_argument("--target_milliseconds_delta", required=False, default=0, type=int)
    parser.add_argument("--target_microseconds_delta", required=False, default=0, type=int)

    parser.add_argument("--weeks_delta", required=False, default=0, type=int)
    parser.add_argument("--days_delta", required=False, default=0, type=int)
    parser.add_argument("--hours_delta", required=False, default=0, type=int)
    parser.add_argument("--minutes_delta", required=False, default=0, type=int)
    parser.add_argument("--seconds_delta", required=False, default=0, type=int)
    parser.add_argument("--milliseconds_delta", required=False, default=0, type=int)
    parser.add_argument("--microseconds_delta", required=False, default=0, type=int)

    parser.add_argument("--s3_source_bucket_env_name", required=False, type=str)
    parser.add_argument("--s3_target_bucket_env_name", required=False, type=str)
    parser.add_argument("--s3_source_prefix_format", required=False, type=str)
    parser.add_argument("--s3_target_prefix_format", required=False, type=str)

    parser.add_argument("--s3_bucket_env_name", required=False, type=str)
    parser.add_argument("--s3_prefix_format", required=False, type=str) 

    parser.add_argument("--kms_key_id", required=False, type=str)  

    parser.add_argument("--clear_target_path", action="store_true", default=False, required=False)

    return parser.parse_args()


def __get_replace_date(initial_date: datetime, weeks_delta: int, days_delta: int, hours_delta: int, minutes_delta: int,
                       seconds_delta: int, milliseconds_delta: int, microseconds_delta: int) -> datetime:
    return initial_date + timedelta(weeks=weeks_delta, days=days_delta, hours=hours_delta,
                                    minutes=minutes_delta, seconds=seconds_delta,
                                    milliseconds=milliseconds_delta, microseconds=microseconds_delta)


def __get_source_replace_date(args, initial_date) -> datetime:
    result = __get_replace_date(
        initial_date=initial_date,
        weeks_delta=args.source_weeks_delta,
        days_delta=args.source_days_delta,
        hours_delta=args.source_hours_delta,
        minutes_delta=args.source_minutes_delta,
        seconds_delta=args.source_seconds_delta,
        milliseconds_delta=args.source_milliseconds_delta,
        microseconds_delta=args.source_microseconds_delta
    )

    logging.info(f'Replace date for source path: {str(result)}')

    return result


def __get_target_replace_date(args, initial_date) -> datetime:
    result = __get_replace_date(
        initial_date=initial_date,
        weeks_delta=args.target_weeks_delta,
        days_delta=args.target_days_delta,
        hours_delta=args.target_hours_delta,
        minutes_delta=args.target_minutes_delta,
        seconds_delta=args.target_seconds_delta,
        milliseconds_delta=args.target_milliseconds_delta,
        microseconds_delta=args.target_microseconds_delta,
    )

    logging.info(f'Replace date for target path: {str(result)}')

    return result

def __get_common_replace_date(args, initial_date) -> datetime:
    result = __get_replace_date(
        initial_date=initial_date,
        weeks_delta=args.weeks_delta,
        days_delta=args.days_delta,
        hours_delta=args.hours_delta,
        minutes_delta=args.minutes_delta,
        seconds_delta=args.seconds_delta,
        milliseconds_delta=args.milliseconds_delta,
        microseconds_delta=args.microseconds_delta
    )

    logging.info(f'Replace date for path: {str(result)}')

    return result


def __prepare_prefix(prefix_format, replace_date):
    replace_dict = {
        'year': str(replace_date.strftime("%Y")),
        'month': str(replace_date.strftime("%m")),
        'day': str(replace_date.strftime("%d")),
        'hour': str(replace_date.strftime("%H")),
        'minute': str(replace_date.strftime("%M")),
        'second': str(replace_date.strftime("%S")),
    }

    result = prefix_format

    for key, value in replace_dict.items():
        result = result.replace(f'<{key}>', str(value))

    return result


def get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)

def __check_for_none_value(arg_name, arg_value):
    if arg_value is None:
        raise ValueError(f"Required argument {arg_name} has {arg_value} value.")


def __execute_copy(source_bucket_name, source_prefix, target_bucket_name, target_prefix, kms_key_id=None):
    if kms_key_id:
        extra_args3 = {
                'ServerSideEncryption': 'aws:kms', 
                'SSEKMSKeyId': kms_key_id
            }
    else:
        extra_args3 = None

    logging.info(f'ExtraArgs: {extra_args3}')

    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            s3 = boto3.resource('s3')
            source_bucket = s3.Bucket(source_bucket_name)
            target_bucket = s3.Bucket(target_bucket_name)

            for obj in source_bucket.objects.filter(Prefix=source_prefix):
                copy_source = {
                    'Bucket': source_bucket_name,
                    'Key': obj.key
                }
                logging.info(f'__execute_copy: obj.key.replace: {source_prefix} to {target_prefix}')
                target_key = obj.key.replace(source_prefix, target_prefix)
                logging.info(f'__execute_copy: copy: CopySource={copy_source} ExtraArgs= {extra_args3}')
                target_bucket.Object(target_key).copy(CopySource=copy_source, ExtraArgs=extra_args3)

            break
        except Exception as e:
            print(f'Failed to copy data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def __clean_path(bucket, prefix):
    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket)
            for ob in bucket.objects.filter(Prefix=prefix): print(ob.delete())
            break
        except Exception as e:
            print(f'Failed to clean data. Try {i + 1}/{__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS}')
            if (i + 1) == __NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS:
                raise e
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)


def main():
    initial_date = datetime.now()
    args = __get_arguments()

    if args.routine_name == 'clear':

        __check_for_none_value('--s3_bucket_env_name', args.s3_bucket_env_name)
        __check_for_none_value('--s3_prefix_format', args.s3_prefix_format)

        clear_replace_date = __get_common_replace_date(args=args, initial_date=initial_date)
        clear_bucket = get_env_variable(args.s3_bucket_env_name)
        clear_prefix_format = args.s3_prefix_format
        
        clear_prefix = __prepare_prefix(prefix_format=clear_prefix_format, replace_date=clear_replace_date)
        logging.info(f'Clear path: s3://{clear_bucket}/{clear_prefix}')
        
        __clean_path(bucket=clear_bucket, prefix=clear_prefix)

    elif args.routine_name == 'copy':

        __check_for_none_value('--s3_bucket_env_name', args.s3_source_bucket_env_name)
        __check_for_none_value('--s3_source_prefix_format',args.s3_source_prefix_format)
        __check_for_none_value('--s3_target_bucket_env_name', args.s3_target_bucket_env_name)
        __check_for_none_value('--s3_target_prefix_format', args.s3_target_prefix_format)

        source_replace_date = __get_source_replace_date(args=args, initial_date=initial_date)
        target_replace_date = __get_target_replace_date(args=args, initial_date=initial_date)

        source_bucket = get_env_variable(args.s3_source_bucket_env_name)
        target_bucket = get_env_variable(args.s3_target_bucket_env_name)

        source_prefix_format = args.s3_source_prefix_format
        target_prefix_format = args.s3_target_prefix_format

        source_prefix = __prepare_prefix(prefix_format=source_prefix_format, replace_date=source_replace_date)
        logging.info(f'Source path: s3://{source_bucket}/{source_prefix}')
        target_prefix = __prepare_prefix(prefix_format=target_prefix_format, replace_date=target_replace_date)
        logging.info(f'Target path: s3://{target_bucket}/{target_prefix}')

        if args.clear_target_path:
            __clean_path(bucket=target_bucket, prefix=target_prefix)

        __execute_copy(
            source_bucket_name=source_bucket,
            source_prefix=source_prefix,
            target_bucket_name=target_bucket,
            target_prefix=target_prefix,
            kms_key_id=args.kms_key_id
        )

    else:
        logging.info(f'There is no implementation of {args.routine_name} routine')


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()

