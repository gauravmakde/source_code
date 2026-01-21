import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

import boto3

__FILE_MANIFEST_TYPE = 'file'
__DIR_MANIFEST_TYPE = 'dir'
__BOTH_MANIFEST_TYPE = 'both'
__CSV_DATA_FILE_REGEXP = 'csv(.gz)?$'
__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS = 5
__SLEEP_TIME_SECONDS_ON_ERROR = 30
__FILE_MANIFEST_NAME = ''


def __get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--data_path_weeks_delta", required=False, default=0, type=int)
    parser.add_argument("--data_path_days_delta", required=False, default=0, type=int)
    parser.add_argument("--data_path_hours_delta", required=False, default=0, type=int)
    parser.add_argument("--data_path_minutes_delta", required=False, default=0, type=int)
    parser.add_argument("--data_path_seconds_delta", required=False, default=0, type=int)
    parser.add_argument("--data_path_milliseconds_delta", required=False, default=0, type=int)
    parser.add_argument("--data_path_microseconds_delta", required=False, default=0, type=int)

    parser.add_argument("--manifest_path_weeks_delta", required=False, default=0, type=int)
    parser.add_argument("--manifest_path_days_delta", required=False, default=0, type=int)
    parser.add_argument("--manifest_path_hours_delta", required=False, default=0, type=int)
    parser.add_argument("--manifest_path_minutes_delta", required=False, default=0, type=int)
    parser.add_argument("--manifest_path_seconds_delta", required=False, default=0, type=int)
    parser.add_argument("--manifest_path_milliseconds_delta", required=False, default=0, type=int)
    parser.add_argument("--manifest_path_microseconds_delta", required=False, default=0, type=int)

    parser.add_argument("--s3_data_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_manifest_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_data_prefix_format", required=True, type=str)
    parser.add_argument("--s3_manifest_prefix_format_list", required=True, type=str)

    parser.add_argument("--manifest_type", required=False, default=__BOTH_MANIFEST_TYPE)
    parser.add_argument("--data_file_regexp", required=False, default=__CSV_DATA_FILE_REGEXP)

    return parser.parse_args()


def __get_replace_date(initial_date: datetime, weeks_delta: int, days_delta: int, hours_delta: int, minutes_delta: int,
                       seconds_delta: int, milliseconds_delta: int, microseconds_delta: int) -> datetime:
    return initial_date + timedelta(weeks=weeks_delta, days=days_delta, hours=hours_delta,
                                    minutes=minutes_delta, seconds=seconds_delta,
                                    milliseconds=milliseconds_delta, microseconds=microseconds_delta)


def __get_source_replace_date(args, initial_date) -> datetime:
    result = __get_replace_date(
        initial_date=initial_date,
        weeks_delta=args.data_path_weeks_delta,
        days_delta=args.data_path_days_delta,
        hours_delta=args.data_path_hours_delta,
        minutes_delta=args.data_path_minutes_delta,
        seconds_delta=args.data_path_seconds_delta,
        milliseconds_delta=args.data_path_milliseconds_delta,
        microseconds_delta=args.data_path_microseconds_delta
    )

    logging.info(f'Replace date for source path: {str(result)}')

    return result


def __get_target_replace_date(args, initial_date) -> datetime:
    result = __get_replace_date(
        initial_date=initial_date,
        weeks_delta=args.manifest_path_weeks_delta,
        days_delta=args.manifest_path_days_delta,
        hours_delta=args.manifest_path_hours_delta,
        minutes_delta=args.manifest_path_minutes_delta,
        seconds_delta=args.manifest_path_seconds_delta,
        milliseconds_delta=args.manifest_path_milliseconds_delta,
        microseconds_delta=args.manifest_path_microseconds_delta
    )

    logging.info(f'Replace date for target path: {str(result)}')

    return result


def get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)


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


def __prepare_manifest_prefixes(prefixes_format, replace_date):
    return [__prepare_prefix(prefix_format=prefix, replace_date=replace_date) for prefix in prefixes_format]


def __collect_prefixes(bucket, prefix, file_regexp):
    result = []
    regexc = re.compile(file_regexp)

    for i in range(__NUMBER_OF_TRIES_TO_GET_LIST_OBJECTS):
        try:
            s3 = boto3.resource('s3')
            source_bucket = s3.Bucket(bucket)

            objects = source_bucket.objects.filter(Prefix=prefix)
            for obj in objects:
                key = obj.key
                match = regexc.search(key)
                if match:
                    result.append(key)
                    logging.info(f'{key} - added to manifest')
            break
        except Exception as e:
            logging.error(e, exc_info=True)
            time.sleep(__SLEEP_TIME_SECONDS_ON_ERROR)

    return result


def __prepare_file_manifests(prefixes, source_bucket, target_bucket, manifest_target_path_list):
    manifests = []
    for manifest_target_path in manifest_target_path_list:
        entries = []
        manifest_data = {
            'bucket': target_bucket,
            'key': f'{manifest_target_path}/file.manifest',
            'content': {
                'entries': entries
            }
        }

        for prefix in prefixes:
            entries.append({
                'url': f's3://{source_bucket}/{prefix}',
                'mandatory': True
            })

        manifests.append(manifest_data)

    return manifests


def __prepare_dir_manifests(source_bucket, prefix, target_bucket, manifest_target_path_list):
    manifests = []
    for manifest_target_path in manifest_target_path_list:
        manifest_data = {
            'bucket': target_bucket,
            'key': f'{manifest_target_path}/dir.manifest',
            'content': {
                'entries': [
                    {
                        'url': f's3://{source_bucket}/{prefix}/',
                        'mandatory': True
                    }
                ]
            }
        }
        manifests.append(manifest_data)

    return manifests


def __upload_manifest(manifests):
    s3_client = boto3.client('s3')
    for manifest in manifests:
        bucket = manifest['bucket']
        key = manifest['key']

        s3_client.put_object(
            Body=json.dumps(manifest['content']),
            Bucket=bucket,
            Key=key
        )

        logging.info(f's3://{bucket}/{key} ----> SUCCESS')


def main():
    initial_date = datetime.now()
    args = __get_arguments()

    source_replace_date = __get_source_replace_date(args=args, initial_date=initial_date)
    target_replace_date = __get_target_replace_date(args=args, initial_date=initial_date)

    source_bucket = get_env_variable(args.s3_data_bucket_env_name)
    target_bucket = get_env_variable(args.s3_manifest_bucket_env_name)

    data_prefix_format = args.s3_data_prefix_format[0:-1:] if args.s3_data_prefix_format.endswith(
        '/') else args.s3_data_prefix_format
    manifest_prefixes_arg = args.s3_manifest_prefix_format_list.split(',')
    manifest_prefixes_format = []
    for manifest_arg_itm in manifest_prefixes_arg:
        manifest_prefixes_format.append(manifest_arg_itm[0:-1:] if manifest_arg_itm.endswith('/') else manifest_arg_itm)

    source_prefix = __prepare_prefix(prefix_format=data_prefix_format, replace_date=source_replace_date)
    logging.info(f'Source path: 3://{source_bucket}/{source_prefix}')
    target_prefixes = __prepare_manifest_prefixes(prefixes_format=manifest_prefixes_format,
                                                  replace_date=target_replace_date)

    for target_prefix in target_prefixes:
        logging.info(f'Target path: s3://{target_bucket}/{target_prefix}')

    manifest_type = args.manifest_type.lower()

    if manifest_type not in [__FILE_MANIFEST_TYPE, __DIR_MANIFEST_TYPE, __BOTH_MANIFEST_TYPE]:
        raise ValueError(
            f'Illegal value of --manifest_type. Expected: {__FILE_MANIFEST_TYPE},{__DIR_MANIFEST_TYPE}, {__BOTH_MANIFEST_TYPE}. Actual: {manifest_type}')

    manifests = []

    if args.manifest_type in [__FILE_MANIFEST_TYPE, __BOTH_MANIFEST_TYPE]:
        data_file_regexp = args.data_file_regexp
        prefixes = __collect_prefixes(bucket=source_bucket, prefix=source_prefix, file_regexp=data_file_regexp)
        manifests.extend(
            __prepare_file_manifests(prefixes=prefixes, source_bucket=source_bucket, target_bucket=target_bucket,
                                     manifest_target_path_list=target_prefixes)
        )

    if args.manifest_type in [__DIR_MANIFEST_TYPE, __BOTH_MANIFEST_TYPE]:
        manifests.extend(
            __prepare_dir_manifests(source_bucket=source_bucket, prefix=source_prefix, target_bucket=target_bucket,
                                    manifest_target_path_list=target_prefixes)
        )

    __upload_manifest(manifests=manifests)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
