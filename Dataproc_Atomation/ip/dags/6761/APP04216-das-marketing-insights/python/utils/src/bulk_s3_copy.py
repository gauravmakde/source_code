import argparse
import json
import logging
import os
from datetime import datetime

from s3_copy import clean_path, execute_copy, prepare_prefix, get_source_replace_date, get_target_replace_date


def __get_arguments():
    parser = argparse.ArgumentParser()

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

    parser.add_argument("--s3_source_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_target_bucket_env_name", required=True, type=str)

    parser.add_argument("--source_to_target_path_pair", required=True, action='append', type=to_source_target_pair)

    parser.add_argument("--clear_target_path", action="store_true", default=False, required=False)

    parser.add_argument("--base_date_iso_env_name", required=False, type=str)
    return parser.parse_args()


def __get_initial_date(args):
    if args.base_date_iso_env_name:
        return datetime.fromisoformat(get_env_variable(env_key=args.base_date_iso_env_name))

    return datetime.now()


def to_source_target_pair(arg):
    return json.loads(arg.replace("\'", "\""))


def get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)


def verify_arguments(args):
    for pair in args.source_to_target_path_pair:
        if 'source' not in pair or 'target' not in pair:
            raise ValueError(f"Both the 'source' and the 'target' must be present in source-to-target-path pair. "
                             f"Pair: {pair}")


def main():
    args = __get_arguments()
    initial_date = __get_initial_date(args=args)
    verify_arguments(args)

    for pair in args.source_to_target_path_pair:
        source_replace_date = get_source_replace_date(args=args, initial_date=initial_date)
        target_replace_date = get_target_replace_date(args=args, initial_date=initial_date)

        source_bucket = get_env_variable(args.s3_source_bucket_env_name)
        target_bucket = get_env_variable(args.s3_target_bucket_env_name)

        source_prefix_format = pair['source']
        target_prefix_format = pair['target']

        source_prefix = prepare_prefix(prefix_format=source_prefix_format, replace_date=source_replace_date)
        logging.info(f'Source path: 3://{source_bucket}/{source_prefix}')
        target_prefix = prepare_prefix(prefix_format=target_prefix_format, replace_date=target_replace_date)
        logging.info(f'Target path: s3://{target_bucket}/{target_prefix}')

        if args.clear_target_path:
            clean_path(bucket=target_bucket, prefix=target_prefix)

        execute_copy(
            source_bucket_name=source_bucket,
            source_prefix=source_prefix,
            target_bucket_name=target_bucket,
            target_prefix=target_prefix
        )


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
