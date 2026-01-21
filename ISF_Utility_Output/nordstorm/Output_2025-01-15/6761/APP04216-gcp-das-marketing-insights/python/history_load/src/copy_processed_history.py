import argparse
import json
import logging
from datetime import datetime, timedelta

import history_utils


def __get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_plan_bucket_env_name", required=True, type=str)
    parser.add_argument("--execution_plan_prefix", required=True, type=str)

    parser.add_argument("--s3_source_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_source_prefix", required=True, type=str)

    parser.add_argument("--s3_target_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_target_prefix", required=True, type=str)

    parser.add_argument("--clear_target_before_copy", action="store_true", default=False, required=False)

    return parser.parse_args()


def __get_current_load_part(execution_plan_bucket, execution_plan_prefix):
    return json.loads(history_utils.s3_load_object(
        bucket=execution_plan_bucket,
        prefix=execution_plan_prefix)
    )['current']


def __get_initial_date():
    return datetime(year=2000, month=1, day=1)


def __get_target_path_date(execution_plan_bucket, execution_plan_prefix):
    load_part = int(__get_current_load_part(
        execution_plan_bucket=execution_plan_bucket,
        execution_plan_prefix=execution_plan_prefix
    ).replace('part_', ''))

    return __get_initial_date() + timedelta(days=load_part)


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


def main():
    args = __get_arguments()

    execution_plan_bucket = history_utils.get_env_variable(args.execution_plan_bucket_env_name)
    source_bucket = history_utils.get_env_variable(args.s3_source_bucket_env_name)
    target_bucket = history_utils.get_env_variable(args.s3_target_bucket_env_name)

    target_date = __get_target_path_date(
        execution_plan_bucket=execution_plan_bucket,
        execution_plan_prefix=args.execution_plan_prefix
    )

    target_path = __prepare_prefix(prefix_format=args.s3_target_prefix, replace_date=target_date)

    if args.clear_target_before_copy:
        history_utils.clean_path(bucket=target_bucket, prefix=args.s3_target_prefix)

    history_utils.copy_objects(
        source_bucket=source_bucket,
        source_prefix=args.s3_source_prefix,
        target_bucket=target_bucket,
        target_prefix=target_path
    )


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    main()
