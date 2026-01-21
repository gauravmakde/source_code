import datetime
import logging
import argparse
import boto3
import os
from datetime import datetime


def __get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag_name", required=True, type=str)
    parser.add_argument("--prefix", required=False, type=str)
    parser.add_argument("--s3_bucket", required=True, type=str)
    parser.add_argument("--base_date_iso_env_name", required=False, type=str)
    return parser.parse_args()


def __get_initial_date(args):
    if args.base_date_iso_env_name:
        return datetime.fromisoformat(get_env_variable(env_key=args.base_date_iso_env_name))

    return datetime.now()


def __get_initial_prefix(args):
    if args.prefix:
        return args.prefix

    return 'execution_tracking/<DAG_NAME>/year=<year>/month=<month>/day=<day>/SUCCESS'


def get_env_variable(env_key):
    if os.environ.get(env_key) is None:
        raise ValueError(f"Env variable {env_key} is not set.")

    return os.environ.get(env_key)


def prepare_prefix(prefix_format, replace_date, dag_name):
    replace_dict = {
        'year': str(replace_date.strftime("%Y")),
        'month': str(replace_date.strftime("%m")),
        'day': str(replace_date.strftime("%d")),
        'hour': str(replace_date.strftime("%H")),
        'minute': str(replace_date.strftime("%M")),
        'second': str(replace_date.strftime("%S")),
        'DAG_NAME': dag_name,
    }

    result = prefix_format

    for key, value in replace_dict.items():
        result = result.replace(f'<{key}>', str(value))

    return result


def main():
    args = __get_arguments()
    initial_date = __get_initial_date(args=args)
    initial_prefix = __get_initial_prefix(args=args)
    status_bucket = get_env_variable(args.s3_bucket)
    prefix = prepare_prefix(initial_prefix, initial_date, args.dag_name)
    client = boto3.client('s3')
    client.put_object(Body='', Bucket=status_bucket, Key=prefix)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
