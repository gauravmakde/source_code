import argparse
import json
import logging
import re

import history_utils


def __get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_plan_bucket_env_name", required=True, type=str)
    parser.add_argument("--execution_plan_prefix", required=True, type=str)

    return parser.parse_args()


def main():
    args = __get_arguments()

    bucket = history_utils.get_env_variable(args.execution_plan_bucket_env_name)

    execution_plan = json.loads(history_utils.s3_load_object(bucket=bucket, prefix=args.execution_plan_prefix))

    current_part = execution_plan['current']
    match_result = re.match("^(\\D+)(\\d+)$", current_part)
    prefix = match_result.group(1)
    current_part_num = int(match_result.group(2))
    execution_plan['current'] = prefix + str(current_part_num + 1)

    history_utils.s3_put_object(
        bucket=bucket,
        key=args.execution_plan_prefix,
        body=json.dumps(execution_plan)
    )


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    main()
