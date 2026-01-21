import argparse
import json
import logging

import history_utils


def __get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_plan_bucket_env_name", required=True, type=str)
    parser.add_argument("--execution_plan_prefix", required=True, type=str)
    parser.add_argument("--s3_target_bucket_env_name", required=True, type=str)
    parser.add_argument("--s3_target_prefix", required=True, type=str)
    parser.add_argument("--clear_target_before_copy", action="store_true", default=False, required=False)

    return parser.parse_args()


def __get_current_manifest(execution_plan_bucket, execution_plan_prefix):
    execution_plan = json.loads(
        history_utils.s3_load_object(bucket=execution_plan_bucket, prefix=execution_plan_prefix))
    manifest_details = execution_plan[execution_plan['current']]

    return json.loads(
        history_utils.s3_load_object(
            bucket=manifest_details['source_bucket'],
            prefix=manifest_details['key']
        )
    )


def __transform_key(path):
    replace = path.replace('s3://', '', 1)
    bucket = replace.split('/')[0]
    return {
        'Bucket': bucket,
        'Key': replace.replace(f'{bucket}/', '', 1)
    }


def __copy_batch_data(manifest, target_bucket, target_prefix):
    for source_path in manifest['entries']:
        source = __transform_key(source_path['url'])
        history_utils.copy_objects(
            source_bucket=source['Bucket'],
            source_prefix=source['Key'],
            target_bucket=target_bucket,
            target_prefix=target_prefix
        )


def main():
    args = __get_arguments()
    execution_plan_bucket = history_utils.get_env_variable(args.execution_plan_bucket_env_name)
    target_bucket = history_utils.get_env_variable(args.s3_target_bucket_env_name)

    current_manifest = __get_current_manifest(
        execution_plan_bucket=execution_plan_bucket,
        execution_plan_prefix=args.execution_plan_prefix
    )

    if args.clear_target_before_copy:
        history_utils.clean_path(bucket=target_bucket, prefix=args.s3_target_prefix)

    __copy_batch_data(manifest=current_manifest, target_bucket=target_bucket, target_prefix=args.s3_target_prefix)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    main()
