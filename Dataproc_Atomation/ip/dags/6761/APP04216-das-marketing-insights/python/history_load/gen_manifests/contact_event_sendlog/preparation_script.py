import datetime
import json
import threading

import boto3
from botocore.exceptions import ClientError
from dateutil.tz import tzutc

s3_client = boto3.client('s3')
token_error_message = "Provided token invalid or expired. Update token and press enter in console."
manifest_s3_prefix_format = 'manifest/contact_event_sendlog/send_log_history_manifest_folder_part_{}.manifest'

batch_size = 50  # batch size in GB

initial_date = datetime.datetime(1999, 8, 27, 23, tzinfo=tzutc())
end_date = datetime.datetime(2024, 8, 31, 0, tzinfo=tzutc())
thread_num = 2

append_execution_plan = False


def update_s3_client():
    global s3_client
    boto3.setup_default_session()
    s3_client = boto3.client('s3')


def handle_token_error():
    input(token_error_message)
    update_s3_client()


def s3_list_objects(kwargs):
    while True:
        try:
            return s3_client.list_objects_v2(**kwargs)
        except ClientError:
            handle_token_error()


def s3_put_object(target_aws_role, body, bucket, key):
    while True:
        try:
            return boto3.Session(profile_name=target_aws_role).client('s3').put_object(
                Body=body,
                Bucket=bucket,
                Key=key
            )
        except ClientError:
            handle_token_error()


def s3_get_object(target_aws_role, bucket, key):
    while True:
        try:
            return boto3.Session(profile_name=target_aws_role).client('s3').get_object(
                Bucket=bucket,
                Key=key
            )
        except ClientError:
            handle_token_error()


def fetch_common_prefixes_from_s3(bucket, prefix):
    kwargs = {'Bucket': bucket, 'Prefix': prefix, 'Delimiter': '/'}
    return s3_list_objects(kwargs)['CommonPrefixes']


def collect_common_prefixes(bucket, initial_prefix):
    result = []
    for val in fetch_common_prefixes_from_s3(bucket, initial_prefix):
        prefix = val['Prefix']
        if 'hour' not in prefix:
            result.extend(collect_common_prefixes(bucket, prefix))
        else:
            result.append(prefix)
    return result


def get_folder_size(bucket, prefix):
    size = 0.0

    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3_list_objects(kwargs)
        for obj in resp['Contents']:
            size += (obj['Size'] / 1_000_000_000)  # Converts to GB
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return size


def prepare_manifests(partitioned_prefixes, source_bucket):
    manifests = []
    for manifest_data in partitioned_prefixes:
        manifest_entries = []
        for prefix in manifest_data:
            manifest_entries.append(
                {
                    'url': 's3://{}/{}'.format(source_bucket, prefix),
                    'mandatory': True
                }
            )
        manifests.append({
            'entries': manifest_entries
        })
    return manifests


class PreparePrefixesThread(threading.Thread):
    def __init__(self, thread_id: int, step, list, result_list, source_bucket):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.list = list
        self.step = step
        self.result_list = result_list
        self.source_bucket = source_bucket

    def run(self):
        process_idx = self.thread_id
        while process_idx + 1 <= len(self.list):
            self.process_item(self.list[process_idx])
            process_idx += self.step

    def process_item(self, prefix):
        kwargs = {'Bucket': self.source_bucket, 'Prefix': prefix, 'MaxKeys': 1}
        last_modified = s3_list_objects(kwargs)['Contents'][0]['LastModified']
        if initial_date < last_modified < end_date:
            size = get_folder_size(self.source_bucket, prefix)
            self.result_list.append({'Prefix': prefix, 'Size': size, 'LastModified': last_modified})
            print(f"Thread-{self.thread_id}: {prefix} processed")
        else:
            print(f"Thread-{self.thread_id}: {prefix} skipped. [Out of chosen period]")


def prepare_common_prefixes(source_bucket, source_bucket_prefix):
    threads = []
    result = []
    prefixes = collect_common_prefixes(source_bucket, source_bucket_prefix)

    for t_num in range(thread_num):
        thread = PreparePrefixesThread(thread_id=t_num, step=thread_num, list=prefixes, result_list=result,
                                       source_bucket=source_bucket)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    sorted_result = sorted(result, key=lambda k: k['LastModified'])
    return sorted_result


def upload_data_to_s3(execution_plan, manifests, target_aws_role, target_bucket, initial_part):
    for i in range(len(manifests)):
        s3_prefix_to_part = manifest_s3_prefix_format.format(i + initial_part)
        s3_put_object(target_aws_role, json.dumps(manifests[i]), target_bucket, s3_prefix_to_part)

    execution_plan_td = execution_plan

    if append_execution_plan:
        current_execution_plan = load_exectuion_plan(target_aws_role, target_bucket,
                                                     'manifest/contact_event_sendlog/execution_plan.json')
        current_execution_plan.update(execution_plan)
        execution_plan = current_execution_plan

        current_execution_plan_td = load_exectuion_plan(target_aws_role, target_bucket,
                                                        'manifest/contact_event_sendlog/execution_plan_td.json')
        current_execution_plan_td.update(execution_plan)
        execution_plan_td = current_execution_plan_td

    s3_put_object(target_aws_role, json.dumps(execution_plan), target_bucket,
                  'manifest/contact_event_sendlog/execution_plan.json')
    s3_put_object(target_aws_role, json.dumps(execution_plan_td), target_bucket,
                  'manifest/contact_event_sendlog/execution_plan_td.json')


def load_exectuion_plan(target_aws_role, target_bucket, path):
    execution_plan_json = s3_get_object(target_aws_role, target_bucket, path)

    execution_plan = json.loads(execution_plan_json['Body'].read())
    return execution_plan


def prepare_execution_plan(partitioned_manifests, target_bucket, initial_part):
    execution_plan = {}
    for manifest in partitioned_manifests:
        part = partitioned_manifests.index(manifest) + initial_part
        s3_prefix_to_part = manifest_s3_prefix_format.format(part)

        execution_plan['part_{}'.format(part)] = {
            'source_bucket': target_bucket,
            'key': s3_prefix_to_part
        }
    execution_plan['current'] = 'part_' + str(initial_part)
    return execution_plan


def prepare_partitioned_prefixes(content):
    partitioned_prefixes = []
    sub_entries = []
    sub_entries_size = 0
    while len(content) > 0:
        item = content.pop(0)
        sub_entries.append(item['Prefix'])
        sub_entries_size += item['Size']
        if sub_entries_size >= batch_size:
            partitioned_prefixes.append(sub_entries[:])
            sub_entries.clear()
            sub_entries_size = 0
    if len(sub_entries) > 0:
        partitioned_prefixes.append(sub_entries[:])
    return partitioned_prefixes


class ENV_NAMES:
    LOCAL = 'local'
    DEV = 'dev'
    PREPROD = 'preprod'
    PROD = 'prod'


target_bucket_mapping = {
    ENV_NAMES.LOCAL: 'tf-das-marketing-nonprod-teradata-stage',
    ENV_NAMES.DEV: 'tf-das-marketing-nonprod-teradata-stage',
    ENV_NAMES.PREPROD: 'tf-das-marketing-pre-prod-teradata-stage',
    ENV_NAMES.PROD: 'tf-das-marketing-prod-teradata-stage'
}

source_bucket_mapping = {
    ENV_NAMES.LOCAL: 'tf-nap-nonprod-mkt-objectmodel-landing',
    ENV_NAMES.DEV: 'tf-nap-nonprod-mkt-objectmodel-landing',
    ENV_NAMES.PREPROD: 'tf-nap-prod-mkt-objectmodel-landing',
    ENV_NAMES.PROD: 'tf-nap-prod-mkt-objectmodel-landing'
}

def exec():
    # TODO: Set environment
    env = ENV_NAMES.DEV

    target_bucket = target_bucket_mapping[env]
    source_bucket = source_bucket_mapping[env]

    target_aws_role = 'redshift'

    source_bucket_prefix_list = ['event_consumer/customer-contact-event-sendlog-analytical-avro/']
    initial_part = 0

    # TODO: Check append mode
    if append_execution_plan:
        current_execution_plan = load_exectuion_plan(target_aws_role, target_bucket,
                                                     'manifest/contact_event_sendlog/execution_plan.json')
        initial_part = int(current_execution_plan['current'].replace('part_', '')) + 1
        print('Append mode. Continue from part_{}.'.format(initial_part))

    manifests = []
    for source_bucket_prefix in source_bucket_prefix_list:
        prefixes = prepare_common_prefixes(source_bucket, source_bucket_prefix)

        partitioned_prefixes = prepare_partitioned_prefixes(prefixes)
        manifests.extend(prepare_manifests(partitioned_prefixes, source_bucket))

    execution_plan = prepare_execution_plan(manifests, target_bucket, initial_part)

    upload_data_to_s3(execution_plan, manifests, target_aws_role, target_bucket, initial_part)


if __name__ == '__main__':
    exec()
