from datetime import datetime, timezone, timedelta
from airflow.hooks.S3_hook import S3Hook
from nordstrom.utils.aws_creds import aws_creds
UTC_PREV_DATE = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("year=%Y/month=%m/day=%d")

def check_directory_in_s3(prefix):
    # need to use separated nauth connection nauth_default
    with aws_creds(nauth_conn_id="napstore_nauth_jrni", aws_conn_id="napstore_hr_worker_prod_conn") as aws_connection:
        s3 = S3Hook(aws_conn_id=aws_connection)
        bucket_name = 'hr-worker-data-prod'
        full_prefix = prefix + UTC_PREV_DATE
        keys = s3.list_keys(bucket_name=bucket_name,
                            prefix=full_prefix,
                            max_items=1)
        if keys is None:
            return False
        return len(keys) > 0

def callable_func_name(*arg, **kwargs):
    if check_directory_in_s3('historic-events-v2/WorkerHired/'):
        return [{task6_hired_details}, {task6_skip}]
    else:
        return [{task6_skip}]

