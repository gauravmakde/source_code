'''
Write a task that scans the S3 directory (use boto3) and looks at the metadata for the handshake file
- determines if it is current or not (sync on how they are currently sending the handshake)
- Pokes every "x" minutes for "y" hours - fails after an arbitary timeout we set
'''
import argparse
import boto3
import logging
import asyncio
from datetime import datetime, timezone, timedelta, time as tm
from zoneinfo import ZoneInfo
import time
import os
from botocore.exceptions import ClientError


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--function', help='S3 function to call for job. To check the most updated file', required=True, default="timestamp_check")
    arg_parser.add_argument('--bucket_nonprod', help='S3 NonProd bucket for source file - `mybucket` | skips env if bucket or file is empty | space-separate multiple buckets' \
                            , nargs='*', required=False)
    arg_parser.add_argument('--key_nonprod', help='S3 NonProd key for file - `myfile` | skips env if bucket or file is empty | space-separate multiple keys' \
                            , nargs='*', required=False)
    arg_parser.add_argument('--bucket_prod', help='S3 Prod bucket for source file - `mybucket` | skips env if bucket or file is empty | space-separate multiple buckets' \
                            , nargs='*', required=False)    
    arg_parser.add_argument('--key_prod', help='S3 Prod key for file - `myfile` | skips env if bucket or file is empty | space-separate multiple keys' \
                            , nargs='*', required=False)
    arg_parser.add_argument('--max_retry_minutes', help='Max retry minutes to check S3 file updated status', required=False, default=20)
    arg_parser.add_argument('--dir_check_date_diff', help='+/- # of days for dir_check on the date partition', required=False)
    return arg_parser.parse_args()

 
async def main():
    logging.basicConfig(level=logging.INFO)
    args = get_args()
    # This parses the k8s env from what is passed into the gitlab yml via .python-nonprod/prod-template.yml - either `development` or `production`
    # env = "development" # for local testing 
    env = os.environ.get('ENV')
    logging.info(f"Input args: {args}")
    logging.info(f"Environment: {env}")
    if args.function == "timestamp_check":
        await timestamp_check(args, env)
    if args.function == "dir_check":
        await dir_check(args, env)

 
async def timestamp_check(args, env):
    """
    Parses arguments to check a single file's updated status from the S3 bucket
    """
    if env == "development" and args.bucket_nonprod[0] and args.key_nonprod[0]:
        if len(args.key_nonprod[0].split(" ")) > 1 and len(args.bucket_nonprod[0].split(" ")) > 1:
            await asyncio.wait([
                asyncio.create_task(check_with_retries(bucket_to_check, key_to_check, args.max_retry_minutes))
                for key_to_check, bucket_to_check in zip(args.key_nonprod[0].split(" "), args.bucket_nonprod[0].split(" "))
                ])
        else:
            await check_with_retries(args.bucket_nonprod[0], args.key_nonprod[0], args.max_retry_minutes)

    if env == "production" and args.bucket_prod[0] and args.key_prod[0]:
        if len(args.key_prod[0].split(" ")) > 1 and len(args.bucket_prod[0].split(" ")) > 1:
            await asyncio.wait([
                asyncio.create_task(check_with_retries(bucket_to_check, key_to_check, args.max_retry_minutes))
                for key_to_check, bucket_to_check in zip(args.key_prod[0].split(" "), args.bucket_prod[0].split(" "))
                ])
        else:
            await check_with_retries(args.bucket_prod[0], args.key_prod[0], args.max_retry_minutes)

async def dir_check(args, env):
    """
    Parses arguments to check a single directory's updated status from the S3 bucket
    """
    if args.dir_check_date_diff:
        date_diff = int(args.dir_check_date_diff)
    else:
        date_diff = 0

    today = datetime.now().astimezone(ZoneInfo('US/Pacific')) + timedelta(days=date_diff) # in PST form
    today_string = today.strftime('%Y-%m-%d')

    if env == "development" and args.bucket_nonprod[0] and args.key_nonprod[0]:
        if len(args.key_nonprod[0].split(" ")) > 1 and len(args.bucket_nonprod[0].split(" ")) > 1:
            await asyncio.wait([
                asyncio.create_task(dir_check_with_retries(bucket_to_check, key_to_check, args.max_retry_minutes, today_string))
                for key_to_check, bucket_to_check in zip(args.key_nonprod[0].split(" "), args.bucket_nonprod[0].split(" "))
                ])
        else:
            await dir_check_with_retries(args.bucket_nonprod[0], args.key_nonprod[0], args.max_retry_minutes, today_string)

    if env == "production" and args.bucket_prod[0] and args.key_prod[0]:
        if len(args.key_prod[0].split(" ")) > 1 and len(args.bucket_prod[0].split(" ")) > 1:
            await asyncio.wait([
                asyncio.create_task(dir_check_with_retries(bucket_to_check, key_to_check, args.max_retry_minutes, today_string))
                for key_to_check, bucket_to_check in zip(args.key_prod[0].split(" "), args.bucket_prod[0].split(" "))
                ])
        else:
            await dir_check_with_retries(args.bucket_prod[0], args.key_prod[0], args.max_retry_minutes, today_string)

async def dir_check_with_retries(bucket, key, max_retry_minutes, today_string):
    """
    Poke S3 folder every 5 mins to see if it actually exists and has contents in it
    """
    start_time = datetime.utcnow()
    max_retry_time = start_time + timedelta(minutes=int(max_retry_minutes))
    logging.info(f"S3 File folder and file prefix: {bucket}, {key}")
    logging.info(f"Check start time: {start_time}")
    logging.info(f"Check max retry time: {max_retry_time}")

    result = False
    while (start_time <= max_retry_time):
        result = await dir_check_wrapper(bucket, key, today_string)
        next_retry = datetime.utcnow() + timedelta(minutes = 5)
        if (result or next_retry > max_retry_time):
            break             
        logging.info(f"No contents in dir, or does not exist. Retrying at {next_retry}")
        retry_interval_seconds = 5 * 60
        await asyncio.sleep(retry_interval_seconds)
    if (result):
        logging.info(f"Job succeed")
    else:
        raise Exception("Dir check exceeded max retry time, Job failed")

async def dir_check_wrapper(bucket, key, today_string):
    """
    for local testing in order to fix the boto3 403 client error
    session = boto3.Session(profile_name="nordstrom-federated") # for local testing
    s3 = session.client('s3', "us-west-2") # for local testing

    bucket: test-my-bucket
    key: some_key/activity_date_partition=
    """
    s3 = boto3.client('s3', "us-west-2")
    file_path = f"{key}{today_string}"
    result = s3.list_objects(Bucket=bucket, Prefix=file_path)
    if 'Contents' in result:
        logging.info(f"Found contents for {file_path}")
        return True
    else:
        logging.info(f"No contents or directory does not exist for {file_path} - sleeping and trying again")
        return False

async def check_with_retries(bucket, key, max_retry_minutes):
    """
    Poke S3 folder every 5 mins until reach the max_retry_minutes if return False (file hasn't been updated)
    """
    start_time = datetime.utcnow()
    max_retry_time = start_time + timedelta(minutes=int(max_retry_minutes))
    logging.info(f"S3 File folder and name: {bucket}, {key}")
    logging.info(f"Check start time: {start_time}")
    logging.info(f"Check max retry time: {max_retry_time}")


    result = False
    while (start_time <= max_retry_time):
        result = await check_wrapper(bucket, key)
        next_retry = datetime.utcnow() + timedelta(minutes = 5)
        if (result or next_retry > max_retry_time):
            break             
        logging.info(f"File hasn't been updated. Retrying at {next_retry}")
        retry_interval_seconds = 5 * 60
        await asyncio.sleep(retry_interval_seconds)
    if (result):
        logging.info(f"Job succeed")
    else:
        raise Exception("File check exceeded max retry time, Job failed")
    

async def check_wrapper(bucket, key):
    """
    for local testing in order to fix the boto3 403 client error
    session = boto3.Session(profile_name="nordstrom-federated") # for local testing
    s3 = session.client('s3', "us-west-2") # for local testing
    """
    try:    
        s3 = boto3.client('s3', "us-west-2")
        object = s3.get_object(Bucket=bucket, Key=key)
        logging.info(f"Found key {key} in bucket {bucket}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey': 
            logging.info(f"Key {key} not found in bucket {bucket} - sleeping and trying again")
            return False  
        else:
            raise e
    else: 
        today = datetime.combine(datetime.now(timezone.utc), tm.min, timezone.utc)
        if object["LastModified"] >= today:
            last_modified = object["LastModified"].replace(tzinfo=None)
            logging.info(f"File has been updated at {last_modified} UTC")
            return True
        else:
            return False
 

if __name__ == "__main__":
    asyncio.run(main())

