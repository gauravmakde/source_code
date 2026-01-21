import logging
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3:
    def __init__(self, nauth_conn_id, aws_conn_id):
        self.__nauth_conn_id = nauth_conn_id
        self.__aws_conn_id = aws_conn_id

    def __get_s3_hook(self):
        def set_up_credential_for_s3_hook():
            return S3Hook(self.__aws_conn_id)

        return set_up_credential_for_s3_hook()

    def check_for_key(self, key: str, expected_result: bool):
        s3_hook = self.__get_s3_hook()

        logging.info("Checking the S3 key: " + key)
        result = s3_hook.check_for_key(key)

        if result != expected_result:
            raise ValueError(
                "Expected check result: "
                + str(expected_result)
                + ". Actual: "
                + str(result)
            )

    def check_for_file_recursively(self, bucket_name: str, prefix: str, filename: str):
        s3_hook = self.__get_s3_hook()
        logging.info(f"Listing keys for path: s3://{bucket_name}/{prefix}")
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if prefix in keys:
            keys.remove(prefix)
        logging.info(f"Looking for {filename} file in keys: {keys}")
        file_is_found = False
        for key in keys:
            if key.endswith(filename):
                file_is_found = True
                break

        if not file_is_found:
            raise Exception(f"{filename} is not found in the specified location")

    def copy_recursively(
        self,
        source_bucket_name: str,
        source_prefix: str,
        dest_bucket_name: str,
        dest_prefix: str,
    ):
        s3_hook = self.__get_s3_hook()
        logging.info(
            f"Listing keys for path: s3://{source_bucket_name}/{source_prefix}"
        )
        source_keys = s3_hook.list_keys(
            bucket_name=source_bucket_name, prefix=source_prefix
        )
        if source_prefix in source_keys:
            source_keys.remove(source_prefix)
        logging.info(f"S3 keys to be copied: {source_keys}")
        for source_key in source_keys:
            basename = os.path.basename(source_key)
            dest_key = f"{dest_prefix}{basename}"
            logging.info(
                f"Copying from [{source_bucket_name}/{source_key}] to [{dest_bucket_name}/{dest_key}]"
            )
            s3_hook.copy_object(
                source_bucket_name=source_bucket_name,
                source_bucket_key=source_key,
                dest_bucket_name=dest_bucket_name,
                dest_bucket_key=dest_key,
            )

    def copy_object(
        self,
        source_bucket_name: str,
        source_key: str,
        dest_bucket_name: str,
        dest_key: str = "",
        dest_prefix: str = "",
        prefix_flag=False,
    ):
        if not dest_key and not dest_prefix:
            raise Exception("Either destination key or prefix should be provided")

        s3_hook = self.__get_s3_hook()
        if prefix_flag:
            source_basename = os.path.basename(source_key)
            dest_key = f'{dest_prefix.rstrip("/")}/{source_basename}'
        logging.info(
            f"Copying from s3://{source_bucket_name}/{source_key} to "
            f"s3://{dest_bucket_name}/{dest_key}"
        )
        s3_hook.copy_object(
            source_bucket_name=source_bucket_name,
            source_bucket_key=source_key,
            dest_bucket_name=dest_bucket_name,
            dest_bucket_key=dest_key,
        )

    def get_object(self, bucket_name: str, key: str):
        s3_hook = self.__get_s3_hook()
        return s3_hook.get_key(bucket_name=bucket_name, key=key)

    def list_keys(self, bucket_name: str, prefix: str):
        s3_hook = self.__get_s3_hook()
        logging.info(f"Listing keys for path: {bucket_name}/{prefix}")
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if prefix in keys:
            keys.remove(prefix)
        logging.info(f"Keys found: {keys}")
        return keys

    def upload_string_object(self, bucket_name, key, string="", replace=True):
        s3_hook = self.__get_s3_hook()
        logging.info(
            f'Uploading string object to {bucket_name}/{key} with payload: "{string}"'
        )
        s3_hook.load_string(
            bucket_name=bucket_name, key=key, string_data=string, replace=replace
        )
