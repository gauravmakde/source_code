from airflow.hooks.S3_hook import S3Hook
import boto3
import logging

class AwsHelper:
    """
    A helper for working with aws resources.
    """

    def __init__(self, nauth_conn_id, aws_conn_id, gitlab_conn_id):
        """
        Creates a helper for working with aws resources.
        :param config: dictionary with fields:
            nauth_conn_id: napstore_nauth_eta_markets
            gitlab_conn_id: napstore_gitlab
            aws_conn_id: napstore_aws_eta_markets
        """
        self.nauth_conn_id = nauth_conn_id
        self.aws_conn_id = aws_conn_id
        self.gitlab_conn_id = gitlab_conn_id
        logging.info(f"using nauth_conn_id: {self.nauth_conn_id}")
        logging.info(f"using aws_conn_id: {self.aws_conn_id}")
        logging.info(f"using gitlab_conn_id: {self.gitlab_conn_id}")

    def s3_file_check(self, bucket_name, file):
        try:
            self.download_from_s3(bucket_name, file).load()
            return True
        except Exception as error:
            logging.error('Error while S3 download: ' + repr(error))
            return False

    def s3_list_bucket(self, bucket_name):
        with aws_creds(nauth_conn_id=self.nauth_conn_id, aws_conn_id=self.aws_conn_id,
                       gitlab_conn_id=self.gitlab_conn_id):
            logging.info(f"listing s3://{bucket_name}")
            return S3Hook(self.aws_conn_id).list_keys(bucket_name)

    def s3_delete_file(self, bucket_name, file):
        with aws_creds(nauth_conn_id=self.nauth_conn_id, aws_conn_id=self.aws_conn_id,
                       gitlab_conn_id=self.gitlab_conn_id):
            logging.info(f"deleting s3://{bucket_name}/{file}")
            return S3Hook(self.aws_conn_id).delete_objects(bucket_name, file)

    def s3_check_for_key_in_path(self, s3_full_path):
        with aws_creds(nauth_conn_id=self.nauth_conn_id, aws_conn_id=self.aws_conn_id,
                       gitlab_conn_id=self.gitlab_conn_id):
            logging.info(f"Searching for: {s3_full_path}")
            return S3Hook(self.aws_conn_id).check_for_key(s3_full_path)

    def download_from_s3(self, s3_bucket, key):
        with aws_creds(nauth_conn_id=self.nauth_conn_id, aws_conn_id=self.aws_conn_id,
                       gitlab_conn_id=self.gitlab_conn_id):
            return S3Hook(self.aws_conn_id).get_key(key, bucket_name=s3_bucket)

    def load_bucket(self, s3_bucket):
        with aws_creds(nauth_conn_id=self.nauth_conn_id, aws_conn_id=self.aws_conn_id,
                       gitlab_conn_id=self.gitlab_conn_id):
            return S3Hook(self.aws_conn_id).get_bucket(s3_bucket)

    def load_string(self, triggerString, s3_full_path):
        with aws_creds(nauth_conn_id=self.nauth_conn_id, aws_conn_id=self.aws_conn_id,
                       gitlab_conn_id=self.gitlab_conn_id):
            return S3Hook(self.aws_conn_id).load_string(triggerString, s3_full_path)