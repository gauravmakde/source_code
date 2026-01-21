import os
from pypeline_modules.eta.src.aws_helper import AwsHelper


def is_new_data_available():
    s3_full_path = f"s3://{os.environ.get('FILE_CHECK_BUCKET')}/{os.environ.get('FILE_CHECK_BUCKET')}"
    is_found = AwsHelper(os.environ.get('NAUTH_CONN_ID'),
                         os.environ.get('GITLAB_CONN_ID'),
                         os.environ.get('AWS_CONN_ID')).s3_check_for_key_in_path(s3_full_path)
    if is_found:
        return {data_available}
    else:
        return {no_data_available}


def delete_data_available_file():
    aws = AwsHelper(os.environ.get('NAUTH_CONN_ID'),
                    os.environ.get('GITLAB_CONN_ID'),
                    os.environ.get('AWS_CONN_ID'))
    bucket = os.environ.get('FILE_CHECK_BUCKET')
    file = os.environ.get('FILE_CHECK_BUCKET')
    aws.s3_delete_file(bucket, file)


if __name__ == '__main__':
    delete_data_available_file()

