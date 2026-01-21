from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from datetime import datetime
from nordstrom.utils.cloud_creds import cloud_creds
import logging
import os,sys
import pandas.io.sql as psql


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id='nauth-connection-nonprod',
        cloud_conn_id='gcp-onix-connection-nonprod',
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def setup_creds_aws(aws_iam_role_arn: str):
    @cloud_creds(
        nauth_conn_id="nauth-connection-nonprod",
        cloud_conn_id="aws_default",
        aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer",
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()

default_args = {
    'start_date': datetime(2024, 1, 1),
}
with DAG(
    dag_id=f"s3togcscopycheck",
    default_args=default_args,
    start_date= datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=['airflow-gcp-onix-testing@jwn-nap-user1-nonprod-zmnb.iam.gserviceaccount.com'],
    )

    creds_setup_aws = PythonOperator(
        task_id="setup_creds_aws",
        python_callable=setup_creds_aws,
        op_args=["arn:aws:iam::290445963451:role/gcp_data_transfer"],
    )



    s3_to_gcs = S3ToGCSOperator(
        task_id="s3_to_gcs",
        aws_conn_id='aws_default',
        bucket='initialtd-s3-exports',
        prefix = 'file_sync',
        gcp_conn_id='gcp-onix-connection-nonprod',
        dest_gcs='gs://test-data-datametica/s3togcsSync/',
        replace=False,
        gzip=True,
        dag=dag,
    )
    
creds_setup >> creds_setup_aws >> s3_to_gcs
