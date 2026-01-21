# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        20d627573a3de4fe423c6691cdf281f130030d2d
# CI pipeline URL:      https://git.jwn.app/TM01164/nap-payments/app07420-payments-insights-framework/-/pipelines/7239485
# CI commit timestamp:  2024-10-16T12:42:22+00:00
# This DAG file was generated using ETL Framework.
# Documentation can be found at below link
# https://developers.nordstromaws.app/docs/TM01373/insights-framework/docs/index.html

import pendulum
import os
import configparser
import yaml
from datetime import datetime, timedelta
from os import path
import sys
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook


env = os.environ.get('ENVIRONMENT', 'local')
# Fetch the data from config file
python_module_path = path.dirname(__file__).split('app08314-isf-airflow-dags')[0] + 'app08314-isf-airflow-dags/common_libraries/'
python_root_module_path = path.dirname(__file__).split('app08314-isf-airflow-dags')[0] + 'app08314-isf-airflow-dags/'
sql_path = python_root_module_path + '4638/payments/insights/sql/'
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(python_root_module_path)
sys.path.append(python_module_path)
path = os.path.split(__file__)[0]

from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status


start_date = pendulum.datetime(2022, 2, 24)
cron = None
dag_id = 'payment_bin_redemption_service_tpt_load_preprod_4638_payments_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 5,
    'description': 'payment_bin_redemption_service_tpt_load_preprod_4638_payments_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['credit-and-payments-p-aaaak7svy2d5ilbjwirhkdnqxa@nordstrom.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'sla': dag_sla

}


with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=cron,
        max_active_runs=set_max_active_runs,
        template_searchpath=[sql_path],
        concurrency=set_concurrency,
        ) as dag:

    tpt_jdbc_insert_read_from_s3_write_td_stage_0_job_load_control_tbl = BashOperator(
    task_id='tpt_jdbc_insert_read_from_s3_write_td_stage_0_job_load_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)


    tpt_read_from_s3_write_td_stage_0_job_load_load_tbl = SSHOperator(
    task_id='tpt_read_from_s3_write_td_stage_0_job_load_load_tbl',
    command="/db/teradata/bin/tpt_load.sh  -e PRD_NAP_UTL -j PAYMENT_AUTHORIZATION_RESULT_AFTERPAY_ID_LDG -h tdnapprod.nordstrom.net -l 'N' -u SCH_NAP_CUSTPAYMNT_BATCH -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PWD)\" -f s3://tf-nap-preprod-redemption-out-bucket/payment_authorization_result_bin_detokenized_extract/year=2024/month=05/day=31/hour=10/*.csv -o UPDATE -r TD-UTILITIES-EC2",
    timeout=1800,
    ssh_hook=TeraDataSSHHook(ssh_conn_id='TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT'),
    retries=2)


    tpt_jdbc_insert_read_from_s3_write_td_stage_0_job_load_control_tbl >> [tpt_read_from_s3_write_td_stage_0_job_load_load_tbl]

