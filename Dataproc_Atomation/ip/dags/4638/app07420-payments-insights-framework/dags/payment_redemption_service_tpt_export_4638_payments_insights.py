# -*- coding: utf-8 -*-
# DAG version:          default
# CI commit sha:        fa35953ba7eea04d644fc6fbc3d83aba045afc2c
# CI pipeline URL:      https://git.jwn.app/TM01164/nap-payments/app07420-payments-insights-framework/-/pipelines/7066897
# CI commit timestamp:  2024-09-25T17:43:10+00:00
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
cron = '08 00 * * *'
dag_id = 'payment_redemption_service_tpt_export_4638_payments_insights'
paused_upon_creation = True
set_max_active_runs = 1
set_concurrency = 10
CURRENT_TIMESTAMP = datetime.today()
RUN_TIMESTAMP = '{{ ts_nodash.lower() }}'
run_id_dict = {"spark.airflow.run_id": RUN_TIMESTAMP}
dag_sla = timedelta(minutes=int(-1)) if int(-1) > 1 else None

default_args = {
    'retries': 3,
    'description': 'payment_redemption_service_tpt_export_4638_payments_insights DAG Description',
    'retry_delay': timedelta(minutes=10),
    'email': ['TECH_NAP_CREDIT_PAYMENTS_DL@nordstrom.com'],
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

    tpt_jdbc_export_read_from_td_write_s3_stage_0_job_export_control_tbl = BashOperator(
    task_id='tpt_jdbc_export_read_from_td_write_s3_stage_0_job_export_control_tbl',
    bash_command=f'echo "Skipping Control Table Entry"',
    retries=2)


    tpt_read_from_td_write_s3_stage_0_job_export_export_tbl = SSHOperator(
    task_id='tpt_read_from_td_write_s3_stage_0_job_export_export_tbl',
    command="/db/teradata/bin/tpt_export.sh  -e PRD_NAP_UTL -s s3://tf-nap-prod-credit-payments-isf-airflow/apps/etl_framework/airflow_nsk_dag_job_artifacts/prod/4638/payments/insights/sql/teradata/payment_redemption_service_tpt_export_4638_payments_insights_read_from_td_write_s3_stage_0_payment_redemption_service_tpt_export.sql -h tdnapprodcop1.nordstrom.net -l 'N' -u SCH_NAP_CUSTPAYMNT_BATCH -p \"\$tdwallet(SCH_NAP_CUSTPAYMNT_BATCH_PWD)\"  -t s3://tf-nap-{env}-redemption-in-bucket/payments_redemptions/in/{partition}/{filename}.csv -a N -d , -c 1 -q N -f '' -j PRD_NAP_UTL_payment_bank_card_settlement_result_fact_JOB -r TD-UTILITIES-EC2".format(env=eval('"nonprod" if os.environ.get("ENVIRONMENT") == "development" else "prod"'),partition=eval('datetime.today().strftime("YEAR=%Y/MONTH=%m/DAY=%d")'),filename=eval('"redemption_tpt_export_" + str(int(round(datetime.now().timestamp())))')),
    timeout=1800,
    ssh_conn_id="TECH_ISF_NAP_CREDIT_PAYMENTS_TDUTIL_SERVER_S3EXT",
    retries=2)


    tpt_jdbc_export_read_from_td_write_s3_stage_0_job_export_control_tbl >> [tpt_read_from_td_write_s3_stage_0_job_export_export_tbl]


