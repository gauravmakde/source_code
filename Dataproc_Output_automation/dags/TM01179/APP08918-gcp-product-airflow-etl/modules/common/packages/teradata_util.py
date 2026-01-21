# common function modules that will be used in all the dags
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.jdbc_hook import JdbcHook
import configparser
import logging
import os
from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook
from pathlib import Path

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = 'nonprod'
# Read the config files
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
config.read(os.path.join(Path(path).parent, 'configs/teradata.cfg'))


def etl_batch_load_status(subject_area, task_type, **kwargs):
    """
    This function prevents parallel execution by checking the batch_status in XCOM
    of the relevant subject area .
    parameters : subject_Area , Task_type
    return : XCOM.
    """
    ti = kwargs['ti']
    key_name = 'ETL_BATCH_PROGRESS_' + subject_area
    # Get the status of the subject area
    batch_status = ti.xcom_pull(key=key_name, include_prior_dates=True)

    # Batch_status set manually for testing.

    batch_status = 'N'

    logging.info(f'The current batch status for {key_name} is {batch_status}.')

    # for the first time execution, set the status flag to N
    if batch_status is None:
        batch_status = 'N'

    # If a load is in progress and task type is START, then raise exception and fail
    if task_type == 'START' and batch_status == 'Y':
        raise Exception(f'ETL batch is in progress for {subject_area}')

    # Set the ETL batch process flag based on the value of task_type.
    if task_type == 'START':
        batch_status = 'Y'
    else:
        batch_status = 'N'

    # Push the batch status to XCOM
    ti.xcom_push(key=key_name, value=batch_status)
    logging.info(f'Value of the batch status set for {key_name} is {batch_status}.')


def get_batch_load_status(task_id, subject_area, task_type):
    """
    This function call etl_batch_load_status from a python operator
    parameters : Task_id, Subject_Area , Task_type
    return : None.
    """
    return PythonOperator(task_id=task_id,
                          python_callable=etl_batch_load_status,
                          op_kwargs={'subject_area': subject_area , 'task_type': task_type},
                          provide_context=True,
                          trigger_rule="none_failed",)


