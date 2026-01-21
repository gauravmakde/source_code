import os
import configparser
import logging

from datetime import timedelta

from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from nordstrom.utils.setup_module import setup_module
from nordstrom.sensors.api.dagrun_latest_sensor import ApiDagRunLatestSensor
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

dag_path = os.path.dirname(os.path.abspath(__file__))
common_module_path = os.path.join(dag_path, "__init__.py")

setup_module(
    module_name="clarity_common",
    file_path=common_module_path,
)

from clarity_common.config_utils import get_env, get_global_config

log = logging.getLogger(__name__)
env = get_env()


def get_timeliness_sql_sensor(table_name: str, db_name: str, batch_date_offset: str = '', config: configparser = None, **kwargs) -> SqlSensor:
    if config is None:
        config = get_global_config()

    sql = '''
select 1
from <DBENV>_NAP_BASE_VWS.DATA_TIMELINESS_METRIC_FACT
where table_name = '<table_name>'
and database_name = '<db_name>'
and step_short_desc = 'LOAD_END'
and run_date = date'{{ ds }}' <batch_date_offset>;
'''

    sql = sql.replace('<table_name>', table_name)
    sql = sql.replace('<db_name>', db_name)
    sql = sql.replace('<batch_date_offset>', batch_date_offset)
    sql = sql.replace('<DBENV>', config.get(env, 'td_environment'))

    kwargs.setdefault('timeout', config.getfloat(env, 'sensor_timeout_seconds'))
    kwargs.setdefault('poke_interval', config.getfloat(env, 'sensor_poke_interval_seconds'))
    kwargs.setdefault('mode', config.get(env, 'sensor_mode'))

    return SqlSensor(
        task_id=f'{table_name}_timeliness_sensor',
        conn_id=config.get(env, 'teradata_jdbc_connection'),
        sql=sql,
        retries=3,
        **kwargs
    )


def get_external_task_sensor(dag_name: str, task_name: str, offset_minutes: int, config: configparser = None, **kwargs) -> ExternalTaskSensor:
    if config is None:
        config = get_global_config()

    kwargs.setdefault('timeout', config.getfloat(env, 'sensor_timeout_seconds'))
    kwargs.setdefault('poke_interval', config.getfloat(env, 'sensor_poke_interval_seconds'))
    kwargs.setdefault('mode', config.get(env, 'sensor_mode'))

    return ExternalTaskSensor(
        task_id=f'{dag_name}_external_task_sensor',
        external_dag_id=dag_name,
        external_task_id=task_name,
        execution_delta=timedelta(minutes=offset_minutes),
        retries=3,
        **kwargs
    )


# Many optional return parameters can be given: refer https://developers.nordstromaws.app/docs/TM00717/documentation/APP01729-airflow-docs/airflow/sensor-apidagrunlatestsensor/index.html
# Refer this for jinja template: https://airflow.apache.org/docs/apache-airflow/2.0.1/macros-ref.html
def get_dag_run_date_range():
    context = get_current_context()

    previous_date_time = context['execution_date']
    current_date_time = context['next_execution_date']

    return {
        'execution_date_lte': current_date_time.isoformat(),
        'execution_date_gte': previous_date_time.isoformat()
    }


def get_api_dag_run_latest_sensor(dag_name: str, apiname: str, config: configparser = None, **kwargs) -> ApiDagRunLatestSensor:
    if config is None:
        config = get_global_config()

    http_conn = config.get(env, apiname)

    kwargs.setdefault('timeout', config.getfloat(env, 'sensor_timeout_seconds'))
    kwargs.setdefault('poke_interval', config.getfloat(env, 'sensor_poke_interval_seconds'))
    kwargs.setdefault('mode', config.get(env, 'sensor_mode'))

    return ApiDagRunLatestSensor(
        task_id=f'{dag_name}_apidagrunlatestsensor',
        conn_id=http_conn,
        external_dag_id=dag_name,
        date_query_fn=get_dag_run_date_range,
        **kwargs
    )


def get_completeness_sql_sensor(dq_id: str, allowed_variance: int, config: configparser = None, **kwargs) -> SqlSensor:
    if config is None:
        config = get_global_config()

    sql = '''
select
CASE
    WHEN max(value) = 0 then 0
    ELSE ((max(value) - min(value))*100 / max(value))
END as dly_variance
from (
select feature_name, sum(feature_value) as value
from dq.dq_results dr
join dq.dq_results_data drd
on dr.result_id = drd.result_id
where dr.dq_check_date = date'{{ next_ds }}'
and dr.dq_id = '<dq_id>'
and feature_name not like '%_variance_value' and feature_name not like '%_variance_percent'
group by 1
) feature_rollup;
'''

    sql = sql.replace('<dq_id>', dq_id)

    kwargs.setdefault('timeout', config.getfloat(env, 'sensor_timeout_seconds'))
    kwargs.setdefault('poke_interval', config.getfloat(env, 'sensor_poke_interval_seconds'))
    kwargs.setdefault('mode', config.get(env, 'sensor_mode'))

    def variance_pass(first_cell):
        if first_cell is None:
            return False

        if first_cell < allowed_variance:
            log.info(f'pass: allowed variance is {allowed_variance}%, actual variance is {first_cell}%')
            return True

        return False

    def variance_fail(first_cell):
        if first_cell is None:
            log.info('No variance found, sensor will wait')
            return False

        if first_cell >= allowed_variance:
            log.info(f'fail: allowed variance is {allowed_variance}%, actual variance is {first_cell}%')
            return True

        return False

    return SqlSensor(
        task_id=f'{dq_id}_completeness_sensor',
        conn_id=config.get(env, 'dqp_jdbc_connection'),
        sql=sql,
        success=variance_pass,
        failure=variance_fail,
        retries=3,
        **kwargs
    )


def get_multiple_completeness_sql_sensor_group(table_name: str, dq_id_variance_mapping: dict, config: configparser = None, **kwargs) -> TaskGroup:
    if config is None:
        config = get_global_config()

    with TaskGroup(group_id=f'{table_name}_completeness_checks') as group:
        for key in dq_id_variance_mapping:
            get_completeness_sql_sensor(key, dq_id_variance_mapping[key], config, **kwargs)

    return group
