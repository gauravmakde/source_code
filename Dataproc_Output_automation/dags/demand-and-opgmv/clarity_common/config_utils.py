import configparser
import os
import os.path as path
import datetime
import pendulum

from datetime import timedelta


def get_env() -> str:
    allowed_envs = ['local', 'nonprod', 'uat', 'prod']
    # EOPS-210
    # the Enterprise Scheduling Platform (Airflow) team only supports two environments: nonprod and production
    # our staging/uat environment resides in Airflow's Production server (APP09420)
    # so we also leverage the 'APP_ID' to correctly identify the environment
    env = os.environ['ENVIRONMENT']
    app = os.environ['APP_ID']
    if env == 'prod' and app == 'APP09420':
        env = 'uat'
    if env not in allowed_envs:
        env = 'local'
    return env


def get_config(dag_path: path, file_name: path) -> configparser:
    config = configparser.ConfigParser()
    config.read(os.path.join(dag_path, file_name))
    return config


def get_global_config() -> configparser:
    project_path = os.path.dirname(os.path.abspath(__file__))
    return get_config(project_path, 'global.cfg')


def get_default_args(dag_env:str) -> dict:
    default_args = {
        'email': global_config.get(dag_env, 'email', fallback=None),
        'email_on_retry': global_config.getboolean(dag_env, 'email_on_retry'),
        'email_on_failure': global_config.getboolean(dag_env, 'email_on_failure'),
        'retries': global_config.getint(dag_env, 'retries'),
        'retry_delay': timedelta(minutes=global_config.getint(dag_env, 'retry_delay_minutes')),
        'depends_on_past': global_config.getboolean(dag_env, 'depends_on_past', fallback='True'),
        'wait_for_downstream': global_config.getboolean(dag_env, 'wait_for_downstream', fallback='True'),
        'sla': timedelta(minutes=global_config.getint(dag_env, 'sla_minutes', fallback='120'))
    }

    return default_args


def get_start_date_with_tz(dag_config=None) -> datetime:
    if dag_config is None:
        dag_config = global_config

    date_string = dag_config.get(env, 'start_date')
    tz_string = dag_config.get(env, 'timezone')
    return pendulum.from_format(date_string, 'YYYY-MM-DD', tz=tz_string)


def get_none_if_empty(config: configparser, env: str, config_name: str) -> str:
    val = config.get(env, config_name)
    return None if val == '' or val == 'None' else val


env = get_env()
global_config = get_global_config()
