import configparser
import os
import os.path as path
import datetime
import pendulum

from datetime import timedelta


def get_env() -> str:
    allowed_envs = ['local', 'nonprod', 'prod']
    env = os.environ['ENVIRONMENT']
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


def get_default_args() -> dict:
    default_args = {
        'email': global_config.get(env, 'email', fallback=None),
        'email_on_retry': global_config.getboolean(env, 'email_on_retry'),
        'email_on_failure': global_config.getboolean(env, 'email_on_failure'),
        'retries': global_config.getint(env, 'retries'),
        'retry_delay': timedelta(minutes=global_config.getint(env, 'retry_delay_minutes')),
        'depends_on_past': global_config.getboolean(env, 'depends_on_past', fallback='True'),
        'wait_for_downstream': global_config.getboolean(env, 'wait_for_downstream', fallback='True'),
        # 'sla': timedelta(minutes=global_config.getint(env, 'sla_minutes', fallback='180')),
        'provide_context': True
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


def get_config_value(config_key: str, config: configparser = None):
    if config is None:
        config = get_global_config()

    val = config.get(env, config_key)
    return val


def get_config_replacements(replacements, key_list=None, config: configparser = None):
    if key_list is None:
        key_list = []
    if config is not None:
        if bool(key_list):
            for key in key_list:
                replacements['<' + key + '>'] = config.get(env, key)
        else:
            for key in config.options(env):
                replacements['<' + key + '>'] = config.get(env, key)

    return replacements


def config_has_option(config_key: str, config: configparser = None) -> bool:
    if config is not None:
        return config.has_option(env, config_key)
    else:
        return False


env = get_env()

global_config = get_global_config()
