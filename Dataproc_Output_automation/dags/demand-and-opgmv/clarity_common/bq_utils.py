import fnmatch
import os
import configparser
import logging
import sys

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from nordstrom.utils.setup_module import setup_module



dag_path = os.path.dirname(os.path.abspath(__file__))
#common_module_path = os.path.join(dag_path, "__init__.py")
sys.path.append(dag_path)


# setup_module(
#     module_name="clarity_common",
#     file_path=common_module_path,
# )

# from clarity_common.config_utils import get_env, get_global_config
from config_utils import get_global_config,get_env

log = logging.getLogger(__name__)
# env = get_env()
env = 'nonprod'



# EOPS-210
# this function updates DB environment/schema references in the SQL script
def update_schema_references(file_name: str, config: configparser = None) -> None:
    if config is None:
        config = get_global_config()

    prefix = config.get(env, 'bq_environment')
    jwn_prefix = config.get(env, 'td_jwn_environment')

    with open(file_name, 'r') as sql_file:
        contents = sql_file.read()

    contents = contents.replace('<DBENV>', prefix).replace('<DBJWNENV>', jwn_prefix)

    with open(file_name, 'w') as sql_file:
        sql_file.write(contents)


def get_file_copy_sftp_operator(task_id: str, local_filepath: str, destination_filename: str, config: configparser = None, **kwargs) -> SFTPOperator:
    if config is None:
        config = get_global_config()

    connection = config.get(env, 'teradata_ssh_connection')
    ec2_path = config.get(env, 'td_ec2_path')

    update_schema_references(local_filepath)

    return SFTPOperator(
        task_id=task_id,
        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
        local_filepath=local_filepath,
        remote_filepath=f'{ec2_path}/{destination_filename}',
        operation='put',
        create_intermediate_dirs='True',
        **kwargs
    )


def get_file_copy_sftp_group(group_id: str, sql_abs_dirpath: str, dags_abs_dirpath: str, **kwargs) -> TaskGroup:
    with TaskGroup(group_id=group_id) as group:
        i = 0
        for root, dirs, files in os.walk(sql_abs_dirpath):
            for name in files:
                if fnmatch.fnmatch(name, '*.sql'):
                    sql_abs_filepath = os.path.normpath(os.path.join(root, name))
                    sql_rel_path = os.path.normpath(os.path.relpath(sql_abs_filepath, start=dags_abs_dirpath))
                    get_file_copy_sftp_operator(
                        task_id=f'copy_{name}_{i}',
                        local_filepath=sql_abs_filepath,
                        destination_filename=sql_rel_path,
                        depends_on_past=False,
                        wait_for_downstream=False,
                        **kwargs
                    )
                    i += 1

    return group


def get_static_file_copy_sftp_group(group_id: str, sql_abs_dirpath: str, dags_abs_dirpath: str, file_list: list, **kwargs) -> TaskGroup:
    with TaskGroup(group_id=group_id) as group:
        for file_name in file_list:
            sql_abs_filepath = os.path.normpath(os.path.join(sql_abs_dirpath, file_name))
            sql_rel_path = os.path.normpath(os.path.relpath(sql_abs_filepath, start=dags_abs_dirpath))
            get_file_copy_sftp_operator(
                task_id=f'copy_{file_name}',
                local_filepath=sql_abs_filepath,
                destination_filename=sql_rel_path,
                depends_on_past=False,
                wait_for_downstream=False,
                **kwargs
            )
    return group


def get_bteq_ssh_operator(task_id: str, sql_file: str, config: configparser = None, **kwargs) -> SSHOperator:
    if config is None:
        config = get_global_config()

    command = get_bteq_command(sql_file, config)
    connection = config.get(env, 'teradata_ssh_connection')

    return SSHOperator(
        task_id=task_id,
        command=command,
        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
        **kwargs
    )


def get_bteq_command(sql_file: str, config: configparser = None) -> str:
    if config is None:
        config = get_global_config()

    command = '/db/teradata/bin/bteq_load.sh -l "N"'
    command = command + ' -e ' + config.get(env, 'td_env')
    command = command + ' -h ' + config.get(env, 'td_host')
    command = command + ' -u ' + config.get(env, 'td_user')
    command = command + r' -p "\$tdwallet(' + config.get(env, 'td_wallet_entry') + ')"'
    command = command + ' -f ' + config.get(env, 'td_ec2_path') + '/' + sql_file

    return command


def get_bteq_ssh_operator_unsecured(task_id: str, sql_file: str, config: configparser = None, **kwargs) -> SSHOperator:
    if config is None:
        config = get_global_config()

    command = get_bteq_command_unsecured(sql_file, config)
    connection = config.get(env, 'teradata_ssh_connection')

    return SSHOperator(
        task_id=task_id,
        command=command,
        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
        **kwargs
    )


def get_bteq_command_unsecured(sql_file: str, config: configparser = None) -> str:
    if config is None:
        config = get_global_config()

    command = '/db/teradata/bin/bteq_load.sh -l "N"'
    command = command + ' -e ' + config.get(env, 'td_env')
    command = command + ' -h ' + config.get(env, 'td_host')
    command = command + ' -u ' + config.get(env, 'td_user_unsecured')
    command = command + r' -p "\$tdwallet(' + config.get(env, 'td_wallet_entry_unsecured') + ')"'
    command = command + ' -f ' + config.get(env, 'td_ec2_path') + '/' + sql_file

    return command


def get_jdbc_python_operator(task_id: str, file_name: str, config: configparser = None, **kwargs) -> PythonOperator:
    if config is None:
        config = get_global_config()

    conn_name = config.get(env, 'teradata_jdbc_connection')
    prefix = config.get(env, 'bq_environment')
    jwn_prefix = config.get(env, 'td_jwn_environment')

    return PythonOperator(
        task_id=task_id,
        python_callable=load_and_run_sql,
        op_kwargs={
            'sql_file': file_name,
            'connection': conn_name,
            'prefix': prefix,
            'jwn_prefix': jwn_prefix
        },
        **kwargs
    )


def get_jdbc_python_operator_unsecured(task_id: str, file_name: str, config: configparser = None, **kwargs) -> PythonOperator:
    if config is None:
        config = get_global_config()

    conn_name = config.get(env, 'teradata_jdbc_connection_unsecured')
    prefix = config.get(env, 'bq_environment')

    return PythonOperator(
        task_id=task_id,
        python_callable=load_and_run_sql,
        op_kwargs={
            'sql_file': file_name,
            'connection': conn_name,
            'prefix': prefix
        },
        **kwargs
    )


def get_jdbc_python_operator_timeliness_macro(task_id: str, table_names: list, db_name: str, dag_id: str, step_id: str, bqhook: BigQueryHook,config: configparser = None, **kwargs) -> PythonOperator:
    if config is None:
        config = get_global_config()

    return PythonOperator(
        task_id=task_id,
        python_callable=run_timeliness_macro,
        op_kwargs={
            'table_names': table_names,
            'db_name': db_name,
            'dag_id': dag_id,
            'task_id': task_id,
            'step_id': step_id,
            'bqhook': bqhook,
            'config': config,

        },
        **kwargs
    )


def read_sql(file_name: str, prefix: str, jwn_prefix: str) -> str:
    log.info("Executing in ENVIRONMENT - %s" % env)
    # sql_path = f'{dag_path}/../DAGs/teradata/scripts/{file_name}'
    log.info("Load sql %s " % file_name)
    with open(file_name, 'r') as file:
        return file.read().replace("<DBENV>", prefix).replace("<DBJWNENV>", jwn_prefix)


def run_sql(conn, file_name: str, prefix: str) -> None:
    cursor = conn.cursor()
    statement = read_sql(file_name, prefix)
    cursor.execute(statement)


def run_multi_statement_sql(conn, file_name: str, prefix: str, jwn_prefix: str) -> None:
    bt_str = 'BT;'
    et_str = 'ET;'
    newline = '\n'
    extended_whitespace = ' \t\n\r'
    cursor = conn.cursor()
    statements = read_sql(file_name, prefix, jwn_prefix).split(et_str)
    for statement in statements:
        statement = statement.strip(extended_whitespace)
        if bt_str in statement:
            statement = f'{statement}{newline}{et_str}'
        if statement != '':
            log.info(f'Executing SQL statement:{newline}{statement}')
            cursor.execute(statement)


def load_and_run_sql(sql_file: str, connection: str, prefix: str, jwn_prefix: str) -> None:
    hook = JdbcHook(jdbc_conn_id=connection)
    conn = hook.get_conn()
    try:
        log.info("Execute start SQL from file : %s" % sql_file)
        run_multi_statement_sql(conn, sql_file, prefix, jwn_prefix)
        log.info("Execute end SQL from file : %s" % sql_file)
    finally:
        conn.close()


def run_timeliness_macro(table_names: list, db_name: str, dag_id: str, task_id: str, step_id: str, bqhook: BigQueryHook, config: configparser = None) -> None:
    if config is None:
        config = get_global_config()


    for table_name in table_names:
        sql = '''
            CALL `<DBENV>_NAP_UTL.data_timeliness_metric_fact_ld`
            ('<table_name>',
            '<db_name>',
            '<dag_id>',
            '<task_id>',
            <step_id>,
            'LOAD_START',
            '<table_name> load started',
            current_datetime('PST8PDT'),
            '<table_name>');
             '''

        if '_END' in task_id.upper():
            sql = sql.replace('LOAD_START', 'LOAD_END')
            sql = sql.replace('<table_name> load started', '<table_name> load completed')

        sql = sql.replace('<table_name>', table_name)
        sql = sql.replace('<db_name>', db_name)
        sql = sql.replace('<dag_id>', dag_id)
        sql = sql.replace('<task_id>', task_id)
        sql = sql.replace('<step_id>', step_id)
        sql = sql.replace('<DBENV>', config.get(env, 'bq_environment'))
        logging.info("query  is ",sql)

        bqhook.run_query(sql)


def read_sql_2(file_name: str) -> str:
    log.info("Load sql %s " % file_name)
    with open(file_name, 'r') as file:
        return file.read()


def parse_sql_file(sql: str) -> list:
    extended_whitespace = ' \t\n\r'
    raw_statements = sql.split(';')
    final_statements = []
    for statement in raw_statements:
        statement = statement.strip(extended_whitespace)
        if statement != '':
            final_statements.append(f'{statement};')
    return final_statements


def load_and_run_sql_2(sql_file: str, connection: str, prefix: str, jwn_prefix: str, replacements: dict) -> None:
    hook = JdbcHook(jdbc_conn_id=connection)
    sql = read_sql_2(sql_file)

    if '<DBENV>' not in replacements:
        replacements['<DBENV>'] = prefix
    if '<DBJWNENV>' not in replacements:
        replacements['<DBJWNENV>'] = jwn_prefix
    for key in replacements:
        log.info(f'Replacing {key} with {replacements[key]}')
        sql = sql.replace(key, replacements[key])

    parsed_sql = parse_sql_file(sql)
    hook.run(parsed_sql, autocommit=False)


def get_jdbc_python_operator_2(task_id: str, file_name: str, replacements: dict = {}, config: configparser = None, **kwargs) -> PythonOperator:
    if config is None:
        config = get_global_config()

    conn_name = config.get(env, 'teradata_jdbc_connection')
    prefix = config.get(env, 'bq_environment')
    jwn_prefix = config.get(env, 'td_jwn_environment')

    return PythonOperator(
        task_id=task_id,
        python_callable=load_and_run_sql_2,
        op_kwargs={
            'sql_file': file_name,
            'connection': conn_name,
            'prefix': prefix,
            'jwn_prefix': jwn_prefix,
            'replacements': replacements
        },
        **kwargs
    )


def get_tpt_ssh_operator(task_id: str, sql_file: str, export_date: str, config: configparser = None, **kwargs) -> SSHOperator:
    if config is None:
        config = get_global_config()

    command = get_tpt_export_command(sql_file, export_date, config)

    connection = config.get(env, 'teradata_ssh_connection')

    return SSHOperator(
        task_id=task_id,
        command=command,
        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
        **kwargs
    )


def get_tpt_export_command(sql_file: str, export_date: str, config: configparser = None) -> str:
    if config is None:
        config = get_global_config()

    command = '/db/teradata/bin/tpt_export.sh -l "{}"'.format(config.get(env, 'character_set'))
    command += ' -e {}'.format(config.get(env, 'control_db_name'))
    command += ' -s {}/{}'.format(config.get(env, 'td_ec2_path'), sql_file)
    command += ' -h {}'.format(config.get(env, 'td_host'))
    command += ' -u {}'.format(config.get(env, 'td_user'))
    command += r' -p "\$tdwallet({})"'.format(config.get(env, 'td_wallet_entry'))
    command += ' -a "{}"'.format(config.get(env, 'account_type'))
    command += ' -d "{}"'.format(config.get(env, 'delimiter'))
    command += ' -q {}'.format(config.get(env, 'quote_flag'))
    command += ' -c {}'.format(config.get(env, 'column_flag'))
    command += ' -f {}'.format(config.get(env, 'escape_char'))
    command += ' -t {}'.format(config.get(env, 's3_path')) + '/' + export_date + '/' + format(config.get(env, 'filename'))
    command += ' -r "{}"'.format(config.get(env, 'run_id'))
    command += ' -j "{}"'.format(config.get(env, 'job_name'))
    command += ' -k {}'.format(config.get(env, 'kms_key'))
    return command


def get_tpt_load_ssh_operator(task_id: str, config: configparser = None, s3_folder: str = None, **kwargs) -> SSHOperator:
    if config is None:
        config = get_global_config()

    command = get_tpt_load_command(task_id, config, s3_folder)
    connection = config.get(env, 'teradata_ssh_connection')

    return SSHOperator(
        task_id=task_id,
        command=command,
        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
        **kwargs
    )


def get_tpt_load_command(task_id: str, config: configparser = None, s3_folder: str = None) -> str:
    if config is None:
        config = get_global_config()

    if s3_folder is not None:
        s3_folder += '/'

    command = '/db/teradata/bin/tpt_load.sh -l {}'.format(config.get(env, 'config_file_flag'))
    command += ' -e {}{}'.format(config.get(env, 'td_env'), config.get(env, 'control_db_name'))
    command += ' -j ' + task_id
    command += ' -h {}'.format(config.get(env, 'td_host'))
    command += ' -u {}'.format(config.get(env, 'td_user'))
    command += r' -p "\$tdwallet({})"'.format(config.get(env, 'td_wallet_entry'))
    command += ' -f {}/'.format(config.get(env, 's3_path_prefix')) + s3_folder
    command += ' -r "{}"'.format(config.get(env, 'iam_role'))
    return command


def get_datalab_tpt_load_ssh_operator(task_id: str, config: configparser = None, s3_folder: str = None, **kwargs) -> SSHOperator:
    if config is None:
        config = get_global_config()

    command = get_datalab_tpt_load_command(task_id, config, s3_folder)
    connection = config.get(env, 'teradata_ssh_connection')

    return SSHOperator(
        task_id=task_id,
        command=command,
        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
        **kwargs
    )


def get_datalab_tpt_load_command(task_id: str, config: configparser = None, s3_folder: str = None) -> str:
    if config is None:
        config = get_global_config()

    if s3_folder is not None:
        s3_folder += '/'

    job_id = task_id
    if env == 'uat':
        job_id = f'dev_{task_id}'

    command = '/db/teradata/bin/tpt_load.sh -l {}'.format(config.get(env, 'config_file_flag'))
    command += ' -e {}'.format(config.get(env, 'control_db_name'))
    command += ' -j ' + job_id
    command += ' -h {}'.format(config.get(env, 'td_host'))
    command += ' -u {}'.format(config.get(env, 'td_user'))
    command += r' -p "\$tdwallet({})"'.format(config.get(env, 'td_wallet_entry'))
    command += ' -f {}/'.format(config.get(env, 's3_path_prefix')) + s3_folder
    command += ' -r "{}"'.format(config.get(env, 'iam_role'))
    return command
