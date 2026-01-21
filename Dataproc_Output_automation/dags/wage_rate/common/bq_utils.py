import fnmatch
import os,sys
import configparser
import logging
import pandas.io.sql as psql

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from nordstrom.hooks.teradata_ssh_hook import TeraDataSSHHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
# from nordstrom.utils.setup_module import setup_module
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

dag_path = os.path.dirname(os.path.abspath(__file__))
# common_module_path = os.path.join(dag_path, "__init__.py")

# setup_module(
#     module_name="common",
#     file_path=common_module_path,
# )
sys.path.append(os.path.normpath(os.path.join(dag_path,'../')))
from common.config_utils import  get_env,get_global_config

log = logging.getLogger(__name__)
env = get_env()
# env='prod'

config = get_global_config()
conn_name = config.get(env, 'teradata_jdbc_connection')
location_bq = config.get(env, 'location_bq')
hook=BigQueryHook(gcp_conn_id=conn_name,location=location_bq, use_legacy_sql=False)

# def get_file_copy_sftp_operator(task_id: str, local_filepath: str, destination_filename: str, replacements: dict, config: configparser = None, **kwargs) -> SFTPOperator:
#     if config is None:
#         config = get_global_config()

#     connection = config.get(env, 'teradata_ssh_connection')
#     ec2_path = config.get(env, 'td_ec2_path')

#     return SFTPOperator(
#         task_id=task_id,
#         ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
#         local_filepath=local_filepath,
#         remote_filepath=f'{ec2_path}/{destination_filename}',
#         operation='put',
#         create_intermediate_dirs='True',
#         **kwargs
#     )


# def get_file_copy_sftp_group(group_id: str, sql_abs_dirpath: str, dags_abs_dirpath: str, replacements: dict, **kwargs) -> TaskGroup:
#     with TaskGroup(group_id=group_id) as group:
#         i = 0
#         for root, dirs, files in os.walk(sql_abs_dirpath):
#             for name in files:
#                 if fnmatch.fnmatch(name, '*.sql'):
#                     sql_abs_filepath = os.path.normpath(os.path.join(root, name))
#                     sql_rel_path = os.path.normpath(os.path.relpath(sql_abs_filepath, start=dags_abs_dirpath))
#                     get_file_copy_sftp_operator(
#                         task_id=f'copy_{name}_{i}',
#                         local_filepath=sql_abs_filepath,
#                         destination_filename=sql_rel_path,
#                         depends_on_past=False,
#                         wait_for_downstream=False,
#                         replacements=replacements,
#                         **kwargs
#                     )
#                     i += 1

#     return group


# def get_bteq_ssh_operator(task_id: str, sql_file: str, config: configparser = None, **kwargs) -> SSHOperator:
#     if config is None:
#         config = get_global_config()

#     command = fr'/db/teradata/bin/bteq_load.sh -l "N" -e PRD -h tdnapprod.nordstrom.net -u T2DL_NAP_SCA_BATCH -p "\$tdwallet(T2DL_NAP_SCA_BATCH_PWD)" -f /u01/app/td/prd/batch/sql/sca/scperf/sql/{sql_file}'
#     connection = config.get(env, 'teradata_ssh_connection')

#     return SSHOperator(
#         task_id=task_id,
#         command=command,
#         ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
#         **kwargs
#     )


# def get_tptload_ssh_operator(task_id: str, s3_filename: str, dl_env: str, config: configparser = None, **kwargs) -> SSHOperator:
#     if config is None:
#         config = get_global_config()

#     teradata_host = config.get(env, 'td_host')
#     user = config.get(env, 'td_user')
#     pw = config.get(env, 'td_wallet_entry')
#     control_db = dl_env + config.get(env, 'control_db_name')
#     s3_filepath = s3_filename.strip("[]")

#     command = fr'/db/teradata/bin/tpt_load.sh -l "N" -e {control_db} -j {task_id} -h {teradata_host} -u {user} -p "\$tdwallet({pw})" -f {s3_filepath} -r TD-UTILITIES-EC2'

#     connection = config.get(env, 'teradata_ssh_connection')

#     return SSHOperator(task_id=task_id,
#                        ssh_hook=TeraDataSSHHook(ssh_conn_id=connection),
#                        command=command,
#                        timeout=60,
#                        retries=3)


def get_jdbc_python_operator(hook, task_id: str, file_name: str, replacements: dict,location:str, config: configparser = None, **kwargs) -> PythonOperator:

    if env == 'prod':
        prefix = 't2dl'
    else:
        prefix = 't3dl'

    if '<DBENV>' not in replacements:
        replacements['<DBENV>'] = prefix

    return PythonOperator(
        task_id=task_id,
        python_callable=load_and_run_sql,
        op_kwargs={
            'sql_file': file_name,
            'prefix': prefix,
            'replacements': replacements,
            'hook': hook
        },
        **kwargs
    )


# def get_jdbc_python_operator_all_done(task_id: str, file_name: str, replacements: dict, config: configparser = None, **kwargs) -> PythonOperator:
#     if config is None:
#         config = get_global_config()

#     conn_name = config.get(env, 'teradata_jdbc_connection')

#     if env == 'prod':
#         prefix = 't2dl'
#     else:
#         prefix = 't3dl'

#     if '<DBENV>' not in replacements:
#         replacements['<DBENV>'] = prefix

#     return PythonOperator(
#         task_id=task_id,
#         python_callable=load_and_run_sql,
#         trigger_rule="all_done",
#         op_kwargs={
#             'sql_file': file_name,
#             'connection': conn_name,
#             'prefix': prefix,
#             'replacements': replacements
#         },
#         **kwargs
#     )


def set_s3_filename(hook, task_id: str, replacements: dict, config: configparser = None, **kwargs) -> PythonOperator:
    if env == 'prod':
        prefix = 't2dl'
    else:
        prefix = 't3dl'

    if '<DBENV>' not in replacements:
        replacements['<DBENV>'] = prefix

    return PythonOperator(
        task_id=task_id,
        python_callable=set_tpt_filename,
        op_kwargs={
            'replacements': replacements,
            'provide_context': True,
            'hook': hook
        },
        do_xcom_push=True,
        **kwargs

    )


def set_tpt_filename(hook, replacements: dict, ti):
    source_s3_bucket, prefix, infix, suffix, source_s3_file_extension = '', '', '', '', ''
    log.info(f'Old replacements: {replacements}')
    new_replacements = get_params(hook, replacements)
    if '<source_s3_bucket>' in new_replacements:
        source_s3_bucket = new_replacements['<source_s3_bucket>']
        log.info(f'source_s3_bucket:{source_s3_bucket}')

    if '<prefix>' in new_replacements:
        prefix = new_replacements['<prefix>']
        log.info(f'prefix:{prefix}')

    if '<infix>' in new_replacements:
        infix = new_replacements['<infix>']
        log.info(f'infix:{infix}')

    if '<suffix>' in new_replacements:
        suffix = new_replacements['<suffix>']
        log.info(f'suffix:{suffix}')

    if '<source_s3_file_extension>' in new_replacements:
        source_s3_file_extension = new_replacements['<source_s3_file_extension>']
        log.info(f'source_s3_file_extension:{source_s3_file_extension}')

    s3_path_filename = source_s3_bucket + prefix + infix + suffix + source_s3_file_extension
    log.info(f'S3 path file name: {s3_path_filename}')
    log.info(f'New replacement dicts: {new_replacements}')
    return s3_path_filename



# def get_deploy_python_operator(task_id: str, file_name: str, replacements: dict, config: configparser = None, **kwargs) -> PythonOperator:
#     if config is None:
#         config = get_global_config()

#     conn_name = config.get(env, 'teradata_jdbc_connection')

#     if env == 'prod':
#         prefix = 't2dl'
#     else:
#         prefix = 't3dl'

#     if '<DBENV>' not in replacements:
#         replacements['<DBENV>'] = prefix

#     deploy_project = get_deploy_project()

#     replacements['<subject_id>'] = deploy_project

#     file_name = f'{deploy_project}/scripts/deploy/{file_name}'

#     return PythonOperator(
#         task_id=task_id,
#         python_callable=load_and_run_sql,
#         op_kwargs={
#             'sql_file': file_name,
#             'connection': conn_name,
#             'prefix': prefix,
#             'replacements': replacements
#         },
#         **kwargs
#     )


def read_sql(file_name: str, prefix: str) -> str:
    log.info("Executing in ENVIRONMENT - %s" % env)
    log.info("Read sql %s " % file_name)
    with open(file_name, 'r') as file:
        sql = file.read()
    # sql = "SET QUERY_BAND = 'App_ID=<App_ID>;DAG_ID=<DAG_ID>;Task_Name=<Task_ID>;' FOR SESSION VOLATILE;\nCOMMIT TRANSCATION;\n" + sql
    # sql = sql + "\nBEGIN TRANSCATION;\nSET QUERY_BAND = NONE FOR SESSION;\nCOMMIT TRANSCATION;\n"
    sql = sql.replace("<DBENV>", prefix)
    return sql


def replace_parameters(hook, sql: str, replacements: dict):
    log.info('Collecting Parameters...')
    replacements = get_params(hook, replacements)
    if bool(replacements):
        for key in replacements:
            sql = sql.replace(key, str(replacements[key]))
    return sql


# def run_sql(conn, file_name: str, prefix: str) -> None:
#     cursor = conn.cursor()
#     statement = read_sql(file_name, prefix)
#     cursor.execute(statement)


def run_multi_statement_sql(hook, conn, file_name: str, prefix: str, replacements: dict) -> None:
    bt_str = 'BEGIN TRANSACTION;'
    et_str = 'COMMIT TRANSACTION;'
    newline = '\n'
    extended_whitespace = ' \t\n\r'
    cursor = conn.cursor()
    sql = read_sql(file_name, prefix)
    sql = replace_parameters(hook, sql, replacements)
    statements = sql.split(et_str)
    for statement in statements:
        statement = statement.strip(extended_whitespace)
        if bt_str in statement:
            statement = f'{statement}{newline}{et_str}'
        if statement != '':
            log.info(f'Executing SQL statement:{newline}{newline}{statement}{newline}')
            cursor.execute(statement)


def load_and_run_sql(hook, sql_file: str, prefix: str, replacements: dict, **context) -> None:
    task_id = context['ti'].task_id
    replacements['<Task_ID>'] = task_id
    conn = hook.get_conn()
    try:
        log.info("Execute start SQL from file : %s" % sql_file)
        run_multi_statement_sql(hook, conn, sql_file, prefix, replacements)
        log.info("Execute end SQL from file : %s" % sql_file)
    finally:
        conn.close()


def get_params(hook, replacements: dict = None):
    
    dbenv = replacements['<dl_env>']
    project_id = replacements['<project_id>']
    subject_id = replacements['<subject_id>']
    sql = f"select parameter_key, parameter_value from {dbenv}_SCA_PRF.BATCH_PARAMETERS \
           where lower(interface_id) = '{project_id.lower()}'\
           and lower(subject_id) = '{subject_id.lower()}' \
           and batch_id = (select btch_id from {dbenv}_SCA_PRF.DL_INTERFACE_DT_LKUP where \
           lower(interface_id) = '{project_id.lower()}' \
           and lower(subject_id) = '{subject_id.lower()}' and end_tmstp is null);"

    conn = hook.get_conn()
    # with hook.get_conn() as conn:
    df = psql.read_sql(sql, con=conn)
    #logger
    log.info(df.head())
    if len(df) > 0:
        for ind in df.index:
            replacements[str(df['parameter_key'][ind])] = str(df['parameter_value'][ind])
    else:
        log.info('No parameters found in parameter table...')

    sql = f"select '<batch_id>' as parameter_key, btch_id as parameter_value " \
          f"from {dbenv}_SCA_PRF.DL_INTERFACE_DT_LKUP where " \
          f"lower(interface_id) = '{project_id.lower()}' and lower(subject_id) = '{subject_id.lower()}' and end_tmstp is null;"

    # with hook.get_conn() as conn:

    conn = hook.get_conn()
    df = psql.read_sql(sql, con=conn)
    if len(df) > 0:
        for ind in df.index:
            replacements[str(df['parameter_key'][ind])] = str(df['parameter_value'][ind])
    else:
        log.info('No open batch found ...')

    if bool(replacements):
        replacements_text = '\n\n================================================\n'
        replacements_text = replacements_text + 'PARAMETERS FOR TASK RUN:\n'
        replacements_text = replacements_text + '================================================\n'
        for key, value in replacements.items():
            replacements_text = replacements_text + '\t' + str(key) + ' : ' + str(value) + '\n'
        log.info(replacements_text)
    else:
        log.info("No parameters found for dag run.")

    return replacements


# def get_deploy_project():
#     config = get_global_config()
#     conn_name = config.get(env, 'teradata_jdbc_connection')

#     jdbc_hook = JdbcHook(jdbc_conn_id=conn_name)

#     if env == 'prod':
#         prefix = 't2dl'
#     else:
#         prefix = 't3dl'

#     dbenv = prefix

#     sql = f"select subject_id from {dbenv}_SCA_PRF.DL_INTERFACE_DT_LKUP \
#             where interface_id = 'DEPLOY' and end_tmstp is null;"

#     log.info('Searching parameter table...')

#     df = jdbc_hook.get_pandas_df(sql)

#     if df.empty:
#         deploy_project = 'deploy'
#         log.info(f'No project is set for deploy in {dbenv}_SCA_PRF.DL_INTERFACE_DT_LKUP: {sql}')
#     else:
#         deploy_project = df.iloc[0][0]
#         log.info('\nProject to be deployed: {}\n\n'.format(deploy_project))

#     return deploy_project
