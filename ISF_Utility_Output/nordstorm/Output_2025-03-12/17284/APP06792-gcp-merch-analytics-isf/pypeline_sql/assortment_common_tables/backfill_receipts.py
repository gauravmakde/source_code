# Script is intended to be a tool to run backfill when history date range cannot be done in 1 pass. 
# Run command: pipenv run python backfill_receipts.py prod

# standard imports
import sys
import os
import re
import yaml
import logging
import time

# third party imports
from cerebro.connection_store import Client
from cerebro.models.connection_pb2 import Connection
from cerebro.connectors.teradata import TeradataConnector

# TODO: update variables as input arguments
ddl_filepath = 'ast_pipeline_prod_ddl_receipt_sku_loc_week_agg_fact.sql'
main_filepath = 'ast_pipeline_prod_main_receipt_sku_loc_week_agg_fact.sql'

env = sys.argv[1] # position [0] is script name
if env == 'prod':
    connection_id = 'prod-selection-nap' # T2DL_NAP_SEL_BATCH
    auth_mech = None
    environment_schema = 't2dl_das_assortment_dim'
    env_suffix = ''
else:
    connection_id = 'nonprod-selection-t3dl-clhq' # CLHQ
    auth_mech = 'LDAP'
    environment_schema = 't2dl_das_assortment_dim'
    env_suffix = ''

def get_conn(connection_id, auth_mech):
    """
    Create Teradata connection with appropriate connection_id and auth_mech.

    Parameters
    ----------
    connection_id: str
        Name of MLP connection.
    auth_mech: str
        Authorization mechanism.

    Return
    ----------
    td_conn: Teradata connection
    """
    client = Client()
    connection_object: Connection = client.get_connection(connection_id=connection_id)
    td_conn = TeradataConnector(connection=connection_object, auth_mech=auth_mech, log_verbose=False)
    return td_conn

def get_file_contents(file_path: str, base_path: str = os.path.dirname(os.path.abspath(__file__))) -> str:
    """
        Given a file path, retrieve the file content and returns the string.

    Parameters
    ----------
    file_path: str
        relative path to the file
    base_path: str
        default directory path

    Return
    ----------
    str: string of SQL commands

    Raise
    ----------
    FileNotFoundError: given file_path not found
    NotADirectoryError: given base_path is not a directory
    """
    if not os.path.isdir(base_path):
        raise NotADirectoryError(f"Directory `{base_path}` not found.")
    try:
        with open(os.path.join(base_path, file_path)) as f_stream:
            return f_stream.read()
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"File `{file_path}` not found.")

def get_params(sql, my_params: dict = {}):
    """ Get all parameters from SQL script and replace with values.

    Parameters
    ----------
    sql: str
        SQL script in string format.
    my_params: dict
        Dictionary of known parameters.

    Return
    ----------
    params: str
        Dictionary of parameters to insert into sql query.
    """
    # find all variables in query statement
    variables = re.findall('\{.*?\}', sql)
    params = {var.replace('{', '').replace('}', ''): None for var in variables}

    # replace known variables
    for k in params.keys():
        params[k] = my_params.get(k, None)
        # TODO: raise error if missing param value in config

    # dump params into file
    with open('params.yml', 'w') as yaml_file:
        yaml.dump(params, yaml_file, default_flow_style=False)
    return params

def load_params():
    """ Get parameters from file.

    Return
    ----------
    params: str
        Dictionary of parameters to insert into sql query.
    """
    with open('params.yml', 'r') as stream:
        try:
            params = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.info(exc)

    if not all(params.values()):
        print('Warning: Parameter dictionary contains `None` values.') # TODO: remove after testing
    return params

def make_logger(name: str = 'run_sql'):
    """Create logger to save INFO to file.

    Parameters
    ----------
    name: str
        Optional filename for log

    Return
    ----------
    logger: logger object
    """
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO) # levels: CRITICAL, ERROR, WARNING, INFO, DEBUG
    file_handler = logging.FileHandler(f'{name}.log')
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    with open(f'{name}.log', 'w'):
        pass # truncate any existing file
    return logger

def execute_sql_statements_with_splitter(sql_string: str, splitter: str = ';', file_path: str = 'run_sql') -> None:
    """ Execute SQL statements in the same SQL file. Statements are joined by the splitter.

    Parameters
    ----------
    sql_string: str
        SQL script in string format. Statements should be joined by the splitter.
    splitter: str
        Find the splitter in SQL script between SQL statements. Default is `;`
    """
    logger = make_logger(file_path)

    start_ts = time.time() / 60
    times = []

    for i, statement in enumerate(sql_string.split(splitter)):
        statement = statement.strip()
        if statement.replace('\n', '') == '':
            print(f'Warning: Statement {i} is empty.')
            times.append(0)
            continue
        logger.info(f'\nExecuting statement {i}...\n\n"""\n{statement[:50]}...\n"""')
        temp_start = time.time() / 60
        print(statement)
        td_conn.execute(statement)
        temp_stop = time.time() / 60
        temp_time = round(temp_stop - temp_start, 3)
        times.append(temp_time)
        logger.info(f'Executed statement {i}: {temp_time} minutes')
        logger.info(f'Total so far: {round((time.time() / 60) - start_ts, 3)} minutes\n')

    logger.info('Total execution time: ' + f'{round((time.time() / 60) - start_ts, 3)} minutes')


if __name__ == "__main__":

    make_logger()

    # create Teradata connection
    td_conn = get_conn(connection_id, auth_mech)

    # run ddl
    # TODO: make optional in config
    print('Running DDL...') # TODO: convert print to logger
    sql_ddl = get_file_contents(ddl_filepath)
    my_params = {'environment_schema': environment_schema
                ,'env_suffix': env_suffix
                }
    params = get_params(sql_ddl, my_params=my_params)
    execute_sql_statements_with_splitter(sql_ddl.format(**params))
    print('DDL complete.')

    # split sql up into loop and daily/monthly
    # TODO: raise error if missing comments
    sql_main = get_file_contents(main_filepath)
    if not '-- begin' in sql_main:
        raise LookupError('Missing `-- begin` in sql')
    if not '-- end' in sql_main:
        raise LookupError('Missing `-- end` in sql')
    sql_begin = sql_main.split('-- begin')[0]
    sql_loop = sql_main.split('-- begin')[1].split('-- end')[0]
    temp_table_names = [x.split(' ')[0] for x in sql_loop.split('CREATE MULTISET VOLATILE TABLE ')[1:]]
    sql_drop = '\n'.join(['DROP TABLE ' + name + ';' for name in temp_table_names])
    sql_end = sql_main.split('-- end')[1]

    # Run any sql before loop
    print('Running beginning of query...')
    execute_sql_statements_with_splitter(sql_begin.format(**params))
    print('Beginning of query complete.')

    # run loop over date ranges
    print('Running loop...')
    date_ranges = [ ("DATE '2021-01-31'", "CURRENT_DATE - 1")] # 2021+ for NAP

    for i,(start_date,end_date) in enumerate(date_ranges):
        if i != 0: 
            print('Dropping tables before next loop...')
            execute_sql_statements_with_splitter(sql_drop)
        print(f'Running {i+1} loop for {start_date} to {end_date}.')
        my_params = {'environment_schema': environment_schema
                    ,'env_suffix': env_suffix
                    ,'start_date': start_date
                    ,'end_date': end_date
                    }
        params = get_params(sql_main, my_params=my_params)
        execute_sql_statements_with_splitter(sql_loop.format(**params))
    print('Loops complete.')
    
    print('Running remaining query...')
    execute_sql_statements_with_splitter(sql_end.format(**params))
    print('Query complete.')
 