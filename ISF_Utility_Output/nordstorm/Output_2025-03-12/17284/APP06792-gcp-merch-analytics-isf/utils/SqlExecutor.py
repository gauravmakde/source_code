# standard imports
import os
import re
import yaml
import logging
import time
from typing import Dict

# third party imports
from cerebro.connection_store import Client
from cerebro.models.connection_pb2 import Connection
from cerebro.connectors.teradata import TeradataConnector


class SqlExecutor(object):

    def __init__(self,
                 config_filepath: str = 'utils/sql_config.yml',
                 log_filename: str = 'run_sql') -> None:
        self._make_logger(log_filename)
        self._load_config(config_filepath)
        self._get_conn()
    
    def _make_logger(self,
                     filename) -> None:
        """
        Create logger to save INFO to file.

        Parameters
        ----------
        name: str
            Optional filename for log
        """
        self.logger = logging.getLogger()
        self.logger.handlers.clear()
        self.logger.setLevel(logging.INFO) # levels: CRITICAL, ERROR, WARNING, INFO, DEBUG
        file_handler = logging.FileHandler(f'{filename}.log')
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(formatter)
        # TODO: also write stdout
        self.logger.addHandler(file_handler)
        with open(f'utils/{filename}.log', 'w'):
            pass # truncate any existing file

    def _load_config(self,
                     config_filepath) -> None:
        """
        Load config parameters from file.

        Parameters
        ----------
        name: str
            Optional filename for config
        """
        with open(config_filepath, 'r') as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                self.logger.info(exc)
        # if not all(self.config.values()):
        #     print('Warning: Parameter dictionary contains `None` values.')

    def _get_conn(self) -> None:
        """Create Teradata connection with appropriate connection_id and auth_mech."""
        client = Client()
        connection_object: Connection = client.get_connection(connection_id=self.config['connection']['connection_id'])
        self.td_conn = TeradataConnector(connection=connection_object, auth_mech=self.config['connection']['auth_mech'], log_verbose=False)
    
    def get_file_contents(self,
                          file_path: str) -> str:
        """
        Given a file path, retrieve the file content and returns the string.

        Parameters
        ----------
        file_path: str
            File path for sql statement

        Return
        ----------
        str: string of SQL statement(s)

        Raise
        ----------
        FileNotFoundError: given file_path not found
        NotADirectoryError: given base_path is not a directory
        """
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if not os.path.isdir(base_path):
            raise NotADirectoryError(f"Directory `{base_path}` not found.")
        try:
            with open(os.path.join(base_path, file_path)) as f_stream:
                sql_string = f_stream.read()
                return sql_string
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"File `{file_path}` not found.")

    def _get_params(self,
                    sql: str,
                    my_params: Dict[str, str]) -> dict:
        """ Get all parameters from SQL script and replace with values.

        Parameters
        ----------
        sql: str
            SQL script in string format.
        params: dict
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

    def exec_sql_with_splitter(self,
                               sql_string: str,
                               splitter: str = ';') -> None:
        """ Execute SQL statements in the same SQL file. 
        Statements are joined by the splitter. Note: remove any semicolons from comments to avoid errors!

        Parameters
        ----------
        sql_string: str
            SQL script in string format. Statements should be joined by the splitter.
        splitter: str
            Find the splitter in SQL script between SQL statements. Default is `;`
        """
        start_ts = time.time() / 60
        times = []

        for i, statement in enumerate(sql_string.split(splitter)):
            statement = statement.strip()
            if statement.replace('\n', '') == '':
                print(f'Warning: Statement {i} is empty.')
                times.append(0)
                continue
            self.logger.info(f'\nExecuting statement {i}...\n\n"""\n{statement[:50]}...\n"""')
            temp_start = time.time() / 60
            self.td_conn.execute(statement)
            temp_stop = time.time() / 60
            temp_time = round(temp_stop - temp_start, 3)
            times.append(temp_time)
            self.logger.info(f'Executed statement {i}: {temp_time} minutes')
            self.logger.info(f'Total so far: {round((time.time() / 60) - start_ts, 3)} minutes\n')

        self.logger.info('Total execution time: ' + f'{round((time.time() / 60) - start_ts, 3)} minutes')
