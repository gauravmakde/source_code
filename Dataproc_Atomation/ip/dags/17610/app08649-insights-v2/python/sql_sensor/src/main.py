import argparse
import logging
import os
import sys
import time
from datetime import datetime

from teradata import Teradata
from vault_client import Vault

class CustomSQLSensorTimeout(Exception):
    pass

RESULT_COLUMN_POSITION = 0


def get_default_vault_host():
    return f"https://{os.environ.get('VAULT_PATH')}:{os.environ.get('VAULT_PORT')}"


def __get_arguments():

    parser = argparse.ArgumentParser(description="Send metrics module")
    parser.add_argument("--sql", type=str, help="The sql to run. To pass, it needs to return at least one cell that contains a non-zero / empty string value", required=True)
    parser.add_argument("--time_out", type=int, help="Time, in seconds before the task times out and fails", required=True)
    parser.add_argument("--poke_interval", type=int, help="Time in seconds that the job should wait in between each tries", required=True)
    parser.add_argument("--default_database", type=str, help="Default database for Teradata. E.g. {DBENV}_NAP_STG", required=True)
    parser.add_argument("--vault_mount_point", type=str, help="Vault mount point", default="nordsecrets")
    parser.add_argument("--vault_host", type=str, help="Vault host", default=get_default_vault_host())
    return parser.parse_args()
 

def main():
    args = __get_arguments()

    if args.vault_host is None:
        raise ValueError(
            "Env variable VAULT_HOST is not set.")

    if os.environ.get('VAULT_ROLE_ID') is None or os.environ.get('VAULT_SECRET') is None:
        raise ValueError(
            "Env variables VAULT_ROLE_ID or VAULT_SECRET is not set.")
    role_id = os.environ.get('VAULT_ROLE_ID')
    secret = os.environ.get('VAULT_SECRET')

    env = os.environ.get('ENV')
    td_env = os.environ.get('TD_ENV')
    td_host = os.environ.get('TD_HOST')
    td_vault_path = os.environ.get('TD_VAULT_PATH')

    default_database = args.default_database.format(DBENV=td_env)

    try:
        vault = Vault(args.vault_host, role_id, secret)
        td_creds = vault.fetch_secrets(td_vault_path, args.vault_mount_point)

        td_user = td_creds.get('data').get('data').get('user_name')
        td_password = td_creds.get('data').get('data').get('password')
     
        teradata = Teradata(td_host, td_user, td_password, default_database)
        sql = args.sql.format(DBENV=td_env)
        logging.info(f"Teradata query: {sql}")

        poke_interval = args.poke_interval
        time_out = args.time_out

        is_success = False
        started_at = datetime.now()
        
        while not is_success:
            if (datetime.now() - started_at).total_seconds() > time_out:
                raise CustomSQLSensorTimeout('SQLSensor time is OUT')
            
            # Getting data from Teradata
            query_result = teradata.execute_query(sql)
            logger.info(f"Query result: {str(query_result)}")
            for tupleValue in query_result:
                is_success = tupleValue[RESULT_COLUMN_POSITION] > 0
            
            if not is_success: 
                logging.info(f"Slip for {poke_interval} seconds")
                time.sleep(poke_interval)
                
        logging.info("Success criteria met. Exiting.")
        sys.exit(0)    
    except (CustomSQLSensorTimeout, Exception) as e:
        print(e)
        logging.exception(f"Exception: {e}. Exiting.")
    sys.exit(1)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
