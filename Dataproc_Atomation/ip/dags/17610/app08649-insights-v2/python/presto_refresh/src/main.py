import argparse
import logging
import os
import time
import sys

from sqlalchemy.engine import create_engine
from sqlalchemy import text
from vault_client import Vault

def get_default_vault_host():
    return f"https://{os.environ.get('VAULT_PATH')}:{os.environ.get('VAULT_PORT')}"

def main():

    parser = argparse.ArgumentParser(description="Presto resresh module arguments")
    parser.add_argument("--vault_mount_point", type=str, help="Vault mount point", default="nordsecrets")
    parser.add_argument("--hive_table", type=str, help="Hive tables to refresh", default="")
    parser.add_argument("--vault_host", type=str, help="Vault host", default=get_default_vault_host())
    args = parser.parse_args()

    logging.info(f'Hive table to refresh: {args.hive_table}')

    if args.hive_table is None:
        raise ValueError(
            "Env variable HIVE_TABLE is not set.")

    if args.vault_host is None:
        raise ValueError(
            "Env variable VAULT_HOST is not set.")

    if os.environ.get('VAULT_ROLE_ID') is None or os.environ.get('VAULT_SECRET') is None:
        raise ValueError(
            "Env variables VAULT_ROLE_ID or VAULT_SECRET is not set.")

    role_id = os.environ.get('VAULT_ROLE_ID')
    secret = os.environ.get('VAULT_SECRET')

    bd_vault_path = os.environ.get('BD_VAULT_PATH')
    presto_host = os.environ.get('PRESTO_HOST')
    presto_catalog = os.environ.get('PRESTO_CATALOG')

    try:
        vault = Vault(args.vault_host, role_id, secret)
        presto_cred = vault.fetch_secrets(bd_vault_path, args.vault_mount_point)
        presto_user_name = presto_cred.get('data').get('data').get('user_name')
        presto_password = presto_cred.get('data').get('data').get('password')

        presto_connection_line = f'presto://{presto_user_name}:{presto_password}@{presto_host}:443/{presto_catalog};AuthenticationType=LDAPAuthentication;TimeZoneID=UTC'
        presto_engine = create_engine(presto_connection_line, connect_args={'protocol': 'https'})

        presto_connect = presto_engine.connect()
        presto_connect.autocommit = True

        sync_command = text(f"CALL {presto_catalog}.system.sync_partition_metadata('object_model','{args.hive_table}','add')")
        logging.info(f'RUN sync_command: {sync_command} \n')
        presto_connect.execute(sync_command)

        presto_connect.close()
        sys.exit(0)

    except Exception as e:
        logging.info(e)
        sys.exit(1)

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
