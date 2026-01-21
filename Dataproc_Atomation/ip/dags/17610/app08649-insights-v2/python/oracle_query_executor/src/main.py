import argparse
import logging
import os
import sys
import oracledb
from vault_client import Vault


def get_default_vault_host():
    return f"https://{os.environ.get('VAULT_PATH')}:{os.environ.get('VAULT_PORT')}"


def main():
    parser = argparse.ArgumentParser(description="Oracle query executor module arguments")
    parser.add_argument("--sql", type=str, help="The sql to run", required=True)
    parser.add_argument("--host", type=str, help="Oracle server host", default=os.environ.get('ORA_SIM_HOST'))
    parser.add_argument("--database", type=str, help="Database/service name for Oracle", default=os.environ.get('ORA_SIM_DB'))
    parser.add_argument("--port", type=int, help="Oracle listener port", default=1521)
    parser.add_argument("--vault_mount_point", type=str, help="Vault mount point", default="nordsecrets")
    parser.add_argument("--vault_host", type=str, help="Vault host", default=get_default_vault_host())
    args = parser.parse_args()

    if args.vault_host is None:
        raise ValueError(
            "Env variable VAULT_HOST is not set.")

    if os.environ.get('VAULT_ROLE_ID') is None or os.environ.get('VAULT_SECRET') is None:
        raise ValueError(
            "Env variable VAULT_ROLE_ID or VAULT_SECRET is not set.")

    role_id = os.environ.get('VAULT_ROLE_ID')
    secret = os.environ.get('VAULT_SECRET')

    ora_vault_path = os.environ.get('ORA_SIM_VAULT_PATH')

    try:
        vault = Vault(args.vault_host, role_id, secret)
        ora_cred = vault.fetch_secrets(ora_vault_path, args.vault_mount_point)
        ora_user_name = ora_cred.get('data').get('data').get('user_name')
        ora_password = ora_cred.get('data').get('data').get('password')

        with oracledb.connect(host=args.host, port=args.port, user=ora_user_name, password=ora_password,
                              service_name=args.database) as connection:
            with connection.cursor() as cursor:
                sql = args.sql
                logging.info(f"Oracle query: {sql}")
                cursor.execute(sql)
                connection.commit()
        sys.exit(0)

    except Exception as e:
        logging.info(e)
        sys.exit(1)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
