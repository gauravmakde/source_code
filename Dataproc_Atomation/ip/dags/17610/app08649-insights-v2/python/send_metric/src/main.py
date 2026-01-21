import argparse
import logging
import os
import sys
import time

from new_relic import NewRelic
from teradata import Teradata
from util import read_yaml
from vault_client import Vault


def get_default_vault_host():
    return f"https://{os.environ.get('VAULT_PATH')}:{os.environ.get('VAULT_PORT')}"


def main():
    parser = argparse.ArgumentParser(description="Send metrics module")
    parser.add_argument("--vault_mount_point", type=str, help="Vault mount point", default="nordsecrets")
    parser.add_argument("--metrics_yaml_file", type=str, help="Input YAML file path", required=True)
    parser.add_argument("--nr_metrics_endpoint", type=str, help="New Relic metric endpoint", required=True)
    parser.add_argument("--default_database", type=str, help="Default database for Teradata. E.g. {DBENV}_NAP_STG",
                        required=True)
    parser.add_argument("--preproduction", type=bool, help="Is it for preprod? True/False", default=False)
    parser.add_argument("--vault_host", type=str, help="Vault host", default=get_default_vault_host())
    args = parser.parse_args()

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
    nr_vault_path = os.environ.get('NR_VAULT_PATH')

    if env == 'production' and args.preproduction:
        # only prod template should contain these variables
        env = os.environ.get('ENV_PREPROD')  # should be 'preproduction'
        td_env = os.environ.get('TD_ENV_PREPROD')  # should be 'PREPROD'

    default_database = args.default_database.format(DBENV=td_env)

    try:
        vault = Vault(args.vault_host, role_id, secret)
        td_creds = vault.fetch_secrets(td_vault_path, args.vault_mount_point)
        nr_creds = vault.fetch_secrets(nr_vault_path, args.vault_mount_point)

        td_user = td_creds.get('data').get('data').get('user_name')
        td_password = td_creds.get('data').get('data').get('password')
        nr_license_key = nr_creds.get('data').get('data').get('newrelic_ingest_key')

        new_relic = NewRelic(nr_license_key, args.nr_metrics_endpoint)
        teradata = Teradata(td_host, td_user, td_password, default_database)

        current_dir = os.path.abspath(os.path.dirname(__file__))
        full_path = os.path.join(current_dir, args.metrics_yaml_file)
        logger.info(f"Opening the metrics file: {full_path}")
        yaml_file = read_yaml(full_path)

        for metric_element in yaml_file:
            logger.info(f"Yaml object: {str(metric_element)}")
            metric_name = metric_element['metric_name']
            metric_type = metric_element['metric_type']
            metric_value_position = metric_element['metric_value_position']
            sql_template = metric_element['sql']
            sql = sql_template.format(DBENV=td_env)
            logger.info(f"Sql query: {sql}")

            # Getting data from Teradata
            query_result = teradata.execute_query(sql)
            logger.info(f"Query result: {str(query_result)}")

            # Posting data to New Relic
            if len(query_result) == 0:
                logging.info("There are no records to post to NewRelic")
            else:
                for tupleValue in query_result:
                    metric_value = tupleValue[metric_value_position]
                    tag_value = {}
                    for element in metric_element['tags']:
                        for key, value in element.items():
                            tag_value.update({key: str(tupleValue[value])})
                    tag_value.update({"Environment": env})
                    logging.info(
                        f"Trying to send a metric. Name: {metric_name}, Type: {metric_type}, Value: {metric_value}, Tags: {str(tag_value)}")
                    posting_new_relic_metric = new_relic.send_metric(metric_name, metric_type, metric_value, tag_value)
                    logging.info(f"Metric post request's status: {str(posting_new_relic_metric)}")
                    # sleeping for 3 sec before posting another metric to New Relic
                    time.sleep(3)

    except Exception as e:
        print(e)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
