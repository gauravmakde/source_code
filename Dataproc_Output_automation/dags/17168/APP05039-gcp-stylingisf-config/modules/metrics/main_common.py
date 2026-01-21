import argparse
import logging
import os
import sys
import time
import yaml

current_path = os.path.split(__file__)[0]
sys.path.append(current_path)
from newrelic_client import NewRelic
#from teradata import Teradata
# from util import read_yaml
# from vault_client import Vault

#def get_default_vault_host():
#    return f"https://{os.environ.get('VAULT_PATH')}:{os.environ.get('VAULT_PORT')}"


def __read_yaml(yaml_file_path):
    try:
        with open(yaml_file_path, 'r') as file:
            yaml_input = yaml.safe_load(file)
        return yaml_input
    except FileNotFoundError:
        logging.error(f'ERROR: The metric audit file could not be found by this path: {yaml_file_path}')
        sys.exit(1)
    except yaml.parser.ParserError as e:
        print('ERROR: Check the YAML formatting!')
        print(e)
        sys.exit(1)
    except yaml.scanner.ScannerError as e:
        print('ERROR: Check the YAML formatting!')
        print(e)
        sys.exit(1)


def main(newrelic_connection, hook , bigquery_environment, metrics_yaml_file, isf_dag_name, tpt_job_name, subject_area_name, ldg_table_name):
    #parser = argparse.ArgumentParser(description="Send metrics module")
    #parser.add_argument("--vault_mount_point", type=str, help="Vault mount point", default="nordsecrets")
    #parser.add_argument("--metrics_yaml_file", type=str, help="Input YAML file path", required=True)
    #parser.add_argument("--nr_metrics_endpoint", type=str, help="New Relic metric endpoint", required=True)
    #parser.add_argument("--default_database", type=str, help="Default database for Teradata. E.g. {DBENV}_NAP_STG",
    #                    required=True)
    #parser.add_argument("--preproduction", type=bool, help="Is it for preprod? True/False", default=False)
    #parser.add_argument("--vault_host", type=str, help="Vault host", default=get_default_vault_host())
    #parser.add_argument("--isf_dag_name", type=str, help="DAG name", required=True)
    #parser.add_argument("--tpt_job_name", type=str, help="TPT job", required=True)
    #parser.add_argument("--subject_area_name", type=str, help="Subject area ELT control", required=True)
    #parser.add_argument("--ldg_table_name", type=str, help="Landing table", required=True)
    #args = parser.parse_args()

    #if args.vault_host is None:
    #    raise ValueError(
    #        "Env variable VAULT_HOST is not set.")

    # if os.environ.get('VAULT_ROLE_ID') is None or os.environ.get('VAULT_SECRET') is None:
    #     raise ValueError(
    #         "Env variables VAULT_ROLE_ID or VAULT_SECRET is not set.")
    # role_id = os.environ.get('VAULT_ROLE_ID')
    # secret = os.environ.get('VAULT_SECRET')
    # logger.info("Role ID: ", role_id)

    env = os.environ.get('ENV')
    #td_env = os.environ.get('TD_ENV')
    #td_host = os.environ.get('TD_HOST')
    #td_vault_path = os.environ.get('TD_NONJWN_VAULT_PATH')
    #nr_vault_path = os.environ.get('NR_VAULT_PATH')

    #if env == 'production' and args.preproduction:
    #    # only prod template should contain these variables
    #    env = os.environ.get('ENV_PREPROD')  # should be 'preproduction'
    #    td_env = os.environ.get('TD_ENV_PREPROD')  # should be 'PREPROD'

    #default_database = args.default_database.format(DBENV=td_env)

    try:

        # new_relic = NewRelic(newrelic_connection)
        logging.info(f"Opening the metrics file: {metrics_yaml_file}")
        yaml_file = __read_yaml(metrics_yaml_file)

        for metric_element in yaml_file:
            logging.info(f"Yaml object: {str(metric_element)}")
            metric_name = metric_element['metric_name']
            metric_value_position = metric_element['metric_value_position']
            sql_template = metric_element['sql']
            sql = sql_template.format(DBENV=bigquery_environment,TPT_JOB_NAME=tpt_job_name,SUBJECT_AREA=subject_area_name,ISF_DAG_NAME=isf_dag_name,LDG_TABLE_NAME=ldg_table_name)
            logging.info(f"Sql query: {sql}")

            # Getting data from Teradata
            #query_result = teradata.execute_query(sql)
            query_result = hook.get_records(sql)
            logging.info(f"Query result: {str(query_result)}")

            # Posting data to New Relic
            if len(query_result) == 0:
                logging.info(f"Sql query: {sql}")
                logging.info("There are no records to post to NewRelic 2")
            else:
                for tupleValue in query_result:
                    metric_value = tupleValue[metric_value_position]
                    tag_value = {}
                    for element in metric_element['tags']:
                        for key, value in element.items():
                            tag_value.update({key: str(tupleValue[value])})
                    tag_value.update({"Environment": env})
                    logging.info(
                        f"Trying to send a metric. Name: {metric_name}, Value: {metric_value}, Tags: {str(tag_value)}")
                    # posting_new_relic_metric = new_relic.send_metric(metric_name, metric_value, tag_value)
                    # logging.info(f"Metric post request's status: {str(posting_new_relic_metric)}")
                    # sleeping for 3 sec before posting another metric to New Relic
                    time.sleep(3)

    except Exception as e:
        print(e)
        sys.exit(1)

    sys.exit(0)


#if __name__ == '__main__':
#    logger = logging.getLogger()
#    logger.setLevel(logging.DEBUG)
#    main(newrelic_connection, hook , bigquery_environment, metric_yaml_file, default_database, isf_dag_name, tpt_job_name, subject_area_name, ldg_table_name)
