import argparse
import logging
import os
import sys
import time
import yaml

current_path = os.path.split(__file__)[0]
sys.path.append(current_path)
from newrelic_client import NewRelic


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


def main(newrelic_connection, hook , bigquery_environment, metrics_yaml_file, isf_dag_name='', tpt_job_name='', subject_area_name='', ldg_table_name=''):

    env = os.environ.get('ENVIRONMENT')

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
        logging.error(e)
        sys.exit(1)