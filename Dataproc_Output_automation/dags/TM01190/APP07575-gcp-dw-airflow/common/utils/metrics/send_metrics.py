import logging
import os
import sys
import time

import yaml
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

current_path = os.path.split(__file__)[0]
sys.path.append(current_path)

from newrelic_client import NewRelic


def __read_yaml(yaml_file_path):
    try:
        with open(yaml_file_path, "r") as file:
            yaml_input = yaml.safe_load(file)
        return yaml_input
    except FileNotFoundError:
        logging.error(
            f"ERROR: The metric audit file could not be found by this path: {yaml_file_path}"
        )
        sys.exit(1)
    except yaml.parser.ParserError as e:
        print("ERROR: Check the YAML formatting!")
        print(e)
        sys.exit(1)
    except yaml.scanner.ScannerError as e:
        print("ERROR: Check the YAML formatting!")
        print(e)
        sys.exit(1)


def __get_environment_tag(bigquery_environment):
    if bigquery_environment == "PRD":
        return "production"
    elif bigquery_environment == "PREPROD":
        return "preproduction"
    elif bigquery_environment == "DEV":
        return "development"


def send_newrelic_metrics(
    newrelic_connection, bq_conn_id, region, bigquery_environment, metric_file_path
):
    # newrelic = NewRelic(newrelic_connection)
    hook = BigQueryHook(gcp_conn_id=bq_conn_id, location=region, use_legacy_sql=False)

    environment_tag = __get_environment_tag(bigquery_environment)
    metric_definitions = __read_yaml(metric_file_path)

    for metric in metric_definitions:
        logging.info(f"Yaml object: {str(metric)}")
        metric_name = metric["metricName"]
        metric_value_position = metric["metricValuePosition"]
        sql_template = metric["sql"]
        sql = sql_template.format(DBENV=bigquery_environment)
        logging.info(f"Sql query: {sql}")

        # Getting data from Bigquery
        query_result = hook.get_records(sql)
        logging.info(f"Query result: {str(query_result)}")

        # Posting data to New Relic
        if len(list(query_result)) == 0:
            logging.info("There are no records to post to NewRelic")
        else:
            for tupleValue in query_result:
                metric_value = tupleValue[metric_value_position]
                tag_value = {}
                for tag in metric["tags"]:
                    for key, value in tag.items():
                        tag_value.update({key: str(tupleValue[value])})
                tag_value.update({"Environment": environment_tag})
                logging.info(
                    f"Trying to send a metric. Name: {metric_name}, Value: {metric_value}, Tags: {str(tag_value)}"
                )
                # posting_new_relic_metric = newrelic.send_metric(
                #     metric_name, metric_value, tag_value
                # )
                # logging.info(
                #     f"Metric post request's status: {str(posting_new_relic_metric)}"
                # )
                # sleeping for 3 sec before posting another metric to New Relic
                time.sleep(3)
