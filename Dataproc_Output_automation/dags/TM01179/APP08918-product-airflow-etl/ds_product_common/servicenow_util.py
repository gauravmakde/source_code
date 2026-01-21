# common function modules that will be used in all the dags
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import configparser
import json
import logging
import os
import requests
from pathlib import Path

# fetch environment from OS variable
# env = os.environ['ENVIRONMENT']
env = 'nonprod'
# Read the config files
config = configparser.ConfigParser()
path = os.path.split(__file__)[0]
config.read(os.path.join(Path(path).parent, 'configs/alert_task_failures.cfg'))

# service now configs
sn_service_account_id = config.get(env, 'sn_service_account_id')
sn_connection_id = config.get(env, 'sn_connection_id')
source = config.get(env,'source')
event_class = config.get(env,'event_class')
node = config.get(env,'node')
resource = config.get(env,'resource')
metric_name = config.get(env,'metric_name')
sn_type = config.get(env,'type')
sn_group = config.get(env,'sn_group')
create_inc = config.get(env,'create_inc')
alert_notify = config.get(env,'alert_notify')


def _call_service_now_api(conn_id, data):
    """
      This function makes serviceNow api calls to create incidents based on the
      input json record
        Parameters : ServiceNow Connection id , Incident Data
        Returns    : None
    """
    request_body = data
    connection = BaseHook.get_connection(conn_id)
    try:
        response = requests.post(connection.host,
                                 auth=(connection.login, connection.password),
                                 headers={'Content-type': 'application/json'},
                                 data=json.dumps(request_body),
                                 timeout=60)
        logging.info('ServiceNow API Response code: %s', response)
        logging.info('ServiceNow API response(Json): %s', str(response.json()))
        response.raise_for_status()
    except requests.exceptions.HTTPError as error_message:
        logging.error(error_message)
        logging.error("ServiceNow incident creation failed!")

def invoke_servicenow_api(context, sn_incident_severity):
    """
          This function builds the request json to invoke servicenow api
            Parameters : dag context, incident severity
            Returns    : None
    """
    servicenow_data = {}
    srvcnow_data_addition_info = {}
    servicenow_data["source"] = source
    servicenow_data["event_class"] = event_class
    servicenow_data["node"] = node
    servicenow_data["resource"] = resource
    servicenow_data["metric_name"] = metric_name
    servicenow_data["type"] = sn_type
    servicenow_data["severity"] = sn_incident_severity
    servicenow_data["message_key"] = 'airflow-key-%s-%s-'\
                '%s' % (context['task_instance'].execution_date, context['task'].dag_id, context['task'].task_id)
    servicenow_data["description"] = 'Airflow Dag ~%s[%s] failed'\
                ' in the task ~%s' % (context['task'].dag_id, context['task_instance'].execution_date, context['task'].task_id)
    srvcnow_data_addition_info["sn_group"] = sn_group
    srvcnow_data_addition_info["create_inc"] = create_inc
    srvcnow_data_addition_info["alert_notify"] = alert_notify
    srvcnow_data_addition_info["details"] = "Airflow dag "+context['task'].dag_id +" failed"
    srvcnow_data_addition_info["user"] = sn_service_account_id
    servicenow_data["additional_info"] = srvcnow_data_addition_info

    _call_service_now_api(sn_connection_id, servicenow_data)

