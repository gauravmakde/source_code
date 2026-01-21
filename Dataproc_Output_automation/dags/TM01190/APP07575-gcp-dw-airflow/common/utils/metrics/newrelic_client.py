import json
import logging
import time

import requests
from airflow.hooks.base import BaseHook


class NewRelic:
    def __init__(self, newrelic_connection):
        connection = BaseHook.get_connection(newrelic_connection)
        self.api_key = connection.password
        self.metrics_endpoint = connection.host

    def __get_current_timestamp_milli(self):
        return int(round(time.time() * 1000))

    def send_metric(self, name, value, attributes):
        headers = {"Content-Type": "application/json", "Api-Key": self.api_key}
        data = [
            {
                "metrics": [
                    {
                        "name": name,
                        "type": "gauge",
                        "value": value,
                        "timestamp": self.__get_current_timestamp_milli(),
                        "attributes": attributes,
                    }
                ]
            }
        ]
        data_string = json.dumps(data)

        response = requests.post(
            self.metrics_endpoint, headers=headers, data=data_string
        )
        logging.info(f"{response.status_code}: {response.reason}")
        logging.info(response.json())

        if response.status_code == 202:
            return "succeeded"
        else:
            return "failed"
