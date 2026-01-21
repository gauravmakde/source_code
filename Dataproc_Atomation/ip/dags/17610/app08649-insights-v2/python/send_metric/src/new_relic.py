import json
import logging
import time

import requests


class NewRelic:
    def __init__(self, license_key, metrics_endpoint):
        self.license_key = license_key
        self.metrics_endpoint = metrics_endpoint
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def send_metric(self, name, metric_type, value, attributes):
        current_milli_time = lambda: int(round(time.time() * 1000))

        headers = {
            "Content-Type": "application/json",
            "Api-Key": self.license_key
        }
        data = [{
            "metrics": [
                {
                    "name": name,
                    "type": metric_type,
                    "value": value,
                    "timestamp": current_milli_time(),
                    "interval.ms": 10000,
                    "attributes": attributes
                }]
        }]
        data_string = json.dumps(data)

        response = requests.post(self.metrics_endpoint, headers=headers, data=data_string)
        self.logger.info(f"{response.status_code}: {response.reason}")
        self.logger.info(response.json())

        if response.status_code == 202:
            return "succeeded"
        else:
            return "failed"
