import json
import logging
import pytz
from typing import List, Dict
from cron_converter import Cron
from datetime import datetime
from croniter import croniter

def convert_cron_timezone(cron_expression, from_timezone, to_timezone):
    """Converts a cron expression from one timezone to another."""

    from_tz = pytz.timezone(from_timezone)
    to_tz = pytz.timezone(to_timezone)

    # Get the next execution time in the original timezone
    next_time_from = croniter(cron_expression, datetime.now(from_tz)).get_next(datetime)

    # Convert the next execution time to the target timezone
    next_time_to = next_time_from.astimezone(to_tz)

    # Create a new cron expression based on the converted time
    new_cron_expression = f"{next_time_to.minute} {next_time_to.hour} {next_time_to.day} {next_time_to.month} {next_time_to.weekday()}"

    return new_cron_expression

def dag_schedule_checker(json_files: List[str], config: Dict[str, List[str]]) -> None:
    """
    This function checks the schedule of each DAG to ensure it does not conflict with the schedules of production DAGs
    that are scheduled to run between 4 am to 9 am PST.

    Raises:
        Exception: If a production DAG is scheduled to run between 4 am to 9 am PST.
    """
    prod_dag_exception_list = set(config['prod_exception_dag_list'])
    exclude_time_list = config['exclude_time_list']

    for file in json_files:
        if "template/" in file:
            continue
        with open(file, "r") as f:
            current_json = json.load(f)
        if current_json.get('dag_schedule'):
            # Account for putting the timezone in the cron schedule - default UTC
            tz = current_json.get("timezone", "UTC")
            cron = Cron(current_json.get('dag_schedule')).schedule(timezone_str=tz)
            if current_json['dag_id'] in prod_dag_exception_list:
                continue
            # Check if the next run time is within the excluded time range
            for i in exclude_time_list:
                # Check if the hour of the next run time is within the excluded time range
                # Convert our PST cron to the timezone of the dag
                exclude_time_cron = Cron(convert_cron_timezone(f"0 {i} * * *", "US/Pacific", tz)).schedule(timezone_str=tz)
                if cron.next().hour == exclude_time_cron.next().hour:
                    logging.error(f"{current_json.get('dag_id')} : {current_json.get('dag_schedule')}")
                    raise Exception(
                        f"Error: Dag schedule conflicting with prod critical dags between 4 am to 9 am PST. "
                        f"If your dags need to run in this interval please reach out to AE team "
                        f"for approval. Slack: #analytics-engineering"
                    )
