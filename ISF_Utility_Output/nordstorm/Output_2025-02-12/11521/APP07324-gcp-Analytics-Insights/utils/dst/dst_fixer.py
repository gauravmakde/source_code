# Load all JSON in pypeline_jobs

import glob
import json
import os
import time
from cron_converter import Cron
from datetime import timedelta
import pytz
from cron_converter import Cron
from datetime import datetime
from croniter import croniter


def get_all_json() -> list:
    return glob.glob('./pypeline_jobs/**/*.json', recursive=True)

def load_linter_exception_yml(yml_file="./utils/linter/config.yaml") -> dict:
    # load the YAML and return the prod_exception_dag_list from the YAML file
    import yaml
    with open(yml_file, 'r') as f:
        res = yaml.safe_load(f)
        return res.get('prod_exception_dag_list', [])
    
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
    

def calculate_new_dag_cron(dag_schedule, timezone):
    # Checks the hour of the dag_schedule and returns a new cron expression forward an hour
    cron = Cron(dag_schedule).schedule(timezone_str=timezone)
    exclude_time_cron = Cron(convert_cron_timezone(f"0 17 * * *", "UTC", timezone)).schedule(timezone_str=timezone)
    next_run = cron.next().hour
    exclude_run = exclude_time_cron.next().hour
    if next_run == exclude_run:
        # If the next run time is within the excluded time range, add an hour to the cron
        dag_schedule_split = dag_schedule.split(" ")
        dag_schedule_split[1] = str((int(dag_schedule_split[1]) + 1) % 24)
        dag_schedule_new = " ".join(dag_schedule_split)
        return str(dag_schedule_new)
    return dag_schedule # Return the original expression if no changes were made


all_json = get_all_json()
exception_list = load_linter_exception_yml()    

for job in all_json:
    current_json = json.load(open(job))
    dag_schedule = current_json.get('dag_schedule', None)
    dag_id = current_json.get('dag_id', None)
    if dag_id and dag_schedule:
        if dag_id not in exception_list:
            # Perform validation logic here
            new_dag_cron = calculate_new_dag_cron(dag_schedule, current_json.get("timezone", "UTC"))
            print(new_dag_cron, dag_schedule)
            if dag_schedule == new_dag_cron:
                print(f"{dag_id} cron is identical - skipping.")
            else: # If the cron has changed, update the JSON
                lines = open(job).readlines()
                for (num, line) in enumerate(lines):
                    if 'dag_schedule' in line:
                        lines[num] = f"""    "dag_schedule": "{new_dag_cron}",\n"""
                with open(job, 'w') as file:
                    file.writelines(lines)
                    with open("./utils/dst/dst_changed_dags.txt", "a") as listFile:
                        # Write append-only changes to the `dst_changed_dags.txt` file
                        listFile.write(f'{dag_id} cron updated to {new_dag_cron}.\n')
                print(f"{dag_id} cron updated to {new_dag_cron}.")
        else:
            print(f"{dag_id} is in the exception list, skipping validation.")