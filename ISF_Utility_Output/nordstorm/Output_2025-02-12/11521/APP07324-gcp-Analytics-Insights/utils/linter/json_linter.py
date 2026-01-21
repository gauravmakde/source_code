import json
import os
import re
from data_fetcher import get_all_json


def validate_json_structure(current_json, file):
    if 'stages' not in current_json:
        raise Exception(f"ERROR: JSON does not have any `stages` defined - {file}")
    if not current_json.get('dag_id'):
        raise Exception(f"ERROR: `dag_id` is missing from JSON {file}")
    if not current_json.get('prod_email_on_failure'):
        raise Exception(f"ERROR: `prod_email_on_failure` is missing from JSON {file}")

    regex = re.compile(r"^[\w\.\/]+$")
    if regex.search(current_json.get('dag_id')) is None:
        raise Exception(
            f"ERROR: `dag_id` must only be alphanumeric with underscores - got {current_json.get('dag_id')}")


def validate_json_stages(current_json, file):
    stages = current_json['stages']
    sql_files = []
    for stage in stages:
        curr_stage = current_json.get(stage)
        if not curr_stage:
            raise Exception(f"ERROR: Stage {stage} defined in `stages` does not exist in JSON - {file}")
        for job in curr_stage:
            if 'teradata_engine' in curr_stage[job]:
                for input_dict in curr_stage[job]['teradata_engine']['input_details']:
                    scripts = input_dict['scripts']
                    for script in scripts:
                        sql_files.append(f'./pypeline_sql/{script}')
            if 'spark_engine' in curr_stage[job]:
                for script in curr_stage[job]['spark_engine']['scripts']:
                    sql_files.append(f'./pypeline_sql/{script}')
    for sql_file in sql_files:
        if not os.path.isfile(sql_file):
            raise Exception(f"ERROR: JSON {file} references SQL file that does not exist - file {sql_file} not found")


def lint_json():
    all_json = get_all_json()
    for file in all_json:
        if file.startswith("./utils/") or file.startswith("./templates/") or file.startswith("./lint_stg/"):
            continue  # Skip these files
        if file in ["./pypeline_jobs/nonprod_global.json", "./pypeline_jobs/prod_global.json"]:
            continue  # Also skip these

        with open(file, 'r') as f:
            current_json = json.load(f)

        validate_json_structure(current_json, file)
        validate_json_stages(current_json, file)

