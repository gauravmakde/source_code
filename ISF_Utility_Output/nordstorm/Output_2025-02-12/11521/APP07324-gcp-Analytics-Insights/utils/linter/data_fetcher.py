import glob
import json
from typing import List, Set


def get_all_json() -> List[str]:
    """
    Retrieve all JSON files from the 'pypeline_jobs' directory and its subdirectories.

    Returns:
        List[str]: A list of file paths for each JSON file found.
    """
    return glob.glob('./pypeline_jobs/**/*.json', recursive=True)


def get_all_sql() -> List[str]:
    """
    Retrieve all SQL files from the 'pypeline_sql' directory and its subdirectories.

    Returns:
        List[str]: A list of file paths for each SQL file found.
    """
    return glob.glob('./pypeline_sql/**/*.sql', recursive=True)


def get_all_td_sql() -> Set[str]:
    """
    Retrieve all teradata SQL files its subdirectories.

    Returns:
        Set[str]: A set of file paths for each SQL script associated with a job that uses the Teradata engine.
    """
    all_json = get_all_json()
    sql_files = set()
    for file in all_json:
        if "templates/" in file:
            continue
        with open(file, 'r') as f:
            current_json = json.load(f)
        try:
            stages = current_json['stages']
        except KeyError:
            continue
        for stage in stages:
            curr_stage = current_json.get(stage)
            if not curr_stage:
                continue
            for job in curr_stage:
                is_teradata_engine = curr_stage[job].get('teradata_engine')
                if is_teradata_engine:
                    for input_dict in curr_stage[job]['teradata_engine']['input_details']:
                        scripts = input_dict['scripts']
                        for script in scripts:
                            sql_files.add(f'./pypeline_sql/{script}')
    return sql_files


def get_all_spark_sql() -> Set[str]:
    """
    Retrieve all SQL files associated with Spark engine.

    Returns:
        Set[str]: A set of file paths for each SQL script associated with a job that uses the Spark engine.
    """
    all_json = get_all_json()
    sql_files = set()
    for file in all_json:
        if "templates/" in file:
            continue
        f = open(file)
        current_json = json.load(f)
        # we know that we can get the list of stages - every JSON needs them
        try:
            stages = current_json['stages']
        except KeyError:
            continue

        # iterate over all stages
        for stage in stages:
            # fix bug where a stage does not correspond to a job
            curr_stage = current_json.get(stage, None)
            if not curr_stage:
                continue
            # can be multiple jobs per stage - ensure that we'll pick the Spark one
            for job in curr_stage:
                # find if it's a Spark job
                is_spark_engine = curr_stage[job].get('spark_engine', None)
                # if it's Spark engine - grab all the scripts
                if is_spark_engine:
                    for script in curr_stage[job]['spark_engine']['scripts']:
                        sql_files.add(f'./pypeline_sql/{script}')
    return sql_files


def get_all_spark_teradata_json() -> Set[str]:
    """
    Retrieve all JSON files that contain Teradata and Spark jobs.

    Returns:
        Set[str]: A set of file paths for each JSON file that contains Teradata and Spark jobs.
    """
    all_json = get_all_json()
    spark_teradata_json = set()
    for file in all_json:
        if "templates/" in file:
            continue
        with open(file, 'r') as f:
            current_json = json.load(f)
        try:
            stages = current_json['stages']
        except KeyError:
            continue
        for stage in stages:
            curr_stage = current_json.get(stage)
            if not curr_stage:
                continue
            for job in curr_stage:
                is_teradata_engine = curr_stage[job].get('teradata_engine')
                is_spark_engine = curr_stage[job].get('spark_engine')
                if is_teradata_engine or is_spark_engine:
                    spark_teradata_json.add(file)
    return spark_teradata_json
