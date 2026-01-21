
# Identifying Query Bands for TD loads
# 1. Identifying TDSQL
#   Parse all JSON DAG with a `teradata_engine` section and extract any/all files in the `script` section
#   Adds query bands to them

# APP_ID	Analytics Insights APPID	APP07324
    # static 
# DAG_ID	dag_id	mfp_visualization_evaluation_vw_58295_ACE_ENG
    # extract from JSON
# TASK_NAME	file_name without extension	mfp_evaluation_run_date_monthly_vw
    # strip the file name without .sql

import glob
import json
import os

all_json = glob.glob('./**/*.json', recursive=True) 

sql_files = set()

for file in all_json:
    if "templates/" in file:
        continue
    f = open(file)
    current_json = json.load(f)
    # we know that we can get the list of stages - every JSON needs them
    try:
        stages = current_json['stages']
    except KeyError as e:
        continue
    
    # iterate over all stages
    for stage in stages:
        # fix bug where a stage does not correspond to a job
        curr_stage = current_json.get(stage, None)
        if not curr_stage:
            continue
        # can be multiple jobs per stage - ensure that we'll pick the spark one
        for job in curr_stage:
            # find if it's a spark_engine job
            is_td_engine = curr_stage[job].get('teradata_engine', None)
            # if it's td engine - grab all the scripts
            if is_td_engine:
                for input_dict in curr_stage[job]['teradata_engine']['input_details']:
                    scripts = input_dict['scripts']
                    for script in scripts:
                        sql_files.add(current_json['dag_id']+"_11521_ACE_ENG"+':'+script)


for string in sql_files:
     sql_file = string.split(":")[1]
     sql_path = f'./pypeline_sql/{sql_file}'
     dag_id = string.split(":")[0]
     task_name = sql_file.split("/")[-1].replace(".sql", '')
     query_band = f"""SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID={dag_id};
     Task_Name={task_name};'
     FOR SESSION VOLATILE;
     """
     query_band_footer = 'SET QUERY_BAND = NONE FOR SESSION;'
     file_read = open(sql_path).read()
     if 'SET QUERY_BAND' in file_read:
         continue
     data = open(sql_path).readlines()
     data[0] = f"{query_band}\n"+os.linesep+data[0]
     data[-1] = data[-1]+os.linesep+query_band_footer
     with open(sql_path, 'w') as file_new:
        file_new.writelines(data)