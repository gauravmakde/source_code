# Identifying query bands for Spark->TD loads
# 1. Identify SparkSQL
#   Parse all JSON DAG with a `spark_engine` section that has `output_details`: conn_name, sql_table_reference, AND data_source_name
#   Find the referenced SQL script and INSERT the QueryBand

# APP_ID	Analytics Insights APPID	APP07324
    # static 
# DAG_ID	dag_id	mfp_visualization_evaluation_vw_58295_ACE_ENG
    # extract from JSON
# TASK_NAME	file_name without extension	mfp_evaluation_run_date_monthly_vw
    # strip the file name without .sql

import glob
import json
import os

# Parse all json

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
        curr_stage = current_json[stage]
        # can be multiple jobs per stage - ensure that we'll pick the spark one
        for job in curr_stage:
            # find if it's a spark_engine job
            is_spark_engine = curr_stage[job].get('spark_engine', None)
            # if it's spark engine - determine if it's Spark-> TD
            if is_spark_engine:
                output_details_list = curr_stage[job]['spark_engine'].get('output_details', None)
                if output_details_list:
                    for item in output_details_list:
                        is_spark_to_td = item.get('data_source_name', None)
                        if is_spark_to_td:
                            scripts = curr_stage[job]['spark_engine']['scripts']
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
     FOR SESSION VOLATILE;"""
     data = open(sql_path).readlines()
     if 'SET QUERY_BAND' in open(sql_path).read():
         continue # skip if exists
     data[0] = f"{query_band}\n"+os.linesep+data[0]
     with open(sql_path, 'w') as file_new:
        file_new.writelines(data)

#    for item in dict:
#        sql_file = dict[item]
#        task_name = sql_file.split("/")[-1].replace(".sql", '')
