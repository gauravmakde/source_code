###############################################################################################################
# Code to create specialized Spark -> TD connections that remain on the ISF Team's Vault for Vault localization
###############################################################################################################

import glob
import json
import os
import yaml

all_json = glob.glob('./**/*.json', recursive=True) 

conn_names = set()
td_var_names = set()

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
                        if is_spark_to_td: # we know it is Spark -> TD - change the conn name
                            conn = curr_stage[job]['spark_engine']['output_details'][0]['conn_name']
                            if "SPARK_TD" in conn:
                                continue
                            conn_names.add(conn)
                            td_var_names.add(conn.split("_")[2].lower())
                            curr_stage[job]['spark_engine']['output_details'][0]['conn_name'] = conn+"_SPARK_TD"
                            with open(file, 'w') as out:
                                json.dump(current_json, out, indent=4)

# Get to both YML's and add in new conns
nonprod_dest = './pypeline_connection_yml/nonprod/main.yml'
prod_dest = './pypeline_connection_yml/prod/main.yml'

def add_to_yml(dest, env):
    with open(dest, 'r') as file:
        yml_out = yaml.safe_load(file) 

    td_conn_list = {k: v for k, v in yml_out.items() if k.startswith('teradata_connection_')}
    source_list = {k: v for k, v in yml_out.items() if k.startswith('TERADATA_SOURCE')}

    for td_var in td_var_names:
        data = open(dest).readlines()
        for (num, line) in enumerate(data):
            if 'teradata_connection: &teradata_variable' in line and not f'teradata_connection_{td_var}_spark_td' in td_conn_list:
                if td_var == 'bie':
                    lines = f"""
teradata_connection_{td_var}_spark_td: &teradata_variable_{td_var}_spark_td
  source_type: "TERADATA"
  # AE is using Teradata prod host for dev jobs
  hostname: "tdnapprod.nordstrom.net"
  port: "1025"
  vault_connection_name: "nordsecrets/application/APP03676/11521/ace/ae-isf-central/{env}/teradata/teradta_db_credentials"
"""
                else:
                    lines = f"""
teradata_connection_{td_var}_spark_td: &teradata_variable_{td_var}_spark_td
  source_type: "TERADATA"
  # AE is using Teradata prod host for dev jobs
  hostname: "tdnapprod.nordstrom.net"
  port: "1025"
  vault_connection_name: "nordsecrets/application/APP03676/11521/ace/ae-isf-central/{env}/{td_var}/teradata/teradta_db_credentials"
"""
                # jump five lines
                data[num+5] = data[num+5]+lines
                with open(dest, 'w') as file:
                    file.writelines(data)
    
    for conn in conn_names:
        data = open(dest).readlines()
        for (num, line) in enumerate(data):
            if 'TERADATA_SOURCE:' in line and not f'TERADATA_SOURCE_{conn}_SPARK_TD' in source_list:
                lines = f"""
{conn}_SPARK_TD:
  <<: *teradata_variable_{conn.split("_")[2].lower()}_spark_td
  database_name: "{source_list[conn]['database_name']}"
"""
                # jump two lines
                data[num+2] = data[num+2]+lines
                with open(dest, 'w') as file:
                    file.writelines(data)


add_to_yml(nonprod_dest, 'dev')
add_to_yml(prod_dest, 'prod')