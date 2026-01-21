import json
import shutil
import os

######### UPDATE CONFIG HERE: #########
teradata_source_suffix = "XXX" # Example - BFL, coming from TERADATA_SOURCE_BFL
t2_env_var = "XXX_t2_schema" # Example - bfl_t2_schema, should come from Prod/DEV CFG files
t2_name = "T2DL_DAS_XXX"    # Example - should be the full T2 name, T2DL_DAS_EA_BFL
#######################################

# Overwrite the JSON
with open('./pypeline_jobs/ddl_ace_tpt_control_job_log.json') as f:
    d = json.load(f)
    if not f'stage_{teradata_source_suffix}' in d['stages']:
        d['stages'].append(f'stage_{teradata_source_suffix}')

        input_details = [{'conn_name': f'TERADATA_SOURCE_{teradata_source_suffix}', 
                        'scripts' : [f'ace/tpt_control_job_log/ddl/{t2_name.lower()}.sql']}]

        d[f'stage_{teradata_source_suffix}'] = {f'job_{teradata_source_suffix}': {'teradata_engine' : {'input_details': input_details} }}

        # dump and overwrite the json
        json_out = open("./pypeline_jobs/ddl_ace_tpt_control_job_log.json", "w")
        json_out.write(json.dumps(d, indent=4))

        # Create and clone the SQL file
        filename = f'./pypeline_sql/ace/tpt_control_job_log/ddl/{t2_name.lower()}.sql'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        shutil.copy('./utils/control_table_job_log.sql', filename)

        # replace the REPLACE_ME
        with open(filename, 'r') as file :
            filedata = file.read()
            filedata = filedata.replace('REPLACE_ME', t2_env_var)

        # Now re-write the file out again
        with open(filename, 'w') as file:
            file.write(filedata)

#### TPT YML Updates
nonprod_dest = './pypeline_connection_yml/nonprod/main.yml'
prod_dest = './pypeline_connection_yml/prod/main.yml'

### NonProd
lines = open(nonprod_dest).readlines()

export_nonprod=f"""
TPT_EXPORT_{teradata_source_suffix}:
  hostname: "tdnapprod.nordstrom.net"
  source_type: "TPT"
  user_name: "T2DL_NAP_{teradata_source_suffix}_BATCH"
  tdwallet_password: "tdwallet(T2DL_NAP_{teradata_source_suffix}_BATCH_PWD)"
  database_name: "{t2_name}"
  control_db_name: "{t2_name}"

"""

for (num, line) in enumerate(lines):
    if '*id001' in line:
        target = num

if not f'TPT_{teradata_source_suffix}: *id001' in lines[target]:
    lines[target] = lines[target]+f'TPT_{teradata_source_suffix}: *id001\n'

    # Add TPT Export
    lines[target+1] = export_nonprod

    with open(nonprod_dest, 'w') as file:
        file.writelines(lines)

### Prod
lines = open(prod_dest).readlines() 

tpt_prod=f"""
TPT_{teradata_source_suffix}:
  hostname: "tdnapprod.nordstrom.net"
  source_type: "TPT"
  user_name: "T2DL_NAP_{teradata_source_suffix}_BATCH"
  tdwallet_password: "tdwallet(T2DL_NAP_{teradata_source_suffix}_BATCH_PWD)"
  database_name: "{t2_name}"
  control_db_name: "{t2_name}"
  iam_role: "TD-UTILITIES-EC2"

"""

export_prod=f"""
TPT_EXPORT_{teradata_source_suffix}:
  hostname: "tdnapprod.nordstrom.net"
  source_type: "TPT"
  user_name: "T2DL_NAP_{teradata_source_suffix}_BATCH"
  tdwallet_password: "tdwallet(T2DL_NAP_{teradata_source_suffix}_BATCH_PWD)"
  database_name: "{t2_name}"
  control_db_name: "{t2_name}"
  iam_role: "TD-UTILITIES-EC2"

"""

for (num, line) in enumerate(lines):
    if 'EXPORT' in line:
        target = num
        break

if not t2_name in lines[target-3]:
    lines[target-1] = tpt_prod+export_prod
    with open(prod_dest, 'w') as file:
        file.writelines(lines)

# Execute the Query Band Script
with open("./utils/add_queryband.py") as f:
    exec(f.read())