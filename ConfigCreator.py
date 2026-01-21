import os
import shutil
import configparser
import json
import re
import csv
import glob


def extract_full_number_from_path(file_path):
    # Use a regex to extract numbers from the file path
    match = re.search(r'(\d+)', file_path)
    if match:
        return match.group(1)
    return None
def find_dags_folders(start_path):
    dags_folders = []
    for root, dirs, files in os.walk(start_path):
        dag_path = os.path.join(root, "dags")
        if os.path.exists(dag_path):
            dags_folders.append(dag_path)
    print(dags_folders)
    return dags_folders

#Below 2 functions are use to extract sql file from pypeline jobs
def create_dag_id_script_map(file_paths):
    dag_id_script_map = {}  # Dictionary to hold dag_id as key and scripts as value
    json_files=glob.glob(file_paths+'\\'+"*.json")
    for file_path in json_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)  # Load JSON data from file
                dag_id = data.get("dag_id", None)  # Get the DAG ID
                scripts = extract_scripts(data)  # Extract scripts from the JSON data
                if dag_id:
                    dag_id_script_map[dag_id] = scripts  # Add DAG ID and scripts to the dictionary
                    # print("dag_id_script_map",dag_id_script_map)
                else:
                    print(f"No DAG ID found in file: {file_path}")
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
        else:
            print(f"File not found: {file_path}")

    return dag_id_script_map

# Recursive function to extract all script values
def extract_scripts(data):
    scripts = []  # List to hold script values
    if isinstance(data, dict):  # If the data is a dictionary
        for key, value in data.items():
            if key == "scripts":  # Check if the key is "scripts"
                scripts.extend(value)  # Add the scripts to the list
            else:
                scripts.extend(extract_scripts(value))  # Recursively check nested structures
    elif isinstance(data, list):  # If the data is a list
        for item in data:
            scripts.extend(extract_scripts(item))  # Recursively check each item
    return scripts



def extract_specific_patterns(sql_content):
    """
    Extracts specific patterns like {any_word} and <any_word> from the SQL content.

    Args:
        sql_content (str): The content of a SQL file.

    Returns:
        list: A list containing unique matched patterns from both {any_word} and <any_word>.
    """
    # Combined regex pattern for both {any_word} and <any_word>
    combined_pattern = r"\{([a-zA-Z0-9_]+)\}|\<([a-zA-Z0-9_]+)\>"
    
    # Find all matches for both patterns
    matches = re.findall(combined_pattern, sql_content)
    
    # Use a set to store matches for uniqueness
    unique_matches = set()
    
    for match in matches:
        # Add the first group (curly braces) if present
        if match[0]:
            unique_matches.add(match[0])
        # Add the second group (angle brackets) if present
        if match[1]:
            unique_matches.add(match[1])
    return list(unique_matches)  # Convert the set to a list for return

def process_dag_scripts_for_parameter_finding(dag_id_script_mapping,pipeline_sql_path):
    """
    Reads and processes scripts from the provided DAG-to-scripts mapping,
    extracting specific patterns and combining them into a list of unique values.

    Args:
        dag_id_script_mapping (dict): A dictionary where keys are `dag_id`
                                      and values are lists of script file paths.

    Returns:
        dict: A dictionary mapping each DAG ID to a list of unique patterns.
    """
    result = {}
    
    for dag_id, script_files in dag_id_script_mapping.items():
        # print(f"Processing DAG ID: {dag_id}")
        combined_patterns = set()  # Initialize a set to store unique patterns for this DAG
        
        for script_path in script_files:
            try:
                with open(pipeline_sql_path+"\\"+script_path, 'r') as file:
                    sql_content = file.read()
                    # Extract specific patterns and combine them
                    extracted_patterns = extract_specific_patterns(sql_content)
                    #
                    # print(extracted_patterns)
                    combined_patterns.update(extracted_patterns)  # Add matches to the set
                
            except FileNotFoundError:
                print(f"Script file not found: {script_path}")
            except Exception as e:
                print(f"Error reading script file {script_path}: {e}")
        
        # Store the combined unique patterns for this DAG ID
        result[dag_id] = list(combined_patterns)  # Convert the set to a list for return
    
    return result
###########################################



def extract_sql_env_vars(directory_path, target_dag_id):

    print(directory_path)
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):

            file_path = os.path.join(directory_path, filename)
            # print(file_path)
            with open(file_path, 'r') as file:
                try:
                    data = json.load(file)
                    if data.get('dag_id') == target_dag_id:
                        # print("Json found")
                        # print(data)
                        sql_env_vars = data.get('sql_env_vars')
                        if sql_env_vars:
                            return sql_env_vars
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from file: {filename}")
    return None


def extract_default_args(file_path):
    default_args_pattern = re.compile(r"default_args\s*=\s*{([^}]*)}", re.DOTALL)
    email_pattern = re.compile(r"'email'\s*:\s*\[(.*?)\]", re.DOTALL)
    email_on_failure_pattern = re.compile(r"'email_on_failure'\s*:\s*(True|False)", re.DOTALL)
    email_on_retry_pattern = re.compile(r"'email_on_retry'\s*:\s*(True|False)", re.DOTALL)
    
    dag_id = None
    email_details = {
        'email': None,
        'email_on_failure': None,
        'email_on_retry': None,
        'cron': None
    }
    full_app_id =os.path.basename(os.path.dirname(os.path.dirname(file_path)))
    app_id=full_app_id.split("-")[0]
    with open(file_path, 'r') as file:
        file_content = file.read()
        match = default_args_pattern.search(file_content)
        cron_dag = re.findall(r"cron[\s]*\=[\s]*\'(.*?)\'",file_content,flags=re.I)
        if cron_dag:
            email_details['cron'] = cron_dag[0]
        if match:
            args_content = match.group(1)
            email_match = email_pattern.search(args_content)
            if email_match:
                email_details['email'] = [email.strip() for email in email_match.group(1).strip().split(',')]
            email_on_failure_match = email_on_failure_pattern.search(args_content)
            if email_on_failure_match:
                email_details['email_on_failure'] = email_on_failure_match.group(1).strip() == 'True'
            email_on_retry_match = email_on_retry_pattern.search(args_content)
            if email_on_retry_match:
                email_details['email_on_retry'] = email_on_retry_match.group(1).strip() == 'True'
            with_dag_pattern = r"with DAG\(([^)]*)"
            dag_id_search = re.search(with_dag_pattern, file_content, re.MULTILINE | re.DOTALL)
            if dag_id_search:
                dag_block = dag_id_search.group(1)
                dag_id_pattern = r"dag_id\s*=\s*['\"](.*?)['\"]"
                dag_id_in_block = re.search(dag_id_pattern, dag_block)
                if dag_id_in_block:
                    dag_id = dag_id_in_block.group(1)
            dag_id_pattern_outside = r"dag_id\s*=\s*['\"](.*?)['\"]"
            dag_id_outside = re.search(dag_id_pattern_outside, file_content)
            if dag_id_outside:
                dag_id = dag_id_outside.group(1)
        # dag_Cron=
    return dag_id, email_details, app_id

def update_production_config(config_folder, dag_id, email_details, sql_env_vars, app_id, export_csv_file, dag_id_script_mapping, pipeline_sql_path,target_dag_id):
    config_path = os.path.join(config_folder, "env-dag-specific-production.cfg")
    config_path = config_path.replace("Input_Code", "Output_Code")
    print('config_path', config_path)
    directory = os.path.dirname(config_path)
    os.makedirs(directory, exist_ok=True)

    config_input_path = os.path.join(config_folder, "env-configs-production.cfg")
    config1 = configparser.ConfigParser()
    config1.read(config_input_path)
    if config1.has_section('environment_variables'):
        sql_config_dict = dict(config1['environment_variables'])
    else:
        sql_config_dict = {}

    config = configparser.ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)
    if not config.has_section(dag_id):
        config.add_section(dag_id)
    if email_details.get('email'):
        formatted_emails = [email.strip().replace("'", "") for email in email_details['email']]
        config[dag_id]['email'] = ', '.join(formatted_emails)
    else:
        config[dag_id]['email'] = 'None'
    config[dag_id]['email_on_failure'] = str(email_details.get('email_on_failure', ''))
    config[dag_id]['email_on_retry'] = str(email_details.get('email_on_retry', ''))
    config[dag_id]['cron'] = str(email_details.get('cron', ''))
    if not config.has_section(dag_id + "_db_param"):
        config.add_section(dag_id + "_db_param")
    config[dag_id + "_db_param"]['gcp_project_id'] = 'jwn-nap-dataplex-prod-emue'
    # with open(export_csv_file, 'r') as file:
    #     reader = csv.DictReader(file)
    #     if not config.has_section(dag_id + "_db_param"):
    #         config.add_section(dag_id + "_db_param")
    #     for row in reader:
    #         if row['AppId'] == app_id.upper():
    #             gcp_projects = row['GCP Project'].split()
    #             if gcp_projects:
    #                 for project_id in gcp_projects:
    #                     if '-prod-' in project_id:
    #                         config[dag_id + "_db_param"]['gcp_project_id'] = project_id
    #             else:
    #                 config[dag_id + "_db_param"]['gcp_project_id'] = ''

    parameter_result = process_dag_scripts_for_parameter_finding(dag_id_script_mapping, pipeline_sql_path)

    # print("parameter result value:",parameter_result)
    if parameter_result:
        for parameter_key, parameter_value in parameter_result.items():
            if target_dag_id == parameter_key:
                for param_val in parameter_value:
                    if param_val in sql_config_dict:
                        for key, value in sql_config_dict.items():
                            if key==param_val:
                                config[dag_id + "_db_param"][param_val] = str(value)
                                config.set(dag_id + "_db_param", param_val, value)

    if sql_env_vars:
        for key, value in sql_env_vars.items():
            config[dag_id + "_db_param"][key] = str(value)
            config.set(dag_id + "_db_param", key, value)

    # Ensure 'dbenv' is set to 'PRD' if not already present
    if 'dbenv' not in config[dag_id + "_db_param"]:
        config[dag_id + "_db_param"]['dbenv'] = 'PRD'
        config.set(dag_id + "_db_param", 'dbenv', 'PRD')
    if 'db_env' in config[dag_id + "_db_param"]:
        config.remove_option(dag_id + "_db_param", 'db_env')

    with open(config_path, 'w') as configfile:
        config.write(configfile)


def update_delivery_devlopment_config(config_folder, dag_id, email_details, sql_env_vars,app_id, export_csv_file,dag_id_script_mapping,pipeline_sql_path,target_dag_id):
    config_path = os.path.join(config_folder, "delivery-env-dag-specific-development.cfg")
    config_path = config_path.replace("Input_Code", "Output_Code")
    # print('config_path',config_path)
    directory = os.path.dirname(config_path)
    os.makedirs(directory, exist_ok=True)

    config_input_path = os.path.join(config_folder, "env-configs-development.cfg")
    config1 = configparser.ConfigParser()
    config1.read(config_input_path)
    if config1.has_section('environment_variables'):
        sql_config_dict = dict(config1['environment_variables'])        
    else:
        sql_config_dict = {}

    config = configparser.ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)
    if not config.has_section(dag_id):
        config.add_section(dag_id)
    # if email_details.get('email'):
    #     formatted_emails = [email.strip().replace("'", "") for email in email_details['email']]
    #     config[dag_id]['email'] = ', '.join(formatted_emails)
    # else:
    #     config[dag_id]['email'] = 'None'
    # config[dag_id]['email_on_failure'] = str(email_details.get('email_on_failure', ''))
    # config[dag_id]['email_on_retry'] = str(email_details.get('email_on_retry', ''))
    # config[dag_id]['cron'] = str(email_details.get('cron', ''))
    config[dag_id]['email'] = 'None'
    config[dag_id]['email_on_failure'] = "False"
    config[dag_id]['email_on_retry'] = "False"
    config[dag_id]['cron'] = "None"
    if not config.has_section(dag_id + "_db_param"):
        config.add_section(dag_id + "_db_param")
    config[dag_id + "_db_param"]['gcp_project_id'] = 'jwn-nap-dataplex-nonprod-lpyu'
    # with open(export_csv_file,'r') as file:
    #     reader = csv.DictReader(file)  # Read as dictionary for easy column access
    #     if not config.has_section(dag_id + "_db_param"):
    #         config.add_section(dag_id + "_db_param")
    #     for row in reader:
    #         if row['AppId'] == app_id.upper():
    #             gcp_projects = row['GCP Project'].split()
    #             if gcp_projects:
    #                 for project_id in gcp_projects:
    #                     if '-nonprod-' in project_id:
    #                         config[dag_id + "_db_param"]['gcp_project_id'] = project_id # 'jwn-nap-dataplex-prod-emue'
    #             else:
    #                 config[dag_id + "_db_param"]['gcp_project_id'] = ''


    parameter_result=process_dag_scripts_for_parameter_finding(dag_id_script_mapping,pipeline_sql_path)
    # print("\n\n\n\nparameter_values",parameter_values)
    
    if parameter_result:
        for parameter_key, parameter_value in parameter_result.items():
            if target_dag_id == parameter_key:
                for param_val in parameter_value:
                    if param_val in sql_config_dict:
                        for key, value in sql_config_dict.items():
                            if key == param_val:
                                config[dag_id + "_db_param"][param_val] = str(value)
                                config.set(dag_id + "_db_param", param_val, value)

    if sql_env_vars:
        for key, value in sql_env_vars.items():
            config[dag_id + "_db_param"][key] = str(value)
            config.set(dag_id + "_db_param",key,value)
            # print('Production_sql_env',key, value)

    if 'dbenv' not in config[dag_id + "_db_param"]:
        config[dag_id + "_db_param"]['dbenv'] = 'DEV'
        config.set(dag_id + "_db_param", 'dbenv', 'DEV')

    if 'db_env' in config[dag_id + "_db_param"]:
        config.remove_option(dag_id + "_db_param", 'db_env')


    with open(config_path, 'w') as configfile:
        config.write(configfile)



def update_development_config(config_folder, dag_id, sql_env_vars, dag_id_script_mapping, pipeline_sql_path,target_dag_id):
    config_path = os.path.join(config_folder, "env-dag-specific-development.cfg")
    config_path = config_path.replace("Input_Code", "Output_Code")
    directory = os.path.dirname(config_path)
    os.makedirs(directory, exist_ok=True)

    
    config_input_path = os.path.join(config_folder, "env-configs-development.cfg")
    config1 = configparser.ConfigParser()
    config1.read(config_input_path)
    if 'environment_variables' in config1:
        sql_config_dict = dict(config1['environment_variables'])
    else:
        sql_config_dict = {}

    config = configparser.ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)
    if not config.has_section(dag_id):
        config.add_section(dag_id)
    config[dag_id]['email'] = 'None'
    config[dag_id]['email_on_failure'] = "False"
    config[dag_id]['email_on_retry'] = "False"
    config[dag_id]['cron'] = "None"
    if not config.has_section(dag_id + "_db_param"):
        config.add_section(dag_id + "_db_param")
    config[dag_id + "_db_param"]['gcp_project_id'] = 'jwn-nap-user1-nonprod-zmnb'



    parameter_result=process_dag_scripts_for_parameter_finding(dag_id_script_mapping,pipeline_sql_path)
    # print("\n\n\n\nparameter_values",parameter_values)
    
    if parameter_result:
        for parameter_key, parameter_value in parameter_result.items():
            if target_dag_id == parameter_key:
                for param_val in parameter_value:
                    if param_val in sql_config_dict:
                        for key, value in sql_config_dict.items():
                            if key == param_val:
                                config[dag_id + "_db_param"][param_val] = str(value)
                                config.set(dag_id + "_db_param", param_val, value)

    if sql_env_vars:
        for key, value in sql_env_vars.items():
            config[dag_id + "_db_param"][key] = str(value)
            config.set(dag_id + "_db_param",key,value)
    # for key, value in sql_config_dict.items():
    #     config[dag_id + "_db_param"][key] = str(value)
    # if sql_env_vars:
    #     for key, value in sql_env_vars.items():
    #         config[dag_id + "_db_param"][key] = str(value)
    if 'dbenv' not in config[dag_id + "_db_param"]:
        config[dag_id + "_db_param"]['dbenv'] = 'DEV'
        config.set(dag_id + "_db_param", 'dbenv', 'DEV')
    if 'db_env' in config[dag_id + "_db_param"]:
        config.remove_option(dag_id + "_db_param", 'db_env')

    if 'env_suffix'  in config[dag_id + "_db_param"]:
        config[dag_id + "_db_param"]['env_suffix'] = ''
        config.set(dag_id + "_db_param", 'env_suffix', '')
    with open(config_path, 'w') as configfile:
        config.write(configfile)

def app_rename_folder():
    root_path_files = glob.glob(App_id_location,recursive=True)
    # print("$$$$$$$$$$$$")
    print(root_path_files)
    for folder_name in root_path_files:

        if '-gcp-' not in  folder_name:

            app_name=os.path.basename(folder_name)
            match = re.match(r"^(app\d+)-(.*?)$", app_name, re.IGNORECASE)
            if not match:
                print("Folder name format does not match '<app or AAP><digits>-<dynamic>'")
            prefix = match.group(1)
            suffix = match.group(2)
            prefix= prefix.upper()
            new_folder_name = f"{prefix}-gcp-{suffix}"

            new_folder_path = os.path.join(os.path.dirname(folder_name), new_folder_name)

            if os.path.exists(new_folder_path):
                print(f"The target folder '{new_folder_name}' already exists at {new_folder_path}. Skipping rename.")

            try:
                os.rename(folder_name, new_folder_path)
                print(f"Folder renamed to: {new_folder_name}")
            except FileNotFoundError:
                print("The specified folder was not found.")
            except Exception as e:
                print(f"An error occurred: {e}")

        elif 'APP' not in folder_name:
            print("It has gcp")
            app_name = os.path.basename(folder_name)
            match = re.match(r"^(app\d+)-(.*?)$", app_name, re.IGNORECASE)
            if not match:
                print("Folder name format does not match '<app or AAP><digits>-<dynamic>'")
            prefix = match.group(1)
            suffix = match.group(2)
            prefix = prefix.upper()
            new_folder_name = f"{prefix}-{suffix}"

            new_folder_path = os.path.join(os.path.dirname(folder_name), new_folder_name)

            if os.path.exists(new_folder_path):
                print(f"The target folder '{new_folder_name}' already exists at {new_folder_path}. Skipping rename.")

            try:
                os.rename(folder_name, new_folder_path)
                print(f"Folder renamed to: {new_folder_name}")
            except FileNotFoundError:
                print("The specified folder was not found.")
            except Exception as e:
                print(f"An error occurred: {e}")
# Define the paths

mail_folder_path = r'D:\Input_Code'
output_folder_path = r'D:\Output_Code'#Input_Code\17284\app06792-merch-analytics-isf\env_configs'
export_csv_file = r'D:\export.csv'
App_id_location= r'D:\Output_Code\**\APP*'
# Ensure the output folder exists
os.makedirs(output_folder_path, exist_ok=True)

dags_folders = find_dags_folders(mail_folder_path)
count = 0

for dags_folder in dags_folders:
    parent_dir = os.path.dirname(dags_folder)
    config_folder = os.path.join(parent_dir, "env_configs")
    # print(config_folder)
    pipeline_jobs_path = os.path.join(parent_dir, "pypeline_jobs")
    pipeline_sql_path = os.path.join(parent_dir, "pypeline_sql")
    if not os.path.exists(config_folder) or not os.path.exists(pipeline_jobs_path):
        print(f"Config folder not found: {config_folder} or {pipeline_jobs_path}")
        continue
    for file in os.listdir(dags_folder):
        if file.endswith('.py'):

            file_path = os.path.join(dags_folder, file)
            # print(f"Python file is {file_path}")
            dag_id, default_args, app_id = extract_default_args(file_path)
            print(dag_id, default_args, app_id)
            number_folder = extract_full_number_from_path(file_path)
            # print(number_folder)
            number_folder = '_' + str(number_folder)
            # print(number_folder)
            # print(dag_id)

            if dag_id is not None:
                target_dag_id = dag_id.split(number_folder)[0]

            sql_env_vars = extract_sql_env_vars(pipeline_jobs_path, target_dag_id)
            print(sql_env_vars)

            dag_id_script_mapping = create_dag_id_script_map(pipeline_jobs_path)
            # print(dag_id_script_mapping)
            # break
            if dag_id and default_args['email'] is not None:
                print(f"\n\nUpdating production config for {dag_id} with {default_args}")
                update_production_config(config_folder, 'gcp_' + dag_id, default_args, sql_env_vars, app_id, export_csv_file, dag_id_script_mapping, pipeline_sql_path,target_dag_id)
                update_delivery_devlopment_config(config_folder, 'gcp_' + dag_id, default_args, sql_env_vars, app_id, export_csv_file, dag_id_script_mapping, pipeline_sql_path,target_dag_id)
                update_development_config(config_folder, 'gcp_' + dag_id, sql_env_vars, dag_id_script_mapping, pipeline_sql_path,target_dag_id)
                count += 1
            # Copy the config files (production and development) to the output folder
            for config_file in ["env-dag-specific-production.cfg", "env-dag-specific-development.cfg"]:
                config_file_path = os.path.join(config_folder, config_file)
                if os.path.exists(config_file_path):
                    output_config_file_path = os.path.join(output_folder_path, config_file)
                    shutil.copy2(config_file_path, output_config_file_path)
                    print(f"\n\nCopied {config_file} to {output_folder_path}")

app_rename_folder()

