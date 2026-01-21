import configparser
import os

# Initialize ConfigParser
config = configparser.ConfigParser()

# Get the directory of the current script (config_parser.py)

script_dir = os.path.dirname(os.path.abspath(__file__))
# Print script directory for debug purposes
# print(f"Script Directory: {script_dir}")

# Get the parent directory of the script directory (assumed to be the location of main.py)
parent_dir = os.path.dirname(script_dir)
# Print parent directory for debug purposes
# print(f"Parent Directory: {parent_dir}")

# Construct the full path to the config file
configFilePath = os.path.join(parent_dir, 'config_file.ini')


# Print the config file path for debug purposes
# print(f"Config File Path: {configFilePath}")

# Read the configuration file
config.read(configFilePath)
configFilePath = os.path.join(parent_dir, 'Teradata_Bq_mapping.ini')
config.read(configFilePath)
# print(config)

try:

    Input_Folder_Path = config['Path']['Input_Folder_Path']
    Output_Folder_Path = config['Path']['Output_Folder_Path']
    XML_file_Path = config['Path']['XML_file_Path']
    CSV_file_Path=config['Path']['CSV_file_Path']
    testing_config = config['Path']['testing_config']
    delivery_config=config['Path']['delivery_config']
    App_id_location = config['Path']['App_id_location']
    main_folder_path = config['Path']['main_folder_path']

    Enable_to_run_specific_file=config['Utility']['Enable_to_run_specific_file']

    # Fetch_SQL_From_DAG_Json=config['Utility']['Fetch_SQL_From_DAG_Json']
    # Enable_SQL_copy=config['Utility']['Enable_SQL_copy']
    json_folder=config['folder']['Json_folder_name']
    kafka_json_folder = config['folder']['kafka_json_folder']
    user_config_folder = config['folder']['user_config_folder']
    Input_directory = config['folder']['Input_directory']
    Output_directory = config['folder']['Output_directory']
    project_id = config['project_specific']['project_id']
    dataplex_project_id = config['project_specific']['dataplex_project_id']

    excluded_folders = config['List_folder']['excluded_folders']


    sql_folder_name=config['folder']['sql_folder_name']


except KeyError as e:
    print(f"KeyError: {e}")

# if __name__ == "__main__":
#     print(folder_list)


