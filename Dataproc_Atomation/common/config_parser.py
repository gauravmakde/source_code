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
# print(config)

try:

    Input_Folder_Path = config['Path']['Input_Folder_Path']
    Single_input_Path = config['Path']['Single_input_Path']
    # Output_Folder_Path = config['Path']['Output_Folder_Path']
    testing_sql_directory =config['Path']['testing_sql_directory']
    testing_json_directory=config['Path']['testing_json_directory']
    # XML_file_Path = config['Path']['XML_file_Path']
    # CSV_file_Path=config['Path']['CSV_file_Path']
    # testing_config = config['Path']['testing_config']
    # delivery_config=config['Path']['delivery_config']
    # App_id_location = config['Path']['App_id_location']

    Enable_to_run_specific_file=config['Utility']['Enable_to_run_specific_file']
    Fetch_metadata_newrelic_yaml=config['Utility']['Fetch_metadata_newrelic_yaml']
    os_type=config['Utility']['os_type']


    # Fetch_SQL_From_DAG_Json=config['Utility']['Fetch_SQL_From_DAG_Json']
    # Enable_SQL_copy=config['Utility']['Enable_SQL_copy']
    json_folder=config['folder']['Json_folder_name']
    kafka_json_folder = config['folder']['kafka_json_folder']
    user_config_folder = config['folder']['user_config_folder']
    Input_directory = config['folder']['Input_directory']
    Output_directory = config['folder']['Output_directory']
    csv_file_name= config['folder']['csv_file_name']
    new_relic=config['folder']['new_relic']
    sql_folder_name=config['folder']['sql_folder_name']


except KeyError as e:
    print(f"KeyError: {e}")

# if __name__ == "__main__":
#     print(sql_folder_name)


