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
    Output_Folder_Path = config['Path']['Output_Folder_Path']
    SQL_Copy_TXT = config['Path']['SQL_Copy_TXT']
    SQL_File_Directory=config['Path']['SQL_File_Directory']
    Enable_Title_To_TD=config['Utility']['Enable_Title_To_TD']
    Enable_Analysis_on_dag_csv =config['Utility']['Enable_Analysis_on_dag_csv']
    Fetch_SQL_From_DAG_Json=config['Utility']['Fetch_SQL_From_DAG_Json']
    Enable_dag_location_from_dag_id=config['Utility']['Enable_dag_location_from_dag_id']
    Enable_SQL_copy=config['Utility']['Enable_SQL_copy']
    Json_folder_name=config['folder']['Json_folder_name']
    sql_folder_name=config['folder']['sql_folder_name']


except KeyError as e:
    print(f"KeyError: {e}")

# if __name__ == "__main__":
#     print(SQL_Copy_TXT)


