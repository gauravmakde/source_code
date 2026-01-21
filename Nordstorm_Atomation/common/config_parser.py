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
    Sprint=config['Select_sprint']['Sprint']
    # print(Sprint)
    Input_Folder_Path = config['Path']['Input_Folder_Path']
    Output_Folder_Path = config['Path']['Output_Folder_Path']
    CSV_Input_Folder_Path=config['Path']['CSV_Input_Folder_Path']
    Base_Input_Folder = config['Path']['Base_Input_Folder']
    XML_file_Path=config['Path']['XML_file_Path']
    Output_SQL_Path=config['Path']['Output_SQL_Path']

    Enable_Primary_and_cluster_value = config['Utility']['Enable_Primary_and_cluster_value']
    Enable_ddl_replace_period_sequence=config['Utility']['Enable_ddl_replace_period_sequence']
    Enable_py_file_dag_information=config['Utility']['Enable_py_file_dag_information']
    Enable_csv_fetch_file=config['Utility']['Enable_csv_fetch_file']
    Enable_copy_of_sql=config['Utility']['Enable_copy_of_sql']
    Enable_fetch_json_details=config['Utility']['Enable_fetch_json_details']
    Enable_fetch_inf_SSH_operator=config['Utility']['Enable_fetch_inf_SSH_operator']
    Enable_fetch_inf_K8_operator=config['Utility']['Enable_fetch_inf_K8_operator']


except KeyError as e:
    print(f"KeyError: {e}")

# if __name__ == "__main__":
#     print(Enable_py_file_dag_information)


