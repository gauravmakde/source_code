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

    statics_file_location = config['Path']['statics_file_location']
    temp_table_file_location = config['Path']['temp_table_file_location']
    metadata_regex = config['Regex']['metadata_regex']
    regex_input_file_metadata=config['Regex']['regex_input_file_metadata']

except KeyError as e:
    print(f"KeyError: {e}")



