import configparser
import os

config = configparser.ConfigParser()
execPath = os.getcwd()
config.read(os.path.join(execPath, 'config.ini'))

inputFolderPath=config['DEFAULT']['InputFolderPath']
Raw_InputFolderPath=config['DEFAULT']['Raw_InputFolderPath']

## Nordstrom  Start --------------------------------------------
Enable_Nordstrom=(config['Nordstrom']['Enable_Nordstrom'])
Delivered_patch=(config['Nordstrom']['Delivered_patch'])
Black_format_dag=(config['Nordstrom']['Black_format_dag'])
Config_changes=(config['Nordstrom']['Config_changes'])
Creating_all_config=(config['Nordstrom']['Creating_all_config'])
Json_changes=(config['Nordstrom']['Json_changes'])
Sql_file_changes=(config['Nordstrom']['Sql_file_changes'])

## Nordstrom  End ----------------------------------------------
