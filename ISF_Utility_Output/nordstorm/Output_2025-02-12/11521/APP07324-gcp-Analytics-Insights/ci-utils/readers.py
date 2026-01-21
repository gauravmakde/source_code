import yaml
import json
import logging

def get_data_from_source(teradata_source, table_name):
    env = 'prod'
    for name in table_name:
        if name.split(".")[0].lower() == "t2dl_das_bie_dev":
            env = 'nonprod'
            break
    with open(f"./pypeline_connection_yml/{env}/main.yml", "r") as file:
        content = yaml.safe_load(file)
        try:
            connection = content.get(teradata_source)
            vault_path = connection.get("vault_connection_name").replace("nordsecrets/", "")
            hostname = connection.get("hostname")
            port = connection.get("port")
            return vault_path, hostname, port
        except Exception as e:
            print(e)
            raise(e)

def get_gitlab_json(filename):
    try:
        with open(filename, 'r') as file_content:
            json_data = json.load(file_content)
            return json_data
    except json.decoder.JSONDecodeError as err:
        logging.error("Found errors when reading json: File Name: {0}".format(filename))
        raise err