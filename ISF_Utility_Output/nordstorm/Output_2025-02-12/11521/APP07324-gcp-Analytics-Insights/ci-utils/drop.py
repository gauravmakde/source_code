from readers import get_gitlab_json, get_data_from_source
from td_conn import TeradataConnection
from vault_auth import VaultClient
import logging
import os 
import json

logging.basicConfig(level='INFO')

# Get Environment variables and YAML information
vault_url = os.environ.get("VAULT_HOST")
vault_token = os.environ.get("VAULT_TOKEN")
td_json = get_gitlab_json(os.environ.get("DROP_TD_TABLE"))
tables_to_drop = td_json["table_name"]
vault_path, hostname, port = get_data_from_source(td_json["source"], td_json["table_name"])


if __name__ == "__main__":
    # Connect to Vault
    vault_client = VaultClient(vault_url, vault_token, vault_path)  
    # Retrieve TD credentials
    username, password = vault_client.get_sa_credentials()  
    # Establish TD object
    td_conn = TeradataConnection(hostname, username, password, port)  

    # Drop the tables
    td_conn.connect()
    for table in tables_to_drop:        
        # Run queries
        logging.info(f"Attempting to drop table {table}")
        td_conn.drop_query(table)
    
    # Finish and disconnect
    logging.info(f"Successfully dropped table(s): {tables_to_drop}")
    td_conn.disconnect()