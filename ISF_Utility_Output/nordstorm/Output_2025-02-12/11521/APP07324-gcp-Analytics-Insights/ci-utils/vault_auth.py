import os
import hvac
import json

class VaultClient:  
    def __init__(self, vault_url, vault_token, vault_path):  
        self.vault_url = vault_url  
        self.vault_token = vault_token  
        self.vault_path = vault_path  
        self.client = hvac.Client(url=self.vault_url, verify=False, token=self.vault_token)  
          
        if not self.client.is_authenticated():  
            raise Exception("Vault authentication failed.")  
      
    def get_sa_credentials(self):  
        """  
        Retrieve TD SA credentials from Vault.  
        """  
        read_response = self.client.secrets.kv.read_secret(
                path=self.vault_path,
                mount_point="nordsecrets"
            )
        td_creds = read_response['data']['data']  
        username = td_creds.get('user_name')  
        password = td_creds.get('password')  
        return username, password 