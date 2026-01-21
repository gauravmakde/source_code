"""
Class to authenticate the vault client and fetch secrets
"""

import hvac


class Vault:
    def __init__(self, host, role, secret):
        self.client = hvac.Client(url=host, verify=False)
        self.login(role, secret)

    def login(self, role, secret):
        try:
            self.client.auth_approle(role, secret)
        except Exception as _e:
            raise Exception(f"Failed to login. {_e}") from _e

    def fetch_secrets(self, path, mount_point):
        try:
            return self.client.secrets.kv.v1.read_secret(
                path=path,
                mount_point=mount_point
            )
        except Exception as _e:
            raise Exception(f"Failed to fetch a secret. {_e}") from _e
