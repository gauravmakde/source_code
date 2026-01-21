import requests
import xmltodict
import os
import time

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

BASE_URL = 'https://tableauuat.nordstrom.com/api/3.19'
SITE_CONTENT_URL = 'OnixSandbox'

USERNAME="onix_API"
PASSWORD="7nnjNNDHS62jm0ouEGd5DA==:htWCU5eXo4jMcWatZgVNJ43Cphd3ybX9"

def get_auth_token():
    url = f'{BASE_URL}/auth/signin'
    headers = {'Content-Type': 'application/json'}

    data = {
        'credentials': {
            'personalAccessTokenName': USERNAME,
            'personalAccessTokenSecret': PASSWORD,
            'site': {
                'contentUrl': SITE_CONTENT_URL
            }
        }
    }

    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    token = response_dict['tsResponse']['credentials']['@token']
    site_id = response_dict['tsResponse']['credentials']['site']['@id']
    return token, site_id

try:
    token, site_id = get_auth_token()
    print(token, site_id)
    print("Site_id: ", site_id)
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
except Exception as err:
    print(f"Other error occurred: {err}")

workbook_id = "112e60f7-5c68-4796-96c1-99d444a92141"  # Replace with your workbook ID

url = f"{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/connections"
headers = {
    "X-Tableau-Auth": token
}

response = requests.get(url, headers=headers)

print(response)
print(f"Response Text: {response.text}")
response_dict = xmltodict.parse(response.text)
connections = response_dict['tsResponse']['connections']['connection']

# Check if connections is a dictionary (indicating only one connection)
if isinstance(connections, dict):
    connections = [connections]  # Wrap it in a list to iterate over

print(connections)

def datasource_connection_Change(connection_id, new_datasource_id):

    url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/connections/{connection_id}'

    payload = {
        "connection": {
            "datasource": {
                "id": new_datasource_id
            }
        }
    }

    headers = {
        'X-Tableau-Auth': token,
        'Content-Type': 'application/json'
    }
    response = requests.put(url, json=payload, headers=headers)

    print(url)
    if response.status_code != 200:
        print(f"Error updating connection: {response.status_code}")
        print(response.text)
    else:
        print(f"Connection updated successfully: {response.status_code}")

def retry_with_different_datasource(connection_id):
    # Different data source ID to try
    new_datasource_id = "470e9c68-a176-4247-a841-c3b79e4d84e8"

    print(f"Retrying update with different data source ID: {new_datasource_id}")
    datasource_connection_Change(connection_id, new_datasource_id)

for conn in connections:
    connection_id = conn['@id']
    datasource_id = conn['datasource']['@id']
    datasource_name = conn['datasource']['@name']

    print("Previous connection")
    print("Connection is ", connection_id)
    print("Workbook is : 112e60f7-5c68-4796-96c1-99d444a92141")
    print("Datasource id: ", datasource_id,)
    print("Datasource name: ", datasource_name)

    # datasource_connection_Change(connection_id, new_datasource_id)

    retry_with_different_datasource(connection_id)
