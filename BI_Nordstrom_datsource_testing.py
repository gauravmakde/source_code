import requests
import json
import os
import time
import datetime
import xmltodict

# Directory setup for reports
datestr = time.strftime("%Y-%m-%d")
directory = f'reports {datestr}'
if not os.path.exists(directory):
    os.mkdir(directory)

# Constants
BASE_URL = 'https://tableauuat.nordstrom.com/api/3.19'
SITE_CONTENT_URL = 'OnixStaging'
USERNAME = "onix-reports"
PASSWORD = "VqjiSb3TSbulH+TlPXMMgg==:buK2HRfRyVoqHkwbOSuIejyRIIOS1wSN"
service_account_file = 'service account/SA_new_cf_nordstrom.json'

# Read the service account JSON key file
with open(service_account_file) as f:
    service_account_info = json.load(f)

# Time and start details
timestr = time.strftime("%Y-%m-%d-%H%M")
start_time = datetime.datetime.now()

def get_auth_token():
    url = f'{BASE_URL}/auth/signin'
    headers = {'Content-Type': 'application/xml'}

    # Construct XML payload
    xml_data = f'''
    <tsRequest>
        <credentials personalAccessTokenName="{USERNAME}" personalAccessTokenSecret="{PASSWORD}">
            <site contentUrl="{SITE_CONTENT_URL}" />
        </credentials>
    </tsRequest>
    '''

    response = requests.post(url, headers=headers, data=xml_data)
    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    token = response_dict['tsResponse']['credentials']['@token']
    site_id = response_dict['tsResponse']['credentials']['site']['@id']
    return token, site_id

def publish_data_source(token, site_id):
    publish_url = f"https://tableauuat.nordstrom.com/api/3.19/sites/{site_id}/datasources"

    headers = {
        'X-Tableau-Auth': token,
        'Content-Type': 'application/octet-stream'
    }

    # Adjust the file path and content type as needed
    with open('your_datasource.tds', 'rb') as f:
        tds_content = f.read()

    response = requests.post(publish_url, headers=headers, data=tds_content)
    response.raise_for_status()
    print("Data source published to Tableau Server")
    return response.text

# Main execution block
if __name__ == "__main__":
    try:
        token, site_id = get_auth_token()
        print("Token:", token)
        print("Site_id:", site_id)

        # Publish the data source to Tableau Server
        response_text = publish_data_source(token, site_id)
        print(response_text)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        print(f"Response content: {err.response.text}")
    except Exception as err:
        print(f"Other error occurred: {err}")

