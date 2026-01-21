import requests
import pandas as pd
import xmltodict

import os
import time
import datetime

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)


BASE_URL = 'https://tableauuat.nordstrom.com/api/3.19'
SITE_CONTENT_URL = 'AS'

debug_enable = False

USERNAME="onix_API"
PASSWORD="7nnjNNDHS62jm0ouEGd5DA==:htWCU5eXo4jMcWatZgVNJ43Cphd3ybX9"


timestr = time.strftime("%Y-%m-%d-%H%M")

start_time = datetime.datetime.now()
# def multithreading ():
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
    print("Site_id: ",site_id)
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
except Exception as err:
    print(f"Other error occurred: {err}")

def list_workbooks():
    url = f'{BASE_URL}/sites/{site_id}/workbooks'
    print(url)
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(url, headers=headers)

    # Debugging prints
    # print(f"Status Code: {response.status_code}")
    # print(f"Response Text: {response.text}")

    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    workbooks = response_dict['tsResponse']['workbooks']['workbook']
    if isinstance(workbooks, dict):
        workbooks = [workbooks]
    return workbooks

try:
    workbooks = list_workbooks()

    print("Workbook are : ",workbooks)
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
except Exception as err:
    print(f"Other error occurred: {err}")

def get_workbook_details(workbook_id):
    url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}'
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(url, headers=headers)

    # Debugging prints
    # print(f"Status Code: {response.status_code}")
    # print(f"Response Text: {response.text}")

    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    return response_dict['tsResponse']

    # Debugging prints
    # print(f"Status Code: {response.status_code}")
    # print(f"Response Text: {response.text}")

    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    return response_dict['tsResponse']

def get_views(workbook_id):
    url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/views'
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(url, headers=headers)

    # Debugging prints
    # print(f"Status Code: {response.status_code}")
    # print(f"Response Text: {response.text}")

    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    views = response_dict['tsResponse']['views']['view']
    if isinstance(views, dict):
        views = [views]
    return views

def get_connections(workbook_id):

    url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/connections'
    print("connection for the site: " ,url)
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(url, headers=headers)

    # Debugging prints
    # print(f"Status Code: {response.status_code}")
    # print(f"Response Text: {response.text}")

    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    connections = response_dict['tsResponse']['connections']['connection']
    if isinstance(connections, dict):
        connections = [connections]
    return connections

def get_datasource_details(datasource_id):
    url = f'{BASE_URL}/sites/{site_id}/datasources/{datasource_id}'
    headers = {
        'X-Tableau-Auth': token
    }
    response = requests.get(url, headers=headers)

    # Debugging prints
    print(f"Status Code: {response.status_code}")
    print(f"Response Text: {response.text}")

    response.raise_for_status()

    # Parse XML response
    response_dict = xmltodict.parse(response.text)
    return response_dict['tsResponse']['datasource']

data = []
#
print(workbooks)
# exit()
for workbook in workbooks:

    workbook_id = workbook['@id']
    workbook_name = workbook['@name']
    workbook_web_URL = workbook['@webpageUrl']
    print(workbook_web_URL)
    workbook_details = get_workbook_details(workbook_id)
    workbook_owner = workbook_details['workbook']['owner']['@name']
    views = get_views(workbook_id)
    connections = get_connections(workbook_id)

    for view in views:
        view_id = view['@id']
        view_name = view['@name']

        print(f"Workflow name: {workbook_name}, View name is: {view_name}")

        for conn in connections:
            datasource_id = conn['datasource']['@id']
            datasource_name = conn['datasource']['@name']

            # Debugging: Print connection details
            print(f"Connection details: {conn}")

            custom_sql_query = conn.get('customSql', {}).get('query', None)
            serverAddress = conn['@serverAddress']
            userName = conn['@userName']

            print(serverAddress,userName)

            if custom_sql_query:
                print(f"Custom SQL found: {custom_sql_query}")
            else:
                print("No Custom SQL found for this connection.")

            try:
                datasource_details = get_datasource_details(datasource_id)
                datasource_type = datasource_details['@type']
                datasource_last_updated = datasource_details['@updatedAt']
            except Exception as e:
                print(f"Error fetching datasource details: {e}")
                datasource_type = None
                datasource_last_updated = None

            data.append({
                'Workbook ID': workbook_id,
                'Workbook Name': workbook_name,
                'Workbook Website': workbook_web_URL,
                'View ID': view_id,
                'View Name': view_name,
                'SQL Query': custom_sql_query,
                'Owner': workbook_owner,
                'Datasource ID': datasource_id,
                'Datasource Name': datasource_name,
                'Datasource Type': datasource_type,
                'Datasource Last Updated': datasource_last_updated,
                'serverAddress':serverAddress,
                'userName':userName

            })

print(data)  # Debugging: print the collected data

df = pd.DataFrame(data)
df.to_csv(directory+'\\tableau_queries_'+ timestr + '.csv', index=False)

end_str = datetime.datetime.now()

print("Total time took :", end_str-start_time)

print(df.head())  # Debugging: print the first few rows of the dataframe

