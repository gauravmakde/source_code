import requests
import pandas as pd
import xmltodict
import xml.etree.ElementTree as ET
import time
import threading
import datetime
from google.cloud import bigquery
from google.cloud import storage
import io

# Initialize BigQuery client
client = bigquery.Client()

# Define BigQuery table details
project_id = 'cf-nordstrom'
dataset_id = 'BI_dataset'
table_id = 'master_table_demo'  # Replace with your table name

BASE_URL = 'https://tableauuat.nordstrom.com/api/3.19'
SITE_CONTENT_URL = 'AS'

USERNAME = ""
PASSWORD = ""

start_time = datetime.datetime.now()


def multi_threading(workbooks, data):
    def get_workbook_details(workbook_id):
        url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}'
        headers = {
            'X-Tableau-Auth': token
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_dict = xmltodict.parse(response.text)
        return response_dict['tsResponse']

    def get_views(workbook_id):
        url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/views'
        headers = {
            'X-Tableau-Auth': token
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_dict = xmltodict.parse(response.text)
        views = response_dict['tsResponse']['views']['view']
        if isinstance(views, dict):
            views = [views]
        return views

    def get_connections(workbook_id):
        url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/connections'
        headers = {
            'X-Tableau-Auth': token
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
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
        response.raise_for_status()
        response_dict = xmltodict.parse(response.text)
        return response_dict['tsResponse']['datasource']

    for workbook in workbooks:
        namespace = {'ns': 'http://tableau.com/api'}
        workbook_id = workbook.get('Workbook ID')
        workbook_name = workbook.get('Workbook Name')
        description = workbook.get('Description')
        content_url = workbook.get('Content URL')
        webpage_url = workbook.get('Webpage URL')
        default_view_id = workbook.get('Default View ID')
        project_name = workbook.get('Project Name')
        owner = workbook.get('Owner ID')
        owner_name = workbook.get('Owner Name')

        views = get_views(workbook_id)
        connections = get_connections(workbook_id)

        for view in views:
            view_id = view['@id']
            view_name = view['@name']

            for conn in connections:
                datasource_id = conn['datasource']['@id']
                datasource_name = conn['datasource']['@name']
                custom_sql_query = conn.get('customSql', {}).get('query', None)
                serverAddress = conn['@serverAddress']
                userName = conn['@userName']

                try:
                    datasource_details = get_datasource_details(datasource_id)
                    datasource_type = datasource_details['@type']
                    datasource_last_updated = datasource_details['@updatedAt']
                except Exception as e:
                    datasource_type = None
                    datasource_last_updated = None

                data.append({
                    'Workbook ID': workbook_id,
                    'Workbook Name': workbook_name,
                    'Workbook Website': webpage_url,
                    'View ID': view_id,
                    'View Name': view_name,
                    'SQL Query': custom_sql_query,
                    'Datasource ID': datasource_id,
                    'Datasource Name': datasource_name,
                    'Datasource Type': datasource_type,
                    'Datasource Last Updated': datasource_last_updated,
                    'serverAddress': serverAddress,
                    'userName': owner_name,
                    'Project name': project_name,
                    'Site Name': SITE_CONTENT_URL

                })
    return data


if __name__ == "__main__":
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
        response_dict = xmltodict.parse(response.text)
        token = response_dict['tsResponse']['credentials']['@token']
        site_id = response_dict['tsResponse']['credentials']['site']['@id']
        return token, site_id


    try:
        token, site_id = get_auth_token()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"Other error occurred: {err}")


    def list_workbooks():
        page_size = 100
        page_number = 1
        workbook_data = []
        namespace = {'ns': 'http://tableau.com/api'}
        while True:
            headers = {
                "X-Tableau-Auth": token
            }
            workbook_url = f"{BASE_URL}/sites/{site_id}/workbooks?pageSize={page_size}&pageNumber={page_number}"
            response = requests.get(workbook_url, headers=headers)

            tree = ET.ElementTree(ET.fromstring(response.text))
            root = tree.getroot()
            workbooks = root.findall('.//ns:workbook', namespace)
            if not workbooks:
                break
            for workbook in workbooks:
                workbook_id = workbook.get('id')
                workbook_name = workbook.get('name')
                description = workbook.get('description')
                content_url = workbook.get('contentUrl')
                webpage_url = workbook.get('webpageUrl')
                default_view_id = workbook.get('defaultViewId')
                project = workbook.get('ns:project', namespace)
                owner = workbook.find('ns:owner', namespace)

                if owner is not None:
                    # print("YYYYYYYYYYYYYYYYYYYY")
                    # print(owner)
                    owner_id = owner.get('id')
                    owner_name = owner.get('name')
                    owner_full_name = owner.get('fullname')
                    # print(owner_full_name)
                else:
                    owner_id = None
                    owner_name = None

                if project is not None:
                    project_id = project.get('id')
                    project_name = project.get('name')
                else:
                    project_id = None
                    project_name = None

                workbook_data.append({
                    'Workbook ID': workbook_id,
                    'Workbook Name': workbook_name,
                    'Description': description,
                    'Content URL': content_url,
                    'Webpage URL': webpage_url,
                    'Default View ID': default_view_id,
                    'Project ID': project_id,
                    'Project Name': project_name,
                    'Owner ID': owner_id,
                    'Owner Name': owner_full_name
                })
            page_number += 1
        return workbook_data


    try:
        workbooks = list_workbooks()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

    data = []
    t1 = threading.Thread(target=multi_threading, args=(workbooks, data))
    t1.start()
    t1.join()

    df = pd.DataFrame(data)

    end_str = datetime.datetime.now()
    print("Start time at: ", start_time)
    print("End time at: ", end_str)
    print("Total time took :", end_str - start_time)

    print(df.head())  # Debugging: print the first few rows of the dataframe


    # Upload CSV file to GCS directly
    def upload_to_gcs(bucket_name, destination_blob_name, dataframe):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        csv_buffer = io.BytesIO()
        dataframe.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        blob.upload_from_file(csv_buffer, content_type='text/csv')
        print(f"File uploaded to {destination_blob_name}.")


    bucket_name = 'dm-raven-nordstrom-share'
    destination_blob_name = 'eagle-drive-folder/Shared_by_Nordstrom/Sample_Test/tableau_details.csv'
    upload_to_gcs(bucket_name, destination_blob_name, df)
