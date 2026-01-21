
import xml.etree.ElementTree as ET
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

start_time = datetime.datetime.now()
num_threads =3

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

EXTRACTED_DIR = 'workbook_extracted2'
RENAMED_WORKBOOK_NAME = 'BI_Upload_test.twb'

BASE_URL = 'https://tableauuat.nordstrom.com/api/3.19'
SITE_CONTENT_URL = 'OnixSandbox'

USERNAME="onix_API"
PASSWORD="7nnjNNDHS62jm0ouEGd5DA==:htWCU5eXo4jMcWatZgVNJ43Cphd3ybX9"

#Below function to get auth token for Tableue Server
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
    # print(response_dict)
    token = response_dict['tsResponse']['credentials']['@token']
    site_id = response_dict['tsResponse']['credentials']['site']['@id']
    return token, site_id


#Below function to get the workbook list from AS
def workbook_list():
    page_size = 100
    page_number = 1
    workbook_data = []
    namespace = {'ns': 'http://tableau.com/api'}
    while True:
        headers = {
            "X-Tableau-Auth": token
        }
        workbook_url=f"{BASE_URL}/sites/{site_id}/workbooks?pageSize={page_size}&pageNumber={page_number}"
        print(f"{page_number} Page Fetching")
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
            project = workbook.find('ns:project', namespace)
            owner = workbook.find('ns:owner', namespace)

            if owner is not None:
                owner_id = owner.get('id')
                owner_name = owner.get('name')
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
                'Owner Name': owner_name
            })
        page_number += 1
    return workbook_data



def get_workbook_view(token,site_id,workbook_id):
    url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/views'
    headers = {'X-Tableau-Auth': token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    tree = ET.ElementTree(ET.fromstring(response.text))
    root = tree.getroot()
    namespace = {'ns': 'http://tableau.com/api'}
    wk_view = root.findall('.//ns:view', namespace)
    # if not datasource_connetions:
    #     break
    for view_deatails in wk_view:
        view_id = view_deatails.get('id')
        view_name = view_deatails.get('name')
        view_Url_Name = view_deatails.get('viewUrlName')

        views_df.append({
            'Workbook ID' : workbook_id,
            'View ID': view_id,
            'View Name': view_name,
            'View Url Name': view_Url_Name
        })



def get_workbook_connections(token,site_id,workbook_id):
    url = f'{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/connections'
    headers = {'X-Tableau-Auth': token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    tree = ET.ElementTree(ET.fromstring(response.text))
    root = tree.getroot()
    namespace = {'ns': 'http://tableau.com/api'}
    datasource_connetions = root.findall('.//ns:connection', namespace)
    # if not datasource_connetions:
    #     break
    for datasource in datasource_connetions:
        connection_id = datasource.get('id')
        connection_type = datasource.get('type')
        server_address = datasource.get('serverAddress')
        # serverPort=datasource.get('serverPort')
        userName=datasource.get('userName')
        datasource_details=datasource.find('ns:datasource', namespace)
        if datasource_details is not None:
            datasource_id=datasource_details.get('id')
            datasource_name=datasource_details.get('name')
        else:
            datasource_id=None
            datasource_name=None

        datasource_df.append({
            'Workbook ID' : workbook_id,
            'Connection ID': connection_id,
            'Connection Type': connection_type,
            'UserName': userName,
            'Server Address': server_address,
            'Datasource Id': datasource_id,
            'Datasource Name': datasource_name
        })

def multiprocessing_function(wk_df):
    count=0
    for workbook_id in wk_df['Workbook ID']:
        count=count+1
        print(f"{count} WorkBook Id :- {workbook_id}")
        # para_res.append(pool_jar.apply_async(get_workbook_connections, args=(token,site_id,workbook_id)))
        get_workbook_connections(token,site_id,workbook_id)
        get_workbook_view(token,site_id,workbook_id)
        if count==200:
            break
    # pool_jar.close()
    # pool_jar.join()
    return True


try:
    token, site_id = get_auth_token()
    print(token, site_id)
    print("Site_id: ", site_id)
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
except Exception as err:
    print(f"Other error occurred: {err}")

#url = f"{BASE_URL}/sites/{site_id}/workbooks/{workbook_id}/content"

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
def get_datasource_details(datasource_id):
    url = f'{BASE_URL}/sites/{site_id}/datasources/{datasource_id}'
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
    return response_dict['tsResponse']['datasource']

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


views_df=[]
datasource_df=[]

wk_df = pd.DataFrame(workbook_list())


print(wk_df)  # Debugging: print the collected data
data=[]
count=len(wk_df)
final_view_count=0
total=0

for iter in wk_df.iterrows():
    print("Total workbook fetch", total)
    print("Remaining workbook to fetch",count)
    print('Total View fetch',final_view_count)

    Workbook_id= iter[1]['Workbook ID']
    Workbook_name = iter[1]['Workbook Name']
    Workbook_url = iter[1]['Webpage URL']
    Description = iter[1]['Description']
    project_id= iter[1]['Project ID']
    project_name=iter[1]['Project Name']
    owner_id=iter[1]['Owner ID']
    owner_name=iter[1]['Owner Name']

    # print(Workbook_id)
    views = get_views(Workbook_id)
    connections = get_connections(Workbook_id)

    view_count=len(views)
    final_view_count=final_view_count+view_count

    for view in views:
        # print("Total workbook fetch", total)
        # print("Remaining workbook to fetch", count)
        print(f"View fetch remaining:{view_count}")
        view_id = view['@id']
        view_name = view['@name']

        # print(f"Workflow name: {Workbook_name}, View name is: {view_name}")

        for conn in connections:
            datasource_id = conn['datasource']['@id']
            datasource_name = conn['datasource']['@name']

            # Debugging: Print connection details
            # print(f"Connection details: {conn}")

            custom_sql_query = conn.get('customSql', {}).get('query', None)
            serverAddress = conn['@serverAddress']
            userName = conn['@userName']

            # print(serverAddress, userName)

            try:
                datasource_details = get_datasource_details(datasource_id)
                datasource_type = datasource_details['@type']
                datasource_last_updated = datasource_details['@updatedAt']
            except Exception as e:
                # print(f"Error fetching datasource details: {e}")
                datasource_type = None
                datasource_last_updated = None

            # if custom_sql_query:
                # print(f"Custom SQL found: {custom_sql_query}")
            # else:
                # print("No Custom SQL found for this connection.")

            try:
                datasource_details = get_datasource_details(datasource_id)
                datasource_type = datasource_details['@type']
                datasource_last_updated = datasource_details['@updatedAt']
            except Exception as e:
                # print(f"Error fetching datasource details: {e}")
                datasource_type = None
                datasource_last_updated = None

            data.append({
                'Workbook ID': Workbook_id,
                'Workbook Name': Workbook_name,
                'Workbook Website': Workbook_url,
                'View ID': view_id,
                'View Name': view_name,
                'SQL Query': custom_sql_query,
                'Owner': owner_name,
                'Datasource ID': datasource_id,
                'Datasource Name': datasource_name,
                'Datasource Type': datasource_type,
                'Datasource Last Updated': datasource_last_updated,
                'serverAddress': serverAddress,
                'userName': userName

            })
        view_count=view_count-1
    count=count-1
    total=total+1

    print(data)  # Debugging: print the collected data

    df = pd.DataFrame(data)
    df.to_csv(directory+'\\tableau_queries_As_report.csv', index=False)



# if multiprocessing_function(wk_df):
#     print("Success")
#
# datasource_df_list=pd.DataFrame(datasource_df)
# views_df_list=pd.DataFrame(views_df)
#
# wk_df.to_csv('workbooks_details.csv', index=False)
# datasource_df_list.to_csv('datasource_df.csv', index=False)
# views_df_list.to_csv('View_df.csv', index=False)
#
# print(f'Number of records in DataFrame: {wk_df.shape[0]}')



















# if response.status_code == 200:
#     # Save the content to a .twbx file
#     with open('workbook2.twbx', 'wb') as file:
#         file.write(response.content)
#     print("Workbook downloaded successfully.")
# else:
#     print(f"Error: {response.status_code} - {response.text}")



# Extract the .twbx file
# with zipfile.ZipFile('workbook2.twbx', 'r') as zip_ref:
#     zip_ref.extractall(EXTRACTED_DIR)

# print("Workbook extracted.")

# Locate and rename the .twb file
# for file_name in os.listdir(EXTRACTED_DIR):
#     if file_name.endswith('.twb'):
#         old_file_path = os.path.join(EXTRACTED_DIR, file_name)
#         os.rename(old_file_path, RENAMED_WORKBOOK_NAME)
#         print(f"Workbook renamed to {RENAMED_WORKBOOK_NAME}")
#         break
# else:
#     print("No .twb file found in the extracted contents.")
#     exit()


# file_path = 'D:\\Nordstrom\\BI_Upload_test.twb'

# with open(file_path, 'rb') as file:
#     response = requests.post(url, headers=headers, data=file)
#     print(response.text)

# def upload_workbook(token, site_id, workbook_path, project_id):
#     url = f'{BASE_URL}/sites/{site_id}/workbooks'
#     headers = {'X-Tableau-Auth': token}
    
#     request_payload = f"""
#     <tsRequest>
#         <workbook>
#             <name>{os.path.basename(workbook_path)}</name>
#             <project id="Default" />
#         </workbook>
#     </tsRequest>
#     """
    
#     files = {
#         'request_payload': (None, request_payload, 'text/xml'),
#         'tableau_workbook': (os.path.basename(workbook_path), open(workbook_path, 'rb'), 'application/octet-stream')
#     }

#     response = requests.post(url, headers=headers, files=files)
    
#     print("Workbook uploaded successfully.")
#     print(response.text)

# upload_workbook(token, site_id, RENAMED_WORKBOOK_NAME, "c1128ad6-841f-4b49-ac14-2db413becf7c")



end_str = datetime.datetime.now()
print("Start time at: ",start_time)
print("End time at: ",end_str)
print("Total time took :", end_str-start_time)