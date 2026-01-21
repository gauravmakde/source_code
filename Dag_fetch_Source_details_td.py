
import glob
import os
import re
import json
import time
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

location_kafka_json_file_location="D://Onix//nordstorm//Input_Code//Move_Group_0//Sprint_1//DAG//**//*.json"

all_kafka_json_file_location = glob.glob(location_kafka_json_file_location, recursive=True)
# print(all_kafka_json_file_location)
data=[]
for single_file in all_kafka_json_file_location:
    # print(single_file)
    file_name = os.path.basename(single_file)
    location = os.path.dirname(single_file)
    # print("Json File name is :", file_name)
    # print("Location  is :", location)
    with open(single_file) as files:
        file =files.read()
    # print(file)

    try:
        re_source_details = r"--\s*source_tables_details\s*'([^']*)'"
        re_target_details = r"--\s*target_tables_details\s*'([^']*)'"
        re_teradata_details = r"source_type\"\s*:\s*\"TERADATA\""

        source_match = re.search(re_source_details, file)
        target_match = re.search(re_target_details, file)

        source_details = source_match.group(1) if source_match else None
        target_details = target_match.group(1) if target_match else None

        source_teradata_match = re.search(re_teradata_details, target_details)
        target_teradata_match = re.search(re_teradata_details, source_details)
        re_kafka_key = r"(\w+\s*)=\s*({[^}]*})"
        kafka_group = re.search(re_kafka_key, target_details)

        print(kafka_group)

        if kafka_group and source_teradata_match:

            kafka_key_name = kafka_group.group(2)
            json_kafka_value = json.loads(kafka_key_name)

            print(json_kafka_value['database_name'])
            print(json_kafka_value['data_source_name'])

            data.append({

                'location': location,
                'file_name': file_name,
                'schema.table_name':json_kafka_value['database_name']+"."+json_kafka_value['data_source_name']

            })

            print(data)

            if kafka_group and target_teradata_match:
                kafka_key_name = kafka_group.group(2)
                json_kafka_value = json.loads(kafka_key_name)

                print(json_kafka_value['database_name'])
                print(json_kafka_value['data_source_name'])

                data.append({

                    'location': location,
                    'file_name': file_name,
                    'schema.table_name': json_kafka_value['database_name'] + "." + json_kafka_value['data_source_name']

                })

                print(data)
    except Exception as e:
        print(e)

df=pd.DataFrame(data)
print(df)
df.to_csv(os.path.join(directory,"Fetch_details_teradata_"+timestr+".csv"))