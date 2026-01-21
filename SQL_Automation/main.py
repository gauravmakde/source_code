
import os
import glob
import sys


import os
import time
import re
import json
import pandas as  pd
datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr
from common.config_parser import *
from common.common_fun import common_obj
from util.Utility_function import *

sys.dont_write_bytecode = True

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

common_obj.create_directory(Output_Folder_Path)
# ParamFolderLst = glob.glob(Input_Folder_Path,recursive=True)

if Enable_Analysis_on_dag_csv == "Y":
    print("fetching CSV files")

    file_df = pd.read_csv(os.path.join( "ip","Sprint_dag_name.csv"))
    # print(file_df)
    ParamFolderLst = []
    count=len(file_df['dag_id'])
    for single_file in file_df['dag_id']:
        print(f"Total file need to fetch {count}")
        count=count-1
        # print(single_file)
        python_file=single_file+".py"
        if single_file != 'nan':
            singleFileLst = glob.glob(os.path.join("ip", 'ISF', '**', 'dags' , str(python_file)), recursive=True)
            ParamFolderLst.extend(singleFileLst)

            # print(PyFolderLst)
else:

    # PyFolderLst = glob.glob(os.path.join("ip",'ISF','**','dags','item_intent_10976_tech_nap_merch.py'),recursive=True)
    ParamFolderLst = glob.glob(os.path.join("ip", 'ISF', '**', 'dags', '*.py'),recursive=True)
# print(ParamFolderLst)
# exit()
directory = os.path.join(Output_Folder_Path,'Failure_LOG')

if not os.path.exists(directory):
    os.mkdir(directory)

directory = os.path.join(Output_Folder_Path,'Success_LOG')

if not os.path.exists(directory):
    os.mkdir(directory)
if __name__ == "__main__":
    failed_files = []
    output_files=[]
    # print(Enable_SQL_copy)
    if Enable_SQL_copy=='N':
        for file in ParamFolderLst :
            location = os.path.dirname(file)
            file_name = os.path.basename(file)
            # print("\nLocation", location)
            # print("File name", file_name)

            if Enable_Title_To_TD=='Y':
                print("Title started validating ")
                try:
                    output_tittle_replace = multiple_utility.fun_Enable_Title_To_TD(location, file_name, file)
                    with open(os.path.join(Output_Folder_Path,file_name), 'w') as f_out:
                        f_out.write(output_tittle_replace)
                except Exception as e:
                    # print(e)
                    failed_files.append({'location': location, 'file_name': file_name, 'error': str(e)})

            if Enable_dag_location_from_dag_id=='Y':
                print("Fetching Enable_dag_location_from_dag_id")
                try:

                    output_sql = multiple_utility.Fetch_dag_id_location(location, file_name, file)
                    # print(output_sql)
                    output_files.extend(output_sql)
                    print(output_files)
                    # print(output_files)
                except Exception as e:
                    # print(e)
                    failed_files.append({'location': location, 'file_name': file_name, 'error': str(e)})


            """
            Fetch_SQL_From_DAG_Json retrieves SQL file paths associated with a specific DAG from a given Python file and JSON files.

            The function performs the following steps:
            1. **Read Input File**: Opens the specified Python file to extract SQL file paths using regex.
            2. **Extract DAG ID**: Uses regex to extract the DAG ID from the Python file content.
            3. **Find JSON Files**: Constructs the directory path for JSON files and retrieves all JSON files in that location.
            4. **Process SQL Paths**: Iterates over each SQL file path found in the input file:
                1. If the Json is present it will fetch the sql file name from json and keep sql_Status =True
                2. If Json is not present then it will split the sql name and check the respective sql in directory. sql_Status =CM( cross check manually)
                3. If above condition is not satisfy in then  sql_status =False
            5. **Recursive Search**: If the SQL file is not found, the function recursively shortens the SQL file name and attempts to find the file again.
            6. To enable Fetch_SQL_From_DAG_Json =Y, Input_Folder_Path, Output_Folder_Path ,Json_folder_name ,sql_folder_name

            Returns a list of dictionaries with the following keys:
            - 'dag_id': The ID of the DAG.
            - 'full_sql_name': The complete path of the SQL file.
            - 'fetch_sql_file': The name of the SQL file if found, otherwise an empty string.
            - 'sql_Status': Status indicating whether the SQL file was found or not (e.g., "CM" for found, or False).
            """
            if Fetch_SQL_From_DAG_Json=='Y':
                print("Fetching sql from DAG file")
                try:
                    output_sql = multiple_utility.Fetch_SQL_From_DAG_Json(location, file_name, file,output_sql="")
                    print(output_sql)
                    output_files.extend(output_sql)
                    print("To do")
                    print(output_files)
                except Exception as e:
                    print("Error ")
                    print(e)
                    failed_files.append({'location': location, 'file_name': file_name, 'error': str(e)})


    else:
        print("SQL File Copy started ")
        try:
            output_tittle_replace = multiple_utility.fun_Enable_SQL_copy(SQL_Copy_TXT,SQL_File_Directory,Output_Folder_Path)
            # with open(os.path.join(Output_Folder_Path,file_name), 'w') as f_out:
            #     f_out.write(output_tittle_replace)
        except Exception as e:
            # print(e)
            failed_files.append({'location': location, 'file_name': file_name, 'error': str(e)})


# df=pd.DataFrame(failed_files)
output_file_df=pd.DataFrame(output_files)

if (len(output_file_df)>0):
    print("It has success file")
    # output_file_df = output_file_df.explode('sql_name').reset_index(drop=True)
#     print(output_file_df)
    output_file_df.to_csv(os.path.join(Output_Folder_Path,"Success_LOG","Success_file_log.csv"))
# if (len(df)>0):
#     print("It has some error file ")

    # df.to_csv(os.path.join(Output_Folder_Path,"Failure_LOG","Failed_file_log.csv"))
# print(df)
