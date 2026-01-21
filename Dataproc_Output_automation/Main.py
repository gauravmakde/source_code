import os
import time
import glob
import re
import json
import datetime
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

from common.config_parser import *
from common.common_fun import common_obj
from util.Utility_function import *



start_time = datetime.datetime.now()
# print(Enable_to_run_specific_file)
if Enable_to_run_specific_file == "Y":
    print("fetching CSV files")

    file_df = pd.read_csv(os.path.join( Single_Csv_File,csv_file_name))
    print(file_df)
    PyFolderLst = []
    for single_file in file_df['file_name']:
        print(single_file)

        if single_file != 'nan':
            singleFileLst = glob.glob(os.path.join(Single_Csv_File, "**",'dags' ,str(single_file)), recursive=True)
            PyFolderLst.extend(singleFileLst)


            # print(PyFolderLst)
else:

    # PyFolderLst = glob.glob(os.path.join("ip",'ISF','**','dags','item_intent_10976_tech_nap_merch.py'),recursive=True)
    PyFolderLst = glob.glob(Input_Folder_Path,recursive=True)
print(PyFolderLst)
# exit()
if __name__ == "__main__":
    data = []
    error = []
    final_app_name = ""
    for file in PyFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        app_name = os.path.basename(os.path.dirname(os.path.dirname(file)))

        print("Location of file:", location)
        print("file name :", file_name)
        print("App name is :",app_name)

        # try:
            # operator_list=multiple_utility.list_of_operator(file)
            # print(file)
        details_dataproc, error=multiple_utility.fetch_json_sql(file,error)

            # if Fetch_metadata_newrelic_yaml=="Y":
            #     multiple_utility.list_yaml_file(file, operator_list, location, error)
        # print(all_json_sql_details)
        data.extend(details_dataproc)
        # except Exception as e:
        #     print("It has some exception")
        #     error.append({
        #         'location': location,
        #         'file_name': os.path.basename(file),
        #         'error': str(e)
        #     })



df = pd.DataFrame(data)
df.to_csv(os.path.join( Output_directory,'framework-parameters.csv'), index=False)
end_str = datetime.datetime.now()

if error:
    df_2 = pd.DataFrame(error)
    print(df_2)
    df_2.to_csv(os.path.join(Output_directory,f"error_dag_log.csv"))

print("Total DAG Converted TD to BQ :", len(PyFolderLst))
print("Start time at: ", start_time)
print("End time at: ", end_str)
print("Total time took :", end_str - start_time)




