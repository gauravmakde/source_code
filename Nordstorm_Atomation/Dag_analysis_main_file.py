import os
import glob
import sys
import time
import re
import json
import pandas as pd
from common.config_parser import *
from common.common_fun import common_obj
from dag_analysis.dag_py_file_function import multiple_info

sys.dont_write_bytecode = True

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr
common_obj.create_directory(Output_Folder_Path)

if Enable_csv_fetch_file == "Y":
    print("Enable_csv_file_fetch")
    file_df = pd.read_csv(CSV_Input_Folder_Path)
    # print(file_df['File_name'])
    ParamFolderLst = []
    for single_file in file_df.iterrows():
        # Dag_location = single_file[1]['Dag_location']
        # Dag_name = single_file[1]['Dag_name']
        # SQL_Location = single_file[1]['SQL_Location']
        # SQL_Name = single_file[1]['SQL_Name']
        # Milestone = single_file[1]['Milestone']
        # final_location = single_file[1]['Location_Dag_Name']
        # folder_location = final_location.replace("|", "//").split("/")[0]
        # sourceProduct = single_file[1]['SourceProduct']

        # if Milestone == Sprint and sourceProduct == "teradata":
        #     last_folder = SQL_Location.split("/")[-2]
        #     first_folder = SQL_Location.split("/")[0]
        #     singleFileLst = glob.glob(os.path.join(directory, 'ISF', first_folder, '**', last_folder, SQL_Name), recursive=True)
        #     ParamFolderLst.extend(singleFileLst)

        Dag_location = single_file[1]['DAG_Category']
        Folder = single_file[1]['Folder']
        DAG_List = single_file[1]['DAG_List']
        Dag_id = single_file[1]['Dag_id']
        File_location = single_file[1]['File_location']
        File_name = single_file[1]['File_name']
        # DAG_Category, Folder, DAG_List, Dag_id, File_location, File_name, Owner
        Milestone="Sprint 3"

        # if Milestone == Sprint and sourceProduct == "teradata":
            # last_folder = SQL_Location.split("/")[-2]
            # first_folder = SQL_Location.split("/")[0]
        print(File_name)
        singleFileLst = glob.glob(os.path.join(Base_Input_Folder,"**","dags_py",File_name), recursive=True)
        ParamFolderLst.extend(singleFileLst)


else:
    print("Input file:",Input_Folder_Path)
    ParamFolderLst = glob.glob(Input_Folder_Path, recursive=True)
print(ParamFolderLst)
print(len(ParamFolderLst))


if __name__ == "__main__":
    data = []
    error_file = []
    operator_details=[]
    print("Parameter Folder List:", ParamFolderLst)
    for file in ParamFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        print("\nLocation:", location)
        print("File name:", file_name)
        print("The length of file is:", len(file))

        if Enable_py_file_dag_information == "Y" and len(file) < 260:
            all_imports = multiple_info.fetch_dag_info(file, file_name, location)
            data.extend(all_imports)
        elif len(file) > 260:
            error_file.append({
                'location': location,
                'file_name': file_name,
                'len_of_file': len(file)
            })

        print("Fetching the py_file_dag_information is done")
    # df = pd.DataFrame(data)
    # print(df)
    # df.to_csv(Output_Folder_Path + "\\dag_initial_analysis_" + timestr + ".csv")

    if Enable_fetch_json_details == "Y" and data:
        print("Fetching JSON details")
        json_details = multiple_info.fetch_json_details(data)
        print("JSON details fetched")
        print(json_details)

        operator_details = json_details
        # df_1 = pd.DataFrame(json_details)
        # print(df_1)
        # df_1.to_csv(Output_Folder_Path + "\\dag_initial_analysis_json_details_" + timestr + ".csv")

    if Enable_fetch_inf_SSH_operator=="Y" and operator_details:
        print("Fetching SSH operator Information")
        ssh_detail = multiple_info.fetch_ssh_info_details(json_details,XML_file_Path)
        # print(ssh_detail)

        operator_details=ssh_detail


        # df_1 = pd.DataFrame(ssh_detail)
        # print(df_1.columns)
        # df_1.to_csv(Output_Folder_Path + "\\dag_initial_details_" + timestr + ".csv")

    if Enable_fetch_inf_K8_operator=="Y" and operator_details:
        print("Fetching K8 operator Information")
        k8_detail = multiple_info.fetch_k8_info_details(operator_details)
        operator_details = k8_detail
        print("$$$$$$$$$$$$$$$")
        print(operator_details)
        # df_1 = pd.DataFrame(ssh_detail)
        # print(df_1.columns)
        # df_1.to_csv(Output_Folder_Path + "\\dag_initial_details_" + timestr + ".csv")



    if error_file:
        df_2 = pd.DataFrame(error_file)
        print(df_2)
        df_2.to_csv(Output_Folder_Path + "\\error_file_dag_" + timestr + ".csv")


    df_1 = pd.DataFrame(operator_details)
    print(df_1.columns)
    df_1.to_csv(Output_Folder_Path + "\\dag_initial_details_" + timestr + ".csv")
    # print(f"Final python dag import len:{len(df_1)}")
