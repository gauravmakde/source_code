
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
from util.Utility_function import multiple_utility

sys.dont_write_bytecode = True

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

print(Input_Folder_Path)
print(Output_Folder_Path)
print(Enable_Primary_and_cluster_value)
common_obj.create_directory(Output_Folder_Path)

ParamFolderLst = glob.glob(Input_Folder_Path,recursive=True)
print(ParamFolderLst)



if __name__ == "__main__":

    for sql_file in ParamFolderLst:
        location = os.path.dirname(sql_file)
        file_name = os.path.basename(sql_file)
        print("\nLocation", location)
        print("File name\n", file_name)



        if Enable_Primary_and_cluster_value=='Y':
            print("The main file is excluding")

            key_value,sub=multiple_utility.fetch_key(sql_file,file_name,location)
            print("\n*****After Replacement*****\n")
            print(sub)
            df = pd.DataFrame(key_value)
            df.to_csv(directory + "\\Key_status_" + timestr + ".csv")
            output_file_path = os.path.join(Output_Folder_Path, file_name)
            output_folder = os.path.dirname(output_file_path)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)  # Create the directory if it doesn't exist
                print(f"Directory {output_folder} created.")
            else:
                print(f"Directory {output_folder} already exists.")

            with open(output_file_path, 'w') as f_out:
                f_out.write(sub)

        if Enable_ddl_replace_period_sequence=='Y':
            output_file_value = multiple_utility.replace_period_sequence(location, file_name, sql_file)
            print("\nAfter Replacement")
            print(output_file_value)
            output_file_path = os.path.join(Output_Folder_Path, file_name)
            output_folder = os.path.dirname(output_file_path)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)  # Create the directory if it doesn't exist

            with open(output_file_path, 'w') as f_out:
                f_out.write(output_file_value)

# PyFolderLst = glob.glob(Input_Folder_Path+'\**\source\order_line_detail_8761_DAS_SC_OUTBOUND_sc_outbound_insights_framework.py', recursive=True)
# print(PyFolderLst)
# if not os.path.exists(Output_Folder_Path):
#     os.mkdir(Output_Folder_Path)

