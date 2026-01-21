import os
import time
import glob
import re

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

output_directory = os.path.join(directory, "Output")
if not os.path.exists(output_directory):
    os.mkdir(output_directory)

PyFolderLst = glob.glob(r'C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-06-26//temp_sp2//**//NA//translated_DL_CMA_CMBR.customer_activation.sql', recursive=True)
# ParamFolderLst =

print(PyFolderLst)
final_list = []
for sql_file in PyFolderLst:
    location = os.path.dirname(sql_file)
    file_name = os.path.basename(sql_file)
    print("Location of file:", location)
    print("file name :", file_name)
    with open(sql_file, "r") as file:
        print(sql_file)
        f = file.read()
        dag_id = re.sub("([\w]+)([\s]+TIMESTAMP.*)\n\)", r"\1\2,\n\1_tz STRING )", re.sub(r"([\w]+)([\s]+TIMESTAMP.*,)", r"\1\2\n\1_tz STRING,", f))
        # print(dag_id)
        output_file_path = os.path.join(output_directory, file_name)
        output_folder = os.path.dirname(output_file_path)# Modified line
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)  # Create the directory if it doesn't exist
        with open(output_file_path, 'w') as f_out:
            f_out.write(dag_id)
