import os
import time
import re
import glob
import pandas as pd

# Create date strings for directory and filenames
datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.mkdir(directory)

output_directory = os.path.join(directory, "Output")

# Get list of all .sql files in the specified directory and subdirectories
ParamFolderLst = glob.glob(
    r'C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-07-02//sp2_table//**//*.sql',
    recursive=True)

print(ParamFolderLst)
# C:\Users\Gaurav.Makde\PycharmProjects\pythonProject\reports 2024-07-01\sp2_table\PRD_NAP_DIM.CASEPACK_SKU_XREF.sql
def replace_period_sequence(location,file_name,file):
    print(location)

    with open(file, "r") as f:
        py = f.read()
        print(py)
        replace_period=""

        pattern_period=re.compile("([^\w]PERIOD\s*FOR\s*[^;\n]+,)")
        present_period=pattern_period.search(py)

        if present_period:

            replace_period=re.sub("SEQUENCED VALIDTIME","/* SEQUENCED VALIDTIME */",pattern_period.sub("--\\1",py))

            return replace_period

    return py
if __name__ == "__main__":
    data = []
    for file in ParamFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        print("Location", location)
        print("File name",file_name)
        output_file_value=replace_period_sequence(location,file_name,file)
        print("\nAfter Replacement")
        print(output_file_value)
        output_file_path = os.path.join(output_directory, file_name)
        output_folder = os.path.dirname(output_file_path)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)  # Create the directory if it doesn't exist

        with open(output_file_path, 'w') as f_out:
            f_out.write(output_file_value)
