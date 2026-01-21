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
r'C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-07-01//sp2_table//**//PRD_NAP_STG.PRODUCT_PROMOTION_TIMELINE_GTT.sql',
    recursive=True)

def various_key (sql_file,file_name,location):

    data=[]
    with open(sql_file, "r") as f:
        py = f.read()
        print(py)
        table_name=""
        primary_status=""
        upi_status=""
        table_status=""

        if "CREATE MULTISET TABLE" in py:
            print("It has multiset")
            pattern_multiset=re.compile("CREATE\s*MULTISET\s*TABLE\s*([^,]+),")
            multiset=pattern_multiset.search(py)
            if multiset:
                table_name=multiset.group(1)
                table_status='MULTISET TABLE'
                print(table_name)

        elif  "CREATE SET TABLE" in py:
            print("It has set")
            pattern_set=re.compile("CREATE\s*SET\s*TABLE\s*([^,]+),")
            sets=pattern_set.search(py)
            if sets:
                table_name=sets.group(1)
                table_status='SET TABLE'
                print(table_name)

        elif "CREATE MULTISET GLOBAL TEMPORARY TABLE" in py:
            # print("It has set")
            pattern_global_set=re.compile("CREATE\s*MULTISET\s*GLOBAL\s*TEMPORARY\s*TABLE\s*([^,]+),")
            global_Set=pattern_global_set.search(py)
            if global_Set:
                table_name=global_Set.group(1)
                table_status='Global set TABLE'
                print(table_name)

        if "UNIQUE PRIMARY INDEX" in py:
            upi_status="It has UPI"

        elif "PRIMARY INDEX" in py:
            print("It has primary index")
            primary_status="It has primary index"

        data.append({
            'location': location,
            'File_name': file_name,
            'table_name':table_name,
            'table_status':table_status,
            'primary_key_status': primary_status,
            'UPI_status':upi_status

        })
    return data


if __name__ == "__main__":
    data = []
    for sql_file in ParamFolderLst:
        location = os.path.dirname(sql_file)
        file_name = os.path.basename(sql_file)
        print("\nLocation", location)
        print("File name\n", file_name)

        key_value=various_key(sql_file,file_name,location)
        print("\n*****After Replacement*****\n")
        print(key_value)
        df = pd.DataFrame(key_value)
        df.to_csv(directory + "\\UPI_Primary_Key_status_" + timestr + ".csv")
