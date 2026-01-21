import os
import time
import re
import glob
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

# ParamFolderLst = glob.glob(directory+'//DAG//'+'/**/*.sql', recursive=True)ii

df=pd.read_csv(directory+'//Input_file.csv')
# print(df)
df_file=[]
for i in df.iterrows():
    sql_name=i[1]['SQL Name']
    print(sql_name)

    PyFolderLst = glob.glob(directory+'//Shared_by_Nordstrom_Airflow_DAGs_Custom DAG_Airflow_repos_for_datametica//**/'+sql_name, recursive=True)

    second_list=[]
    print(PyFolderLst)
    if (len(PyFolderLst)>0):
        for sql_file in PyFolderLst:

            location="/".join(sql_file.split("\\")[1:])
            file_name=sql_file.split("\\")[-1]
            with open(sql_file, "r") as file:
                print("Directory is: ", location)
                print("The file name is: ", sql_file.split("\\")[-1])
                second_list=[]

                for line in file:
                    tables = re.findall(r'UPDATE[\s]+[\w]*\.*[\w]+\.[\w]+|INTO[\s]+[\w]*\.*[\w]+\.[\w]+|FROM[\s]+[\w]*\.*[\w]+\.[\w]+|JOIN[\s]+[\w]*\.*[\w]+\.[\w]+|TABLE[\s]+[\w]*\.*[\w]+\.[\w]+|EXISTS[\s]+[\w]*\.*[\w]+\.[\w]+|UPDATE[\s]+\{[\w]+\}[\w]+\.[\w]+|INTO[\s]+\{[\w]+\}[\w]+\.[\w]+|FROM[\s]+\{[\w]+\}[\w]+\.[\w]+|JOIN[\s]+\{[\w]+\}[\w]+\.[\w]+|TABLE[\s]+\{[\w]+\}[\w]+\.[\w]+|EXISTS[\s]+\{[\w]+\}[\w]+\.[\w]+|UPDATE[\s]+\{[\w]+\}\.[\w]+|INTO[\s]+\{[\w]+\}\.[\w]+|FROM[\s]+\{[\w]+\}\.[\w]+|JOIN[\s]+\{[\w]+\}\.[\w]+|TABLE[\s]+\{[\w]+\}\.[\w]+|EXISTS[\s]+\{[\w]+\}\.[\w]+',line.upper())
                    first_list=[]
                    for table in tables:
                        if len(table)>0:
                            print(table)
                            first_list = [location, file_name, table.split()[0], table.split()[1]]
                            second_list.append(first_list)

            df_file.extend(second_list)
    else:
        location=""
        file_name = sql_name
        first_list = [location, file_name, "", ""]
        second_list.append(first_list)
        df_file.extend(second_list)

print(df_file)
df=pd.DataFrame(df_file)
df.columns=['Directory','File_name','DDL_statement','Table_name']
df.to_csv(directory+"\\Table_Fetch_From_SQL_File_"+timestr+".csv")







