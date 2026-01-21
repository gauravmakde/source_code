import os
import time
import glob
import re
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

PyFolderLst = glob.glob(r'D://Onix//nordstorm//Input_Code//Move_Group_0//Sprint_1//DAG//ISF//**//*.py', recursive=True)
# PyFolderLst = glob.glob(directory+'//Shared_by_Nordstrom_Airflow_DAGs_Custom DAG_Airflow_repos_for_datametica//**/*.sql', recursive=True)


JsonFolderLst = glob.glob(directory+'//DAG//**//pypeline_jobs/*.json', recursive=True)

df_file=[]
def py_file_json(ParamFolderLst):

    final_list=[]
    for sql_file in ParamFolderLst:

        location="/".join(sql_file.split("\\")[1:])
        file_name=sql_file.split("\\")[-1]
        second_list=[]
        with open(sql_file, "r") as file:
            print("Directory is: ", location)
            print("The file name is: ", file_name)
            f=file.read()

            pattern=re.compile("dag_id[\s]*=[\s]*[\w'{}]*,")# to fetch the dag_id=''
            pattern_1=re.compile("(=[f']?)([\s]*[\w'{}]*)")# To fetch the ''

            pattern_dag=pattern.search(f)
            if pattern_dag:

                dag_id=pattern_1.search(pattern_dag.group(0)).group(2)


                # dag_id=re.findall("'[\w]+'",re.findall("dag_id[\s]*=[\s]*[\w'{}]*,",f)[0])[0].replace("'","")
            print(dag_id)
            json_file_name=re.findall("user_config[//][\w]+.json",f)

            first_list=[location,sql_file.split("\\")[-1],dag_id,json_file_name]


        second_list.append(first_list)
        final_list.extend(second_list)

    return final_list

def json_file_sql(ParamFolderLst):

    final_list=[]
    for sql_file in ParamFolderLst:

        location="/".join(sql_file.split("\\")[1:])
        file_name=sql_file.split("\\")[-1]
        second_list=[]
        with open(sql_file, "r") as file:
            print("Directory is: ", location)
            print("The file name is: ", file_name)
            f=file.read()

            sql_file=re.findall("[\w]+\.sql\"",f)
            print(sql_file)

            dag_id =  re.findall("\"[\w]+\"",re.findall("dag_id\"[ ]?:[ ]?\"[\w]+\"", f)[0])[0].replace("\"","")

            first_list=[location,file_name,dag_id,sql_file]

        second_list.append(first_list)
        final_list.extend(second_list)
    return final_list

def json_file_present(df):

    final_df=[]
    for input in df.iterrows():

        Directory = input[1]['Directory']
        File_name = input[1]['File_name']
        Dag_id = input[1]['Dag_id']
        list_Json_file_name=input[1]['Json_file_name']
        # print(type(list_Json_file_name))

        if len(list_Json_file_name)>0:
            for json_file in list_Json_file_name:
                print(json_file.split("/")[1])

                PyFolderLst = glob.glob(directory + '//DAG//**/' + json_file, recursive=True)
                if (len(PyFolderLst)>0):
                    file_present=True
                    json_location=PyFolderLst[0]
                else:
                    file_present = False
                    json_location = None

                first_list=[Directory,File_name,Dag_id,json_file,json_location,file_present]
                final_df.append(first_list)

        else:
            file_present="False"
            json_location=""
            json_file=""
            first_list = [Directory, File_name, Dag_id, json_file, json_location, file_present]
            final_df.append(first_list)

    return final_df

def sql_file_present(df):

    final_df=[]
    for input in df.iterrows():


        Directory = input[1]['Directory']
        File_name = input[1]['File_name']
        Dag_id = input[1]['Dag_id']
        list_sql_file_name=input[1]['Sql_file_name']

        if (len(list_sql_file_name))>0:
            for sql_file in list_sql_file_name:

                print(sql_file.replace("\"",""))
                sqlFolderLst = glob.glob(directory + '//DAG//**/' + sql_file.replace("\"",""), recursive=True)

                if (len(sqlFolderLst)>0):
                    file_present=True
                    sql_location=sqlFolderLst[0]
                else:
                    file_present = False
                    sql_location=None

                first_list=[Directory,File_name,Dag_id,sql_file.replace("\"",""),sql_location,file_present]
                final_df.append(first_list)

        else:
            file_present="False"
            sql_location=""
            sql_file=""
            first_list = [Directory,File_name,Dag_id,sql_file,sql_location,file_present]
            final_df.append(first_list)

    return final_df
py_file_json=py_file_json( PyFolderLst)
df = pd.DataFrame(py_file_json, columns=['Directory', 'File_name', 'Dag_id','Json_file_name'])
# print(df)
json_file_present=json_file_present(df)
df_1=pd.DataFrame(json_file_present,columns=['Python_file_Dir','Py_file_name','Dag_id','Json_file','Json_location','File_present_status'])
df_1.to_csv(directory + "\\Python_file_Json_Mapping_status_" + timestr + ".csv")
# print(df_1)
#
json_file_sql=json_file_sql(JsonFolderLst)
df_1 = pd.DataFrame(json_file_sql, columns=['Directory', 'File_name', 'Dag_id','Sql_file_name'])

sql_file_present=sql_file_present(df_1)
df_2=pd.DataFrame(sql_file_present,columns=['Json_file_Dir','Json_file_name','Dag_id','SQL_file','SQL_location','File_present_status'])
df_2.to_csv(directory + "\\Json_file_SQL_Mapping_status_" + timestr + ".csv")



