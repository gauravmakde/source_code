#
# from functions.common.config_parser import *
# from functions.common.common_fun import *
import glob,sys
import re
import os
import json
import shutil
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Append 'common' directory to sys.path if not already there
common_path = os.path.join(project_root, "common")
# if common_path not in sys.path:
sys.path.append(common_path)

# Import directly from common_fun.py
from common_fun import common_obj
from config_parser import *


class Util:
    def list_of_operator(self,file):

        with open(file, "r") as f:
            py = f.read()
            # print(py)
            py_imports = re.findall(r"import[\s]*([\w]*)(,)?[\s]*([\w]*)", py)
            d = []
            d = [multioperator for operator in py_imports for multioperator in operator if multioperator not in d]
            return d

    def fetch_sql_file_name(self,all_json_file,spark_Sql,dag_id,location):
        # print("Need to fetch the sql file name ")
        base_location = os.path.dirname(location)
        # print(all_json_file)
        # print(spark_Sql)

        sql_Status = False
        fetch_sql_file = ""
        if all_json_file:
            for single_json in all_json_file:
                parsed_json = common_obj.fetch_json(single_json)
                stage_value = parsed_json['stages']
                json_dag_id = parsed_json['dag_id']

                # print(json_dag_id)
                if json_dag_id in dag_id:
                    # print("Found Json ")
                    # print(single_json)
                    # print(json_dag_id)
                    for single_stage in stage_value:
                        # print(single_stage)
                        full_sql_name = spark_Sql.split("/")[-1]
                        # print(full_sql_name)
                        list_directory_Sql = spark_Sql.split("/")[:-1]
                        # print(list_directory_Sql)
                        if list_directory_Sql:  # Check if the list is not empty
                            directory = os.path.join(*list_directory_Sql)
                        else:
                            directory = ""  # Set to an empty string or a default value if needed
                        # print("Dag_id:",dag_id)
                        # print("Single_stage:", single_stage)
                        sql_name = full_sql_name
                        length_sql = len(os.path.join(base_location, sql_folder_name, directory, sql_name))
                        list_sql_file = glob.glob(os.path.join(os.path.dirname(location), sql_folder_name, directory, sql_name))
                        # print(list_sql_file)
                        if list_sql_file:
                            sql_Status = True
                            fetch_sql_file = sql_name
                            sql_location = list_sql_file
                            length_sql = ""
                            # print("Sql has found")
                        else:
                            # print(f"Sql not found for {single_stage}")
                            # print(f"checking to split the sql {single_stage}")
                            # print("Before calling Sql_complexity")
                            sql_name = full_sql_name.replace(f"{dag_id}_{single_stage}_", "").strip()
                            # print(sql_name)
                            list_sql_file = glob.glob(os.path.join(base_location, sql_folder_name, directory, sql_name))
                            if list_sql_file:
                                sql_Status = True
                                fetch_sql_file = sql_name
                                sql_location = list_sql_file
                                # print("Sql has found")
                                # print(fetch_sql_file)
                                break

                    break
        # print(sql_name,sql_location)
        if sql_Status == False or sql_Status == "" or sql_Status == "CM":
            # print("No json has value")
            full_sql_name = spark_Sql.split("/")[-1]
            # print(full_sql_name)
            list_directory_Sql = spark_Sql.split("/")[0:-1]
            # print(list_directory_Sql)

            if list_directory_Sql:  # Check if the list is not empty
                directory = os.path.join(*list_directory_Sql)
            else:
                directory = ""  # Set to an empty string or a default value if needed
            # directory = os.path.join(*list_directory_Sql)
            sql_name = full_sql_name
            length_sql = len(os.path.join(base_location, sql_folder_name, directory, sql_name))
            # print(length_sql)
            def recursion_split(sql_name):
                while sql_name:
                    # print("^^^^^^^^^^")
                    # print(sql_name)
                    # print(os.path.join(os.path.dirname(location), sql_folder_name, directory, sql_name))
                    list_sql_file = glob.glob(os.path.join(base_location, sql_folder_name, directory, sql_name))

                    if list_sql_file:
                        # print(f"Current sql_name is found: {sql_name}")
                        # print("Before calling Sql_complexity")
                        # print("After calling Sql_complexity")
                        return "CM", sql_name, list_sql_file
                    else:
                        # print(f"Current sql_name not found: {sql_name}")
                        # print(f"directory name is ",directory)
                        # print(os.path.join(os.path.dirname(location), sql_folder_name, directory, sql_name))
                        sql_name = "_".join(sql_name.split("_")[1:])

                return False, full_sql_name, "", length_sql, "", ""

            recussion_sql_Status = recursion_split(sql_name)
            # print(recussion_sql_Status)
            if recussion_sql_Status:
                sql_name = recussion_sql_Status[1]
                sql_Status = recussion_sql_Status[0]
                sql_location = recussion_sql_Status[2]

        # print(sql_name, sql_location)
        return sql_name, sql_location
    def list_json_from_livy(self,file,list_operator,location,error):
        inside_error=[]
        re_sql_file_location = r"--\s*sql_file_location\s*([^\n]*)"

        base_location=os.path.dirname(location)
        json_location=os.path.join(base_location,kafka_json_folder)
        # print(json_location)
        details_livy = []
        sql_location=""
        cloud_sql =""
        cloud_json=""
        local_json=""
        if 'LivyOperator' in list_operator:
            # print("lIVY is present")
            with open(file, "r") as f:
                py = f.read()
                # print(py)
                livy_block_patterns = re.findall(r"(\w+)\s*=\s*LivyOperator\((.*?)(?=\)\s*\n\s*\w+\s*=|\)\s*$)",py, re.DOTALL)
                # print(livy_block_patterns)
                details_livy=[]
                re_dag_id = r"\s+dag_id[\s]*=[\s]*\'([\w]*)\'"
                search_dag_id = re.search(re_dag_id, py)
                dag_id = search_dag_id.group(1) if search_dag_id else 'None'
                for dag_name_livy, full_livy in livy_block_patterns:
                    task = dag_name_livy
                    task_id = re.findall("task_id\s*=\s*'([^']*)'", full_livy)[0]
                    # print(f"Task is {task} and task_id is {task_id}")

                    app_args = re.findall("app_args\s*=\s*\[\s*'@*(.*?)\]", full_livy)[0]  # to modify s3: to gs bucket re.sub("s3:","gs:",re.findall("app_args\s*=\s*\[\s*'@*(.*?)\]",full_livy)[0])
                    re_json_file = re.search("/(\w*.json)", app_args)

                    json_file_name = re_json_file.group(1) if re_json_file else ""
                    # print(f"Json file is {json_file_name}")
                    # print(f"Json file location is {json_location}")
                    json_file_location=glob.glob(os.path.join(json_location,json_file_name))
                    # print(json_file_location)
                    if len(json_file_location)>0:
                        # print("We got the json")
                        # print(json_file_location)
                        for single_json in json_file_location:
                            with open(single_json) as file_content:
                                file_contents = file_content.read()
                                # print(file_contents)

                                sql_file_location_match = re.search(re_sql_file_location, file_contents)

                                sql_file_locations = sql_file_location_match.group(1) if sql_file_location_match else None
                                # print(sql_file_locations)
                                multiple_sql_file_locations= sql_file_locations.split(",")
                                # print(sql_file_locations)

                                for single_sql in multiple_sql_file_locations:

                                    sparksql_full_path = single_sql.split("sql/")[1]

                                    # print(sparksql_full_path)
                                    folder_json_location = os.path.join(base_location, json_folder,"*.json")
                                    all_json_file = glob.glob(folder_json_location)
                                    # print(all_json_file)

                                    sql_name, list_sql_location=self.fetch_sql_file_name(all_json_file,sparksql_full_path,dag_id,location)
                                    # print(sql_name, sql_location,dag_id,single_json)
                                    spark_json_name=os.path.basename(single_json)
                                    # print("#####")
                                    # print(list_sql_location)
                                    if list_sql_location:
                                        sql_location=list_sql_location[0]
                                        # print("It has some list_sql_location")
                                        # print(f"Dag is is {dag_id}")
                                        # print(f"Sql file name is {sql_name}")
                                        # print(f"sql location is {sql_location}")
                                        # print(f"Spark json is {spark_json_name}")
                                        # print(f"Spark json location is {single_json}")
                                        # print("Spark json location {spa")
                                    else:
                                        # print("spark sql is not present")
                                        # print(f"location is {location}")
                                        # print(f"file name is {file}")
                                        # print(f"spark sql is {sparksql_full_path}")
                                        error.append({
                                            'location': location,
                                            'dag_file_name': os.path.basename(file),
                                            'task':task,
                                            'missing_file_name': sparksql_full_path,
                                            'json_file_location': os.path.join(json_location, json_file_name),
                                            # 'sql_file_name': sparksql_full_path,
                                            'error': f"spark sql is not present or sql file is greater, len is :{len(os.path.join(json_location, json_file_name))}"
                                        })
                                        # print(error)
                        # print(sql_location)
                        if sql_location:
                            # print(f"It has sql_location {sql_location}")
                            cloud_sql = sql_location.split(sql_folder_name)[1]
                            # cloud_sql= cloud_sql.replace("\\","/")
                            cloud_sql =testing_sql_directory+cloud_sql
                            # print(cloud_sql)
                            # print(f"Cloud location {cloud_sql}")

                            cloud_json = single_json.split(kafka_json_folder)[1]
                            # cloud_json= cloud_json.replace("\\","/")
                            cloud_json =testing_json_directory+cloud_json
                            # print(f"The sql location {sql_location}")

                            if Input_directory in sql_location:
                                sql_location= os.path.normpath(sql_location).split(Input_directory+"\\")[1]
                                print(sql_location)

                            if Input_directory in single_json:
                                single_json=os.path.normpath(single_json).split(Input_directory+"\\")[1]

                            details_livy.append({
                                'dag':dag_id,
                                'dataproc_task_id':task_id,
                                'local_sql':common_obj.get_normalized_path(sql_location,os_type),
                                'cloud_sql': common_obj.get_normalized_path(cloud_sql, os_type),
                                'local_json':common_obj.get_normalized_path(single_json,os_type),
                                'cloud_json': common_obj.get_normalized_path(cloud_json, os_type),

                            })

                    else:

                        error.append({
                                'location': location,
                                'dag_file_name':os.path.basename(file),
                                'task': task,
                                'missing_file_name': os.path.basename(json_file_name),
                                'json_file_location':os.path.join(json_location,json_file_name),
                                # 'sql_file_name': "",
                                'error': f"It don't have json or length of file is greater {len(os.path.join(json_location,json_file_name))}"
                                })
        return details_livy, error
    def list_yaml_file(self,file,operator_list,location,error):


        if 'launch_k8s_api_job_operator' in operator_list:
            with open(file, "r") as f:
                py = f.read()

            block_patterns = re.findall(r"(\w+)\s*=\s*launch_k8s_api_job_operator\((.*?)\)", py,re.DOTALL)


            for dag_name_k8, full_k8 in block_patterns:
                re_container_command = "container_image\s*=.*/(.*)',"
                task = dag_name_k8
                task_id = re.findall("task_id\s*=\s*'([^']*)'", full_k8)[0]
                container_Command = re.search(re_container_command, full_k8).group(1) if re.search(re_container_command, full_k8).group(1) else "None"

                if "send_metric" in container_Command:

                    re_full_metrics_yaml_file = "container_command\s*=\s*\[\s*.*'--metrics_yaml_file'\s*,\s*'([./\w+]+)'"
                    re_yaml_file = "(\w+.yaml)$"
                    full_metrics_yaml_file = re.search(re_full_metrics_yaml_file, py).group(1) if re.search(re_full_metrics_yaml_file,py) else None
                    yaml_file = re.search(re_yaml_file, full_metrics_yaml_file).group(1)
                    yaml_status=glob.glob(os.path.join(os.path.dirname(location),"**",new_relic,yaml_file),recursive=True)

                    if len(yaml_status)==0:
                        error.append({
                            'location': location,
                            'dag_file_name': os.path.basename(file),
                            'task': task_id,
                            'missing_file_name': yaml_file,
                            'json_file_location': "",
                            'error': f"It don't have yaml or length of file is greater {len(os.path.join(os.path.dirname(location),"**",new_relic,yaml_file))}"
                        })
                    # else:
                        # print("We have yaml")


multiple_utility = Util()


