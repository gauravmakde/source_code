#
# from functions.common.config_parser import *
# from functions.common.common_fun import *
import glob,sys
import re
import json
import os

class dag_info:

    def dag_name_from_csv(self,python_file, file_name, location):
        with open(sql_file, "r", encoding='utf-8') as f:
            py = f.read()

        pattern = r'dag_id\s*=\s*\'(\w+)\''



    def fetch_dag_info(self,sql_file, file_name, location):

        data=[]
        with open(sql_file, "r", encoding='utf-8') as f:
            py = f.read()
            # print(py)

            pattern = r'(?:from\s+\S+\s+import\s+\((?:\s*\w+\s*,?)+\s*\)|from\s+\S+\s+import\s+(?:\w+\s*,\s*)*\w+|import\s+\S+)'

             # Find all matches
            all_imports = re.findall(pattern, py)
            print("%%%%%%%%%%%%%")
            print(all_imports)
            print("%%%%%%%%%%%%%")
            dag_id=""
            # dag_pattern=re.compile("(dag_id\s*=\s*f?'([^\n)]+))'\s*,*")
            dag_pattern = re.compile("(dag_id\s*=\s*f?'*([^\n]+))'*\s*,*")

            search_dag_id=dag_pattern.search(py)
            if search_dag_id:
                # print("Dag_group_1",search_dag_id.group(1))
                dag_id=search_dag_id.group(1)
            else:
                dag_pattern = re.compile("dag\s*=\s*DAG\(([^,]*)")
                search_dag_id = dag_pattern.search(py)
                if search_dag_id:
                    dag_id = search_dag_id.group(1)
                else:
                    dag_id=None

            # print(f"Dag_id is {dag_id}")
            if dag_id!=None and dag_id.lower().strip()=="dag_name" :
                # print("Came inside the dag_id")
                search_dag_name=re.search("dag_name\s*=\s*([^\n]*)",py.lower())
                dag_id=search_dag_name.group(1) if search_dag_name else " "
            # print(f"final Dag_id is {dag_id}")
            counts=0
            for imp in all_imports:
                # print(imp)
                pattern = r'(?:import\s+\((?:\s*\w+\s*,?)+\s*\)|import\s+(?:\w+\s*,\s*)*\w+|import\s+\S+)'

                # Find all matches
                imports = re.findall(pattern, imp)
                counts = 0
                for split_imports in imports:
                    # print(split_imports)
                    for a_single_import in split_imports.split(","):
                        text = re.sub(r'\s*import\s*\(\s*|\s*import\s*', '', a_single_import)
                        final_module = text.strip().replace(")", "")
                        # print(final_module)
                        if len(final_module)!=1:

                            print(final_module)

                            find_module_count = py.count(final_module)
                        print("Count is :",find_module_count)
                        data.append({
                            'location': location,
                            'dag_id': dag_id,
                            'file_name': file_name,
                            'import': imp,
                            'single_import': final_module,
                            'count': find_module_count

                        })
                print(data)

                # for single_import in imp.split(","):
                #     text = re.sub(r'\s*import\s*\(\s*|\s*import\s*', '', single_import)
                #     # print("proper: ",text)
                #     final_module = text.replace(")", "").replace("from","").strip()
                #     print(final_module)
                #     counts = len(re.findall( final_module , py))
                #     print(f"{final_module}  count is: {counts}")




        return data


    def count_modules_use(self,sql_file, file_name, location,from_import,dag_id):

        data=[]
        with open(sql_file, "r") as f:
            py = f.read()
            # print(py)
            print(from_import)

            # print(re.findall(single_import,py))
            pattern = r'(?:import\s+\((?:\s*\w+\s*,?)+\s*\)|import\s+(?:\w+\s*,\s*)*\w+|import\s+\S+)'

            # Find all matches
            imports = re.findall(pattern, py)
            counts = 0
            for split_imports in imports:
                # print(split_imports)
                for single_import in split_imports.split(","):
                    text = re.sub(r'\s*import\s*\(\s*|\s*import\s*', '', single_import)
                    final_module = text.strip().replace(")", "")
                    print(final_module)
                    counts = len(re.findall(" " + final_module + " ", py))

                    data.append({
                        'location': location,
                        'dag_id': dag_id,
                        'file_name': file_name,
                        'import': from_import,
                        'single_import':final_module,
                        'count':counts


                    })
            return data

    def fetch_sql_file(self, sql_file, file_name, location,Output_SQL_Path):
        # with open(sql_file, "r") as f:
        #     py = f.read()
        #     print(py)

        print("Output_sql",Output_SQL_Path)
        print("Location",location)
        common_prefix = os.path.commonprefix([Output_SQL_Path.replace('/Output', ''), location])
        relative_location = os.path.relpath(location, common_prefix)
        print(relative_location)

        # Join paths to create output file path
        output_file_path = os.path.join(Output_SQL_Path, relative_location)
        print(output_file_path)

        # Determine output folder path
        output_folder = os.path.dirname(output_file_path)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            print("Directory created")
        else:
            print("Directory exists")
        # with open(output_file_path, 'w') as f_out:
        #     f_out.write(py)

        with open(sql_file, 'r') as src_file:
            with open(output_file_path, 'w') as dest_file:
                dest_file.write(src_file.read())

    def fetch_json_details(self, import_details):
        source_target = []
        unique_entries = set()

        fetch_count = len(import_details)
        print(f"Total imports to process: {fetch_count}")

        for import_detail in import_details:
            print(f"Pending to fetch: {fetch_count}")
            location = import_detail['location']
            dag_id = import_detail['dag_id']
            file_name = import_detail['file_name']
            file_import = import_detail['import']
            single_import = import_detail['single_import']
            count = import_detail['count']
            print(f"Operator is {single_import}")

            if single_import == "LivyOperator" and count > 1:
                print("Livy Operator Detected")
                with open(os.path.join(location, file_name), "r") as f:
                    dag_file = f.read()
                    # print(dag_file)
                    print("#############")
                    livy_operater_patterns = re.findall(r"(\w+)\s*=\s*LivyOperator\((.*?)\)\s*", dag_file, re.DOTALL)
                    print(livy_operater_patterns)
                    for dag_name_livy, full_livy in livy_operater_patterns:
                        json_file = re.search("/(\w*.json)", full_livy)
                        print("Json file is :",json_file)
                        if json_file:
                            script_dir = os.path.dirname(location)
                            singleFileLst = glob.glob(os.path.join(script_dir, '**', json_file.group(1)), recursive=True)
                            print("Dag file name ",location)
                            print("json file name:",singleFileLst, json_file.group(1))
                            for full_json_file in singleFileLst:
                                with open(full_json_file, "r") as r_files:
                                    json_file_name = os.path.basename(full_json_file)
                                    r_file = r_files.read()
                                    source_details = re.search("--\s*source_tables_details\s*([^;\n]*)", r_file)
                                    target_details = re.search("--\s*target_tables_details\s*([^;\n]*)", r_file)

                                    source_list = source_details.group(1).split("},") if source_details else []
                                    target_list = target_details.group(1).split("},") if target_details else []


                                    for each_source in source_list:
                                        source_entry = each_source.strip()
                                        source_types = re.search(r'"source_type":\s*"(\w*)",', source_entry)
                                        source_type = source_types.group(1) if source_types else []
                                        entry_key = (location,dag_id,file_name,json_file_name,file_import,single_import, source_entry,source_type, "NA",count)
                                        if entry_key not in unique_entries:
                                            source_target.append({
                                                'location': location,
                                                'dag_id': dag_id,
                                                'dag_file_name': file_name,
                                                'json_file_name': json_file_name,
                                                'import': file_import,
                                                'single_import': single_import,
                                                'source_details': source_entry,
                                                'source_type':source_type,
                                                'target_details': "NA",
                                                'operator_count': count,
                                            })
                                            unique_entries.add(entry_key)

                                    for each_target in target_list:
                                        target_entry = each_target.strip()
                                        entry_key = (
                                            (location, dag_id, file_name, json_file_name, file_import, single_import, "NA","NA", target_entry, count)
                                         )
                                        # entry_key = (single_import, "NA", target_entry)
                                        if entry_key not in unique_entries:
                                            source_target.append({
                                                'location': location,
                                                'dag_id': dag_id,
                                                'dag_file_name': file_name,
                                                'json_file_name': json_file_name,
                                                'import': file_import,
                                                'single_import': single_import,
                                                'source_details': "NA",
                                                'source_type': "NA",
                                                'target_details': target_entry,
                                                'operator_count': count,
                                            })
                                            unique_entries.add(entry_key)
            else:
                # entry_key = (location, dag_id, file_name, json_file_name, file_import, single_import, "NA", target_entry, count)
                # if entry_key not in unique_entries:
                source_target.append({
                    'location': location,
                    'dag_id': dag_id,
                    'dag_file_name': file_name,
                    'json_file_name': "NA",
                    'import': file_import,
                    'single_import': single_import,
                    'source_details': "NA",
                    'source_type': "NA",
                    'target_details': "NA",
                    'operator_count': count,
                })
                    # unique_entries.add(entry_key)
            fetch_count -= 1
            print(f"Processed import {import_detail['file_name']} with {import_detail['single_import']}")

        print("Final return from fetch_json_details")
        return source_target

    def fetch_ssh_info_details(self, json_details,XML_file_Path):

        ssh_details=[]
        XMLFolderLst = glob.glob(XML_file_Path, recursive=True)
        ssh_operator_regex = re.compile(r'(\w+)\s*=\s*SSHOperator\((.*?)\)\n', re.DOTALL)
        dag_pattern = re.compile(r"dag_id\s*=\s*'([^']+)'", re.DOTALL)
        command_regex = re.compile(r'command\s*=\s*"([^"\\]*(?:\\.[^"\\]*)*)"', re.DOTALL)
        pattern1_regex = re.compile(r'-j\s+(\S+)')
        pattern2_regex_f = re.compile(r'-f\s+(s3://\S+)')
        pattern2_regex_t = re.compile(r'-t\s+(s3://\S+)')

        fetch_count = len(json_details)
        print(f"Total imports to process: {fetch_count}")



        for import_detail in json_details:
            print(f"Pending to fetch: {fetch_count}")
            location = import_detail['location']
            dag_id = import_detail['dag_id']
            dag_file_name = import_detail['dag_file_name']
            json_file_name=import_detail['json_file_name']
            file_import = import_detail['import']
            single_import = import_detail['single_import']
            operator_count = import_detail['operator_count']
            source_type = import_detail['source_type']
            target_detail = import_detail['target_details']
            source_details = import_detail['source_details']


            print(f"Operator is {single_import} and {operator_count}")

            if single_import == "SSHOperator" and operator_count > 1:
                with open(os.path.join(location, dag_file_name), "r") as f:
                    dag_file = f.read()


                    ssh_operators = ssh_operator_regex.findall(dag_file)

                    for var_name, ssh_operator_details in ssh_operators:
                        formatted_details = f"{var_name} = SSHOperator({ssh_operator_details.strip()})"
                        print(formatted_details)
                        command_match = command_regex.search(ssh_operator_details)
                        command = command_match.group(0) if command_match else 'N/A'

                        Job_pattern_1 = pattern1_regex.search(command)
                        Pattern_Value1 = Job_pattern_1.group(1) if Job_pattern_1 else 'N/A'

                        # Job_pattern_2 = pattern2_regex.search(command)
                        # Pattern_Value2 = Job_pattern_2.group(1) if Job_pattern_2 else 'N/A'
                        job_pattern_2_f = pattern2_regex_f.search(command)
                        job_pattern_2_t = pattern2_regex_t.search(command)
                        if job_pattern_2_f:
                            Pattern_Value2 = job_pattern_2_f.group(1)
                        elif job_pattern_2_t:
                            Pattern_Value2 = job_pattern_2_t.group(1)
                        else:
                            Pattern_Value2 = 'N/A'

                        print(Pattern_Value1)

                        for xls_file in XMLFolderLst:

                            location = os.path.dirname(xls_file)
                            file_name = os.path.basename(xls_file).replace(".xls", ".csv")
                            job_name = Pattern_Value1
                            with open(xls_file, "r") as f:
                                txs = f.read()
                                # print(txs)
                                match_database_name = re.search(job_name + ",(\w*),", txs)
                                if match_database_name:
                                    print("Database has find :",match_database_name.group(1))
                                    database_name = match_database_name.group(1)

                                    ssh_details.append({
                                        'Location': location,
                                        'Dag_ID': dag_id,
                                        'Task_ID':var_name,
                                        'Dag_File_Name': dag_file_name,
                                        'Json_File_Name': json_file_name,
                                        'Import': file_import,
                                        'Single_Import': single_import,
                                        'Source_Details': source_details,
                                        'Source_Type': source_type,
                                        'Target_Details': target_detail,
                                        'SSHOperator_Details': formatted_details,
                                        'Command':command,
                                        'Job_name':Pattern_Value1,
                                        'Database_Name':database_name,
                                        'AWS URL':Pattern_Value2,
                                        'Operator_Count': operator_count,
                                    })
                                    break

            else:
                ssh_details.append({
                    'Location': location,
                    'Dag_ID': dag_id,
                    'Task_ID': "NA",
                    'Dag_File_Name': dag_file_name,
                    'Json_File_Name': json_file_name,
                    'Import': file_import,
                    'Single_Import': single_import,
                    'Source_Details': source_details,
                    'Source_Type': source_type,
                    'Target_Details': target_detail,
                    'SSHOperator_Details': "NA",
                    'Command': "NA",
                    'Job_name': "NA",
                    'Database_Name': "NA",
                    'AWS URL': "NA",
                    'Operator_Count': operator_count,
                })

        return ssh_details

    def fetch_k8_info_details(self, all_details):
        print("Fetch K8 details ")
        k8_details=[]
        k8_operator_regex=re.compile(r"(\w+)\s*=\s*launch_k8s_api_job_operator\(([^&]*)\)")
        container_command_regex =re.compile("\s*container_command\s*=\s*[^;\n]*")
        fetch_count = len(all_details)
        print(f"Total imports to process: {fetch_count}")


        for import_detail in all_details:
            print(f"Pending to fetch: {fetch_count}")
            location = import_detail['Location']
            dag_id = import_detail['Dag_ID']
            dag_file_name = import_detail['Dag_File_Name']
            json_file_name=import_detail['Json_File_Name']
            file_import = import_detail['Import']
            single_import = import_detail['Single_Import']
            operator_count = import_detail['Operator_Count']
            source_type = import_detail['Source_Type']
            target_detail = import_detail['Target_Details']
            source_details = import_detail['Source_Details']
            SSHOperator_Details = import_detail['SSHOperator_Details']
            command = import_detail['Command']
            Job_name = import_detail['Job_name']
            Database_Name = import_detail['Database_Name']
            AWS_URL = import_detail['AWS URL']
            Task_ID= import_detail['Task_ID']


            print(f"Operator is {single_import} and {operator_count}")


            if single_import == "launch_k8s_api_job_operator" and operator_count > 1:
                with open(os.path.join(location, dag_file_name), "r") as f:
                    dag_file = f.read()
                    # print(dag_file)

                    k8_operators = k8_operator_regex.findall(dag_file,
                                                               re.DOTALL)
                    print(k8_operators)
                    for var_name, k8_operator_details in k8_operators:
                        formatted_details = f"{var_name} = SSHOperator({k8_operator_details.strip()})"
                        print(formatted_details)
                        command_match=container_command_regex.search(k8_operator_details)
                        # print(command)
                        K8_command = command_match.group(0) if command_match else 'N/A'
                        # if "--sql_files" in command :
                        k8_details.append({
                            'Location': location,
                            'Dag_ID': dag_id,
                            'Task_ID': Task_ID,
                            'Dag_File_Name': dag_file_name,
                            'Json_File_Name': json_file_name,
                            'Import': file_import,
                            'Single_Import': single_import,
                            'Source_Details': source_details,
                            'Source_Type': source_type,
                            'Target_Details': target_detail,
                            'SSHOperator_Details': SSHOperator_Details,
                            'Command': K8_command.strip(),
                            'Job_name': Job_name,
                            'Database_Name': Database_Name,
                            'AWS URL': AWS_URL,
                            'Operator_Count': operator_count,
                        })
                        break

            else:
                k8_details.append({
                    'Location': location,
                    'Dag_ID': dag_id,
                    'Task_ID': Task_ID,
                    'Dag_File_Name': dag_file_name,
                    'Json_File_Name': json_file_name,
                    'Import': file_import,
                    'Single_Import': single_import,
                    'Source_Details': source_details,
                    'Source_Type': source_type,
                    'Target_Details': target_detail,
                    'SSHOperator_Details': SSHOperator_Details,
                    'Command': command.strip(),
                    'Job_name': Job_name,
                    'Database_Name': Database_Name,
                    'AWS URL': AWS_URL,
                    'Operator_Count': operator_count
                })

        return k8_details

# with open(sql_file, "r") as f:


multiple_info = dag_info()

