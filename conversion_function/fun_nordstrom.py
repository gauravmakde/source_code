
from ast import Break
from conversion_function.common.config_parser import *
from conversion_function.common.common_fun import *
import os
import pandas as pd
import black


class Nordstrom:

    def fun_Delivered_patch(self, file_list):
        # try:
        if 1==1:
            # print("file_list", file_list)
            for file in [file for file in file_list if os.path.isfile(file)]:
                # print("file", file)
                if '.py' in file and ('/dags/' in file or '\\dags\\' in file):
                    # print("file", file)
                    file_obj = common_obj.Read_file(file)

                    ## App Name
                    find_app_name = re.findall(r'\.split\(\'(APP\d+.*?)\'\)', file_obj, flags=flagID)
                    # print("find_app_name", find_app_name)
                    if find_app_name:
                        find_app_name = find_app_name[0]

                        if '-gcp-' not in find_app_name:
                            app_name = sub.subI(r"(APP)(\d+)(.*?)", r"APP\2-gcp\3", find_app_name)
                            # print("app_name", app_name)

                            file_obj = file_obj.replace(find_app_name, app_name)
                            # print("file_obj", file_obj)

                    ## = Space 
                    file_obj = sub.subI(r"(\w+)\s*\=\s*(\w+)", r"\1 = \2", file_obj)
                    file_obj = sub.subI(r"(\w+)\s*\=\s*\'(\w+)", r"\1 = '\2", file_obj)
                    file_obj = sub.subI(r"(\w+)\s*\=\s*\"(\w+)", r'\1 = "\2', file_obj)

                    ## Cron
                    file_obj = sub.subI(r"cron\s*\=.*", """cron = None if config.get(dag_id, 'cron').lower() == 'none' else config.get(dag_id, 'cron')""", file_obj)

                    ## Schedule_interval
                    file_obj = sub.subI(r"schedule_interval\s*\=.*", "schedule_interval = cron,", file_obj)

                    ## Metastore_service_path
                    # file_obj = sub.subID(r"etl_jar_version\'\)", "etl_jar_version')\nmetastore_service_path = config.get('dataproc', 'metastore_service_path')", file_obj)

                    # file_obj = sub.subID(r"subnet_url\,\s*\}", """subnet_url,\n                            },\n                            "peripherals_config": {"metastore_service": metastore_service_path}""", file_obj)

                    ## livy_ remove
                    file_obj = sub.subI(r"livy_", '', file_obj)

                    ## Email changes
                    file_obj = sub.subI(r"\#\s*\'email\'\s*\:\s*None\s+if", "'email': None if", file_obj)
                    file_obj = sub.subI(r"\'email\'\s*\:\s*None\s+if.*", """'email': None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, "email"),""", file_obj)

                    
                    ## config_env change
                    file_obj = sub.subI(r"config_env\s+\=\s+\'development\'\s+if\s+airflow_environment.*", 'config_env = ("development" if airflow_environment in ["nonprod", "development"] else "production")', file_obj)

                    ## Remove commnet
                    file_obj = sub.subI(r"\#\s*\'--aws_user_role_external_id\'", "'--aws_user_role_external_id'", file_obj)
                    file_obj = sub.subI(r"\#\s*dag_sla", "dag_sla", file_obj)
                    file_obj = sub.subI(r"\#\s*\'sla\'", "'sla'", file_obj)
                    file_obj = sub.subI(r"\#\s*source_table", "source_table", file_obj)
                    file_obj = sub.subI(r"\'email_on_failure\'\s*\:\s*False.*", """'email_on_failure': config.getboolean(dag_id, 'email_on_failure'),""", file_obj)
                    file_obj = sub.subI(r"\'email_on_retry\'\s*\:\s*False.*", """'email_on_retry': config.getboolean(dag_id, 'email_on_retry'),""", file_obj)

                    file_obj = sub.subI(r"\#\s*\'email_on_failure\'\s*\:\s*.*", "", file_obj)
                    file_obj = sub.subI(r"\#\s*\'email_on_retry\'\s*\:\s*.*", "", file_obj)


                    ## teradata remove
                    final_obj = ''
                    for line in file_obj.splitlines():
                        # Skip lines with 'consumer_group_name ' directly
                        if 'consumer_group_name ' in line or 'dag_id = ' in line:
                            final_obj += line + '\n'
                            continue

                        # Process lines containing '.json, .sql'
                        if '.json' in line or '.sql' in line:
                            find_json_file = re.search(r'\w+\.json|\w+\.sql', line, flags=flagI)
                            # print("find_json_file", find_json_file)
                            if find_json_file:
                                find_json_file = find_json_file.group()
                                # print("find_json_file", find_json_file)
                                new_find_json_file = sub.subI(r'teradata_|td_', 'bq_', find_json_file)
                                line = line.replace(find_json_file, new_find_json_file)

                        # Process all other lines
                        else:
                            line = sub.subI(r'teradata', 'bigquery', line)

                        final_obj += line + '\n'

                    # if Black_format_dag == 'Y':
                    #     final_obj = black.format_str(final_obj, mode=black.FileMode())  # Added formater for dag formater

                    common_obj.Write_file(file, final_obj)

                    ## Add gcp in file name
                    directory, filename = os.path.split(file)
                    if 'gcp_' not in file:    
                        os.rename(file, os.path.join(directory, 'gcp_'+filename))

            ## Folder name
            fd = [fd for fd in file_list if os.path.isdir(fd) and re.findall(r'APP\d+.*?', fd, flags=flagI)]
            # print("fd", fd)
            if fd:
                for fd1 in fd:
                    # print("fd11", fd1)
                    find1 = re.findall(r'app\d+[\-a-zA-Z0-9]+$', fd1, flags=flagID)
                    # print("find1",find1)
                    if '-gcp-' not in fd1 and find1:
                        # print("fd11", fd1)
                        new_fd = sub.subI(r"(APP)(\d+)(.*?)", r"APP\2-gcp\3", fd1)
                        # print("new_fd", new_fd)
                        os.rename(fd1, new_fd)


        # except Exception as e:
        #     print("Error in processing: class-Nordstrom Fun-fun_Delivered_patch", e)

    def fun_Config_changes(self, file_list):
        try:
        # if 1==1:
            app_id_lst=[]
            for file in [file for file in file_list if os.path.isfile(file) and '.py' in file and ('/dags/' in file or '\\dags\\' in file)]:
                # print("file", file)
                dag_id = ''
                sql_file_lst = []
                file_obj = common_obj.Read_file(file)
                # print("file_obj", file_obj)

                ## Dag Id  Name
                dag_id = re.findall(r'dag_id\s*\=\s*[\'\"\(\s*](\w+)[\s*\'\"\)]', file_obj, flags=flagID)
                # print("dag_id", dag_id)
                dag_id = dag_id[0]
                # print("dag_id", dag_id)

                app_id = re.findall(r'(app\d+)\-', file_obj, flags=flagID)[0]
                # print("app_id", app_id)
                app_id_lst.append(app_id.lower())

            app_id_lst=list(set(app_id_lst))
            # print("app_id_lst", app_id_lst)

            for app_ele in app_id_lst:
                dev_final_code=''
                prod_final_code=''
                for file in [file for file in file_list if os.path.isfile(file) and '.py' in file and ('/dags/' in file or '\\dags\\' in file)]:
                    if app_ele.lower() in file.lower():
                        # print("file", file)
                        dag_id = ''
                        sql_file_lst = []
                        file_obj = common_obj.Read_file(file)
                        # print("file_obj", file_obj)

                        ## Dag Id  Name
                        dag_id = re.findall(r'dag_id\s*\=\s*[\'\"\(\s*](\w+)[\s*\'\"\)]', file_obj, flags=flagID)
                        # print("dag_id", dag_id)
                        dag_id = dag_id[0]
                        # print("dag_id", dag_id)

                        app_id = re.findall(r'(app\d+)\-', file_obj, flags=flagID)[0]
                        # print("app_id", app_id)

                        find_sql_file = re.findall(r"\/(\w+\.sql)", file_obj, flags=flagI)
                        # print("find_sql_file", find_sql_file)
                        sql_file_lst.extend(find_sql_file)
                        # if find_sql_file:

                        # print("dag_id", dag_id)
                        # print("sql_file_lst", sql_file_lst)

                        sql_param_lst = []
                        for file in [file for file in file_list if os.path.isfile(file) and '.sql' in file and ('/sql/' in file or '\\sql\\' in file)]:
                            # print("file", file)

                            for dag_sql in sql_file_lst:
                                if dag_sql == os.path.basename(file) and app_id.lower() in file.lower():
                                    # print("ww")
                                    # print("file", file)
                                    sql_file_obj = common_obj.Remove_comments(file)
                                    # print("sql_file_obj", sql_file_obj)

                                    find_param = list(set(re.findall(r"\{\{params.(\w+)\}\}", sql_file_obj, flags=flagID)))
                                    # print("find_param", find_param)
                                    if find_param:
                                        sql_param_lst.extend(find_param)

                        sql_param_lst=list(set(sql_param_lst))
                        # print("sql_param_lst", sql_param_lst)

                        for cfg_file in [file for file in file_list if os.path.isfile(file) and '.cfg' in file and '-specific-' in file and app_id.lower() in file.lower()]:
                            if '-specific-dev' in cfg_file:
                                # print("cfg_file", cfg_file)
                                cfg_file_obj = common_obj.Read_file(cfg_file)

                                ## email_on_failure / email_on_retry
                                cfg_file_obj = sub.subI(r"email_on_failure\s*\=\s*None", 'email_on_failure=False', cfg_file_obj)
                                cfg_file_obj = sub.subI(r"email_on_retry\s*\=\s*None", 'email_on_retry=False', cfg_file_obj)

                                ## = remove space
                                cfg_file_obj = sub.subI(r"\s*\=\s*", '=', cfg_file_obj)

                                find_con_dag1 = re.findall(r'(\[' +dag_id+ '_db_param\])(.*?)\[', cfg_file_obj, flags=flagID)
                                find_con_dag2 = re.findall(r'(\[' +dag_id+ '_db_param\])(.*?)\s*$', cfg_file_obj, flags=flagID)
                                find_con_dag = find_con_dag1+find_con_dag2
                                # print("find_con_dag", len(find_con_dag))

                                if find_con_dag:
                                    find_con_dag = find_con_dag[0][1]+'\n'
                                    # print("find_con_dag", find_con_dag)

                                    final_con = []
                                    for ele in sql_param_lst:
                                        # print("ele", ele)
                                        find_param_in_conf = re.findall(r'('+ele+'\=.*?)\n', find_con_dag, flags=flagID)
                                        # print("find_param_in_conf", find_param_in_conf)
                                        if find_param_in_conf:
                                            final_con.append(find_param_in_conf[0])

                                    final_con = sorted(final_con)
                                    final_con = '\n'.join(final_con)
                                    
                                    # print("final_con", final_con)
                                    # print("find_con_dag", find_con_dag)
                                    # print('[' +dag_id+ '_db_param]'+find_con_dag)
                                    # print('\n\n[' +dag_id+ '_db_param]'+'\n'+final_con+'\n\n')
                                    # print("cfg_file_obj---", cfg_file_obj)
                                    # cfg_file_obj = cfg_file_obj.replace('[' +dag_id+ '_db_param]'+find_con_dag, '\n\n[' +dag_id+ '_db_param]'+'\n'+final_con+'\n\n')

                                    find_pro_id = re.findall(r'(\[' +dag_id+ '].*?)\[', cfg_file_obj, flags=flagID)
                                    # print("find_pro_id", find_pro_id[0])
                                    if final_con:
                                        cfg_file_obj =  find_pro_id[0]+'\n[' +dag_id+ '_db_param]'+'\n'+final_con+'\n\n'
                                        # print("cfg_file_obj---", cfg_file_obj)
                                        dev_final_code+=cfg_file_obj
                                    else:
                                        cfg_file_obj =  find_pro_id[0]
                                        dev_final_code+=cfg_file_obj

                            elif '-specific-prod' in cfg_file:
                                # print("cfg_file", cfg_file)
                                cfg_file_obj = common_obj.Read_file(cfg_file)

                                ## email_on_failure / email_on_retry
                                cfg_file_obj = sub.subI(r"email_on_failure\s*\=\s*None", 'email_on_failure=False', cfg_file_obj)
                                cfg_file_obj = sub.subI(r"email_on_retry\s*\=\s*None", 'email_on_retry=False', cfg_file_obj)

                                ## = remove space
                                cfg_file_obj = sub.subI(r"\s*\=\s*", '=', cfg_file_obj)

                                find_con_dag1 = re.findall(r'(\[' +dag_id+ '_db_param\])(.*?)\[', cfg_file_obj, flags=flagID)
                                find_con_dag2 = re.findall(r'(\[' +dag_id+ '_db_param\])(.*?)\s*$', cfg_file_obj, flags=flagID)
                                find_con_dag = find_con_dag1+find_con_dag2
                                # print("find_con_dag", len(find_con_dag), find_con_dag)

                                if find_con_dag:
                                    find_con_dag = find_con_dag[0][1]+'\n'
                                    # print("find_con_dag", find_con_dag)

                                    final_con = []
                                    for ele in sql_param_lst:
                                        # print("ele", ele)
                                        find_param_in_conf = re.findall(r'('+ele+'\=.*?)\n', find_con_dag, flags=flagID)
                                        # print("find_param_in_conf", find_param_in_conf)
                                        if find_param_in_conf:
                                            final_con.append(find_param_in_conf[0])

                                    final_con = sorted(final_con)
                                    final_con = '\n'.join(final_con)
                                    
                                    # print("final_con", final_con)
                                    # print("find_con_dag", find_con_dag)
                                    # print('[' +dag_id+ '_db_param]'+find_con_dag)
                                    # print('\n\n[' +dag_id+ '_db_param]'+'\n'+final_con+'\n\n')
                                    # print("cfg_file_obj---", cfg_file_obj)
                                    # cfg_file_obj = cfg_file_obj.replace('[' +dag_id+ '_db_param]'+find_con_dag, '\n\n[' +dag_id+ '_db_param]'+'\n'+final_con+'\n\n')

                                    find_pro_id = re.findall(r'(\[' +dag_id+ '].*?)\[', cfg_file_obj, flags=flagID)
                                    # print("find_pro_id", find_pro_id[0])
                                    if final_con:
                                        cfg_file_obj =  find_pro_id[0]+'\n[' +dag_id+ '_db_param]'+'\n'+final_con+'\n\n'
                                        # print("cfg_file_obj---", cfg_file_obj)
                                        prod_final_code+=cfg_file_obj
                                    else:
                                        cfg_file_obj =  find_pro_id[0]
                                        prod_final_code+=cfg_file_obj

                # print("dev_final_code", dev_final_code)
                # print("prod_final_code", prod_final_code)
                for cfg_file in [file for file in file_list if os.path.isfile(file) and '.cfg' in file and '-specific-' in file and app_id.lower() in file.lower()]:
                    # print("cfg_file", cfg_file)
                    if '-specific-dev' in cfg_file:
                        common_obj.Write_file(cfg_file, dev_final_code)
                    elif '-specific-prod' in cfg_file:
                        common_obj.Write_file(cfg_file, prod_final_code)

        except Exception as e:
            print("Error in processing: class-Nordstrom Fun-fun_Config_changes", e)

    def fun_Creating_all_config(self, file_list):

        try:            
            # CSV_file_Path = [file for file in file_list if os.path.isfile(file) and file.endswith('export.csv')][0]
            CSV_file_Path = os.getcwd()+"/data_files/export.csv"
            # print("CSV_file_Path", CSV_file_Path)
        except:
            print(f"Put export.csv file in the data_files folder")

        try:
            # delivery_config = [file for file in file_list if os.path.isfile(file) and file.endswith('delivery-env-configs.cfg')][0]
            delivery_config = os.getcwd()+"/data_files/delivery-env-configs.cfg"
            # print("delivery_config", delivery_config)
        except:
            print(f"Put delivery-env-configs.cfg file in the data_files folder")

        df= pd.read_csv(CSV_file_Path)
        # print(df)

        whole_path = [file for file in file_list if os.path.isfile(file) and ('/dags/' in file or '\\dags\\' in file)]
        # print("whole_path", whole_path)
        set_wp_path=[]
        for wp in whole_path:
            # print("wp", wp)
            wp=wp.split('dags')
            # print("wp", wp)
            set_wp_path.append(wp[0])
        set_wp_path=list(set(set_wp_path))
        # print("set_wp_path", set_wp_path)

        for app_folder_path in set_wp_path:

            # app_folder_path = whole_path.split('dags')[0]
            app_folder_path=app_folder_path
            # print("app_folder_path", app_folder_path)

            app_name = re.findall(r"APP\d+", app_folder_path, flags=flagI)[0]
            # print("app_name", app_name)
            file_app_id=app_name

            # try:
            if 1==1:
                not_found_app_id=set()
                for index,row in df.iterrows():
                    Project_Name=row['Project Name']
                    AppId = row['AppId']
                    gcp_project = row['GCP Project']
                    Service_Accounts = row['Service Accounts']
                    PROD_buckets = row['PROD Buckets']
                    Nonprod_Buckets = row['NONPRODBuckets']
                    NONPRO_Subnet = row['NONPROD Subnet']
                    Prod_Subnet = row['Prod Subnet']

                    # print("Project_Name:", Project_Name)
                    # print("AppId:", AppId)
                    # print("gcp_project:", gcp_project)
                    # print("Service_Accounts:", Service_Accounts)
                    # print("PROD_buckets:", PROD_buckets)
                    # print("Nonprod_Buckets:", Nonprod_Buckets)
                    # print("NONPRO_Subnet:", NONPRO_Subnet)
                    # print("Prod_Subnet:", Prod_Subnet)

                    # print(file_app_id)

                    if AppId.lower()==file_app_id.lower():
                        # print("ININ app")
                    
                        with open(delivery_config,"r") as delivery_files:
                            delivery_file=delivery_files.read()
                        # print("$$$$$$$$$$$$")
                        all_project=str(gcp_project).split()
                        all_Service_Accounts = str(Service_Accounts).split()
                    
                        all_PROD_buckets=str(PROD_buckets).split()
                        # print("all_PROD_buckets", all_PROD_buckets)
                        all_Nonprod_Buckets = str(Nonprod_Buckets).split()

                        for single_project in all_project:
                            # print(single_project)
                            if 'nonprod' in single_project:
                                nonprod_gcp_project=single_project
                            else:
                                prod_gcp_project = single_project

                        for single_service_account in all_Service_Accounts:
                            # print(single_project)
                            if 'nonprod' in single_service_account:
                                nonprod_Sa=single_service_account
                            else:
                                prod_Sa = single_service_account

                        prod_bucket=''
                        nonprod_bucket=''
                        for single_PROD_buckets in all_PROD_buckets:
                            # print(single_project)
                            if 'internal' in single_PROD_buckets:
                                prod_bucket=single_PROD_buckets
                
                        for single_all_Nonprod_Buckets in all_Nonprod_Buckets:
                            # print(single_project)
                            if 'internal' in single_all_Nonprod_Buckets:
                                nonprod_bucket=single_all_Nonprod_Buckets


                        # print(prod_gcp_project,nonprod_gcp_project)
                        prod_delivery_file=delivery_file.format(gcp_project=prod_gcp_project,subnet=Prod_Subnet,service_Account=prod_Sa,bucket=prod_bucket,dataplex_project="jwn-nap-dataplex-prod-emue",app_id=AppId.lower(),nauth="nauth-connection-prod",gcp_con="gcp-connection-prod")
                        prod_delivery_file=prod_delivery_file.replace("dev","prod")
                        nonprod_delivery_file = delivery_file.format(gcp_project=nonprod_gcp_project,subnet=NONPRO_Subnet,service_Account=nonprod_Sa,bucket=nonprod_bucket,dataplex_project="jwn-nap-dataplex-nonprod-lpyu",app_id=AppId.lower(),nauth="nauth-connection-nonprod",gcp_con="gcp-connection-nonprod")
                        # print("prod_delivery_file", prod_delivery_file)
                        # print("nonprod_delivery_file", nonprod_delivery_file)

                        # location_without_dags = os.path.dirname(location)
                        # Paths setup
                        location_env_config = os.path.join(app_folder_path, 'configs')
                        # print("location_env_config", location_env_config)

                        if not os.path.exists(os.path.join(location_env_config)):
                                os.makedirs(os.path.join(location_env_config))
                        # with open(os.path.join(location_env_config,'env-configs-development.cfg'),"w") as cfgs:
                        #     cfgs.write(tst_file)
                        with open(os.path.join(location_env_config, 'env-configs-production.cfg'), "w", encoding='utf-8') as cfgs:
                            cfgs.write(prod_delivery_file)
                        with open(os.path.join(location_env_config, 'env-configs-development.cfg'), "w", encoding='utf-8') as cfgs:
                            cfgs.write(nonprod_delivery_file)

                    else:
                        not_found_app_id.add(file_app_id)
                        # print("file_app_id not found in the export.csv", file_app_id)

            # print("class-Nordstrom Fun-fun_Creating_all_config APP_ID not found in the export.csv", not_found_app_id)

        # except Exception as e:
        #     print("Error in processing: class-Nordstrom Fun-fun_Creating_all_config", e)

    def fun_Json_changes(self, file_list, Raw_InputFolderPath):
        # try:
        if 1==1:
            app_id_lst=[]
            whole_path = [file for file in file_list if os.path.isfile(file) and os.path.join('dags', '') in file]
            # print("whole_path", whole_path)
            for app_ele in whole_path:

                file_app_id = re.findall(r"APP\d+", app_ele, flags=flagI)
                # print("file_app_id", file_app_id)
                app_id_lst.append(file_app_id[0])

            app_id_lst = list(set(app_id_lst))
            # print("app_id_lst", app_id_lst)

            raw_filelst = common_obj.FileList(Raw_InputFolderPath)
            raw_json_lst = [file for file in raw_filelst if os.path.isfile(file) and '.json' in file]

            # print("raw_json_lst", len(raw_json_lst), raw_json_lst)
            for file_app_id in app_id_lst:
                # print("file_app_id.lower()", file_app_id.lower())
                user_config = [file for file in file_list if os.path.isfile(file) and (os.path.join('user-config', '') in file or os.path.join('user_config', '') in file) and file_app_id.lower()+'-' in file.lower()]
                # print("user_config", len(user_config), user_config)
                if user_config:
                    for user_json in user_config:
                        if os.path.join('production', '') in user_json or os.path.join('prod', '') in user_json:
                            # print("user_json", user_json)
                            for raw_json in raw_json_lst:
                                # print("raw_json", raw_json)
                                # print("file_app_id.lower()", file_app_id.lower())
                                if os.path.basename(user_json) in raw_json and file_app_id.lower()+'-' in raw_json.lower():
                                    # print("raw_json", raw_json)
                                    raw_json_file_obj = common_obj.Read_file(raw_json)
                                    # print(raw_json_file_obj)

                                    find_roleid_arn = re.findall(r'--aws_secret_manager_vault_roleid_arn.*', raw_json_file_obj, flags=flagI)
                                    find_secretid_arn = re.findall(r'--aws_secret_manager_vault_secretid_arn.*', raw_json_file_obj, flags=flagI)
                                    find_user_role_arn = re.findall(r'--aws_user_role_arn.*', raw_json_file_obj, flags=flagI)

                                    # print("find_roleid_arn", find_roleid_arn)
                                    # print("find_secretid_arn", find_secretid_arn)
                                    # print("find_user_role_arn", find_user_role_arn)

                                    find_source_tables_details = re.findall(r'--source_tables_details.*?\"vault_connection_name\"\:\s+\"(.*?)\".*', raw_json_file_obj, flags=flagI)
                                    # print("find_source_tables_details", find_source_tables_details)

                                    user_json_file_obj = common_obj.Read_file(user_json)
                                    # print(user_json_file_obj)

                                    ## aws_secret_manager_vault_roleid_arn
                                    ## aws_secret_manager_vault_secretid_arn
                                    ## aws_user_role_arn
                                    user_json_file_obj = sub.subI(r"\-\-aws_secret_manager_vault_roleid_arn.*", find_roleid_arn[0], user_json_file_obj)
                                    user_json_file_obj = sub.subI(r"\-\-aws_secret_manager_vault_secretid_arn.*", find_secretid_arn[0], user_json_file_obj)
                                    user_json_file_obj = sub.subI(r"\-\-aws_user_role_arn.*", find_user_role_arn[0], user_json_file_obj)

                                    user_json_file_obj = sub.subI(r"\-\-consumer_group_name\s+\w+-", '--consumer_group_name gcp-', user_json_file_obj)

                                    ## app_config_file
                                    user_json_file_obj = sub.subI(r"\-\-app_config_file\s+gs\:\/\/.*?\/", '--app_config_file gs://prod-nap-dataplex-internal/', user_json_file_obj)

                                    ## source_tables_details
                                    user_json_file_obj = sub.subI(r"(--source_tables_details.*?\"vault_connection_name\"\:\s+\")(.*?)(\".*)", r'\1'+find_source_tables_details[0]+r'\3', user_json_file_obj)

                                    ## target_tables_details
                                    user_json_file_obj = sub.subI(r"(--target_tables_details.*?\"project\"\:\s+\")(.*?)(\".*)", r'\1jwn-nap-dataplex-prod-emue\3', user_json_file_obj)

                                    common_obj.Write_file(user_json, user_json_file_obj)

                            ## 
                            try:            
                                # CSV_file_Path = [file for file in file_list if os.path.isfile(file) and file.endswith('export.csv')][0]
                                CSV_file_Path = os.getcwd()+"/data_files/export.csv"
                                # print("CSV_file_Path", CSV_file_Path)
                            except:
                                print(f"Put export.csv file in the data_files folder")

                            df = pd.read_csv(CSV_file_Path)


                            # print(df)

                            user_json_file_obj = common_obj.Read_file(user_json)

                            for index,row in df.iterrows():
                                # print(row)
                                Project_Name = row['Project Name']
                                AppId = row['AppId']
                                gcp_project = row['GCP Project']
                                PROD_buckets = row['PROD Buckets']

                                # print("AppId.lower()", AppId.lower())
                                # print("file_app_id.lower()", file_app_id.lower())
                                if AppId.lower() == file_app_id.lower():

                                    all_PROD_buckets=str(PROD_buckets).split()
                                    all_project=str(gcp_project).split()
                                    # print(all_PROD_buckets)
                                    temp_prod_bucket=''
                                    internal_prod_bucket=''
                                    for single_PROD_buckets in all_PROD_buckets:
                                        # print(single_PROD_buckets)
                                        if 'temp' in single_PROD_buckets:
                                            temp_prod_bucket=single_PROD_buckets
                                        else:
                                            internal_prod_bucket=single_PROD_buckets

                                    
                                    prod_gcp_project=''
                                    for single_project in all_project:
                                        # print(single_project)
                                        if 'nonprod' not in single_project:
                                            prod_gcp_project = single_project

                                    # print("temp_prod_bucket", temp_prod_bucket)
                                    # print("internal_prod_bucket", internal_prod_bucket)

                                    ## target_tables_details temp_bucket from export.csv
                                    user_json_file_obj = sub.subI(r"(--target_tables_details.*?\"temp_bucket\"\:\s+\")(.*?)(\".*)", r'\1'+temp_prod_bucket+r'\3', user_json_file_obj)

                                    ## sql_file_location internal bucket from export.csv
                                    user_json_file_obj = sub.subI(r"(--sql_file_location\s+gs\:\/\/)(.*?\/)(app\d+)(\/.*)", r'\1'+internal_prod_bucket+'/'+file_app_id.lower()+r'\4', user_json_file_obj)

                                    ## gcp_project_id prod project_id from export.csv
                                    user_json_file_obj = sub.subI(r"(--gcp_project_id\s+)(.*)", r'\1'+prod_gcp_project, user_json_file_obj)

                                    common_obj.Write_file(user_json, user_json_file_obj)

                        # elif os.path.join('development', '') in user_json:
                        #     pass

                        ## _teradata_, _td_ replace with _bq_ in file name
                        directory, filename = os.path.split(user_json)
                        # print(filename)
                        if '_bq_' not in user_json: 
                            filename = sub.subI(r'teradata_|td_', 'bq_', filename)
                            os.rename(user_json, os.path.join(directory, filename))

        # except Exception as e:
        #     print("Error in processing: class-Nordstrom Fun-fun_Json_changes", e)

    def fun_Sql_file_changes(self, file_list):
        try:
        # if 1==1:
            for file in [file for file in file_list if os.path.isfile(file) and '.sql' in file and ('/sql/' in file or '\\sql\\' in file)]:
                file_obj = common_obj.Read_file(file)

                ## Call upper object
                find_file_obj = re.findall(r"CALL\s+\`\{\{params\.gcp_project_id\}\}\`\.\{\{params\.\w+\}\}_\w+\.*\w+\s*\(", file_obj, flags=flagID)
                # print("find_file_obj", find_file_obj)
                if find_file_obj:
                    for ele in find_file_obj:
                        find_obj = re.findall(r"\`\{\{params\.gcp_project_id\}\}\`\.\{\{params\.\w+\}\}(_\w+\.*\w+)\s*\(", ele, flags=flagI)
                        # print("find_obj", find_obj)
                        new_ele=ele.replace(find_obj[0], find_obj[0].upper())
                        # print("ele", ele)
                        # print("new_ele", new_ele)

                        file_obj=file_obj.replace(ele, new_ele)
                
                ## jwn_udf function upper
                find_jwn_fun = re.findall(r"jwn_udf\.\w+\s*\(", file_obj, flags=flagID)
                # print("find_jwn_fun", len(find_jwn_fun), find_jwn_fun)
                if find_jwn_fun:
                    for ele in find_jwn_fun:
                        file_obj = file_obj.replace(ele, ele.upper())

                common_obj.Write_file(file, file_obj)

                ## _teradata_, _td_ replace with _bq_ in file name
                directory, filename = os.path.split(file)
                # print(filename)
                if '_bq_' not in file: 
                    filename = sub.subI(r'teradata_|td_', 'bq_', filename)
                    os.rename(file, os.path.join(directory, filename))

        except Exception as e:
            print("Error in processing: class-Nordstrom Fun-fun_Sql_file_changes", e)

    def fun_Black_format_dag(self, file_list):
        try:
        # if 1==1:
            for file in [file for file in file_list if os.path.isfile(file)]:
                # print("file", file)
                if '.py' in file and ('/dags/' in file or '\\dags\\' in file):
                    # print("file", file)
                    file_obj = common_obj.Read_file(file)

                    file_obj = black.format_str(file_obj, mode=black.FileMode())  # Added formater for dag formater

                    common_obj.Write_file(file, file_obj)

        except Exception as e:
            print("Error in processing: class-Nordstrom Fun-fun_Black_format_dag", e)            


