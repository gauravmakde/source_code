import os
# import time
import glob
import re
import json
import shutil
import pandas as pd

from common.config_parser import *
from common.common_fun import common_obj
# print("%%%%%%%%%%%%%")
# print(sql_folder_name)
# def camel_Case_app_name(input,final_app_name=""):
#     dic_final_app_name=[]
#     for single_split in input.split("-"):
#         single_split=single_split.title()
#         dic_final_app_name.append(single_split)
#     final_app_name="-".join(dic_final_app_name)
#     final_app_name=final_app_name.replace("App","APP")
#     final_app_name = final_app_name.replace("isf", "ISF")
#     return final_app_name
def remove_hyperlinks(text):
    if isinstance(text, str) and 'HYPERLINK' in text:
        # Regex to match the display text in the HYPERLINK formula
        match = re.search(r'HYPERLINK\(".*?"\s*,\s*"(.*?)"\)', text)
        if match:
            return match.group(1)  # Return only the display text
    return text
def creating_all_config(location,user_config_folder,output_directory,error,CSV_file_Path,directory,app_name):

    print(f"Csv file is :{CSV_file_Path}")
    print(CSV_file_Path)
    df= pd.read_csv(CSV_file_Path)
    try:
        status_app_id_got = False
        for index,row in df.iterrows():

            Project_Name=row['Project Name']
            AppId = row['AppId']
            gcp_project = row['GCP Project']
            Service_Accounts = row['Service Accounts']
            PROD_buckets = row['PROD Buckets']
            Nonprod_Buckets = row['NONPRODBuckets']
            NONPRO_Subnet = row['NONPROD Subnet']
            Prod_Subnet = row['Prod Subnet']
            # print("#############")
            # print(AppId)
            file_app_id=app_name.split("-")[0]
            print(file_app_id)
            # print("#############")
            if AppId.lower()==file_app_id.lower():
                print("App_id we got")
                print(PROD_buckets)
                status_app_id_got=True

                with open(testing_config,"r") as tst_files:
                    tst_file=tst_files.read()
                with open(delivery_config,"r") as delivery_files:
                    delivery_file=delivery_files.read()

                # print(tst_file)
                # print(delivery_file)
                all_project=str(gcp_project).split()
                all_Service_Accounts = str(Service_Accounts).split()
                all_PROD_buckets=PROD_buckets.split()
                # print(all_PROD_buckets)
                all_Nonprod_Buckets = str(Nonprod_Buckets).split()

                for single_project in all_project:
                    if 'nonprod' in single_project:
                        nonprod_gcp_project=single_project
                    else:
                        prod_gcp_project = single_project

                # print(nonprod_gcp_project,prod_gcp_project)
                for single_service_account in all_Service_Accounts:
                    if 'nonprod' in single_service_account:
                        nonprod_Sa=single_service_account
                    else:
                        prod_Sa = single_service_account

                for single_PROD_buckets in all_PROD_buckets:
                    if 'internal' in single_PROD_buckets:
                        prod_bucket=single_PROD_buckets

                for single_all_Nonprod_Buckets in all_Nonprod_Buckets:

                    if 'internal' in single_all_Nonprod_Buckets:
                        nonprod_bucket=single_all_Nonprod_Buckets

                prod_delivery_file=delivery_file.format(gcp_project=prod_gcp_project,subnet=Prod_Subnet,service_Account=prod_Sa,bucket=prod_bucket,dataplex_project="jwn-nap-dataplex-prod-emue",app_id=AppId.lower(),nauth="nauth-connection-prod",gcp_con="gcp-connection-prod")
                # print("%%%%%%%%%%%%%%%%%%%%%%%%%")
                # print(prod_delivery_file)
                prod_delivery_file=prod_delivery_file.replace("dev","prod")
                nonprod_delivery_file = delivery_file.format(gcp_project=nonprod_gcp_project,subnet=NONPRO_Subnet,service_Account=nonprod_Sa,bucket=nonprod_bucket,dataplex_project="jwn-nap-dataplex-nonprod-lpyu",app_id=AppId.lower(),nauth="nauth-connection-nonprod",gcp_con="gcp-connection-nonprod")
                # print("&&&&&&&&&&&&&&")
                # print(location)
                location_without_dags = os.path.dirname(location)
                # print(location_without_dags)
                # Paths setup
                location_env_config = os.path.join(location_without_dags, user_config_folder)

                # print(location_env_config)
                location_env_config= location_env_config.replace(Input_directory,output_directory)
                # print("To do")
                # print(location_env_config)
                if not os.path.exists(os.path.join(location_env_config)):
                        os.makedirs(os.path.join(location_env_config))

                # print(os.path.join,location_env_config))
                with open(os.path.join(location_env_config,'env-configs-development.cfg'),"w") as cfgs:
                    cfgs.write(tst_file)
                with open(os.path.join(location_env_config,'env-configs-production.cfg'),"w") as cfgs:
                    cfgs.write(prod_delivery_file)
                with open(os.path.join(location_env_config, 'delivery-env-configs-development.cfg'), "w") as cfgs:
                    cfgs.write(nonprod_delivery_file)

        if status_app_id_got==False:

            print("It dont got app id")
            with open(testing_config, "r") as tst_files:
                tst_file = tst_files.read()
            with open(delivery_config, "r") as delivery_files:
                delivery_file = delivery_files.read()

            # print(tst_file)
            location_without_dags = os.path.dirname(location)
            location_env_config = os.path.join(location_without_dags, user_config_folder)
            location_env_config = location_env_config.replace(Input_directory, output_directory)

            if not os.path.exists(os.path.join( location_env_config)):
                os.makedirs(os.path.join( location_env_config))

            # print(os.path.join(Output_directory,location_env_config))
            with open(os.path.join( location_env_config, 'env-configs-development.cfg'), "w") as cfgs:
                cfgs.write(tst_file)
            # with open(os.path.join(Output_directory, location_env_config, 'env-configs-development.cfg'), "w") as cfgs:
            #     cfgs.write(tst_file)"
            prod_delivery_file = delivery_file.format(gcp_project="", subnet="",
                                                      service_Account="", bucket="",
                                                      dataplex_project="jwn-nap-dataplex-prod-emue",
                                                      app_id=AppId.lower(), nauth="nauth-connection-prod",
                                                      gcp_con="gcp-connection-prod")

            prod_delivery_file = prod_delivery_file.replace("dev", "prod")
            nonprod_delivery_file = delivery_file.format(gcp_project="", subnet="",
                                                         service_Account="", bucket="",
                                                         dataplex_project="jwn-nap-dataplex-nonprod-lpyu",
                                                         app_id=AppId.lower(), nauth="nauth-connection-nonprod",
                                                         gcp_con="gcp-connection-nonprod")

            with open(os.path.join(location_env_config, 'env-configs-production.cfg'), "w") as cfgs:
                cfgs.write(prod_delivery_file)
            with open(os.path.join(location_env_config, 'delivery-env-configs-development.cfg'), "w") as cfgs:
                cfgs.write(nonprod_delivery_file)

    except Exception as e:

        print("Error:",str(e))
        error.append({
            'location': "",
            'file_name': "",
            'error': str(e)
        })

def copy_all_json(PyFolderLst, kafka_json_folder,Output_directory,error):
    print(Output_directory)
    # exit()
    for file in PyFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        # print("Location of file:", location)
        # print("file name :", file_name)
        # app_name = os.path.basename(os.path.dirname(os.path.dirname(file)))
        location_without_dags = os.path.dirname(location)
        # Paths setup
        location_kafka_json = os.path.join(location_without_dags, kafka_json_folder)
        print(location_kafka_json)
        output_with_new_app=location_kafka_json.replace(Input_directory,Output_directory)


        development_kafka_json_output_folder = os.path.join(output_with_new_app, 'development')
        # print(f"JSON Output Folder: {development_kafka_json_output_folder}")
        production_kafka_json_output_folder = os.path.join(output_with_new_app, 'production')

        # delivery_kafka_json_output_folder = os.path.join(output_with_new_app, 'del_dev')
        print(f"JSON Output Folder: {production_kafka_json_output_folder}")

        if not os.path.exists(development_kafka_json_output_folder):
            os.makedirs(development_kafka_json_output_folder)  # Create the directory if it doesn't exist

        if not os.path.exists(production_kafka_json_output_folder):
            os.makedirs(production_kafka_json_output_folder)  #

        # if not os.path.exists(delivery_kafka_json_output_folder):
        #     os.makedirs(delivery_kafka_json_output_folder)  #

        source_files = glob.glob(os.path.join(location_kafka_json, "*.json"))
        # print(f"Source Files: {source_files}")

        for file_path in source_files:
            # Normalize paths to ensure consistency
            file_path = os.path.normpath(file_path)
            development_destination_path = os.path.join(development_kafka_json_output_folder, os.path.basename(file_path))
            development_destination_path = os.path.normpath(development_destination_path)

            development_destination_dir = os.path.dirname(development_destination_path)
            if not os.path.exists(development_destination_dir):
                # print(f"Creating directory: {development_destination_dir}")
                os.makedirs(development_destination_dir)

            production_destination_path = os.path.join(production_kafka_json_output_folder,os.path.basename(file_path))
            production_destination_path = os.path.normpath(production_destination_path)

            production_destination_dir = os.path.dirname(production_destination_path)
            if not os.path.exists(production_destination_dir):
                # print(f"Creating directory: {development_destination_dir}")
                os.makedirs(production_destination_dir)

            # delivery_destination_path = os.path.join(delivery_kafka_json_output_folder,os.path.basename(file_path))
            # delivery_destination_path = os.path.normpath(delivery_destination_path)
            #
            # delivery_destination_dir = os.path.dirname(delivery_destination_path)
            # if not os.path.exists(delivery_destination_dir):
            #     # print(f"Creating directory: {development_destination_dir}")
            #     os.makedirs(delivery_destination_dir)
            try:
                shutil.copy(file_path, development_destination_path)
                print(f"Copied: {file_path} to {development_destination_path}")
            except Exception as e:
                print("len of the file: ",len(development_destination_path))
                print(f"Error copying {file_path} to {development_destination_path}: {e}")
                error.append({

                    'location': location,
                    'file_name': file_name,
                    'comment': f"Error copying {file_path} to {development_destination_path}: {e}"

                })
            try:
                shutil.copy(file_path, production_destination_path)
                print(f"Copied: {file_path} to {production_destination_path}")
            except Exception as e:
                print("len of the file: ",len(production_destination_path))
                print(f"Error copying {file_path} to {production_destination_path}: {e}")
                error.append({

                    'location': location,
                    'file_name': file_name,
                    'comment': f"Error copying {file_path} to {development_destination_path}: {e}"

                })
            # try:
            #     shutil.copy(file_path, delivery_destination_path)
            #     print(f"Copied: {file_path} to {delivery_destination_path}")
            # except Exception as e:
            #     print("len of the file: ", len(delivery_destination_path))
            #     print(f"Error copying {file_path} to {delivery_destination_path}: {e}")
            #     error.append({
            #
            #         'location': location,
            #         'file_name': file_name,
            #         'comment': f"Error copying {file_path} to {delivery_destination_path}: {e}"
            #
            #     })
def xml_fetching(XML_file_Path,job_name):
    print("XML Fetching started")
    XMLFolderLst = glob.glob(XML_file_Path, recursive=True)
    print(XMLFolderLst)

    for xls_file in XMLFolderLst:

        location = os.path.dirname(xls_file)
        file_name = os.path.basename(xls_file).replace(".xls", ".csv")

        with open(xls_file, "r") as f:
            txs = f.read()
            # print(txs)
        match_database_name = re.search(job_name + ",(\w+),(\w+),", txs)
        if match_database_name:

            dataset_name = match_database_name.group(1)
            table_name = match_database_name.group(2)

            break

    return (dataset_name,table_name)


def copy_all_files_except(PyFolderLst):
    error=[]
    """
    Copies files and directories from a source directory to a destination directory,
    excluding specified directories.

    :param PyFolderLst: List of files whose parent directories will be used as the source.
    """

    for file in PyFolderLst:
        base_location = os.path.normpath(os.path.dirname(file))  # Normalize path
        file_name= os.path.basename(file)
        parent_location = os.path.dirname(base_location)  # Get the parent directory
        out_directory_location = parent_location.replace(Input_directory, Output_directory)  # Destination path

        print(f"Output directory location: {out_directory_location}")

        # Ensure the parent location exists
        try:
            if not os.path.exists(parent_location):
                print(f"Base location does not exist: {parent_location}")
                continue
            elif not os.path.isdir(parent_location):
                print(f"Base location is not a directory: {parent_location}")
                continue
        except Exception as e:
            print(f"Error while checking parent location: {e}")
            continue

        # Create output directory if it doesn't exist
        try:
            if not os.path.exists(out_directory_location):
                print(f"Creating output directory: {out_directory_location}")
                os.makedirs(out_directory_location)
        except Exception as e:
            print(f"Error while creating output directory: {e}")
            continue

        try:
            # Walk through the directory structure
            for root, dirs, files in os.walk(parent_location):
                print(f"Processing Root: {root}")
                print(f"Original Directories: {dirs}")
                print(f"Original Files: {files}")

                # Filter out excluded folders
                dirs[:] = [d for d in dirs if d not in excluded_folders]

                print(f"Filtered Directories: {dirs}")

                # Copy files in the current directory
                for file in files:
                    try:
                        print(f"Processing file: {file}")
                        src_file = os.path.join(root, file)
                        print(f"Source File: {src_file}")

                        relative_path = os.path.relpath(root, parent_location)
                        dest_dir = os.path.join(out_directory_location, relative_path)
                        dest_file = os.path.join(dest_dir, file)

                        # Create destination sub-directory if needed
                        if not os.path.exists(dest_dir):
                            print(f"Creating destination directory: {dest_dir}")
                            os.makedirs(dest_dir)

                        # Copy file
                        shutil.copy2(src_file, dest_file)
                        print(f"Copied: {src_file} to {dest_file}")
                    except Exception as e:
                        print(f"Error while copying file {file}: {e}")
                        error.append({
                            'location': base_location,
                            'file_name': file_name,
                            'error': str(e)
                        })

                # Log skipped excluded folders
                skipped_dirs = [d for d in dirs if d in excluded_folders]
                if skipped_dirs:
                    print(f"Skipped excluded directories: {skipped_dirs}")
        except Exception as e:
            print(f"Error while walking through the directory structure: {e}")
            error.append({
                'location' : base_location,
                'file_name':file_name,
                'error': str(e)
            })
    return error

def fetch_sql_file_name(all_json_file,spark_Sql,dag_id,base_location):
        print("Need to fetch the sql file name ")
        # print(f"location is {base_location}")
        # base_location = os.path.dirname(base_location)
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
                        # print(os.path.join(base_location, sql_folder_name, directory, sql_name))
                        length_sql = len(os.path.join(base_location, sql_folder_name, directory, sql_name))
                        list_sql_file = glob.glob(os.path.join(base_location, sql_folder_name, directory, sql_name))
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
                                print(fetch_sql_file)
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
        # print("%%%%%%%%")
        # print(sql_name, sql_location)
        return sql_name, sql_location
def json_Change_kf_BQ(location_kafka_json,XML_file_Path,project_id,production_kafka_json_output_folder,job_name_dict,dag_id):

    data=[]
    print("Json copy started ")
    # print(job_name_dict)
    dataset_name=""
    table_name=""
    re_source_details = r"--\s*source_tables_details\s*'([^']*)'"
    re_target_details = r"--\s*target_tables_details\s*'([^']*)'"
    re_app_config_file = r"--\s*app_config_file\s*([^\n]*)"
    re_sql_file_location = r"--\s*sql_file_location\s*([^\n]*)"
    re_consumer_group_name = r"--\s*consumer_group_name\s*([^\n]*)"
    re_aws_secret_manager_vault_roleid_arn= r"--\s*aws_secret_manager_vault_roleid_arn\s*([^\n]*)"
    re_aws_secret_manager_vault_secretid_arn = r"--\s*aws_secret_manager_vault_secretid_arn\s*([^\n]*)"
    re_aws_user_role_arn = r"--\s*aws_user_role_arn\s*([^\n]*)"
    re_hive_offset_table = r"(--\s*hive_offset_table\s*([^\n]*))"
    re_offset_table= r"(--\s*hive_offset_table\s*)"
    re_delivery_app_config_file=r"--\s*app_config_file\s*(.*)\/(.*).json"
    re_delivery_sql_file_location = r"--\s*sql_file_location\s*(.*.sql)"

    # if job_name!="In Json":
    # print(len(location_kafka_json))
    development_kafka_json_file_location = os.path.join(location_kafka_json)
    print(development_kafka_json_file_location)
    development_specific_json_file = glob.glob(development_kafka_json_file_location, recursive=True)
    print(development_specific_json_file)
    production_kafka_json_file_location = os.path.join(production_kafka_json_output_folder)
    production_specific_json_file = glob.glob(production_kafka_json_file_location, recursive=True)


    # all_prod_dev=development_specific_json_file+production_specific_json_file
    all_dev= development_specific_json_file
    target_table=""

    # else:
    #     development_kafka_json_file_location = os.path.join(location_kafka_json)
    #     development_specific_json_file = glob.glob(development_kafka_json_file_location, recursive=True)
    #
    #     production_kafka_json_file_location = os.path.join(production_kafka_json_output_folder )
    #     production_specific_json_file = glob.glob(production_kafka_json_file_location, recursive=True)
    #
    #     # all_prod_dev = development_specific_json_file + production_specific_json_file
    #
    #     all_dev = development_specific_json_file
    #
    #     print("All json are:",all_dev)


    # if all_prod_dev:
    if all_dev:
        print("Json file is present")


        # for single_json in all_prod_dev:
        for single_json in all_dev:
            file_name = os.path.basename(single_json)
            location = os.path.dirname(single_json)


            try:
                with open(single_json) as file_content:
                    file_contents = file_content.read()

                source_match = re.search(re_source_details, file_contents)
                target_match = re.search(re_target_details, file_contents)
                app_config_file_match = re.search(re_app_config_file, file_contents)
                sql_file_location_match = re.search(re_sql_file_location, file_contents)
                consumer_group_name_match = re.search(re_consumer_group_name, file_contents)
                aws_secret_manager_vault_roleid_arn_match = re.search(re_aws_secret_manager_vault_roleid_arn, file_contents)
                aws_secret_manager_vault_secretid_arn_match = re.search(re_aws_secret_manager_vault_secretid_arn, file_contents)
                aws_user_role_arn_match = re.search(re_aws_user_role_arn, file_contents)
                hive_offset_table_match= re.search(re_hive_offset_table, file_contents)
                offset_table_match = re.search(re_offset_table, file_contents)



                source_details = source_match.group(1) if source_match else None
                target_details = target_match.group(1) if target_match else None
                app_config_files = app_config_file_match.group(1) if app_config_file_match else None
                sql_file_locations= sql_file_location_match.group(1) if sql_file_location_match else None
                consumer_group_names = consumer_group_name_match.group(1) if consumer_group_name_match else None
                aws_secret_manager_vault_roleid_arns = aws_secret_manager_vault_roleid_arn_match.group(1) if aws_secret_manager_vault_roleid_arn_match else None
                aws_secret_manager_vault_secretid_arn = aws_secret_manager_vault_secretid_arn_match.group(1) if aws_secret_manager_vault_secretid_arn_match else None
                aws_user_role_arn = aws_user_role_arn_match.group( 1) if aws_user_role_arn_match else None
                hive_offset_table = hive_offset_table_match.group(2) if hive_offset_table_match else None
                offset_tables = offset_table_match.group(1) if offset_table_match else None


                existing_target_details=target_details
                delivery_existing_target_details=target_details
                existing_source_details = source_details
                existing_app_config_files=app_config_files
                existing_sql_file_locations = sql_file_locations
                existing_consumer_group_names = consumer_group_names
                existing_aws_secret_manager_vault_roleid_arns = aws_secret_manager_vault_roleid_arns
                existing_aws_secret_manager_vault_secretid_arn = aws_secret_manager_vault_secretid_arn
                existing_aws_user_role_arn = aws_user_role_arn
                existing_hive_offset_table = hive_offset_table
                existing_offset_tables = offset_tables

                #
                # print("Source Details:", source_details)
                # print("Target Details:", target_details)
                # print("app_config_file:",app_config_files)
                # print("sql_file_locations: ",sql_file_locations)
                # print("consumer_group_names:", consumer_group_names)
                # print("aws_secret_manager_vault_roleid_arns:", aws_secret_manager_vault_roleid_arns)
                # print("aws_secret_manager_vault_secretid_arn:", aws_secret_manager_vault_secretid_arn)

                app_config_files=app_config_files.replace(app_config_files,"gs://test-data-datametica/app09392/isf-artifacts/app-config/emr_application_dev.json")
                file_contents=file_contents.replace(existing_app_config_files,app_config_files)

                hive_folder_sql = sql_file_locations.split("sql/")[1]

                # print(hive_folder_sql)
                offset_tables = offset_tables.replace(offset_tables, "--offset_table ")
                # print(offset_tables)

                file_contents = file_contents.replace(existing_offset_tables, offset_tables)
                # print(file_contents)
                user_config_location = os.path.dirname(location)
                base_location = os.path.dirname(user_config_location)
                folder_json_location = os.path.join(base_location, json_folder, "*.json")
                all_json_file = glob.glob(folder_json_location)
                sql_name, list_sql_location = fetch_sql_file_name(all_json_file, hive_folder_sql, dag_id, base_location)
                if sql_name:
                    hive_folder_sql = sql_name
                    # print("%%%%%%%")
                    # print(hive_folder_sql)
                # print(hive_folder_sql)
                if 'gs://test-data-datametica' not in sql_file_locations:
                    sql_file_locations=sql_file_locations.replace(sql_file_locations,"gs://test-data-datametica/app09392/isf-artifacts/sql/"+hive_folder_sql)
                    file_contents = file_contents.replace(existing_sql_file_locations, sql_file_locations)


                if  consumer_group_names!=None and 'onix-' not in consumer_group_names :
                    consumer_group_names=consumer_group_names.replace(consumer_group_names,"onix-"+consumer_group_names)
                    file_contents = file_contents.replace(existing_consumer_group_names, consumer_group_names)


                if 'jwn-nap-dataplex-nonprod-lpyu' not in hive_offset_table:
                    # file_contents=re.sub(re_hive_offset_table,"'`jwn-nap-dataplex-nonprod-lpyu."+hive_offset_table+"`'")
                    # hive_offset_table=hive_offset_table.replace(hive_offset_table,"'`jwn-nap-dataplex-nonprod-lpyu."+hive_offset_table.strip()+"`'") # remove if we need offset value dynamic from file

                    hive_offset_table=hive_offset_table.replace(hive_offset_table,"'`jwn-nap-user1-nonprod-zmnb.onehop_etl_app_db.kafka_consumer_offset_batch_details`'")
                    file_contents = file_contents.replace(existing_hive_offset_table, hive_offset_table)


                aws_secret_manager_vault_roleid_arns=aws_secret_manager_vault_roleid_arns.replace(aws_secret_manager_vault_roleid_arns,"arn:aws:secretsmanager:us-west-2:290445963451:secret:APP09392_VAULT_APPROLE_ROLE_ID-Psikfr")
                file_contents = file_contents.replace(existing_aws_secret_manager_vault_roleid_arns, aws_secret_manager_vault_roleid_arns)
                #
                aws_secret_manager_vault_secretid_arn=aws_secret_manager_vault_secretid_arn.replace(aws_secret_manager_vault_secretid_arn,"arn:aws:secretsmanager:us-west-2:290445963451:secret:APP09392_VAULT_APPROLE_SECRET_ID-themT6")
                file_contents = file_contents.replace(existing_aws_secret_manager_vault_secretid_arn, aws_secret_manager_vault_secretid_arn)
                #
                aws_user_role_arn=aws_user_role_arn.replace(aws_user_role_arn,"arn:aws:iam::290445963451:role/APP09392_AWS_Secrets_Role")
                file_contents = file_contents.replace(existing_aws_user_role_arn, aws_user_role_arn)


                if 'aws_user_role_external_id' not in file_contents :
                    file_contents=file_contents.strip()+"\n--aws_user_role_external_id=testUser\n"
                #
                if 'gcp_project_id' not in file_contents :
                    file_contents=file_contents+"--gcp_project_id jwn-nap-user1-nonprod-zmnb\n"

                # print(file_contents)

                with open(single_json, "w") as file_content:
                    file_content.write(file_contents)

                # print(target_details)
                if "\"source_type\": \"KAFKA\"," in source_details or "\"source_type\": \"TERADATA\","  in target_details:
                    print("This has kafka or teradata")
                    app_config_files = app_config_files.replace(app_config_files,"gs://test-data-datametica/app09392/isf-artifacts/app-config/emr_application_dev.json")
                    file_contents = file_contents.replace(existing_app_config_files, app_config_files)

                    hive_folder_sql = sql_file_locations.split("sql/")[1]


                    user_config_location = os.path.dirname(location)
                    base_location=os.path.dirname(user_config_location)
                    folder_json_location = os.path.join(base_location, json_folder, "*.json")
                    all_json_file = glob.glob(folder_json_location)
                    sql_name, list_sql_location = fetch_sql_file_name(all_json_file, hive_folder_sql, dag_id,base_location)
                    if sql_name:
                        hive_folder_sql=sql_name
                        # print("%%%%%%%")
                        # print(hive_folder_sql)
                    # print(hive_folder_sql)
                    offset_tables = offset_tables.replace(offset_tables, "--offset_table ")
                    file_contents = file_contents.replace(existing_offset_tables, offset_tables)

                    if 'gs://test-data-datametica' not in sql_file_locations:
                        sql_file_locations = sql_file_locations.replace(sql_file_locations,"gs://test-data-datametica/app09392/isf-artifacts/sql/" + hive_folder_sql)

                        file_contents = file_contents.replace(existing_sql_file_locations, sql_file_locations)
                        # print(file_contents)

                    if  consumer_group_names!=None and 'onix-' not in consumer_group_names :
                        consumer_group_names = consumer_group_names.replace(consumer_group_names,
                                                                            "onix1-" + consumer_group_names)
                        file_contents = file_contents.replace(existing_consumer_group_names, consumer_group_names)

                    if 'jwn-nap-dataplex-nonprod-lpyu' not in hive_offset_table:
                        # file_contents=re.sub(re_hive_offset_table,"'`jwn-nap-dataplex-nonprod-lpyu."+hive_offset_table+"`'")
                        # hive_offset_table = hive_offset_table.replace(hive_offset_table,"'`jwn-nap-dataplex-nonprod-lpyu." + hive_offset_table.strip() + "`'")
                        hive_offset_table=hive_offset_table.replace(hive_offset_table,"'`jwn-nap-user1-nonprod-zmnb.onehop_etl_app_db.kafka_consumer_offset_batch_details`'")

                        file_contents = file_contents.replace(existing_hive_offset_table, hive_offset_table)

                    aws_secret_manager_vault_roleid_arns = aws_secret_manager_vault_roleid_arns.replace(
                        aws_secret_manager_vault_roleid_arns,
                        "arn:aws:secretsmanager:us-west-2:290445963451:secret:APP09392_VAULT_APPROLE_ROLE_ID-Psikfr")
                    file_contents = file_contents.replace(existing_aws_secret_manager_vault_roleid_arns,
                                                          aws_secret_manager_vault_roleid_arns)
                    #
                    aws_secret_manager_vault_secretid_arn = aws_secret_manager_vault_secretid_arn.replace(
                        aws_secret_manager_vault_secretid_arn,
                        "arn:aws:secretsmanager:us-west-2:290445963451:secret:APP09392_VAULT_APPROLE_SECRET_ID-themT6")
                    file_contents = file_contents.replace(existing_aws_secret_manager_vault_secretid_arn,
                                                          aws_secret_manager_vault_secretid_arn)
                    #
                    aws_user_role_arn = aws_user_role_arn.replace(aws_user_role_arn,
                                                                  "arn:aws:iam::290445963451:role/APP09392_AWS_Secrets_Role")
                    file_contents = file_contents.replace(existing_aws_user_role_arn, aws_user_role_arn)

                    if 'aws_user_role_external_id' not in file_contents:
                        file_contents = file_contents.strip() + "\n--aws_user_role_external_id=testUser\n"
                    #
                    if 'gcp_project_id' not in file_contents:
                        file_contents = file_contents + "--gcp_project_id jwn-nap-user1-nonprod-zmnb\n"

                    re_kafka_key = r"(\w+\s*)=\s*({[^}]*})"

                    kafka_group = re.search(re_kafka_key, source_details)

                    if kafka_group:
                        kafka_key_name = kafka_group.group(2)
                        json_kafka_value = json.loads(kafka_key_name)

                        keys_to_remove = ['start_from_timestamp_utc', 'date_format', 'offset_strategy']
                        for key in keys_to_remove:
                            json_kafka_value.pop(key,None)  # Use None as default to avoid KeyError if the key doesn't exist

                        json_kafka_value.update({
                            'vault_connection_name': 'nordsecrets/application/APP09392/meadow/onix/oauth',
                            'kafka.sasl.mechanism': 'OAUTHBEARER'
                        })
                        updated_job_name = kafka_group.group(1) + "=" + json.dumps(json_kafka_value)
                        source_details=source_details.replace(source_details,updated_job_name)
                        file_contents = file_contents.replace(existing_source_details, source_details)
                        # print("$$$$$$$$$$$")
                        # print(file_contents)
                        with open(single_json, "w") as file_content:
                            file_content.write(file_contents)


                        re_job_name = r"(\w+\s*=\s*{[^}]*})"

                        all_job_name = re.findall(re_job_name, target_details) if target_details else None
                        # print("All job name:",all_job_name )
                        # print(target_details)

                        if all_job_name!=None  and "\"source_type\": \"S3\"," in target_details :

                            print("Teradata ssh")
                            for full_single_job_name in all_job_name:


                                re_single_job_name = r"(\w+)\s*=\s*({[^}]*})"
                                single_job_name_group = re.search(re_single_job_name, full_single_job_name)

                                if single_job_name_group:
                                    target_key_name = single_job_name_group.group(1)
                                    single_job_name = single_job_name_group.group(2)

                                    # print("target_key_name:", target_key_name)
                                    # print("single_job_name:", single_job_name)
                                    # print("Json File name is :", file_name)
                                    # print("Location  is :", location)

                                    if '_err' not in target_key_name:

                                        try:
                                            # Parse the job JSON
                                            # print(job_name_dict)
                                            for job_name,s3_path in job_name_dict.items():
                                                # print(job_name)
                                                # print(s3_path)
                                                json_single_job_name = json.loads(single_job_name)
                                                compare_s3_path = "/".join(s3_path.rsplit('/')[:-1])
                                                # print(compare_s3_path)
                                                target_s3_path=json_single_job_name['path']+target_key_name
                                                # print("Target s3 path:", target_s3_path)
                                                # print("compare s3 path:", compare_s3_path)
                                                if (target_s3_path==compare_s3_path.replace("/*","")):
                                                    # print("Both are same")
                                                    # print(s3_path)
                                                    # print(compare_s3_path)
                                                    # print(target_s3_path)
                                                    # print(json_single_job_name['path'])
                                                    # print("The comparison we got ")
                                                    dataset_name,table_name=xml_fetching(XML_file_Path,job_name) # xml fetching

                                                    keys_to_remove = ['format', 'header', 'sep', 'path', 'compression', 'quoteAll' ,'emptyValue']
                                                    for key in keys_to_remove:
                                                        json_single_job_name.pop(key, None)

                                                    json_single_job_name.update({
                                                        'source_type': 'BIGQUERY',
                                                        'project': project_id,
                                                        'dataset': dataset_name,
                                                        'table': table_name,
                                                        'mode': 'overwrite',
                                                        'temp_bucket': 'test-data-datametica'
                                                    })

                                                    # print(json_single_job_name)
                                                    # print(target_key_name)
                                                    updated_job_name = target_key_name + "=" + json.dumps(json_single_job_name)
                                                    # print(updated_job_name)
                                                    # print(target_details)
                                                    target_details = target_details.replace(full_single_job_name, updated_job_name)
                                                    file_contents=file_contents.replace(existing_target_details,target_details)
                                                    # print(file_contents)
                                                    # print(single_json)
                                                    # file_contents = file_contents.replace("--hive_offset_table", "--offset_table")
                                                    with open(single_json, "w") as file_content:
                                                        file_content.write(file_contents)
                                                    existing_target_details = target_details
                                        except Exception as e:
                                            # print(e)
                                            data.append({

                                                'location': location,
                                                'file_name': file_name,
                                                'comment': f"Error fetching the  {file_name} location: {location}: {e}"

                                            })
                                    else:
                                        # print("It has error s3 file")

                                        # print(job_name_dict)
                                        json_single_job_name = json.loads(single_job_name)

                                        updated_job_name = target_key_name + "=" + json.dumps(json_single_job_name)

                                        target_s3_path = json_single_job_name['path']
                                        path_match = re.search(r"s3:\/\/[^\/]*(.*)\/", target_s3_path)
                                        # print(path_match)
                                        final_path=""
                                        if path_match:
                                            error_bucket=path_match.group(1)
                                            final_path="gs://test-data-datametica"+ error_bucket +"/"
                                            # print(final_path)
                                        json_single_job_name.update({
                                            'source_type': 'gs',
                                            'path':final_path

                                        })

                                        updated_job_name = target_key_name + "=" + json.dumps(json_single_job_name)
                                        target_details = target_details.replace(full_single_job_name, updated_job_name)
                                        file_contents = file_contents.replace(existing_target_details, target_details)
                                        with open(single_json, "w") as file_content:
                                            file_content.write(file_contents)
                                        # print("existing_target_details:",existing_target_details)
                                        existing_target_details=target_details
                                        # break
                        # print("========================")
                        # print(target_details)
                        if all_job_name!=None  and  "\"source_type\": \"TERADATA\","  in target_details:

                            # print("It has teradata target")
                            count=0
                            for full_single_job_name in all_job_name:
                                if "\"source_type\": \"TERADATA\"," in full_single_job_name:
                                    if count>0:
                                        # this is for multiple target teradata
                                        self_target_match = re.search(re_target_details, file_contents)
                                        self_target_details = self_target_match.group(1) if self_target_match else None
                                        existing_target_details = self_target_details
                                    # print("All job name:", all_job_name)
                                    # print("Single_job_name:",full_single_job_name)
                                    # print("existing target:",existing_target_details)
                                    re_single_job_name = r"(\w+)\s*=\s*({[^}]*})"
                                    single_job_name_group = re.search(re_single_job_name, full_single_job_name)

                                    if single_job_name_group:
                                        target_key_name = single_job_name_group.group(1)
                                        single_job_name = single_job_name_group.group(2)
                                        # print(target_key_name)
                                        try:
                                            # Parse the job JSON
                                            re_dataset="\"database_name\"\s*:\s*\"(\w+)\""
                                            re_table_name="\"data_source_name\"\s*:\s*\"(\w+)\""


                                            dataset_match=re.search(re_dataset,single_job_name)

                                            table_name_match = re.search(re_table_name, single_job_name)

                                            dataset_name=dataset_match.group(1) if dataset_match else None
                                            table_name = table_name_match.group(1) if table_name_match else None

                                            json_single_job_name = json.loads(single_job_name)

                                            # print(json_single_job_name)
                                            keys_to_remove = ['format', 'header', 'sep', 'path', 'compression', 'quoteAll','vault_connection_name','hostname','port','emptyValue']
                                            for key in keys_to_remove:
                                                json_single_job_name.pop(key, None)
                                            json_single_job_name['dataset'] = json_single_job_name.pop('database_name')
                                            json_single_job_name['table'] = json_single_job_name.pop('data_source_name')
                                            json_single_job_name.update({
                                                'source_type': 'BIGQUERY',
                                                'project': project_id,
                                                'mode': 'overwrite',
                                                'temp_bucket': 'test-data-datametica'
                                            })

                                            updated_job_name = target_key_name + "=" + json.dumps(json_single_job_name)
                                            # print(updated_job_name)
                                            target_details = target_details.replace(full_single_job_name, updated_job_name)
                                            #
                                            # print("existing target",existing_target_details)
                                            # print("Updated target",target_details)

                                            file_contents=file_contents.replace(existing_target_details,target_details)

                                            # print(file_contents)
                                            # file_contents = file_contents.replace("--hive_offset_table", "--offset_table")
                                            with open(single_json, "w") as file_content:
                                                file_content.write(file_contents)
                                            existing_target_details = target_details
                                            count=count+1
                                            # break


                                        except Exception as e:

                                            # print(e)
                                            data.append({

                                                'location': location,
                                                'file_name': file_name,
                                                'comment': f"Error fetching the  {file_name} location: {location}: {e}"

                                            })
            except Exception as e:
                print(e)
                data.append({

                    'location': location,
                    'file_name': file_name,
                    'comment': f"Error fetching the  {file_name} location: {location}: {e}"

                })

    if production_specific_json_file:
        print("Need to change the production config file ")
        # print(target_details)
        prod_bucket_internal=""
        prod_bucket_temp=""
        for single_json in production_specific_json_file:
            file_name = os.path.basename(single_json)
            location = os.path.dirname(single_json)
            app_name = os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(single_json))))
            file_app_id = app_name.split("-")[0]
            # print("Json File name is :",file_name)
            # print("Location  is :", location)
            # print("App name is :",file_app_id)


            try:
                with open(single_json) as file_content:
                    file_contents = file_content.read()

                source_match = re.search(re_source_details, file_contents)
                app_config_file_match = re.search(re_app_config_file, file_contents)
                sql_file_location_match = re.search(re_sql_file_location, file_contents)
                consumer_group_name_match = re.search(re_consumer_group_name, file_contents)
                aws_secret_manager_vault_roleid_arn_match = re.search(re_aws_secret_manager_vault_roleid_arn, file_contents)
                aws_secret_manager_vault_secretid_arn_match = re.search(re_aws_secret_manager_vault_secretid_arn, file_contents)
                aws_user_role_arn_match = re.search(re_aws_user_role_arn, file_contents)
                hive_offset_table_match= re.search(re_hive_offset_table, file_contents)
                offset_table_match = re.search(re_offset_table, file_contents)


                delivery_app_config_file_match = re.search(re_delivery_app_config_file, file_contents)
                delivery_sql_file_location_match = re.search(re_delivery_sql_file_location, file_contents)

                source_details = source_match.group(1) if source_match else None
                app_config_files = app_config_file_match.group(1) if app_config_file_match else None
                sql_file_locations= sql_file_location_match.group(1) if sql_file_location_match else None
                consumer_group_names = consumer_group_name_match.group(1) if consumer_group_name_match else None
                aws_secret_manager_vault_roleid_arns = aws_secret_manager_vault_roleid_arn_match.group(1) if aws_secret_manager_vault_roleid_arn_match else None
                aws_secret_manager_vault_secretid_arn = aws_secret_manager_vault_secretid_arn_match.group(1) if aws_secret_manager_vault_secretid_arn_match else None
                aws_user_role_arn = aws_user_role_arn_match.group( 1) if aws_user_role_arn_match else None
                hive_offset_table = hive_offset_table_match.group(2) if hive_offset_table_match else None
                offset_tables = offset_table_match.group(1) if offset_table_match else None

                delivery_app_config_tables = delivery_app_config_file_match.group(1) if delivery_app_config_file_match else None
                delivery_app_config_tables_json = delivery_app_config_file_match.group(2) if delivery_app_config_file_match else None
                delivery_sql_file_location_tables = delivery_sql_file_location_match.group(1) if delivery_sql_file_location_match else None

                # existing_target_details=target_details
                # existing_source_details = source_details
                # existing_app_config_files=app_config_files
                # existing_sql_file_locations = sql_file_locations
                existing_consumer_group_names = consumer_group_names
                # existing_aws_secret_manager_vault_roleid_arns = aws_secret_manager_vault_roleid_arns
                # existing_aws_secret_manager_vault_secretid_arn = aws_secret_manager_vault_secretid_arn
                # existing_aws_user_role_arn = aws_user_role_arn
                existing_hive_offset_table = hive_offset_table
                existing_offset_tables = offset_tables
                existing_delivery_app_config_tables = delivery_app_config_tables
                existing_delivery_sql_file_location_match = delivery_sql_file_location_tables
                hive_folder_sql = sql_file_locations.split("sql/")[1]

                # print("Source Details:", source_details)
                # print("Target Details:", target_details)
                # print("app_config_file:",app_config_files)
                # print("sql_file_locations: ",sql_file_locations)
                # print("consumer_group_names:", consumer_group_names)
                # print("aws_secret_manager_vault_roleid_arns:", aws_secret_manager_vault_roleid_arns)
                # print("aws_secret_manager_vault_secretid_arn:", aws_secret_manager_vault_secretid_arn)
                # print("existing app_config_files:",existing_delivery_app_config_tables)
                # print("existing sql_file_location:", existing_delivery_sql_file_location_match)

                user_config_location = os.path.dirname(location)
                base_location = os.path.dirname(user_config_location)
                folder_json_location = os.path.join(base_location, json_folder, "*.json")
                all_json_file = glob.glob(folder_json_location)
                sql_name, list_sql_location = fetch_sql_file_name(all_json_file, hive_folder_sql, dag_id, base_location)
                if sql_name:
                    hive_folder_sql = sql_name
                    # print("%%%%%%%")
                    # print(hive_folder_sql)
                # print("$$$$$$$$$$$$$$")
                # print(hive_folder_sql)
                offset_tables = offset_tables.replace(offset_tables, "--offset_table ")
                # print("Existing target:",delivery_existing_target_details)
                # print("target details:", target_details)

                file_contents = file_contents.replace(existing_offset_tables, offset_tables)


                # print(delivery_existing_target_details)
                # print(file_contents)

                if "s3:"  in existing_delivery_app_config_tables:
                    delivery_app_config_tables=f"gs://{dataplex_project_id}/app09203/isf-artifacts/app-config"
                    file_contents = file_contents.replace(existing_delivery_app_config_tables, delivery_app_config_tables)
                    # print(file_contents)


                if 'jwn-nap-dataplex-nonprod-lpyu' not in hive_offset_table:
                    # file_contents=re.sub(re_hive_offset_table,"'`jwn-nap-dataplex-nonprod-lpyu."+hive_offset_table+"`'")
                    hive_offset_table = hive_offset_table.replace(hive_offset_table, "'`jwn-nap-dataplex-prod-emue." + hive_offset_table.strip() + "`'")
                    file_contents = file_contents.replace(existing_hive_offset_table, hive_offset_table)

                if  consumer_group_names!=None and 'gcp-' not in consumer_group_names :
                    consumer_group_names=consumer_group_names.replace(consumer_group_names,"gcp-"+consumer_group_names)
                    file_contents = file_contents.replace(existing_consumer_group_names, consumer_group_names)


                df=pd.read_csv(CSV_file_Path)
                for index, row in df.iterrows():
                    Project_Name = row['Project Name']
                    AppId = row['AppId']
                    gcp_project = row['GCP Project']
                    Service_Accounts = row['Service Accounts']
                    PROD_buckets = row['PROD Buckets']
                    Nonprod_Buckets = row['NONPRODBuckets']
                    NONPRO_Subnet = row['NONPROD Subnet']
                    Prod_Subnet = row['Prod Subnet']
                    # print(AppId)
                    # print(str(PROD_buckets).strip()=="")
                    all_PROD_buckets = str(PROD_buckets).split()
                    # print("&&&&&&&&&&&&&")
                    # print(all_PROD_buckets)

                    all_project = str(gcp_project).split()
                    if AppId.lower()==file_app_id.lower():

                        for single_PROD_buckets in all_PROD_buckets:
                            if 'internal' in single_PROD_buckets:
                                prod_bucket_internal = single_PROD_buckets
                            elif 'temp' in single_PROD_buckets:
                                prod_bucket_temp = single_PROD_buckets

                        if 'test-data-datametica' in target_details:

                            target_details = target_details.replace("test-data-datametica", prod_bucket_temp)
                            # print(target_details)
                            # print(file_contents)
                            # print(existing_target_details)
                            file_contents = file_contents.replace(delivery_existing_target_details,target_details)
                            # print(file_contents)
                            delivery_existing_target_details=target_details
                        # user_config_location = os.path.dirname(location)
                        # base_location = os.path.dirname(user_config_location)
                        # folder_json_location = os.path.join(base_location, json_folder, "*.json")
                        # all_json_file = glob.glob(folder_json_location)
                        # sql_name, list_sql_location = fetch_sql_file_name(all_json_file, hive_folder_sql, dag_id,base_location)
                        # if sql_name:
                        #     hive_folder_sql = sql_name

                        # print(existing_delivery_sql_file_location_match)
                        # print(delivery_app_config_tables_json)
                        if "s3:" in existing_delivery_sql_file_location_match:
                            delivery_app_config_tables = f"gs://{prod_bucket_internal}/{file_app_id.lower()}/isf-artifacts/{hive_folder_sql}"
                            # print(delivery_app_config_tables)
                            file_contents = file_contents.replace(existing_delivery_sql_file_location_match,delivery_app_config_tables)

                        if 'gcp_project_id' not in file_contents:
                            for single_project in all_project:
                                if 'nonprod' in single_project:
                                    nonprod_gcp_project = single_project
                                else:
                                    prod_gcp_project = single_project
                            gcp_name=f"--gcp_project_id {prod_gcp_project}"
                            # print(gcp_name)

                            file_contents = file_contents + f"--gcp_project_id {prod_gcp_project}\n"
                            # print(file_contents)


                        break
                # Replace all occurrences of the project ID directly in file_contents
                file_contents = file_contents.replace("jwn-nap-user1-nonprod-zmnb", "jwn-nap-dataplex-prod-emue")

                # Write the modified contents back to the file
                with open(single_json, "w", encoding="utf-8") as file_content:
                    file_content.write(file_contents)

            except Exception as e:
                # print(e)
                data.append({

                    'location': location,
                    'file_name': file_name,
                    'comment': f"Error fetching the  {file_name} location: {location}: {e}"

                })
    return data