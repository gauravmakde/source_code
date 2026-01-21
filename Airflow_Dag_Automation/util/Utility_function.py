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

    def last_replacement(seled,match,title):
        # Check if "TITLE" is already in the matched column
        if "TITLE" in match.group(1):
            return match.group(0)  # Return the original match if it contains TITLE
        else:
            return f"{match.group(1)} TITLE '{title}'{match.group(2)})"  # Add TITLE

    def replacement(self,match,title):
        # Check if "TITLE" is already in the matched column
        if "TITLE" in match.group(1):
            return match.group(0)  # Return the original match if it contains TITLE
        else:
            return f"{match.group(1)} TITLE '{title}'{match.group(2)},"  # Add TITLE

    def datatype(self,sql):

        # find_char = re.findall(r"[\w]+[\s]+VARCHAR[\s]*\([\d]+\)|[\w]+[\s]+CHAR[\s]*\([\d]+\)|[\w]+[\s]+BYTE[\s]*\([\d]+\)|[\w]+[\s]+VARCHAR[\s]+|[\w]+[\s]+VARCHAR\)|[\w]+[\s]+VARCHAR\,|[\w]+[\s]+CHAR[\s]+|[\w]+[\s]+CHAR\,|[\w]+[\s]+CHAR\)|[\w]+[\s]+BLOB[\s]+|[\w]+[\s]+BLOB\,|[\w]+[\s]+BLOB\)|[\w]+[\s]+BLOB[\s]*\([\d]+\)|[\w]+[\s]+CLOB[\s]+|[\w]+[\s]+CLOB\,|[\w]+[\s]+CLOB\)|[\w]+[\s]+CLOB[\s]*\([\d]+\)",compress_replace, flags=re.I)
        find_datatype = re.findall(
            r"(\w+\s+(?:INTEGER|VARCHAR|BIGINT|CHAR|DECIMAL|NUMBER|FLOAT|DATE|PERIOD|BYTEINT|SMALLINT|INTERVAL|BLOB|CLOB|VARBYTE|BYTE|REAIL|TIMESTAMP|TIME).*(?:,\s*$|,\s*\n))",
            sql)


        all_datatype = []
        for single_datatype in find_datatype:

            all_datatype.append(single_datatype.strip())


        for i in all_datatype:

            if 'TITLE' not in i:
                print("Tittle not find")
                sql = re.sub(i, i + " TITTLE 'NULL',", sql)
                sql = re.sub(", TITTLE 'NULL'", " TITTLE 'NULL'", sql)

        return sql
    def fun_Enable_Title_To_TD(self,location, file_name, file):
        print(location)
        compress_replace=""
        # re_Compress=r"COMPRESS\s*\(([^()]*|\([^()]*\))*\),"
        re_Compress = r"COMPRESS\s*\(([^()]*|\([^()]*\))*\)(?:,\s*\n|\s*\n)"
        re_Tittle=""


        with open(file, "r") as f:
            input_sql = f.read()

            create_table = re.search(r"CREATE\s+\w+\s+TABLE\s+[^()]+\(.*?\);.*", input_sql, re.DOTALL)

            if create_table:

                compress_replace = re.sub(re_Compress, lambda m: ",\n" if m.group(0).endswith(",\n") else "\n", create_table.group())
                flag = 0
                for single_line in compress_replace.split("\n"):
                    if 'COLUMN COMMENTS'==single_line or flag==1:
                        flag=1
                        try:
                            title = single_line.split("|")[-1]
                            column = single_line.split("|")[-2]
                            print("Title is :", title)
                            print("Column is :", column)

                            column_pattern = r"((^\s*)\b" + re.escape(column) + r"\b\s*[^\n]*?)(,\s*$|,\s*\n)"
                            last_parn= r"((^\s*)\b" + re.escape(column) + r"\b\s*[^\n]*?)(\)\s*$|\)\s*\n)"

                            compress_replace = re.sub(r"(\b" + re.escape(column) + r"\b\s*[^\n]*)( TITLE '[^']*')?", r"\1",
                                                      compress_replace, flags=re.MULTILINE)

                            compress_replace = re.sub(column_pattern, lambda m: self.replacement(m, title), compress_replace, flags=re.MULTILINE)

                            compress_replace = re.sub(last_parn, lambda m: self.last_replacement(m, title), compress_replace, flags=re.MULTILINE)

                        except Exception as e:
                            print(e)
                compress_replace = re.sub(";\n[\s\S]*", ";", compress_replace)
                tittle_sql=multiple_utility.datatype(compress_replace)
            else:
                tittle_sql=input_sql
        return tittle_sql

    def Fetch_SQL_From_DAG_Json(self,location, file_name, file):
        result=[]
        with open(file, "r") as f:
            file_py = f.read()
        re_sql_file_name = re.findall(r"(sql/.*?\.sql)", file_py)
        dag_id_match=re.search(r"dag_id\s*=\s*'(\w+)'",file_py)
        dag_id = dag_id_match.group(1) if dag_id_match else "None"
        folder_json_location=os.path.join(os.path.dirname(location),Json_folder_name,"*.json")
        # print(folder_json_location)
        all_json_file=glob.glob(folder_json_location)

        for single_sql in re_sql_file_name:
            for sub_string_sql in single_sql.split(","):
                # print(sub_string_sql)
                sql_Status = False
                fetch_sql_file = ""
                if all_json_file:
                    for single_json in all_json_file:
                        parsed_json=common_obj.fetch_json(single_json)
                        stage_value = parsed_json['stages']
                        json_dag_id = parsed_json['dag_id']
                        if json_dag_id in dag_id:
                            for single_stage in stage_value:

                                full_sql_name=sub_string_sql.split("/")[-1]
                                list_directory_Sql=sub_string_sql.split("/")[1:-1]
                                directory = os.path.join(*list_directory_Sql)
                                sql_name =full_sql_name.replace(f"{dag_id}_{single_stage}_","").strip()

                                list_sql_file=glob.glob(os.path.join(os.path.dirname(location),sql_folder_name,directory,sql_name))
                                if list_sql_file:
                                    sql_Status=True
                                    fetch_sql_file=sql_name
                                    sql_location=list_sql_file
                                    break

                if sql_Status==False or sql_Status=="" or sql_Status=="CM":
                    # print("SQL Not Found")
                    # print(sub_string_sql)
                    full_sql_name = sub_string_sql.split("/")[-1]
                    list_directory_Sql = sub_string_sql.split("/")[1:-1]
                    # print(list_directory_Sql)
                    # print(directory)
                    directory = os.path.join(*list_directory_Sql)
                    sql_name = full_sql_name.replace(f"{dag_id}_", "").strip()

                    def recursion_split(sql_name):
                        while sql_name:
                            list_sql_file = glob.glob(os.path.join(os.path.dirname(location), sql_folder_name, directory, sql_name))

                            if list_sql_file:
                                print(f"Current sql_name is found: {sql_name}")
                                return "CM", sql_name ,list_sql_file
                            else:
                                print(f"Current sql_name not found: {sql_name}")
                                print(f"directory name is ",directory)
                                print(os.path.join(os.path.dirname(location), sql_folder_name, directory, sql_name))
                                sql_name = "_".join(sql_name.split("_")[1:])

                        return False, "" ,""
                    recussion_sql_Status = recursion_split(sql_name)
                    # print(recussion_sql_Status)
                    if recussion_sql_Status:
                        fetch_sql_file=recussion_sql_Status[1]
                        sql_Status = recussion_sql_Status[0]
                        sql_location = recussion_sql_Status[2]
                result.append({
                    'dag_id': dag_id,
                    'full_sql_name': sub_string_sql,
                    'fetch_sql_file':fetch_sql_file,
                    'fetch_sql_location':sql_location,
                    'sql_Status': sql_Status
                })


        print(result)
        return  result


    def fun_Enable_SQL_copy(self,SQL_Copy_TXT,SQL_File_Directory,Output_Folder_Path):
        print(SQL_Copy_TXT)

        with open(SQL_Copy_TXT, "r") as f:
            for single_sql in f:  # Iterates through each line in the file
                sql_file_name=single_sql.strip()
                print("File name is :",single_sql.strip())
                if sql_file_name:
                    if '/' in  sql_file_name:
                        print("It has folder structure")
                        split_sql_file_name=sql_file_name.split("/")
                        folder_sql_file=sql_file_name.split("/")[0]
                        sql_file_name=sql_file_name.split("/")[1]
                        # print(folder_sql_file,sql_file_name)

                        SQL_FolderLst = glob.glob(os.path.join(SQL_File_Directory,folder_sql_file, sql_file_name), recursive=True)
                        # print(SQL_FolderLst)
                        # with open()
                        for copy_sql_file in SQL_FolderLst:
                            output_sql_location = copy_sql_file.split("/ip/")[1]
                            # print(output_sql_location)
                            full_output_location = os.path.join(Output_Folder_Path, output_sql_location)
                            # print("Location Output", full_output_location)
                            output_dir = os.path.dirname(full_output_location)
                            if not os.path.exists(output_dir):
                                os.path.normpath(output_dir)
                                os.makedirs(os.path.normpath(output_dir))
                            shutil.copy(copy_sql_file, full_output_location)
                            print(f"Copied {copy_sql_file} to {full_output_location}")

                    else:
                        print("It has only file")
                        SQL_FolderLst = glob.glob(os.path.join(SQL_File_Directory,sql_file_name),recursive=True)
                        # print(SQL_FolderLst)
                        # with open()
                        for copy_sql_file in SQL_FolderLst:
                            output_sql_location=copy_sql_file.split("/ip/")[1]
                            # print(output_sql_location)
                            full_output_location=os.path.join(Output_Folder_Path,output_sql_location)
                            # print("Location Output",full_output_location)
                            output_dir = os.path.dirname(full_output_location)
                            if not os.path.exists(output_dir):
                                # print("Directory creation:",os.path.normpath(output_dir))
                                os.path.normpath(output_dir)
                                os.makedirs(os.path.normpath(output_dir))
                            shutil.copy(copy_sql_file, full_output_location)
                            print(f"Copied {copy_sql_file} to {full_output_location}")
                    # break


multiple_utility = Util()


