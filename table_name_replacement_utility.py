import os

table_list=["gaurav","saurav"]
replace_name ="table_name"
Input_dag_file_location="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-11-06//Utility//table_name_daily_dag.py"
Input_python_file_location="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-11-06//Utility//table_name.py"
Input_json_file_location="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-11-06//Utility//table_name_config.json"
Output_code_location="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-11-06//"


def to_change_tablename(single_tablename,Input_python_file):
    with open(Input_python_file, "r") as f:
        python_file = f.read()
        # print(python_file)
        file_name = os.path.basename(Input_python_file)
        replace_file_name = file_name.replace(replace_name, single_tablename)
        # print(replace_file_name)
        python_file = python_file.replace(replace_name, single_tablename)
        # print(python_file)

        if not os.path.exists(Output_code_location):
            os.mkdir(Output_code_location)

        output_file_name = os.path.join(Output_code_location, replace_file_name)
        with open(output_file_name, "w") as f:
            f.write(python_file)



for single_tablename in table_list:
    
    to_change_tablename(single_tablename,Input_dag_file_location)
    to_change_tablename(single_tablename, Input_python_file_location)
    to_change_tablename(single_tablename, Input_json_file_location)

