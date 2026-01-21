import os
import time
import glob
import re
import json
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

Enable_to_run_specific_file = "N"
json_folder = "pypeline_jobs"

if Enable_to_run_specific_file == "Y":
    print("fetching CSV files")

    file_df = pd.read_csv(directory + "//Sprint_2_dag_name.csv")

    PyFolderLst = []
    for single_file in file_df['file_name']:
        print(single_file)

        if single_file != 'nan':
            singleFileLst = glob.glob(directory + '//Sprint_2//**//dags//' + str(single_file), recursive=True)
            PyFolderLst.extend(singleFileLst)

            print(PyFolderLst)
else:

    PyFolderLst = glob.glob(
        directory + '//Sprint_3//**//dags//*.py',
        recursive=True)
print(PyFolderLst)
# exit()
output_directory = os.path.join(directory, "Output")

if not os.path.exists(output_directory):
    os.mkdir(output_directory)


def list_of_import(file):
    with open(file, "r") as f:
        py = f.read()
        py_imports = re.findall(r"import[\s]*([\w]*)(,)?[\s]*([\w]*)", py)
        d = []
        d = [multioperator for operator in py_imports for multioperator in operator if multioperator not in d]
        return d


def static_changes(file, id):
    with open(file, "r") as f:
        existing_import = "from\s*.*import.*(EtlClusterSubdag|TeraDataSSHHook|get_cluster_connection_id)"
        new_import = ""

        input_import = """import logging
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from nordstrom.utils.cloud_creds import cloud_creds
from airflow.configuration import conf
"""
        python_change = """
def get_batch_id(dag_id,task_id):  
   current_datetime = datetime.today()  

   if len(dag_id)<=22:
      dag_id = dag_id.replace('_','-').rstrip('-')
      ln_task_id = 45 - len(dag_id)
      task_id = task_id[-ln_task_id:].replace('_','-').strip('-') 
   elif len(task_id)<=22:
      task_id = task_id.replace('_','-').strip('-')
      ln_dag_id = 45 - len(task_id)
      dag_id = dag_id[:ln_dag_id].replace('_','-').rstrip('-')    
   else:
     dag_id = dag_id[:23].replace('_','-').rstrip('-') 
     task_id = task_id[-22:].replace('_','-').strip('-') 

   date = current_datetime.strftime('%Y%m%d%H%M%S')
   return f'''{dag_id}--{task_id}--{date}'''


def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id=config.get('dag', 'nauth_conn_id'),
        cloud_conn_id=config.get('dag', 'gcp_conn'),
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()


default_args = {
        """

        dag_as = """)as dag:

    project_id = config.get('dag', 'gcp_project_id')
    service_account_email = config.get('dag', 'service_account_email')
    region = config.get('dag', 'region')
    gcp_conn = config.get('dag', 'gcp_conn')
    subnet_url = config.get('dag', 'subnet_url')
    env = os.getenv('ENVIRONMENT')

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )
"""

        updated_dafault_agr = """
# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP02432-gcp-napstore-insights')[
                0] + 'APP02432-gcp-napstore-insights/'
dags_path = os.path.abspath(os.path.dirname(__file__))
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment == 'nonprod' else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
sql_config_dict = dict(config['db'])

os.environ['NEWRELIC_API_KEY'] = 'TECH_ISF_NAP_STORE_TEAM_NEWRELIC_CONNECTION_ID_DEV'

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path)

# Importing DAG modules
# from modules.utils import safe_replace   
                """
        existing_email_on_failure= r"(\s*'email_on_failure'\s*:\s*[^;]*,)"
        updated_email_on_failure="""
        'email_on_failure': config.getboolean('dag', 'email_on_failure'),
        """

        existing_email = r"(\s*'email'\s*:\s*([^;]*),)"
        updated_email = r"""
    'email' : 'cf_nordstrom@datametica.com' if conf.get("webserver","instance_name") == 'Datametica Composer Environment' else """

        existing_schedule = r"(\s*schedule_interval\s*=([^,]*),)"
        updated_schedule = r"""
    schedule_interval = None if config.get("dag","cron") == 'none' else cron,"""

        py = f.read()
        dag_import_change = re.sub(r"(from\s*airflow\s*import\s*DAG\n)", r"\1" + input_import, py)
        dag_dag_id = re.sub("(dag_id[\s]*=[\s]*)(\')([\w]*)(\')", r"\1\2gcp_\3\4", dag_import_change)
        dag_python_fun = re.sub(r"(default_args[\s]*=[\s]*\{)", python_change, dag_dag_id)
        dag_as = re.sub(r"(\)[\s]*as[\s]*dag:)", dag_as, dag_python_fun)
        dag_replace_id = re.sub("DM_id", id, dag_as)
        dag_default_agr = re.sub(
            "env\s*=\s*os.environ.get\('ENVIRONMENT', 'local'\)[^@]*os\.path\.split\(__file__\)\[0\]",
            updated_dafault_agr, dag_replace_id)
        email_on_failure = re.sub(existing_email, updated_email + "\\2", dag_default_agr)
        schedule_interval = re.sub(existing_schedule, updated_schedule, email_on_failure)
        change_import = re.sub(existing_import, new_import, schedule_interval, flags=re.MULTILINE)
        remove_Space = re.sub(r'\n{2,}', '\n\n', change_import)

        return remove_Space


def check_operator(full_py_file, operator_list, location_json_file, id):
    print(operator_list)

    final_dag = full_py_file
    final = []

    if 'launch_k8s_api_job_operator' not in operator_list and 'LivyOperator' not in operator_list:
        print("Not has operator")
        return final, final_dag
    if 'launch_k8s_api_job_operator' in operator_list:

        print("launch_k8s_api_job_operator detected")
        existing_operator = "from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status"
        new_existing_operator = "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator"

        dag_bigquery_operator = re.sub(existing_operator, new_existing_operator, full_py_file)

        block_patterns = re.findall(r"(\w+)\s*=\s*launch_k8s_api_job_operator\((.*?)\)", dag_bigquery_operator,
                                    re.DOTALL)
        print(block_patterns)
        re_dag_id = re.search("dag_id\s*=\s*\'(\w*)\'", full_py_file)
        dag_id = re_dag_id.group(1) if re_dag_id else ""

        for dag_name_k8, full_k8 in block_patterns:
            # print("launch_k8s_api_job_operator operator details:")
            print(full_k8)

            task = dag_name_k8
            task_id = re.findall("task_id\s*=\s*'([^']*)'", full_k8)[0]
            print("Task_id:", task_id)
            list_container_Command_sql = re.findall(r'sql/(.*.sql)', full_k8)
            print("SQL in operator ", list_container_Command_sql)

            if list_container_Command_sql:
                container_Command_sql = re.findall(r'sql/(.*.sql)', full_k8)[0]
                print("Splited sql:", container_Command_sql)
            else:
                container_Command_sql = ""

            if len(container_Command_sql.split('/')) > 1:

                sql_folder = container_Command_sql.split('/')[0]


            else:
                sql_folder = ""
            json_file = dag_id.split(id)[0].rstrip("_").replace("gcp_", "")
            print("JSon file:", json_file)

            json_file_location = os.path.join(location_json_file, json_file + ".json")

            specific_json_file = glob.glob(json_file_location, recursive=True)
            print("Json location:", specific_json_file)
            print("Len of json", len(specific_json_file))

            if specific_json_file:
                print("Json file is present")

                for single_json in specific_json_file:

                    file_name = os.path.basename(single_json)

                    with open(single_json) as file_content:
                        file_contents = file_content.read()

                    parsed_json = json.loads(file_contents)
                    stage_value = parsed_json['stages'][0]

                    try:
                        sql_file = container_Command_sql.split(dag_id.replace("gcp_", "") + "_" + stage_value)[
                            1].lstrip("_")

                        print(sql_file)

                        sql = sql_file
                    except:
                        print("In exception")
                        sql = container_Command_sql

                    sql_file_variable = sql.replace(".sql", "")
                    sub_json = {"task": task, "task_id": task_id, 'sql_folder': sql_folder, 'Sql_file': sql,
                                'operator': 'launch_k8s_api_job_operator', 'sql_file_variable': sql_file_variable,
                                'json_file_name': "", 'spark_conf':""}
                    final.append(sub_json)

            else:
                sql = container_Command_sql

                sql_file_variable = container_Command_sql.replace(".sql", "").replace("/", "_")

                sub_json = {"task": task, "task_id": task_id, 'sql_folder': sql_folder, 'Sql_file': sql,
                            'operator': 'launch_k8s_api_job_operator', 'sql_file_variable': sql_file_variable,
                            'json_file_name': "", 'spark_conf':""}
                final.append(sub_json)

            final_dag = dag_bigquery_operator

        if 'LivyOperator' in operator_list:
            print("LivyOperator detected")

            existing_operator = "from nordstrom_plugins import LivyOperator, LivySensor|from nordstrom.operators.livy_operator import LivyOperator"
            new_existing_operator = "from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator"

            dag_import_operator = re.sub(existing_operator, new_existing_operator, final_dag)

            block_patterns = re.findall(r"(\w+)\s*=\s*LivyOperator\((.*?)(?=\)\s*\n\s*\w+\s*=|\)\s*$)", dag_import_operator,
                                        re.DOTALL)

            for dag_name_livy, full_livy in block_patterns:
                task = dag_name_livy
                task_id = re.findall("task_id\s*=\s*'([^']*)'", full_livy)[0]
                app_args = re.findall("app_args\s*=\s*\[\s*'@*(.*?)\]", full_livy)[
                    0]  # to modify s3: to gs bucket re.sub("s3:","gs:",re.findall("app_args\s*=\s*\[\s*'@*(.*?)\]",full_livy)[0])
                re_json_file = re.search("/(\w*.json)", app_args)
                json_file_name = re_json_file.group(1) if re_json_file else ""
                session_name = re.findall("session_name\s*=\s*'(.*?),", full_livy)[0]
                re_spark_conf=re.search(r"spark_conf\s*=\s*{([^)]*)}",full_livy)

                spark_conf=re_spark_conf.group(1) if re_spark_conf else ""


                sub_json = {"task": task, "task_id": task_id, 'app_args': app_args, 'session_name': session_name,
                            'json_file_name': json_file_name,
                            'operator': 'LivyOperator', 'sql_file_variable': "", 'spark_conf':spark_conf}

                final.append(sub_json)

            final_dag = dag_import_operator

        return final, final_dag


def replace_operator(semifile, check_k8_operator):
    print(check_k8_operator)

    final_pattern = semifile
    for meta_data in check_k8_operator:
        task = meta_data['task']
        task_id = meta_data['task_id']
        operator = meta_data['operator']
        sql_file_variable = meta_data['sql_file_variable']
        json_file_name = meta_data['json_file_name']

        if 'launch_k8s_api_job_operator' in operator:
            print("Detected launch_k8s_api_job_operator")
            sql_folder = meta_data['sql_folder']
            Sql_file = meta_data['Sql_file']
            if len(sql_folder) > 1:
                sub = f'''

    {sql_folder}_{sql_file_variable} = open(
    os.path.join(sql_path, '{sql_folder}/{Sql_file}'),
    "r").read()

    {task}=BigQueryInsertJobOperator(
        task_id="{task_id}",
        configuration={{
            "query": {{
                "query": {sql_folder}_{sql_file_variable},
                "useLegacySql": False
                   }}
                       }},
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=sql_config_dict,
        location=region,
        dag = dag
    )
               '''
            else:
                sub = f'''
    {sql_file_variable} = open(
    os.path.join(sql_path, '{Sql_file}'),
    "r").read()

    {task}=BigQueryInsertJobOperator(
            task_id="{task_id}",
            configuration={{
                "query": {{
                    "query": {sql_file_variable},
                    "useLegacySql": False
                       }}
                           }},
            project_id=project_id,
            gcp_conn_id=gcp_conn,
            params=sql_config_dict,
            location=region,
            dag = dag
        )
                '''

            block_patterns = re.sub('(' + task + "\s*=\s*launch_k8s_api_job_operator\([^)]*\))", sub, final_pattern,
                                    re.DOTALL)
            print(block_patterns)

            final_pattern = block_patterns

        if 'LivyOperator' in operator:
            print(meta_data)
            app_args = meta_data['app_args']
            session_name = meta_data['session_name']
            spark_conf = meta_data['spark_conf']

            sub = f'''
    session_name=get_batch_id(dag_id=dag_id,task_id="{task_id}")
    {task} = DataprocCreateBatchOperator(
        task_id="{task_id}",
        project_id=project_id,
        region=region,
        gcp_conn_id=gcp_conn,
#       op_args={{ "version":"1.1"}},
        batch={{
            "runtime_config": {{"version": "1.1",
                               "properties":{{ {spark_conf}}}}},
            "spark_batch": {{
                "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                "jar_file_uris": [config.get('dataproc', 'spark_jar_path') + "/delta-core_2.12-2.1.0.jar",
                                  config.get('dataproc', 'spark_jar_path') + "/uber-onehop-etl-pipeline.jar"],
                "args": [
                    config.get('dataproc', 'user_config_path') + '/{json_file_name}',
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')
                ],

            }},
            "environment_config": {{
                "execution_config": {{
                    "service_account": service_account_email,
                    "subnetwork_uri": subnet_url,
                }},
                "peripherals_config": {{
                    "metastore_service": config.get('dataproc', 'metastore_service_path')
                }}
            }}
        }},
        batch_id=session_name,
        dag=dag

'''

            pattern = re.compile(re.escape(task) + r"\s*=\s*LivyOperator\((.*?)(?=\)\s*\n\s*\w+\s*=|\)\s*$)", re.DOTALL)
            block_patterns = re.sub(pattern, sub, final_pattern, re.DOTALL)

            final_pattern = block_patterns

    return final_pattern


def comment_status_monitor_operator(operator_comment):
    monitor_k8s_api_job_status_pattern = re.compile(r"(\w+\s*=\s*monitor_k8s_api_job_status\((.*?)\))", re.DOTALL)
    LivySensor_pattern = re.compile(r"(\w+\s*=\s*LivySensor\((?:[^()]|\([^()]*\))+\))", re.DOTALL)
    SubDagOperator_pattern = re.compile(r"(\w+\s*=\s*SubDagOperator\((.*?)\)\))", re.DOTALL)
    block_patterns = re.sub(monitor_k8s_api_job_status_pattern, "\"\"\"\\1\"\"\"", operator_comment)
    block_patterns = re.sub(LivySensor_pattern, "\"\"\"\\1\"\"\"", block_patterns)
    block_patterns = re.sub(SubDagOperator_pattern, "\"\"\"\\1\"\"\"", block_patterns)
    return block_patterns


def changing_parameter(parameter_change):
    search = re.search(r"[\s]*([\w]*[\s]*>>)", parameter_change)
    replace = re.sub(search.group(0), "\n\n    creds_setup >> " + search.group(0).strip(), parameter_change)
    comment_operator_name = re.findall(r"[\"]{3}(\w*)\s=", replace)

    final_Response = replace
    for single_operator in comment_operator_name:
        operator_remove = re.sub(single_operator + "[\s]*>>|\[" + single_operator + "\]", "", final_Response)
        final_Response = operator_remove

    pattern = re.compile(r"((creds_setup\s*>>)(.*?))\s*$", re.DOTALL)
    response = pattern.search(final_Response)

    task_sequence = response.group(0)

    tasks = task_sequence.replace("\n", " ").replace("\t", " ")
    tasks = re.sub(r'\s+', ' ', tasks)

    order_series = []
    for order in tasks.strip().split(">>"):
        if len(order.split(" ")) > 1:
            for sub_order in order.split(" "):
                if sub_order not in order_series and sub_order.strip() != "":
                    order_series.append(sub_order)
        else:
            order_series.append(order)

    remove_last_null = [item for item in order_series if item]

    value_to_replace_param = " >> ".join(remove_last_null)

    value_to_replace_param = value_to_replace_param.replace(", >>", ",")

    modified_content = pattern.sub(value_to_replace_param, final_Response)
    return (modified_content)


if __name__ == "__main__":
    data = []
    error = []
    for file in PyFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        print("Location of file:", location)
        print("file name :", file_name)
        location_without_dags = os.path.dirname(location)
        location_json_file = os.path.join(location_without_dags, json_folder)

        pattern = re.compile(r"_([0-9]{4,5})_")
        response = pattern.search(file_name)
        if response:
            id = response.group(0).replace("_", "")
            print(id)
            operator_list = list_of_import(file)

            file_static_changes = static_changes(file, id)

            file_check_operator, semi_file = check_operator(file_static_changes, operator_list, location_json_file, id)

            file_replace_operator = replace_operator(semi_file, file_check_operator)

            file_comment_status_monitor_operator = comment_status_monitor_operator(file_replace_operator)

            print(file_comment_status_monitor_operator)

            try:
                file_changing_parameter = changing_parameter(file_comment_status_monitor_operator)
                output_file_path = os.path.join(output_directory, location, file_name)
                output_folder = os.path.dirname(output_file_path)
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)  # Create the directory if it doesn't exist
                with open(output_file_path, 'w') as f_out:
                    f_out.write(file_changing_parameter)

            except Exception as e:

                print(f"Error copying python file {file_name}: {e}")
                error.append({
                    'location': location,
                    'file_name': file_name,
                    'error': str(e)
                })

        else:
            data.append({

                'location': location,
                'file_name': file_name,
                'comment': "Issue with the file"

            })
            print(data)
#
print(data)  # Debugging: print the collected data

df = pd.DataFrame(data)
df.to_csv(directory + '\\output\\file_issue_' + timestr + '.csv', index=False)

if error:
    df_2 = pd.DataFrame(error)
    print(df_2)
    df_2.to_csv(os.path.join(directory, f"error_dag_file_{timestr}.csv"))






