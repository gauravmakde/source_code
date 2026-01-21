import os
import time
import glob
import re
import json
import datetime
import pandas as pd
from ETL_DAG_Automation_json import *
import shutil
import black
from ConfigCreator import *


datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

from common.config_parser import *
from common.common_fun import common_obj
from util.Utility_function import *

XMLFolderLst = glob.glob(XML_file_Path, recursive=True)

start_time = datetime.datetime.now()
# print(Enable_to_run_specific_file)
if Enable_to_run_specific_file == "Y":
    print("fetching CSV files")

    file_df = pd.read_csv(os.path.join( "Input_Code","Sprint_2_dag_name.csv"))
    print(file_df)
    PyFolderLst = []
    for single_file in file_df['file_name']:
        print(single_file)


        if single_file != 'nan':
            singleFileLst = glob.glob(os.path.join("Input_Code", '**', 'dags' , str(single_file)), recursive=True)
            PyFolderLst.extend(singleFileLst)

            # print(PyFolderLst)
else:

    # PyFolderLst = glob.glob(os.path.join("ip",'ISF','**','dags','item_intent_10976_tech_nap_merch.py'),recursive=True)
    PyFolderLst = glob.glob(Input_Folder_Path,recursive=True)
print(PyFolderLst)
# exit()
# output_directory = os.path.join(directory, "Output")

# if not os.path.exists(output_directory):
#     os.mkdir(output_directory)


def list_of_import(file):

    with open(file, "r") as f:
        py = f.read()
        py_imports = re.findall(r"import[\s]*([\w]*)(,)?[\s]*([\w]*)", py)
        d = []
        d = [multioperator for operator in py_imports for multioperator in operator if multioperator not in d]
        return d

def reduce_spaces(match):
    return re.sub(r"\s{2,}", " ", match.group(0))

# def camel_Case_app_name(input,final_app_name=""):
#     dic_final_app_name=[]
#     for single_split in input.split("-"):
#         single_split=single_split.title()
#         dic_final_app_name.append(single_split)
#     final_app_name="-".join(dic_final_app_name)
#     final_app_name=final_app_name.replace("App","APP")
#     final_app_name = final_app_name.replace("isf", "ISF")
#     return final_app_name
def static_changes(file, id,operator_list,app_name):
    re_dataproc_variable="def\s*get_batch_id\(dag_id, task_id\):"
    change_in_Dataproc_variable="""
delta_core_jar = config.get('dataproc', 'delta_core_jar')
etl_jar_version = config.get('dataproc', 'etl_jar_version')
metastore_service_path = config.get('dataproc', 'metastore_service_path')
spark_jar_path = config.get('dataproc', 'spark_jar_path')
user_config_path = config.get('dataproc', 'user_config_path')

def get_batch_id(dag_id, task_id):   
    """
    with open(file, "r") as f:
        existing_import = "from\s*.*import.*(EtlClusterSubdag|TeraDataSSHHook|get_cluster_connection_id)"
        new_import = ""

        input_import = """from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sensors.base import BaseSensorOperator
from nordstrom.utils.cloud_creds import cloud_creds
import logging
"""
        python_change = """
def get_batch_id(dag_id, task_id):
    current_datetime = datetime.today()       
    if len(dag_id) <= 22:
        dag_id = dag_id.replace('_', '-').rstrip('-')
        ln_task_id = 45 - len(dag_id)
        task_id = task_id[-ln_task_id:].replace('_', '-').strip('-')
    elif len(task_id) <= 22:
        task_id = task_id.replace('_', '-').strip('-')
        ln_dag_id = 45 - len(task_id)
        dag_id = dag_id[:ln_dag_id].replace('_', '-').rstrip('-')
    else:
        dag_id = dag_id[:23].replace('_', '-').rstrip('-')
        task_id = task_id[-22:].replace('_', '-').strip('-')

    date = current_datetime.strftime('%Y%m%d%H%M%S')
    return f'''{dag_id}--{task_id}--{date}'''
    
def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id = config.get('dag', 'nauth_conn_id'),
        cloud_conn_id = config.get('dag', 'gcp_conn'),
        service_account_email = service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def pelican_check():
    return Pelican.validate(dag_name=dag_id, env=airflow_environment)

class PelicanJobSensor(BaseSensorOperator):
    def poke(self, context):
        return Pelican.check_job_status(context, dag_name=dag.dag_id, env=airflow_environment)

def toggle_pelican_validator(pelican_flag,dag):
    if pelican_flag == True:
        pelican_sensor_task = PelicanJobSensor(
            task_id='pelican_job_sensor',
            poke_interval=300,  # check every 5 minutes
            timeout=4500,  # timeout after 75 minutes
            mode='poke'
        )

        return pelican_sensor_task
    else:
        dummy_sensor = DummyOperator(task_id="dummy_sensor",dag=dag)

        return dummy_sensor

def toggle_pelican_sensor(pelican_flag,dag):
    if pelican_flag == True:
        pelican_validation = PythonOperator(
            task_id="pelican_validation",
            python_callable=pelican_check,
            dag=dag
        )

        return pelican_validation
    else:
        dummy_validation = DummyOperator(task_id="dummy_validation",dag=dag)
        
        return dummy_validation    
              
default_args = {
        """

        dag_as = """ ) as dag:

    project_id = config.get('dag', 'gcp_project_id')
    service_account_email = config.get('dag', 'service_account_email')
    region = config.get('dag', 'region')
    gcp_conn = config.get('dag', 'gcp_conn')
    subnet_url = config.get('dag', 'subnet_url')
    pelican_flag = config.getboolean('dag', 'pelican_flag')
    env = os.getenv('ENVIRONMENT')

    creds_setup = PythonOperator(
        task_id = "setup_creds",
        python_callable = setup_creds,
        op_args = [service_account_email],
    )  
    
    pelican_validator = toggle_pelican_validator(pelican_flag,dag)
    pelican_sensor = toggle_pelican_sensor(pelican_flag,dag)

    #Please add the consumer_group_name,topic_name from kafka json file and remove if kafka is not present 
    fetch_batch_run_id_task = FetchKafkaBatchRunIdOperator(
        task_id = 'fetch_batch_run_id_task',
        gcp_project_id = project_id,
        gcp_connection_id = gcp_conn,
        gcp_region = region,
        topic_name = 'humanresources-job-changed-avro',
        consumer_group_name = 'humanresources-job-changed-avro_hr_job_events_load_2656_napstore_insights_hr_job_events_load_0_stg_table',
        offsets_table = f"`{dataplex_project_id}.onehop_etl_app_db.kafka_consumer_offset_batch_details`",
        source_table = f"`{dataplex_project_id}.onehop_etl_app_db.source_kafka_consumer_offset_batch_details`",
        default_latest_run_id = int((datetime.today() - timedelta(days=1)).strftime('%Y%m%d000000')),
        dag = dag
    )
"""
        updated_dafault_agr = """
# Airflow variables
airflow_environment = os.environ.get('ENVIRONMENT', 'local')
root_path = path.dirname(__file__).split('APP02432-gcp-napstore-insights')[0] + 'APP02432-gcp-napstore-insights/'
configs_path = root_path + 'configs/'
sql_path = root_path + 'sql/'
module_path = root_path + 'modules/'

# Fetch data from config file
config_env = 'development' if airflow_environment in ['nonprod', 'development'] else 'production'
config = configparser.ConfigParser()
config.read(os.path.join(configs_path, f'env-configs-{config_env}.cfg'))
config.read(os.path.join(configs_path, f'env-dag-specific-{config_env}.cfg'))

dag_id = 'DM_id'
sql_config_dict = dict(config[dag_id+'_db_param'])

os.environ['NEWRELIC_API_KEY'] = config.get('framework_setup', 'airflow_newrelic_api_key_name')

# Append directories
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))
sys.path.append(root_path)
sys.path.append(module_path) 

from modules.fetch_kafka_batch_run_id_operator import FetchKafkaBatchRunIdOperator
from modules.pelican import Pelican
from modules.metrics.main_common import main
                """
        re_dag_id=r"\s+dag_id[\s]*=[\s]*\'([\w]*)\'"

        existing_cron = r"cron\s*=\s*.*\n+"
        updated_cron_dataplex = """cron = None if config.get(dag_id, 'cron').lower() =="none" else config.get(dag_id, 'cron')\ndataplex_project_id = config.get('dag', 'dataplex_project_id')\n"""

        existing_retries = r"(\s+)'retries'\s*:\s*(\d+),"
        updated_retries = r"\1'retries': 0 if config_env == 'development' else \2,"

        existing_email_on_failure = r"(\'email_on_failure\'\s*:\s*)\w*,"
        updated_email_on_failure = """config.getboolean(dag_id, 'email_on_failure'),"""

        existing_paused_upon_creation = r"paused_upon_creation\s*=\s*(\w+)"
        updated_paused_upon_creation = r"paused_upon_creation = \1"

        existing_set_max_active_runs = r"set_max_active_runs\s*=\s*(\w+)"
        updated_set_max_active_runs = r"set_max_active_runs = \1"

        existing_set_concurrency = r"set_concurrency\s*=\s*(\w+)"
        updated_set_concurrency = r"set_concurrency = \1"

        existing_dag_sla = r"dag_sla\s*=(\s*[^\n]*)"

        existing_run_id_dict = r"run_id_dict\s*=\s*([^\n]*)"
        updated_run_id_dict = r"run_id_dict = \1"

        existing_email_on_retry = r"(\'email_on_retry\'\s*:)\s*\w*,"
        updated_email_on_retry = """ config.getboolean(dag_id, 'email_on_retry'),"""

        existing_email= r"(\'email\'\s*:)\s*.*,"
        updated_existing_email = """None if config.get(dag_id, "email").lower() == "none" else config.get(dag_id, 'email').split(','),"""

        existing_schedule = r"(\s*schedule_interval\s*=([^,]*),)"
        updated_schedule = r"""
        schedule_interval = cron,"""

        existing_space="(\w+)=(\w+)"
        updated_space = "\\1 = \\2" # to dag_id='abc' then  dag_id = 'abc'

        existing_livy="livy_"
        updated_livy = ""

        existing_teradata="_teradata_"
        updated_teradata = "_bigquery_"



        py = f.read()

        py = re.sub(r"(from\s*airflow\s*import\s*DAG\n)", r"\1" + input_import, py)
        py = re.sub("(dag_id[\s]*=[\s]*)(\')([\w]*)(\')", r"\1\2gcp_\3\4", py)

        py = re.sub(existing_paused_upon_creation, updated_paused_upon_creation, py)
        py = re.sub(existing_retries, updated_retries, py)
        py = re.sub(existing_set_max_active_runs, updated_set_max_active_runs, py)
        py = re.sub(existing_set_concurrency, updated_set_concurrency, py)
        py = re.sub(existing_run_id_dict, updated_run_id_dict, py)
        py = re.sub(existing_livy, updated_livy, py)
        # py = re.sub(existing_teradata, updated_teradata, py)
        # py = re.sub(existing_space, updated_space, py)


        # Apply the substitution
        py = re.sub(existing_dag_sla, reduce_spaces, py)

        search_dag_id=re.search(re_dag_id,py)
        dag_id =search_dag_id.group(1) if search_dag_id else 'None'
        py=re.sub(re_dag_id, "", py)
        py = re.sub(r"(default_args[\s]*=[\s]*\{)", python_change, py)
        py = re.sub(r"(\)[\s]*as[\s]*dag:)", dag_as, py)
        py = re.sub("env\s*=\s*os.environ.get\('ENVIRONMENT', 'local'\)[^@]*os\.path\.split\(__file__\)\[0\]",updated_dafault_agr, py)

        py = re.sub("DM_id", dag_id, py)
        py = re.sub(existing_email_on_failure, r"\1" + updated_email_on_failure, py)
        py = re.sub(existing_email_on_retry, r"\1" + updated_email_on_retry, py)
        py = re.sub(existing_email, r"\1" + updated_existing_email, py)
        py = re.sub(existing_schedule, updated_schedule, py)
        py = re.sub(existing_cron, updated_cron_dataplex, py)
        # str_final_app_name = camel_Case_app_name(app_name, final_app_name)
        # py = re.sub("APP02432-gcp-napstore-insights", str_final_app_name, py)
        py = re.sub("APP02432-gcp-napstore-insights", app_name, py)
        py = re.sub(existing_import, new_import, py, flags=re.MULTILINE)
        py = re.sub(r'\n{2,}', '\n\n', py)
        # py = re.sub(r"\s{2,}", " ", py)


        if 'LivyOperator' in operator_list:
            py=re.sub(re_dataproc_variable,change_in_Dataproc_variable,py)
        # return py,str_final_app_name
        return py

def check_operator(full_py_file, operator_list, location_json_file, id,location_kafka_json,XML_file_Path,project_id,production_kafka_json_output_folder,filename,location):
    # print(operator_list)
    file_issue = []
    final_dag = full_py_file
    final = []
    re_dag_id = re.search("dag_id\s*=\s*\'(\w*)\'", full_py_file)
    dag_id = re_dag_id.group(1) if re_dag_id else ""

    if 'launch_k8s_api_job_operator' not in operator_list and 'LivyOperator' not in operator_list:
        # print("Not has operator")
        return final, final_dag

    if 'launch_k8s_api_job_operator' in operator_list:

        # print("launch_k8s_api_job_operator detected")
        existing_operator = "from k8s_libs.operators import launch_k8s_api_job_operator, monitor_k8s_api_job_status"
        new_existing_operator = ""

        dag_bigquery_operator = re.sub(existing_operator, new_existing_operator, full_py_file)

        block_patterns = re.findall(r"(\w+)\s*=\s*launch_k8s_api_job_operator\((.*?)\)", dag_bigquery_operator, re.DOTALL)
        re_dag_id = re.search("dag_id\s*=\s*\'(\w*)\'", full_py_file)
        dag_id = re_dag_id.group(1) if re_dag_id else ""

        for dag_name_k8, full_k8 in block_patterns:
            re_container_command="container_image\s*=.*/(.*)',"
            task = dag_name_k8
            task_id = re.findall("task_id\s*=\s*'([^']*)'", full_k8)[0]
            container_Command=re.search(re_container_command,full_k8).group(1) if re.search(re_container_command,full_k8).group(1) else "None"

            # print("container_Command:", container_Command)

            if "etl-executor" in container_Command:
                list_container_Command_sql = re.findall(r'sql/(.*.sql)', full_k8)
                # list_container_Command_sql = re.findall(r'(sql/.*?\.sql)', full_k8)
                # print("SQL in operator ", list_container_Command_sql)

                if list_container_Command_sql:
                    # container_Command_sql = re.findall(r'(sql/.*?\.sql)', full_k8)[0]
                    container_Command_sql = re.findall(r'sql/(.*.sql)', full_k8)[0]
                    # print("Splited sql:", container_Command_sql)
                else:
                    container_Command_sql = ""

                if len(container_Command_sql.split('/')) > 1:

                    sql_folder = container_Command_sql.split('/')[0]


                else:
                    sql_folder = ""
                json_file = dag_id.split(id)[0].rstrip("_").replace("gcp_", "")
                # print("JSon file:", json_file)

                json_file_location = os.path.join(location_json_file, json_file + ".json")

                specific_json_file = glob.glob(json_file_location, recursive=True)
                # print("Json location:", specific_json_file)
                # print("Len of json", len(specific_json_file))

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

                            # print(sql_file)

                            sql = sql_file
                        except:
                            print("In exception")
                            sql = container_Command_sql

                        sql_file_variable = sql.replace(".sql", "")
                        sub_json = {"task": task, "task_id": task_id, 'sql_folder': sql_folder, 'Sql_file': sql,
                                    'operator': 'launch_k8s_api_job_operator', 'sql_file_variable': sql_file_variable,
                                    'json_file_name': "", 'spark_conf':"",'container_Command':container_Command}
                        final.append(sub_json)

                else:
                    sql = container_Command_sql

                    sql_file_variable = container_Command_sql.replace(".sql", "").replace("/", "_")

                    sub_json = {"task": task, "task_id": task_id, 'sql_folder': sql_folder, 'Sql_file': sql,
                                'operator': 'launch_k8s_api_job_operator', 'sql_file_variable': sql_file_variable,
                                'json_file_name': "", 'spark_conf':"",'container_Command':container_Command}
                    final.append(sub_json)
            elif "send_metric" in container_Command:
                print("launch k8 doesn't has sql ")

                existing_operator = "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator"
                new_existing_operator = "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator\nfrom metric import main_common"

                dag_bigquery_operator = re.sub(existing_operator, new_existing_operator, dag_bigquery_operator)
                sub_json = {"task": task, "task_id": task_id, 'sql_folder': "", 'Sql_file': "",
                            'operator': 'launch_k8s_api_job_operator', 'sql_file_variable': "",
                            'json_file_name': "", 'spark_conf': "", 'container_Command': container_Command}
                final.append(sub_json)



            final_dag = dag_bigquery_operator

    if 'LivyOperator' in operator_list:
        print("LivyOperator detected")

        existing_operator = "from nordstrom_plugins import LivyOperator, LivySensor|from nordstrom.operators.livy_operator import LivyOperator|from nordstrom.sensors.livy_sensor import LivySensor"
        new_existing_operator = ""

        dag_import_operator = re.sub(existing_operator, new_existing_operator, final_dag)

        block_patterns = re.findall(r"(\w+)\s*=\s*LivyOperator\((.*?)(?=\)\s*\n\s*\w+\s*=|\)\s*$)", dag_import_operator,re.DOTALL)

        for dag_name_livy, full_livy in block_patterns:
            task = dag_name_livy
            task_id = re.findall("task_id\s*=\s*'([^']*)'", full_livy)[0]
            app_args = re.findall("app_args\s*=\s*\[\s*'@*(.*?)\]", full_livy)[0]  # to modify s3: to gs bucket re.sub("s3:","gs:",re.findall("app_args\s*=\s*\[\s*'@*(.*?)\]",full_livy)[0])
            re_json_file = re.search("/(\w*.json)", app_args)
            json_file_name = re_json_file.group(1) if re_json_file else ""
            session_name = re.findall("session_name\s*=\s*'(.*?),", full_livy)[0]
            re_spark_conf=re.search(r"spark_conf\s*=\s*{([^)]*)}",full_livy)
            spark_conf=re_spark_conf.group(1) if re_spark_conf else ""


            sub_json = {"task": task, "task_id": task_id, 'app_args': app_args, 'session_name': session_name,
                        'json_file_name': json_file_name,
                        'operator': 'LivyOperator', 'sql_file_variable': "", 'spark_conf':spark_conf,'container_Command':''}

            final.append(sub_json)
            job_name="In Json"
            s3_path="In Json"

            job_name_dic=dict()
            if 'SSHOperator' in operator_list:

                print("SSSH Detected")


                block_patterns = re.findall(r"(\w+)\s*=\s*SSHOperator\((.*?)\)\s+", final_dag, re.DOTALL)
                # print(block_patterns)
                # try:
                for task_name_ssh, full_ssh in block_patterns:

                    command_match = re.search("command\s*=\s*\".*-j(\s+\w+\s+).*-f\s*([^\s+]*)", full_ssh)
                    # job_name,s3_path= re.search("command\s*=\s*\".*-j\s+(\w+)\s+.*-f\s*([^\s+]*)",full_ssh).group(1),re.search("command\s*=\s*\".*-j\s+(\w+)\s+.*-f\s*([^\s+]*)",full_ssh).group(2) if command_match,full_ssh)!=None else None

                    if command_match:
                        job_name = command_match.group(1)
                        s3_path = command_match.group(2)
                        print(job_name, s3_path)

                    else:
                        job_name, s3_path = None, None
                    job_name_dic[str(job_name).strip()] = s3_path
                    # print(job_name_dic)
                    # if job_name and s3_path:
                    #     file_issue = json_Change_kf_BQ(job_name, s3_path, location_kafka_json, XML_file_Path, project_id,production_kafka_json_output_folder\)
            if json_file_name!="":
                specific_json=os.path.join(production_kafka_json_output_folder,json_file_name)
                print("Specific json file:", specific_json)
                dev_specific_json=os.path.join(location_kafka_json,json_file_name)
                file_issue = json_Change_kf_BQ( dev_specific_json, XML_file_Path, project_id,specific_json,job_name_dic,dag_id )

        final_dag = dag_import_operator




    # if 'SSHOperator' in operator_list:
    #
    #     print("SSSH Detected")
    #
    #     block_patterns = re.findall(r"(\w+)\s*=\s*SSHOperator\((.*?)\)\s+", final_dag, re.DOTALL)
    #     # print(block_patterns)
    #     # try:
    #     for task_name_ssh, full_ssh in block_patterns:
    #         # print("Inside the ssh operator")
    #
    #         task_id = re.search("task_id\s*=\s*'([^']*)'", full_ssh).group(0) if re.search("task_id\s*=\s*'([^']*)'", full_ssh) else None
    #         # print(re.search("command\s*=\s*\".*(-j\s+\w+\s+).*-f\s*([^\s+]*)",full_ssh))
    #         # print(full_ssh)
    #         command_match=re.search("command\s*=\s*\".*-j(\s+\w+\s+).*-f\s*([^\s+]*)", full_ssh)
    #         # job_name,s3_path= re.search("command\s*=\s*\".*-j\s+(\w+)\s+.*-f\s*([^\s+]*)",full_ssh).group(1),re.search("command\s*=\s*\".*-j\s+(\w+)\s+.*-f\s*([^\s+]*)",full_ssh).group(2) if command_match,full_ssh)!=None else None
    #
    #         if command_match:
    #             job_name = command_match.group(1)
    #             s3_path = command_match.group(2)
    #             print(job_name,s3_path)
    #         else:
    #             job_name, s3_path = None, None
    #         if job_name and s3_path:
    #             file_issue = json_Change_kf_BQ(
    #                 job_name, s3_path, location_kafka_json, XML_file_Path, project_id,
    #                 production_kafka_json_output_folder
    #             )
            # file_issue=json_Change_kf_BQ(job_name,s3_path,location_kafka_json,XML_file_Path,project_id,production_kafka_json_output_folder)

        # except Exception as e:
        #     print(e)
        #     error.append({
        #         'location': location,
        #         'file_name': filename,
        #         'error': str(e)
        #     })




    return final, final_dag, file_issue


def replace_operator(semifile, check_k8_operator,file_name,location):
    # print(check_k8_operator)
    # print("file_name is :",file_name)
    # print("location is :", location)
    final_pattern = semifile
    for meta_data in check_k8_operator:
        task = meta_data['task']
        task_id = meta_data['task_id']
        operator = meta_data['operator']
        sql_file_variable = meta_data['sql_file_variable']
        json_file_name = meta_data['json_file_name']
        container_Command=meta_data['container_Command']
        # print(container_Command)
        if 'launch_k8s_api_job_operator' in operator and 'etl-executor' in container_Command :
            # print("Detected launch_k8s_api_job_operator with sql")
            sql_folder = meta_data['sql_folder']
            Sql_file = meta_data['Sql_file']

            if len(sql_folder) > 1:
                sub = f'''
        
    {sql_folder}_{sql_file_variable} = open( os.path.join(sql_path, '{sql_folder}/{Sql_file}'),"r").read()

    {task} = BigQueryInsertJobOperator(
        task_id = "{task_id}",
        configuration = 
        {{
            "query": 
            {{
                "query": {sql_folder}_{sql_file_variable},
                "useLegacySql": False
            }}
        }},
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
       )
               '''
            else:
                sub = f'''
    {sql_file_variable} = open(
    os.path.join(sql_path, '{Sql_file}'),
    "r").read()

    {task} = BigQueryInsertJobOperator(
        task_id = "{task_id}",
        configuration = 
        {{
            "query": 
            {{
                "query": {sql_file_variable},
                "useLegacySql": False
            }}
        }},
        project_id = project_id,
        gcp_conn_id = gcp_conn,
        params = sql_config_dict,
        location = region,
        dag = dag
        )
                '''

            block_patterns = re.sub('(' + task + "\s*=\s*launch_k8s_api_job_operator\([^)]*\))", sub, final_pattern,
                                    re.DOTALL)
            # print(block_patterns)

            final_pattern = block_patterns

        if 'LivyOperator' in operator:
            # print(meta_data)
            app_args = meta_data['app_args']
            session_name = meta_data['session_name']
            spark_conf = meta_data['spark_conf'].replace("'spark.airflow.run_id': '{{ ts_nodash }}'","'spark.airflow.run_id':\"{{ ti.xcom_pull(task_ids='fetch_batch_run_id_task')}}\"")

            sub = f'''
            
        session_name=get_batch_id(dag_id=dag_id,task_id="{task_id}")   
    {task} = DataprocCreateBatchOperator(
            task_id = "{task_id}",
            project_id = project_id,
            region = region,
            gcp_conn_id = gcp_conn,
           #op_args = {{ "version":"1.1"}},
            batch=
            {{    
                "runtime_config": {{"version": "1.1","properties":{{ {spark_conf}}}}},
                "spark_batch": 
                    {{
                    "main_class": "com.nordstrom.nap.onehop.etl.app.ExecSQLWrapper",
                    "jar_file_uris": [f"{{spark_jar_path}}/{{delta_core_jar}}",f"{{spark_jar_path}}/uber-onehop-etl-pipeline-{{etl_jar_version}}.jar"],
                    "args": [f"{{user_config_path}}/{json_file_name}",
                    '--aws_user_role_external_id', Variable.get('aws_role_externalid')],
                    }},
                    "environment_config": 
                                        {{
                                        "execution_config": 
                                                        {{
                                                        "service_account": service_account_email,
                                                        "subnetwork_uri": subnet_url,
                                                        }},
                                      #  "peripherals_config": {{"metastore_service": metastore_service_path}}
                                        }}
            }},
            batch_id = session_name,
            dag = dag

'''

            pattern = re.compile(re.escape(task) + r"\s*=\s*LivyOperator\((.*?)(?=\)\s*\n\s*\w+\s*=|\)\s*$)", re.DOTALL)
            block_patterns = re.sub(pattern, sub, final_pattern, re.DOTALL)

            final_pattern = block_patterns

        if 'launch_k8s_api_job_operator' in operator and 'send_metric' in container_Command :
            # print("Detected launch_k8s_api_job_operator with newrelic")

            # re_full_path_python_file = "container_command\s*=\s*\[\s*'python'\s*,\s*'([/\w+.]+)'"
            re_full_path_python_file="container_command\s*=\s*\[\s*.*python/send_metric([/\w+.]+)"
            re_python_file="(\w+).py$"
            re_full_metrics_yaml_file = "container_command\s*=\s*\[\s*.*'--metrics_yaml_file'\s*,\s*'([./\w+]+)'"
            re_yaml_file = "(\w+).yaml$"

            # print("########")

            full_path_python_file = re.search(re_full_path_python_file, final_pattern).group(1) if re.search(re_full_path_python_file, final_pattern) else None
            full_metrics_yaml_file = re.search(re_full_metrics_yaml_file, final_pattern).group(1) if re.search(re_full_metrics_yaml_file,
                                                                                                 final_pattern) else None

            # print(full_path_python_file)
            python_file_match=re.search(re_python_file,full_path_python_file)

            if python_file_match:
                """
                older code 
                # python_file=re.search(re_python_file, full_path_python_file).group(1) if re.search(re_python_file,full_path_python_file) else None
                # yaml_file = re.search(re_yaml_file, full_metrics_yaml_file).group(1) if re.search(re_yaml_file,full_metrics_yaml_file) else None
                """
                python_file=re.search(re_python_file, full_path_python_file).group(1)
                yaml_file = re.search(re_yaml_file, full_metrics_yaml_file).group(1)
            else:
                python_file=None
                yaml_file=None
            sub=f"""
    {task}=PythonOperator(
        task_id='{task_id}',
        python_callable=main,
        op_kwargs={{
        'newrelic_connection' : config.get('dag', 'newrelic_connection'),
        'hook' : BigQueryHook(gcp_conn_id=gcp_conn,location='US', use_legacy_sql=False),
        'bigquery_environment' : sql_config_dict['dbenv'],
        'metric_yaml_file' : os.path.join(module_path,'yaml/{yaml_file}.yaml'),
        'isf_dag_name' : sql_config_dict['dag_name'],
        'tpt_job_name' : sql_config_dict['tpt_job_name'],
        'subject_area_name' : sql_config_dict['subject_area'],
        'ldg_table_name': sql_config_dict['ldg_table_name']
                    }} )
                """
            block_patterns = re.sub('(' + task + "\s*=\s*launch_k8s_api_job_operator\([^)]*\))", sub, final_pattern,re.DOTALL)
            # print(block_patterns)

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
    replace = re.sub(search.group(0), "\n\n    creds_setup >> fetch_batch_run_id_task >> " + search.group(0).strip(), parameter_change)
    comment_operator_name = re.findall(r"[\"]{3}(\w*)\s=", replace)

    # print(comment_operator_name)
    final_Response = replace
    for single_operator in comment_operator_name:
        operator_remove = re.sub(single_operator + "[\s]*>>|\[" + single_operator + "\]", "", final_Response)
        final_Response = operator_remove
    # print(final_Response)
    pattern = re.compile(r"((creds_setup\s*>>)(.*?))\s*$", re.DOTALL)
    response = pattern.search(final_Response)

    task_sequence = response.group(0)

    task_sequence = re.sub(r"\s+\[", "[", task_sequence)
    task_sequence = re.sub(r">>\s+\n+", ">>", task_sequence)


    # print(task_sequence)

    modified_content = pattern.sub(task_sequence, final_Response)     # task_sequence = re.sub(r"^\s+", "", task_sequence, flags=re.MULTILINE)

    modified_content=modified_content.strip()+" >> pelican_validator >> pelican_sensor"

    modified_content = re.sub(r">>\s+>>", ">>", modified_content)

    # modified_content = black.format_str(modified_content, mode=black.FileMode())  # Added formater for dag formater


    return (modified_content)


def app_rename_folder():
    root_path_files = glob.glob(App_id_location,recursive=True)
    print("$$$$$$$$$$$$")
    print(root_path_files)
    for folder_name in root_path_files:

        if '-gcp-' not in  folder_name:
            # print()
            app_name=os.path.basename(folder_name)
            print(app_name)
            match = re.match(r"^(app\d+)-(.*?)$", app_name, re.IGNORECASE)
            print(match)
            if match:
                print("Folder name format does not match '<app or AAP><digits>-<dynamic>'")
                prefix = match.group(1)
                suffix = match.group(2)
                prefix= prefix.upper()
                new_folder_name = f"{prefix}-gcp-{suffix}"

                new_folder_path = os.path.join(os.path.dirname(folder_name), new_folder_name)

                if os.path.exists(new_folder_path):
                    print(f"The target folder '{new_folder_name}' already exists at {new_folder_path}. Skipping rename.")

                try:
                    os.rename(folder_name, new_folder_path)
                    print(f"Folder renamed to: {new_folder_name}")
                except FileNotFoundError:
                    print("The specified folder was not found.")
                except Exception as e:
                    print(f"An error occurred: {e}")

        elif 'APP' not in folder_name:
            print("It has gcp")
            app_name = os.path.basename(folder_name)
            match = re.match(r"^(app\d+)-(.*?)$", app_name, re.IGNORECASE)
            if match:
                print("Folder name format does not match '<app or AAP><digits>-<dynamic>'")
                prefix = match.group(1)
                suffix = match.group(2)
                prefix = prefix.upper()
                new_folder_name = f"{prefix}-{suffix}"

                new_folder_path = os.path.join(os.path.dirname(folder_name), new_folder_name)

                if os.path.exists(new_folder_path):
                    print(f"The target folder '{new_folder_name}' already exists at {new_folder_path}. Skipping rename.")

                try:
                    os.rename(folder_name, new_folder_path)
                    print(f"Folder renamed to: {new_folder_name}")
                except FileNotFoundError:
                    print("The specified folder was not found.")
                except Exception as e:
                    print(f"An error occurred: {e}")



def final_root_path_change(app_name,new_folder_path,output_file_path,root_folder_location):
    (app_name,new_folder_path,output_file_path,os.path.dirname(os.path.dirname(file)))

    updated_path = output_file_path.replace(app_name, new_folder_path)

    with open(updated_path, "r+") as file:
        content = file.read().replace(app_name, new_folder_path)
        file.seek(0)
        file.write(content)
        file.truncate()


if __name__ == "__main__":
    data = []
    error = []
    file_error=[]
    final_app_name = ""
    print(Output_directory)

    file_error=copy_all_files_except(PyFolderLst)
    copy_all_json(PyFolderLst,kafka_json_folder,Output_directory,error)

    config_dag_specific_creation()


    for file in PyFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        app_name = os.path.basename(os.path.dirname(os.path.dirname(file)))

        print("Location of file:", location)
        print("file name :", file_name)
        print("App name is :",app_name)

        creating_all_config(location,user_config_folder,Output_directory,error,CSV_file_Path,directory,app_name)
        location_without_dags = os.path.dirname(location)
        # Paths setup
        # print(location_without_dags)
        location_json_file = os.path.join(location_without_dags, json_folder)
        location_kafka_json = os.path.join(location_without_dags, kafka_json_folder)
        # Define the output directory (with 'development' in the destination)
        # print("#################")
        # print(location_kafka_json)
        location_kafka_json=location_kafka_json.replace(Input_directory,Output_directory)
        development_kafka_json_output_folder = os.path.join( location_kafka_json, 'development')
        # print(f"JSON Output Folder: {development_kafka_json_output_folder}")

        production_kafka_json_output_folder = os.path.join( location_kafka_json, 'production')
        # print(f"JSON Output Folder: {production_kafka_json_output_folder}")

        pattern = re.compile(r"_([0-9]{4,5})_")
        response = pattern.search(file_name)

        if response:
            try:
                id = response.group(0).replace("_", "")

                operator_list = list_of_import(file)

                # file_static_changes,str_final_app_name = static_changes(file, id,operator_list,app_name)
                file_static_changes= static_changes(file, id, operator_list, app_name)
                # print("$$$$$$$$$")
                # print(file_static_changes)
                # exit()
                file_check_operator, semi_file ,file_issue= check_operator(file_static_changes, operator_list, location_json_file, id,development_kafka_json_output_folder,XML_file_Path,project_id,production_kafka_json_output_folder,file_name,location)
                # print("$$$$$$$$$$$$")
                # print(file_check_operator, semi_file ,file_issue)
                # exit()
                file_replace_operator = replace_operator(semi_file, file_check_operator,file_name,location)

                file_comment_status_monitor_operator = comment_status_monitor_operator(file_replace_operator)



                py = changing_parameter(file_comment_status_monitor_operator)
                py = re.sub(r'\s*""".*?"""\s*', '\n\n', py, flags=re.DOTALL) # remove unwanted commented
                py = re.sub(r'\n{2,}', '\n\n', py)

                # location=location.replace(app_name, str_final_app_name)

                # print(location)
                output_location= location.replace(Input_directory,Output_directory)
                output_file_path = os.path.join(output_location, "gcp_"+file_name)
                # print(output_file_path)
                output_folder = os.path.dirname(output_file_path)
                # print(output_folder)
                match = re.match(r"^(app\d+)-(.*?)$", app_name, re.IGNORECASE)
                if not match:
                    print("Folder name format does not match '<app or AAP><digits>-<dynamic>'")
                prefix = match.group(1).upper()
                suffix = match.group(2)
                new_folder_name = f"{prefix}-gcp-{suffix}"

                py=py.replace(app_name,new_folder_name)
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)  # Create the directory if it doesn't exist
                with open(output_file_path, 'w') as f_out:
                    f_out.write(py)
                # new_folder_path=app_rename_folder(app_name,os.path.dirname(os.path.dirname(file)))
                # final_root_path_change(app_name,new_folder_path,output_file_path,os.path.dirname(os.path.dirname(file)))

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

    app_rename_folder() # need to check To do



df = pd.DataFrame(data)

df.to_csv(os.path.join( Output_Folder_Path,'file_issue_' + timestr + '.csv'), index=False)
end_str = datetime.datetime.now()

if error:
    error.extend(file_issue)
    df_2 = pd.DataFrame(error)

    df_2.to_csv(os.path.join(Output_Folder_Path,f"error_dag_file_{timestr}.csv"))

if file_error:
    file_error.extend(file_issue)
    df_3 = pd.DataFrame(file_error)

    df_3.to_csv(os.path.join(Output_Folder_Path,f"error_file_in_copy__{timestr}.csv"))

print("Total DAG Converted TD to BQ :", len(PyFolderLst))
print("Start time at: ", start_time)
print("End time at: ", end_str)
print("Total time took :", end_str - start_time)




