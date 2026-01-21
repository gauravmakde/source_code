############################################
#
# ------------------------------- Modification Log ----------------------------------
# Programmer            | Date      |  Description
# -----------------------------------------------------------------------------------
# Gaurav M            06/23/2023   Initial version
# -----------------------------------------------------------------------------------
# Program Name : MVP_hist_kimba_set_variable.py
# Usage:
#
# Python script used in Google Cloud Composer to setup SAM application Variables. The Variables are in JSON
#  to Optimize Composer Env Optimization.
#
# This script is used to setup variables in COMPOSER Env.
# #################################################################################################
 
 
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import os
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# var_list_json = Variable.get("C2T_PUB_A", deserialize_json=True)
 

 ############################## Please update the variable name accordingly (replace WFA with SOR name) ##############################
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET_NAME = "dm-del-composer-bucket"
GCS_OBJECT_PATH = "dags/WF_MVP/JSON/cdm_next_topic.json" # This is the path relative to the bucket root
 
 
def variable_set():
      
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # Read the content of the GCS file. This downloads the content into memory.
    # The download method returns bytes.
    file_content_bytes = gcs_hook.download(
        bucket_name=GCS_BUCKET_NAME,
        object_name=GCS_OBJECT_PATH
    )
    
    # Decode the bytes content to a UTF-8 string, then parse it as JSON.
    cdm_next_json = json.loads(file_content_bytes.decode('utf-8'))
 
    Variable.set(key="MVP_Hist_K", value= cdm_next_json    , serialize_json=True)
 
     
def variable_get():
   print(Variable.get(key="MVP_Hist_K", deserialize_json=True))
 
 
default_args={ "owner":"mvp", 'email_on_failure': False, 'email_on_retry': False, 'retries': 0 }
with DAG (
    "MVP_hist_kimba_set_variable",
    catchup = False,
    start_date = airflow.utils.dates.days_ago(0),
    schedule_interval = None, default_args=default_args) as dag:
 
   dummy_mvp_set_var_start_task = DummyOperator(task_id='dummy_mvp_set_var_start_task')
   python_mvp_set_var = PythonOperator(task_id='python_mvp_set_variables', python_callable=variable_set, do_xcom_push=False)
   python_mvp_get_var = PythonOperator(task_id='python_mvp_get_variables', python_callable=variable_get, do_xcom_push=False)  
   dummy_mvp_set_var_end_task = DummyOperator(task_id='dummy_mvp_set_var_end_task')  
   
   dummy_mvp_set_var_start_task >>  python_mvp_set_var >> python_mvp_get_var >> dummy_mvp_set_var_end_task