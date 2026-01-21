from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from google.cloud import bigquery
from google.cloud import storage
import pandas
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def call_bqinsert_operator(
    dag, task_id, sql_file_location, project_id, parameters, location, gcp_conn_id=None
):
    with open(sql_file_location, "r") as file1:
        sql_string = file1.read()

    return BigQueryInsertJobOperator(
        dag=dag,
        task_id=task_id,
        configuration={
            "query": {
                "query": sql_string,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        params=parameters,
        gcp_conn_id=gcp_conn_id,
        location=location,
    )


def execute_elt_control_load_macro(
    bq_conn_id: str,
    location,
    subject_area: str,
    bigquery_environment: str,
    task_type: str,
    cust_schema: bool = False,
):
    hook = BigQueryHook(gcp_conn_id=bq_conn_id, location=location, use_legacy_sql=False)
    if task_type not in ["START", "END"]:
        raise Exception("task_type should be either START or END")
    if cust_schema:
        schema = "NAP_CUST_UTL"
    else:
        schema = "NAP_UTL"
    task_type = task_type.lower()
    bigquery_environment = bigquery_environment.lower()
    schema = schema.lower()
    sql_query = f"CALL {bigquery_environment}_{schema}.ELT_CONTROL_{task_type.upper()}_LOAD('{subject_area}')"
    logging.info(f"sql string: {sql_query}")

    query_job = hook.run(sql_query)
    logging.info(f"Result from BigQuery: {query_job}")


def get_elt_control_table_dates(
    bq_conn_id: str,
    location,
    subject_area: str,
    bigquery_environment: str,
    cust_schema: bool = False,
    **kwargs,
):
    ti = kwargs["ti"]
    hook = BigQueryHook(gcp_conn_id=bq_conn_id, location=location, use_legacy_sql=False)
    if cust_schema:
        schema = "NAP_CUST_BASE_VWS"
    else:
        schema = "NAP_UTL"

    sql_string = f"""
    
    SELECT CAST(EXTRACT_FROM_TMSTP AS STRING) AS Extract_Begin_Date,
           CAST(EXTRACT_TO_TMSTP AS STRING) AS Extract_End_Date,
           CAST(CURR_BATCH_DATE AS STRING) AS CURR_BATCH_DATE
    FROM `{bigquery_environment}_{schema}.ELT_CONTROL`
    WHERE Subject_Area_Nm='{subject_area}'
    """
    logging.info(f"sql string: {sql_string}")
    result = hook.get_first(sql_string)
    if result is None:
        raise Exception("No records found")
    else:
        cntrl_dates = result[0] + "_" + result[1] + "_" + result[2]
        logging.info(cntrl_dates)
        return cntrl_dates
