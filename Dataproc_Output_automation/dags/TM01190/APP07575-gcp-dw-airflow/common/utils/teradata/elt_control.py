import logging

from airflow.providers.jdbc.hooks.jdbc import JdbcHook


def execute_elt_control_load_macro(
    jdbc_conn_id: str,
    subject_area: str,
    teradata_env: str,
    task_type: str,
    cust_schema: bool = False,
):
    jdbc_hook = JdbcHook(jdbc_conn_id=jdbc_conn_id)
    if task_type not in ["START", "END"]:
        raise Exception("task_type should be either START or END")
    if cust_schema:
        schema = "NAP_CUST_UTL"
    else:
        schema = "NAP_UTL"

    sql = f"EXEC {teradata_env}_{schema}.ELT_CONTROL_{task_type}_LOAD('{subject_area}')"
    jdbc_hook.run(sql)


def get_elt_control_table_dates(
    jdbc_conn_id: str, subject_area: str, teradata_env: str, cust_schema: bool = False
):
    jdbc_hook = JdbcHook(jdbc_conn_id=jdbc_conn_id)

    if cust_schema:
        schema = "NAP_CUST_BASE_VWS"
    else:
        schema = "NAP_UTL"

    sql = f"""
    LOCK ROW FOR ACCESS
    SELECT CAST(EXTRACT_FROM_TMSTP AS CHAR(10)) AS Extract_Begin_Date,
           CAST(EXTRACT_TO_TMSTP AS CHAR(10)) AS Extract_End_Date,
           CAST(CURR_BATCH_DATE AS CHAR(10)) AS CURR_BATCH_DATE
    FROM {teradata_env}_{schema}.ELT_CONTROL
    WHERE Subject_Area_Nm='{subject_area}'
    """

    result = jdbc_hook.get_first(sql)

    if result is None:
        raise Exception("No records found")
    else:
        cntrl_dates = result[0] + "_" + result[1] + "_" + result[2]
        logging.info(cntrl_dates)
        return cntrl_dates
