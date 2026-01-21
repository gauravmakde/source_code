import logging
from datetime import date
from datetime import datetime, timezone, timedelta
import time
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')


query = """
SELECT CASE
			     WHEN COUNT(*) > 0
			     THEN 1
			     ELSE 0
		   END as isCompleted
FROM PRD_NAP_USR_VWS.ELT_CONTROL
WHERE SUBJECT_AREA_NM = 'SALES_FACT' AND
	    ACTIVE_LOAD_IND = 'N' AND
	    CAST(BATCH_END_TMSTP as date) = CURRENT_DATE();
"""
max_duration_minutes = 60


def get_db_output():
    td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN"
    jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
    result = jdbc_hook.get_first(query)
    if "1" == f"{result[0]}":
        print("SUCCESS - load is complete for today")
        return True
    else:
        return False


def callable_func_name(*arg, **kwargs):
    start_time = datetime.utcnow()
    max_retry_time = start_time + timedelta(minutes=int(max_duration_minutes))
    print(f"Check start time: {start_time}")
    print(f"Check max retry time: {max_retry_time}")
    result = False

    while (start_time <= max_retry_time):
        result = get_db_output()
        next_retry = datetime.utcnow() + timedelta(minutes = 5)
        if (result or next_retry > max_retry_time):
            break 
        print(f"TD hasn't been updated. Retrying at {next_retry}")
        retry_interval_seconds = 5 * 60
        time.sleep(retry_interval_seconds)

    if (result):
        print(f"Job succeed")
        if 1 == 1:
            return "run_sales_and_returns_fact_base_job_0_product_reg_price_dim"
        else:
            return "run_sales_and_returns_fact_base_job_0_product_reg_price_dim"
    else:
        raise Exception("TD check exceeded max retry time, job failed")