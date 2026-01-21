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
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW
WHERE dw_sys_load_dt = CURRENT_DATE();
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
            return "location_inventory_tracking_total_job_1_run_sql_to_update_t2_table"
        else:
            return "location_inventory_tracking_total_job_1_run_sql_to_update_t2_table"
    else:
        raise Exception("TD check exceeded max retry time, job failed")