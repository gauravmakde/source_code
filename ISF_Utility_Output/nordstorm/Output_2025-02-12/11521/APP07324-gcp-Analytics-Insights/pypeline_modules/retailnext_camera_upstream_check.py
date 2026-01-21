import logging
from datetime import date
from datetime import datetime, timezone, timedelta
import time
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')


query = """
SELECT CASE
			     WHEN sum(traffic_in) > 0
			     THEN 1
			     ELSE 0
		   END as isCompleted
FROM prd_nap_base_vws.store_traffic_vw
WHERE business_date = CURRENT_DATE()-1
AND source_type = 'RetailNext'
;
"""
max_duration_minutes = (60*5)/4 #starts at 4am, want this to timeout after 5 hours (9am) but it will retry 4 times before it actually fails


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
            return {task1}
        else:
            return {task2}
    else:
        raise Exception("TD check exceeded max retry time, job failed")
