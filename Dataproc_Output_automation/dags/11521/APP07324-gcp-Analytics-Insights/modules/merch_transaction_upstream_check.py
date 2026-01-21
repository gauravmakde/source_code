import logging
from datetime import date
from datetime import datetime, timezone, timedelta
import time

logging.basicConfig(level="INFO")


query = """
SELECT CASE
			     WHEN COUNT(*) > 0
			     THEN 1
			     ELSE 0
		   END as isCompleted
FROM DEV_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW
WHERE dw_sys_load_dt = CURRENT_DATE('PST8PDT');
"""
max_duration_minutes = 60


def get_db_output(bq_hook):
    # td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN"
    # jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)

    result = bq_hook.get_first(query)
    if "1" == f"{result[0]}":
        print("SUCCESS - load is complete for today")
        return True
    else:
        return False


def callable_func_name(*arg, **kwargs):
    start_time = datetime.utcnow()
    bq_hook = arg[0]
    print(f"printing hooks: {bq_hook}")
    max_retry_time = start_time + timedelta(minutes=int(max_duration_minutes))
    print(f"Check start time: {start_time}")
    print(f"Check max retry time: {max_retry_time}")
    result = False

    while start_time <= max_retry_time:
        result = get_db_output(bq_hook)
        next_retry = datetime.utcnow() + timedelta(minutes=5)
        if result or next_retry > max_retry_time:
            break
        print(f"BQ hasn't been updated. Retrying at {next_retry}")
        retry_interval_seconds = 5 * 60
        time.sleep(retry_interval_seconds)

    if result:
        print(f"Job succeed")
        if 1 == 1:
            return {task1}
        else:
            return {task2}
    else:
        raise Exception("BQ check exceeded max retry time, job failed")
