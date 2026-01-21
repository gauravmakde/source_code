import logging
from datetime import date
from datetime import datetime, timezone, timedelta
import time
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')


query = """
SELECT
(
SELECT CASE
			     WHEN COUNT(transaction_id) > 0
			     THEN 1
			     ELSE 0
		   END as isCompleted
FROM PRD_NAP_JWN_METRICS_BASE_VWS.JWN_CLARITY_TRANSACTION_FACT
WHERE business_day_date = CURRENT_DATE()-1
AND record_source in ('C', 'G', 'W') --for margin in clarity source data
)
    +
(SELECT CASE
			     WHEN COUNT(*) > 0
			     THEN 1
			     ELSE 0
		   END as isCompleted
FROM PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_fact
WHERE tran_Date = CURRENT_DATE()-1
AND record_source in ('O', 'S') --for demand and opGMV in first mothership table
) as isCompleted
;
"""
max_duration_minutes = (60*8)/4 #starts at 3:05am, want this to timeout after 8 hours (until 11am) but it will retry 4 times before it actually fails
#margin data might not be ready until after 7


def get_db_output():
    td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN_CLARITY"
    jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
    result = jdbc_hook.get_first(query)
    if "2" == f"{result[0]}":
        print("SUCCESS - load of first mothership and clarity margin is complete for today")
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
            return "run_clarity_finance_margin_fact_job_0"
        else:
            return "run_clarity_finance_margin_fact_job_0"
    else:
        raise Exception("TD check exceeded max retry time, job failed")
