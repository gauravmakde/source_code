import logging
from datetime import date
from datetime import datetime, timezone, timedelta
import time
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from airflow.models import Variable
logging.basicConfig(level='INFO')
import ast
import sys



bq_hook = BigQueryHook(gcp_conn_id=gcp_conn,location=region,use_legacy_sql=False)

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
    query_job = bq_hook.get_first(query)
    result = query_job.result()  
    row = next(result) 
    print('#'*50)
    print(row[0])
    print('#'*50)
    # result = jdbc_hook.get_first(query)
    if "1" == f"{row[0]}":
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
        print(f"BQ hasn't been updated. Retrying at {next_retry}")
        retry_interval_seconds = 5 * 60
        time.sleep(retry_interval_seconds)

    if (result):
        print(f"Job succeed")
        """ 
            We have check the task1 and task 2 it has same value run_sales_and_returns_fact_base but that is incorrect
            as there is no task labeled run_sales_and_returns_fact_base, so we pass correct label value.
            """
        if 1 == 1:
            
            return "run_sales_and_returns_fact_base_job_0_product_reg_price_dim" 
        else:
            return "run_sales_and_returns_fact_base_job_1_retail_tran_price_type_fact"
    else:
        raise Exception("BQ check exceeded max retry time, job failed")