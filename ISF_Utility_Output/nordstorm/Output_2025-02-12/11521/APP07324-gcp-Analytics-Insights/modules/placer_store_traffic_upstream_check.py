import logging
from datetime import date
from datetime import datetime, timezone, timedelta
import time
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')


query = """
select
case when count(distinct CASE WHEN traffic > 0 then p.store_num else null end) > 300 then 1 else 0 end as isCompleted
-- expected # of stores is ~317 but I'm allowing wiggle room for stores opening/closing/having placer bad data flags
from t2dl_das_fls_traffic_model.store_traffic_daily_placer as p
inner join PRD_NAP_base_vws.STORE_DIM as sd on p.store_num = sd.store_num
where 1=1
and sd.business_unit_desc in ('Rack', 'FULL LINE') -- NL and CC also under them
and sd.store_type_code in ('RK', 'FL')
and cast(p.dw_sys_load_tmstp as date) = current_date
and p.day_date between current_date-5 and current_date-3 
--placer lag varies, so use day_date and load_tmstp to be sure
;
"""
max_duration_minutes = (60*2.5)/4 #starts at 3am, want this to timeout after 2.5 hours (to avoid start of third placer load job) but it will retry 4 times before it actually fails



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


    if datetime.today().weekday() >= 5: #Return the day of the week as an integer, where Monday is 0 and Sunday is 6
      
      #on weekend the check should automatically pass even though placer data isn't loaded. This job is downstream of rn_camera_and_trips_daily
      print(f"Job succeed")
      if 1 == 1:
          return "run_clarity_finance_sales_demand_margin_traffic_fact_job_0"
      else:
          return "run_clarity_finance_sales_demand_margin_traffic_fact_job_0"
        
    else:    #I only want to run this on weekdays to make sure placer data is ready
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
              return "run_clarity_finance_sales_demand_margin_traffic_fact_job_0"
          else:
              return "run_clarity_finance_sales_demand_margin_traffic_fact_job_0"
      else:
          raise Exception("TD check exceeded max retry time, job failed")
