import logging
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')

 
demand_query = """
with s1 as (
SELECT
business_unit_desc,
CASE
    WHEN business_unit_desc='FULL LINE' and sum(fulfilled_demand_usd_amt) between 10000000 and 70000000 THEN 1
    WHEN business_unit_desc='OFFPRICE ONLINE' and sum(fulfilled_demand_usd_amt) between 1200000 and 10000000 THEN 1
    WHEN business_unit_desc='RACK' and sum(fulfilled_demand_usd_amt) between 5500000 and 33000000 THEN 1
    WHEN business_unit_desc='N.COM' and sum(fulfilled_demand_usd_amt) between 8000000 and 70000000 THEN 1
    ELSE 0
    END as isCompleted
FROM
  PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_fact
WHERE tran_date = CURRENT_DATE()-1
and business_unit_desc in ('FULL LINE', 'OFFPRICE ONLINE', 'RACK', 'N.COM')
and tran_date not in (
    '2024-12-25','2025-12-25','2026-12-25', --xmas
    '2024-11-28','2025-11-27','2026-11-26', --thanksgiving
    '2024-03-31','2025-04-20','2026-04-05' --easter
  )
group by 1
)
Select
sum(isCompleted) as isCompleted
from s1
;
"""

gmv_query = """
with s1 as (
SELECT
business_unit_desc,
CASE
    WHEN business_unit_desc='FULL LINE' and sum(op_gmv_usd_amt) between 5000000 and 60000000 THEN 1
    WHEN business_unit_desc='OFFPRICE ONLINE' and sum(op_gmv_usd_amt) between 300000 and 8000000 THEN 1
    WHEN business_unit_desc='RACK' and sum(op_gmv_usd_amt) between 5000000 and 30000000 THEN 1
    WHEN business_unit_desc='N.COM' and sum(op_gmv_usd_amt) between 500000 and 60000000 THEN 1
    ELSE 0
    END as isCompleted
FROM
  PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_fact
WHERE tran_date = CURRENT_DATE()-1
and business_unit_desc in ('FULL LINE', 'OFFPRICE ONLINE', 'RACK', 'N.COM')
and tran_date not in (
    '2024-12-25','2025-12-25','2026-12-25', --xmas
    '2024-11-28','2025-11-27','2026-11-26', --thanksgiving
    '2024-03-31','2025-04-20','2026-04-05' --easter
  )
group by 1
)
Select
sum(isCompleted) as isCompleted
from s1
;
"""


def get_db_output(query):
    td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN_CLARITY"
    jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
    result1 = jdbc_hook.get_first(query)
    result = f"{result1[0]}"
    return result


def callable_func_name(*arg, **kwargs):
    demand_result = get_db_output(query = demand_query)
    gmv_result = get_db_output(query = gmv_query)
    if demand_result == "4" and gmv_result == "4":
        print(f"Demand and opGMV are within normal limits for all business units")
        return "run_clarity_finance_sales_demand_margin_traffic_fact_job_0"
    elif demand_result != "4" and gmv_result == "4":
        raise Exception("Data quality check failed, Demand is unusually high or low for at least one business unit")
    elif demand_result == "4" and gmv_result != "4":
        raise Exception("Data quality check failed, opGMV is unusually high or low for at least one business unit")
    else: #demand_result < 4 and gmv_result < 4:
        raise Exception("Data quality check failed, Demand and opGMV are both unusually high or low for at least one business unit")
