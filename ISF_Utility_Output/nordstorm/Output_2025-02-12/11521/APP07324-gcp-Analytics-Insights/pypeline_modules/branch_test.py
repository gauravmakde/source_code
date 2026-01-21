import logging
from datetime import date
from datetime import datetime
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')
sql_query = """
select max(dates) from T2DL_DAS_BIE_DEV.ace_data_quality_checks 
where object_name = 'product_funnel_daily';
"""

td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN"
today = date.today()

def get_db_output():
    jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
    result = jdbc_hook.get_first(sql_query)
    if result is None:
        raise Exception('no records found')
    else:
        logging.info(result)
        return result


def callable_func_name(*arg, **kwargs):
    query_output = get_db_output()
    query_output_dt = datetime.strptime(query_output[0], '%Y-%m-%d').date()
    print("sql result = ", query_output_dt)
    print("Today's date = ", today)
    if query_output_dt == today:  
        return {task1}
    else:
        return {task2}