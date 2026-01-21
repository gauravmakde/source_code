from airflow.hooks.jdbc_hook import JdbcHook

query = """
locking row for access
SELECT
  case when week_454_num = 1 then day_454_num else 0 end day_454_num
FROM
  PRD_NAP_BASE_VWS.DAY_CAL
WHERE
  DAY_DATE = DATE
"""
max_duration_minutes = 60


def get_db_output():
    td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN"
    jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
    result = jdbc_hook.get_first(query)
    if result is None:
        raise Exception('No results returned from Teradata')
    else:
        print(result)
        return result


def callable_func_name(*arg, **kwargs):
    result = get_db_output()
    if result[0] == 2 or result[0] == 3 or result[0] == 4:
        print("It is the 2nd, 3rd, or 4th day of the 454 calendar month")
        return {task1}
    else:
        print("It is NOT the 2nd, 3rd, or 4th day of the 454 calendar month, skipping task.")
        return {task2}
