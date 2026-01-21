import logging
from datetime import date
from datetime import datetime
from airflow.hooks.jdbc_hook import JdbcHook

logging.basicConfig(level='INFO')
sql_query = """
WITH BIE_DEV_DELETE_LIST AS (
    SELECT a.*, CASE WHEN MaximumLastAccessDate <= CURRENT_DATE()- 15 THEN 'DELETE' 
    WHEN MaximumLastAccessDate is null and MaximumCreateDate <= CURRENT_DATE()- 15  then 'DELETE'
        WHEN left(a.TableName, 3) = 'tmp' THEN 'DELETE'
        ELSE 'KEEP' END  AS TableAction,
    CASE WHEN TableKind in ('T', 'O') then 'TABLE'
    else 'VIEW' end as ActionPrefix
    FROM ( -- OBJECT Date info
        SELECT tv.DataBaseName
            , tv.TableName
            , tv.TableKind
            , CAST(MAX(tv.CreateTimeStamp) AS DATE) AS MaximumCreateDate
            , CAST(MAX(tv.LastAlterTimeStamp) AS DATE) AS MaximumLastAlterDate
            , CAST(MAX(tv.LastAccessTimeStamp) AS DATE) AS MaximumLastAccessDate
        FROM DBC.TablesV tv
        WHERE tv.TableKind IN ('T', 'O', 'V')
        AND tv.DataBaseName = 'T2DL_DAS_BIE_DEV' -- LIKE ANY ('labs_%', 'T2DL_%', 'T3DL_%', 'DL_ACEBI%', 'DL_CMA_%', 'DL_SCM_%')
        AND tv.TableName NOT LIKE 'priv_%'
        GROUP BY 1, 2, 3
         ) a
    )

SELECT
  'DROP ' || ActionPrefix || ' '|| DatabaseName || '.' || TableName || ';' AS DeleteList
    FROM BIE_DEV_DELETE_LIST
    WHERE TableAction = 'DELETE'
        AND TableName NOT IN ('dashboard_catalog', 'techex_dag_run', 'techex_dag')
;
"""

def get_db_output():
    try:
        td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN"
        jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
        result = jdbc_hook.get_records(sql_query)
    except:
        logging.info('Dev connection failed, assuming deployment in prod environment.')
        td_jdbc_conn_id = "TECH_ACE_ENGINEERING_APP07324_JDBC_CONN"
        jdbc_hook = JdbcHook(jdbc_conn_id=td_jdbc_conn_id)
        result = jdbc_hook.get_records(sql_query)
    if result is None:
        raise Exception('No records found')

    for row in result:
        res = jdbc_hook.run(sql=f"{row[0]}", autocommit=True)
        print(f"Executed {row[0]}")


def callable_func_name(*arg, **kwargs):
    query_output = get_db_output()
    if 1 == 1:  
        return "dummy_task_stage_dummy_task"
    else:
        return "always_skip_dummy_task"