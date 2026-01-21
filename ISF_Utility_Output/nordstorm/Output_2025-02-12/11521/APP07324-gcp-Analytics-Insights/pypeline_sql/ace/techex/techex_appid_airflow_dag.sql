SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_appid_airflow_dag_11521_ACE_ENG;
     Task_Name=techex_appid_airflow_dag;'
     FOR SESSION VOLATILE;

COLLECT STATISTICS COLUMN (dag_id) ON {techex_t2_schema}.techex_appid_airflow_dag_ldg;

delete
from    {techex_t2_schema}.techex_appid_airflow_dag
;

insert into {techex_t2_schema}.techex_appid_airflow_dag
SELECT
	 job_type
         , gitlab_repo
	 , file_name
	 , dag_name
	 , app_id
	 , dag_id
         , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from   {techex_t2_schema}.techex_appid_airflow_dag_ldg
;

COLLECT STATISTICS COLUMN (dag_id) ON {techex_t2_schema}.techex_appid_airflow_dag;

SET QUERY_BAND = NONE FOR SESSION;
