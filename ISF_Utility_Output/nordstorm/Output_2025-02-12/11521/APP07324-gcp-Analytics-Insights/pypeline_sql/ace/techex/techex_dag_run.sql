SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_dag_run_11521_ACE_ENG;
     Task_Name=techex_dag_run;'
     FOR SESSION VOLATILE;

delete
from    {techex_t2_schema}.techex_dag_run
where   execution_date between {start_date} and {end_date}
;

insert into {techex_t2_schema}.techex_dag_run
SELECT 
         id                          
         , dag_id                   
         , state                    
         , run_id                   
         , external_trigger         
         , start_date               
         , end_date                 
         , execution_date
         , CURRENT_TIMESTAMP as dw_sys_load_tmstp           
from   {techex_t2_schema}.techex_dag_run_ldg
where  execution_date between {start_date} and {end_date}
;

COLLECT STATISTICS COLUMN (PARTITION) ON {techex_t2_schema}.techex_dag_run;
COLLECT STATISTICS COLUMN (PARTITION, DAG_ID) ON {techex_t2_schema}.techex_dag_run;

SET QUERY_BAND = NONE FOR SESSION;
