SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_dag_11521_ACE_ENG;
     Task_Name=techex_dag;'
     FOR SESSION VOLATILE;

delete
from    {techex_t2_schema}.techex_dag
;

insert into {techex_t2_schema}.techex_dag 
SELECT 
           dag_id                      
         , is_paused                   
         , is_subdag                   
         , is_active                   
         , last_scheduler_run          
         , last_expired                
         , fileloc                     
         , owners                      
         , description                 
         , default_view                
         , schedule_interval           
         , CURRENT_TIMESTAMP as dw_sys_load_tmstp           
from   {techex_t2_schema}.techex_dag_ldg 
;

COLLECT STATISTICS COLUMN (DAG_ID) ON {techex_t2_schema}.techex_dag;

SET QUERY_BAND = NONE FOR SESSION;
