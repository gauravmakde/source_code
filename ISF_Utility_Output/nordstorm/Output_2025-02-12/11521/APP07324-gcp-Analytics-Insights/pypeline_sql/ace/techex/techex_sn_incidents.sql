SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_sn_incidents_11521_ACE_ENG;
     Task_Name=techex_sn_incidents;'
     FOR SESSION VOLATILE;

COLLECT STATISTICS COLUMN (inc_number) ON {techex_t2_schema}.techex_sn_incidents_ldg;
delete
from    {techex_t2_schema}.techex_sn_incidents
;

insert into {techex_t2_schema}.techex_sn_incidents
SELECT
         inc_number
         , short_description
         , description
         , category
         , dv_assigned_to
         , dv_caller_id
         , priority
         , dv_priority
         , dv_state
         , urgency
         , sys_created_on
         , sys_created_by
         , resolved_at
         , time_worked
         , close_notes
         , comments_and_work_notes
         , work_notes_list
         , assignment_group_name
         , app_id
         , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from   {techex_t2_schema}.techex_sn_incidents_ldg
;

COLLECT STATISTICS COLUMN (inc_number) ON {techex_t2_schema}.techex_sn_incidents;

SET QUERY_BAND = NONE FOR SESSION;
