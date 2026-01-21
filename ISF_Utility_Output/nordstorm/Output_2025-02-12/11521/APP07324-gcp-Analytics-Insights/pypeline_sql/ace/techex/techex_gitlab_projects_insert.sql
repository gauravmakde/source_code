SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_pd_incidents_11521_ACE_ENG;
     Task_Name=techex_gitlab_projects_insert;'
     FOR SESSION VOLATILE;

COLLECT STATISTICS COLUMN (app_id) ON {techex_t2_schema}.gitlab_projects_ldg;

delete
from    {techex_t2_schema}.gitlab_projects
;

insert into {techex_t2_schema}.gitlab_projects
SELECT
     app_id
     , team_id
     , archived
     , fullPath
     , httpUrlToRepo
	 , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from   {techex_t2_schema}.gitlab_projects_ldg
;

COLLECT STATISTICS COLUMN (app_id) ON {techex_t2_schema}.gitlab_projects;


SET QUERY_BAND = NONE FOR SESSION;
