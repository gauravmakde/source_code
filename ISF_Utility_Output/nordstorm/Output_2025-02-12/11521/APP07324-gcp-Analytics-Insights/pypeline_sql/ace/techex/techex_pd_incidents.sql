SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_pd_incidents_11521_ACE_ENG;
     Task_Name=techex_pd_incidents;'
     FOR SESSION VOLATILE;

COLLECT STATISTICS COLUMN (incident_number) ON {techex_t2_schema}.techex_pd_incidents_ldg;

delete
from    {techex_t2_schema}.techex_pd_incidents
;

insert into {techex_t2_schema}.techex_pd_incidents
SELECT
	   incident_number
	 , incident_title
	 , incident_summary
	 , created_at
	 , updated_at
	 , urgency
	 , status
	 , incident_id
     , app_id
	 , service_name
	 , service_id
	 , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from   {techex_t2_schema}.techex_pd_incidents_ldg
;

COLLECT STATISTICS COLUMN (incident_number) ON {techex_t2_schema}.techex_pd_incidents;

SET QUERY_BAND = NONE FOR SESSION;
