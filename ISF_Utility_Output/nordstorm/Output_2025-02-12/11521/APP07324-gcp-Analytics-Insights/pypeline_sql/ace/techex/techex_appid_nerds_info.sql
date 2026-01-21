SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_appid_nerds_info_11521_ACE_ENG;
     Task_Name=techex_appid_nerds_info;'
     FOR SESSION VOLATILE;

delete
from    {techex_t2_schema}.techex_appid_nerds_info
;

insert into {techex_t2_schema}.techex_appid_nerds_info
SELECT
	    app_id 
	  , app_name
	  , description
	  , application_category
	  , application_tier
	  , operational_status
	  , support_group
	  , sys_created_on 
      , parent_application 
      , app_long_name 
      , assessment 
      , approval_group 
      , tier_1_assignment_group 
	  , group_manager 
	  , group_director 
	  , group_vp 
	  , app_health
      , CURRENT_TIMESTAMP as dw_sys_load_tmstp           
from   {techex_t2_schema}.techex_appid_nerds_info_ldg
;

COLLECT STATISTICS COLUMN (app_id) ON {techex_t2_schema}.techex_appid_nerds_info;

SET QUERY_BAND = NONE FOR SESSION;

