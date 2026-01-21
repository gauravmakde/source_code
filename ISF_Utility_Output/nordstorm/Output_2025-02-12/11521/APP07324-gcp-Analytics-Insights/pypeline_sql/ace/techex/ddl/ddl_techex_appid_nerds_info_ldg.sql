SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_appid_nerds_info_11521_ACE_ENG;
     Task_Name=ddl_techex_appid_nerds_info_ldg;' 
     FOR SESSION VOLATILE;


create multiset table {techex_t2_schema}.techex_appid_nerds_info_ldg,
fallback,
no before journal,
no
after
    journal,
    checksum = default,
    default mergeblockratio (
	  app_id varchar(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  app_name varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  description varchar(3000) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  application_category varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  application_tier varchar(6) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  operational_status varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  support_group varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  sys_created_on timestamp,
      parent_application varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      app_long_name varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      assessment varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      approval_group varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tier_1_assignment_group varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  group_manager varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  group_director varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  group_vp varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  app_health varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ) PRIMARY INDEX(app_id);

SET
    QUERY_BAND = NONE FOR SESSION;
