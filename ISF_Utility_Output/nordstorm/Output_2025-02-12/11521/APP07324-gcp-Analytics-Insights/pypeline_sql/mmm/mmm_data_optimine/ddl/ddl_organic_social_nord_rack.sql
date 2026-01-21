SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=organic_social_nord_rack_11521_ACE_ENG;
     Task_Name=ddl_organic_social_nord_rack;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.organic_social_nord_rack
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

--drop table {mmm_t2_schema}.organic_social_nord_rack;
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'organic_social_nord_rack', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.organic_social_nord_rack
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Day_Date DATE FORMAT 'YY/MM/DD',
Channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
funnel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
funding_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Impressions FLOAT,
Reach FLOAT,
Likes FLOAT,
Cost FLOAT,	    
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Day_Date,Channel,Platform,funnel,funding_type)
;

COMMENT ON  {mmm_t2_schema}.organic_social_nord_rack IS 'organic_social_nord_rack Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;





