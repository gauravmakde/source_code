SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=organic_social_nord_11521_ACE_ENG;
     Task_Name=ddl_organic_social_nord;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.organic_social_nord
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'organic_social_nord', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.organic_social_nord
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Day_Date DATE FORMAT 'YY/MM/DD',
Facebook_reach FLOAT,
Facebook_visit FLOAT,
Pinterest_Impressions FLOAT,
Facebook_Likes FLOAT,	     
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Day_Date)
;

COMMENT ON  {mmm_t2_schema}.organic_social_nord IS 'organic_social_nord Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;





