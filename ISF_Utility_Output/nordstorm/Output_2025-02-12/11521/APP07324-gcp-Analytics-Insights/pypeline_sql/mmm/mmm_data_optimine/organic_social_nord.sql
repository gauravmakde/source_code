SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=organic_social_nord_11521_ACE_ENG;
     Task_Name=organic_social_nord;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.organic_social_nord
Owner: Analytics Engineering
Modified:02/20/2024

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.organic_social_nord
;

INSERT INTO {mmm_t2_schema}.organic_social_nord
SELECT 
	Day_Date,
Facebook_reach,
Facebook_visit,
Pinterest_Impressions,
Facebook_Likes,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.organic_social_nord_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;







