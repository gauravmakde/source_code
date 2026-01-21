SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=direct_mail_11521_ACE_ENG;
     Task_Name=direct_mail;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.direct_mail
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.direct_mail
;

INSERT INTO {mmm_t2_schema}.direct_mail
SELECT start_date,
	campaign_name,
	channel_type,
	bar,
	sales_channel,
	region,
	circulation,
	cost,
	store_no,
	funding_type,
	funnel_type,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.direct_mail_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;



