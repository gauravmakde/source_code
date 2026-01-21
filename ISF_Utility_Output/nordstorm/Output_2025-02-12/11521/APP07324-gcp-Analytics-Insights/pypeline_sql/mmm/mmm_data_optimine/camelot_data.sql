SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=camelot_data_11521_ACE_ENG;
     Task_Name=camelot_data;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.camelot_data
Owner: Analytics Engineering
Modified:02/19/2024

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.camelot_data
;

INSERT INTO {mmm_t2_schema}.camelot_data
SELECT start_date,
end_date,
banner,
campaign,
dma,
platform,
channel,
BAR,
Ad_Type,
Funnel,
External_Funding,
Impressions,
Cost,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.camelot_data_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
