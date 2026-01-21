
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=mktg_dm_customer_audience_11521_ACE_ENG;
     Task_Name=mktg_dm_customer_audience_s3_td;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.mktg_dm_customer_audience
Team/Owner: Nicole Miao
Date Created/Modified: Dec 5, 2023

Note:
-- What is the the purpose of the table: The purpose of this table is to create an one-stop view, containing marketable/reachable information for four marketing channels (Paid, Email, DM, and Site), as well as customer attributes on the ACP_ID level. 
-- What is the update cadence/lookback window: Monthly update, 12 months look back window

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    {mktg_audience_t2_schema}.mktg_dm_customer_audience
;

insert into  {mktg_audience_t2_schema}.mktg_dm_customer_audience
select  acp_id
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {mktg_audience_t2_schema}.mktg_dm_customer_audience_ldg
;

-- drop staging table
drop table {mktg_audience_t2_schema}.mktg_dm_customer_audience_ldg
;

collect statistics column (acp_id)
on {mktg_audience_t2_schema}.mktg_dm_customer_audience
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
