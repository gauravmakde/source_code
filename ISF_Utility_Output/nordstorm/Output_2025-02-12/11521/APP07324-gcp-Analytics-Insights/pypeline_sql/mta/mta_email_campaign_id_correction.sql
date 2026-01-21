/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08300;
     DAG_ID=mta_email_campaign_correction_11521_ACE_ENG;
     Task_Name=mta_email_campaign_id_correction;'
     FOR SESSION VOLATILE;

/*


T2/Table Name: T2DL_DAS_MTA.MTA_EMAIL_CAMPAIGN_id_CORRECTION
Team/Owner: MOA/MTA
Date Created/Modified: 6/2023

Note:
-- this table is used to correct campaign name and category for td2l_das_mta.mta_email_performance

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (campaign_id_version),
                    COLUMN (campaign_category_new)
on {mta_t2_schema}.mta_email_campaign_id_correction_ldg
;

delete
from    {mta_t2_schema}.mta_email_campaign_id_correction
;

insert into {mta_t2_schema}.mta_email_campaign_id_correction
select  campaign_id_version
        , campaign_category_new
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {mta_t2_schema}.mta_email_campaign_id_correction_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (campaign_id_version),
                    COLUMN (campaign_category_new)
on {mta_t2_schema}.mta_email_campaign_id_correction
;

-- drop staging table
drop table {mta_t2_schema}.mta_email_campaign_id_correction_ldg
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


