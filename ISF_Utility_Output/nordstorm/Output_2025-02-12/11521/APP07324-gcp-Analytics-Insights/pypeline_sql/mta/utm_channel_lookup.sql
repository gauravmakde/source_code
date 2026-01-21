SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=utm_channel_lookup_11521_ACE_ENG;
     Task_Name=utm_channel_lookup;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_PRODUCT_FUNNEL.utm_channel_lookup
Owner: Analytics Engineering
Modified: 2022-11-03

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

delete
from    {mta_t2_schema}.utm_channel_lookup
;

insert into {mta_t2_schema}.utm_channel_lookup

select
    bi_channel
    , utm_mkt_chnl
    , strategy
    , funding_type
    , utm_prefix
    , funnel_type
    , utm_suffix
    , create_timestamp
    , update_timestamp
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

from  {mta_t2_schema}.utm_channel_lookup_ldg

where 1=1
;

COLLECT STATISTICS COLUMN (utm_mkt_chnl, create_timestamp) ON {mta_t2_schema}.utm_channel_lookup;

-- drop staging table
drop table {mta_t2_schema}.utm_channel_lookup_ldg
;

SET QUERY_BAND = NONE FOR SESSION;