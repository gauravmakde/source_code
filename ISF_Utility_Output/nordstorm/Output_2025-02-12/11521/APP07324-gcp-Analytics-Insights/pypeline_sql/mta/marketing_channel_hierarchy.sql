SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=marketing_channel_hierarchy_11521_ACE_ENG;
     Task_Name=marketing_channel_hierarchy;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_PRODUCT_FUNNEL.marketing_channel_hierarchy
Owner: Analytics Engineering
Modified: 2022-11-15

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

delete
from   {mta_t2_schema}.marketing_channel_hierarchy
;

insert into {mta_t2_schema}.marketing_channel_hierarchy

select

    join_channel
    , marketing_type
    , finance_rollup
    , marketing_channel
    , finance_detail
    , marketing_channel_detailed
    , model_channel
    , mmm_channel
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

from  {mta_t2_schema}.marketing_channel_hierarchy_ldg

where 1=1
;

COLLECT STATISTICS COLUMN (join_channel) ON {mta_t2_schema}.marketing_channel_hierarchy;

-- drop staging table
drop table {mta_t2_schema}.marketing_channel_hierarchy_ldg
;

SET QUERY_BAND = NONE FOR SESSION;