
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09035;
     DAG_ID=promo_event_desc_historical_data_11521_ACE_ENG;
     Task_Name=promo_event_desc_historical_data_s3_td;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA
Team/Owner: Nicole Miao
Date Created/Modified: Oct 30, 2023
*/

-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (event_name, event_id, promo_name, promo_id), -- column names used for primary index
                    COLUMN (event_name),
                    COLUMN (event_id),
                    COLUMN (promo_name),
                    COLUMN (promo_id)
on {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA_ldg
;


delete 
from   {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA
;

insert into  {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA
select  tbl_source 
        , event_id
        , event_name
        , promo_id
        , promo_name
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA_ldg
;


collect statistics column (event_id, promo_id),
                   column (event_id),
                   column (promo_id)
on {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

