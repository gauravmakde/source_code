/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09035;
     DAG_ID=ddl_promo_event_desc_historical_data_11521_ACE_ENG;
     Task_Name=ddl_promo_event_desc_historical_data_s3_ldg;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.PROMOTION_EVENT_HISTORICAL_DATA
Team/Owner: Nicole Miao
Date Created/Modified: Dec 4, 2023
*/
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{price_matching_t2_schema}', 'PROMOTION_EVENT_HISTORICAL_DATA_ldg', OUT_RETURN_MSG);

create multiset table {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     tbl_source           varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC
     , event_id           integer 
     ,event_name          varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC 
     ,promo_id            integer 
     ,promo_name          varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC 
    )
primary index(event_name, event_id, promo_name, promo_id)
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;



