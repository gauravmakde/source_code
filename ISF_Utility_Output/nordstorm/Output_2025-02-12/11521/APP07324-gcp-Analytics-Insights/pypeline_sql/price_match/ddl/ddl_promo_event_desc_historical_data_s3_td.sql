/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09035;
     DAG_ID=ddl_promo_event_desc_historical_data_11521_ACE_ENG;
     Task_Name=ddl_promo_event_desc_historical_data_s3_td;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.PROMOTION_EVENT_HISTORICAL_DATA
Team/Owner: Nicole Miao
Date Created/Modified: Dec 4, 2023
*/
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'PROMOTION_EVENT_HISTORICAL_DATA', OUT_RETURN_MSG);

create multiset table  {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA
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
     ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(event_name, event_id, promo_name, promo_id)
;

-- Table Comment (STANDARD)
COMMENT ON {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA IS 'The historical price match promotion data';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

