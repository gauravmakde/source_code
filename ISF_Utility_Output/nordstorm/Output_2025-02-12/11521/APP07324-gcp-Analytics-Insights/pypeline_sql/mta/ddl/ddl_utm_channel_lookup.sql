SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_utm_channel_lookup_11521_ACE_ENG;
     Task_Name=ddl_utm_channel_lookup;'
     FOR SESSION VOLATILE;

/*
Table:  T2DL_DAS_MTA.utm_channel_lookup
Owner: Analytics Engineering
Modified: 2022-11-02
*/

create multiset table {mta_t2_schema}.utm_channel_lookup
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
     (
        bi_channel varchar (50) CHARACTER SET UNICODE NOT CASESPECIFIC
        , utm_mkt_chnl varchar (50) CHARACTER SET UNICODE NOT CASESPECIFIC
        , strategy varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , funding_type varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , utm_prefix varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , funnel_type varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , utm_suffix varchar (30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , create_timestamp date
        , update_timestamp date
        , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )

PRIMARY INDEX (utm_mkt_chnl, create_timestamp)
;
SET QUERY_BAND = NONE FOR SESSION;