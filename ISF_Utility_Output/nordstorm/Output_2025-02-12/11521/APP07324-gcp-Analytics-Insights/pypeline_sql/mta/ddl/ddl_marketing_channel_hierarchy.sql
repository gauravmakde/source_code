SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_marketing_channel_hierarchy_11521_ACE_ENG;
     Task_Name=ddl_marketing_channel_hierarchy;'
     FOR SESSION VOLATILE;

/*
Table:  T2DL_DAS_MTA.marketing_channel_hierarchy
Owner: Analytics Engineering
Modified: 2022-11-15
*/

create multiset table {mta_t2_schema}.marketing_channel_hierarchy
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
        join_channel varchar (40) CHARACTER SET UNICODE NOT CASESPECIFIC
        , marketing_type varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , finance_rollup varchar (20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , marketing_channel varchar (30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , finance_detail varchar (30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , marketing_channel_detailed varchar (30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , model_channel varchar (30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , mmm_channel varchar (30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )

PRIMARY INDEX (join_channel)
;
SET QUERY_BAND = NONE FOR SESSION;