SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=marketing_channel_hierarchy_11521_ACE_ENG;
     Task_Name=ddl_marketing_channel_hierarchy_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MTA.marketing_channel_hierarchy_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-11-15

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'marketing_channel_hierarchy_ldg', OUT_RETURN_MSG);

create multiset table {mta_t2_schema}.marketing_channel_hierarchy_ldg
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
      )

PRIMARY INDEX (join_channel)
;
SET QUERY_BAND = NONE FOR SESSION;