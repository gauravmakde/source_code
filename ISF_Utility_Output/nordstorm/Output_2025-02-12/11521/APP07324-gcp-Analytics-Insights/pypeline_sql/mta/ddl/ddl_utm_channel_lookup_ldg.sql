SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=utm_channel_lookup_11521_ACE_ENG;
     Task_Name=ddl_utm_channel_lookup_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MTA.utm_channel_lookup_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-11-02

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'utm_channel_lookup_ldg', OUT_RETURN_MSG);

create multiset table {mta_t2_schema}.utm_channel_lookup_ldg
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
      )

PRIMARY INDEX (utm_mkt_chnl, create_timestamp)
;

SET QUERY_BAND = NONE FOR SESSION;