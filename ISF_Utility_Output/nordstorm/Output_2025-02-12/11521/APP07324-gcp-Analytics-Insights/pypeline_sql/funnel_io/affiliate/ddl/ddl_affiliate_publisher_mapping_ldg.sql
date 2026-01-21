SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=affiliate_publisher_mapping_11521_ACE_ENG;
     Task_Name=ddl_affiliate_publisher_mapping_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliate_publisher_mapping
Team/Owner: Analytics Engineering
Date Created/Modified: 2023-09-13

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'affiliate_publisher_mapping_ldg', OUT_RETURN_MSG);

create multiset table {funnel_io_t2_schema}.affiliate_publisher_mapping_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (publisher_id INTEGER,
      encrypted_id VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      publisher_name VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      publisher_group VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      publisher_subgroup VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC
      )

PRIMARY INDEX (publisher_id)
;


SET QUERY_BAND = NONE FOR SESSION;

