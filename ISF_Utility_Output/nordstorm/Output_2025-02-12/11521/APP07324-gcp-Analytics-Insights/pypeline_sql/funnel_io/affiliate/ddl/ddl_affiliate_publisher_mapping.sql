SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=affiliate_publisher_mapping_11521_ACE_ENG;
     Task_Name=ddl_affiliate_publisher_mapping;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliate_publisher_mapping
Team/Owner: Analytics Engineering
Date Created/Modified:2023-09-13


*/
-- Comment out prior to merging to production.
--drop table {funnel_io_t2_schema}.affiliate_publisher_mapping;

create multiset table {funnel_io_t2_schema}.affiliate_publisher_mapping
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    publisher_id INTEGER,
      encrypted_id VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      publisher_name VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      publisher_group VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      publisher_subgroup VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(publisher_id)
;

COMMENT ON  {funnel_io_t2_schema}.affiliate_publisher_mapping IS 'Mapping Affiliates Publisher Data';

SET QUERY_BAND = NONE FOR SESSION;
