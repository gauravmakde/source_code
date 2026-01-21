SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=ddl_fp_rakuten_data_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.fp_rakuten_data_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-12-21

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{funnel_io_t2_schema}', 'fp_rakuten_data_ldg', OUT_RETURN_MSG);
create multiset table {funnel_io_t2_schema}.fp_rakuten_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
    stats_date DATE
    , sourcetype VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , currency CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
    , platform VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , platform_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , adgroup_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , adgroup_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , order_number VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , estimated_net_total_cost DECIMAL(18,2)
    , gross_commissions DECIMAL(18,2)
    , gross_sales DECIMAL(18,2)
    , sales DECIMAL(18,2)
    , campaign_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , clicks FLOAT
      )

PRIMARY INDEX (stats_date)
;


SET QUERY_BAND = NONE FOR SESSION;