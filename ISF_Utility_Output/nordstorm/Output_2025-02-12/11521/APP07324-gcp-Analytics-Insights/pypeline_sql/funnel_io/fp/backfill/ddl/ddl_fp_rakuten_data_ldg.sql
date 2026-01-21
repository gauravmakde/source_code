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
    , sourcetype VARCHAR(255)
    , currency CHAR(3)
    , platform VARCHAR(255)
    , platform_id VARCHAR(255)
    , adgroup_name VARCHAR(255)
    , adgroup_id VARCHAR(255)
    , order_number VARCHAR(255)
    , estimated_net_total_cost DECIMAL(18,2)
    , gross_commissions DECIMAL(18,2)
    , gross_sales DECIMAL(18,2)
    , sales DECIMAL(18,2)
    , campaign_name VARCHAR(255)
      )

PRIMARY INDEX (stats_date)
;

