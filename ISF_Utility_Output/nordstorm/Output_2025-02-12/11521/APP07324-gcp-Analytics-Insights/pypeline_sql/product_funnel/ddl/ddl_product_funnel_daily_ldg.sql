SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=product_funnel_daily_11521_ACE_ENG;
     Task_Name=ddl_product_funnel_daily_ldg;'
     FOR SESSION VOLATILE;

-- Table:  T2DL_DAS_PRODUCT_FUNNEL.product_funnel_daily_ldg
-- Owner: Analytics Engineering
-- Modified: 2022-10-17

-- This table is created as part of the product_funnel_daily job that loads
-- data from S3 to teradata.  The landing table is dropped when the job completes.

create multiset table {product_funnel_t2_schema}.product_funnel_daily_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
         event_date_pacific         date
         , channelcountry           varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
         , channel                  varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
         , platform                 varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
         , site_source              varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC
         , style_id                 varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC -- varchar for ldg table.  Integer in final table.
         , sku_id                   varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
         , averagerating            float
         , review_count             integer
         , order_quantity           integer
         , order_demand             numeric(12,2)
         , order_sessions           integer
         , add_to_bag_quantity      integer
         , add_to_bag_sessions      integer
         , product_views            numeric(12,2)
         , product_view_sessions    numeric(12,2)
)
PRIMARY INDEX(style_id)
PARTITION BY RANGE_N(event_date_pacific BETWEEN DATE '2018-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY)
;

SET QUERY_BAND = NONE FOR SESSION;