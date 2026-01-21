SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=ddl_twist_daily_11521_ACE_ENG;
     Task_Name=ddl_twist_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: twist_daily
Team/Owner: Meghan Hickey (meghan.d.hickey@nordstrom.com)
Date Created/Modified: 12/19/2023

GOAL: Create Weighted In-Stock Rates for JWN. Calculates the weighted in-stock position of FLS and N.com
*/
-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'twist_daily', OUT_RETURN_MSG);

create multiset table {twist_t2_schema}.twist_daily
     , fallback
     , no before journal
     , no after journal
     , checksum = default
     , default mergeblockratio
 (
   day_date                 date format 'yyyy-mm-dd'
   , country                VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('CA','US')
   , banner                 VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORD','RACK')
   , business_unit_desc     varchar(50) character set unicode not casespecific compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA' )
   , dma                    integer
   , store_num              VARCHAR(35) CHARACTER SET UNICODE NOT CASESPECIFIC
   , web_style_num          varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC compress
   , rms_style_num          varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC compress
   , rms_sku_num            varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC
   , rp_idnt                integer compress (0,1)
   , ownership_price_type   varchar(25) character set unicode not casespecific compress ('R','C','P')
   , current_price_type     varchar(25) character set unicode not casespecific compress ('R','C','P')
   , current_price_amt      numeric(12,4) compress (0.0000)
   , eoh_mc                    integer compress (0,1,2,3,4,5,6,7,8,9)
   , eoh                    integer compress (0,1,2,3,4,5,6,7,8,9)
   , asoh_mc                   integer compress (0,1,2,3,4,5,6,7,8,9)
   , asoh                    integer compress (0,1,2,3,4,5,6,7,8,9)
   , mc_instock_ind         integer compress (0,1)
   , fc_instock_ind         integer compress (0,1)
   , demand                 numeric(12,4) compress (0.0000)
   , items                  integer compress (0,1,2,3,4,5,6,7,8,9)
   , product_views          integer compress (0,1,2,3,4,5,6,7,8,9)
   , traffic                bigint compress(0)
   , hist_items             integer compress (0,1,2,3,4,5,6,7,8,9)
   , hist_items_style       integer compress (0,1,2,3,4,5,6,7,8,9)
   , pct_items              numeric(12,4) compress (0.0000)
   , allocated_traffic      numeric(12,4) compress (0.0000)
   ,dw_sys_load_tmstp       TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
  primary index (rms_sku_num, day_date, store_num)
 partition by range_n(day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
 unknown);

COMMENT ON {twist_t2_schema}.twist_daily IS 'Weighted In-Stock Rates for JWN. Calculates the weighted in-stock position of FLS and N.com';

collect statistics primary index (rms_sku_num, day_date, store_num)
  , column(rms_sku_num)
  , column(day_date)
  , column(store_num)
on {twist_t2_schema}.twist_daily;

SET QUERY_BAND = NONE FOR SESSION;
