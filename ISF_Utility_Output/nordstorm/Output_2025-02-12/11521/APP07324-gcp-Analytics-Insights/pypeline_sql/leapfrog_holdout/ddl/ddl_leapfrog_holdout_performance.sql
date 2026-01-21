/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_leapfrog_holdout_performance_11521_ACE_ENG;
     Task_Name=ddl_leapfrog_holdout_performance;' 
     FOR SESSION VOLATILE;
 

/*
T2/Table Name:
Team/Owner:
Date Created/Modified: 

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window
 
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'leapfrog_holdout_performance', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.leapfrog_holdout_performance
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
          activity_date_partition DATE FORMAT 'YY/MM/DD',
      channel VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      experience VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      engagement_cohort VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_start_day_date DATE FORMAT 'YY/MM/DD',
      week_end_day_date DATE FORMAT 'YY/MM/DD',
      month_start_day_date DATE FORMAT 'YY/MM/DD',
      month_end_day_date DATE FORMAT 'YY/MM/DD',
      quarter_start_day_date DATE FORMAT 'YY/MM/DD',
      quarter_end_day_date DATE FORMAT 'YY/MM/DD',
      fiscal_day_num BIGINT,
      fiscal_week_num BIGINT,
      fiscal_month_num BIGINT,
      fiscal_quarter_num BIGINT,
      fiscal_year_num BIGINT,
      recognized_in_period INTEGER,
      new_recognized_in_period INTEGER,
      new_in_period INTEGER,
      bounced_in_period INTEGER,
      holdout_experimentname VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      holdout_variationname VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      daily_active_user BIGINT,
      daily_ordering_active_user BIGINT,
      daily_total_sessions BIGINT,
      daily_total_demand DECIMAL(12,2),
      daily_total_orders BIGINT,
      wtd_active_user BIGINT,
      wtd_ordering_active_user BIGINT,
      wtd_total_sessions BIGINT,
      wtd_total_demand DECIMAL(12,2),
      wtd_total_orders BIGINT,
      mtd_active_user BIGINT,
      mtd_ordering_active_user BIGINT,
      mtd_total_sessions BIGINT,
      mtd_total_demand DECIMAL(12,2),
      mtd_total_orders BIGINT,
      qtd_active_user BIGINT,
      qtd_ordering_active_user BIGINT,
      qtd_total_sessions BIGINT,
      qtd_total_demand DECIMAL(12,2),
      qtd_total_orders BIGINT,
      week_end_active_user BIGINT,
      week_end_ordering_active_user BIGINT,
      week_end_total_sessions BIGINT,
      week_end_total_demand DECIMAL(12,2),
      week_end_total_orders BIGINT,
      month_end_active_user BIGINT,
      month_end_ordering_active_user BIGINT,
      month_end_total_sessions BIGINT,
      month_end_total_demand DECIMAL(12,2),
      month_end_total_orders BIGINT,
      quarter_end_active_user BIGINT,
      quarter_end_ordering_active_user BIGINT,
      quarter_end_total_sessions BIGINT,
      quarter_end_total_demand DECIMAL(12,2),
      quarter_end_total_orders BIGINT,
      daily_total_stores_purchased_cust BIGINT,
      daily_total_online_purchased_cust BIGINT,
      daily_total_purchased_cust BIGINT,
      daily_trips_stores BIGINT,
      daily_trips_online BIGINT,
      daily_trips BIGINT,
      daily_trips_stores_gross_usd_amt DECIMAL(38,2),
      daily_trips_online_gross_usd_amt DECIMAL(38,2),
      daily_trips_gross_usd_amt DECIMAL(38,2),
      wtd_total_stores_purchased_cust BIGINT,
      wtd_total_online_purchased_cust BIGINT,
      wtd_total_purchased_cust BIGINT,
      wtd_trips_stores BIGINT,
      wtd_trips_online BIGINT,
      wtd_trips BIGINT,
      wtd_trips_stores_gross_usd_amt DECIMAL(38,2),
      wtd_trips_online_gross_usd_amt DECIMAL(38,2),
      wtd_trips_gross_usd_amt DECIMAL(38,2),
      mtd_total_stores_purchased_cust BIGINT,
      mtd_total_online_purchased_cust BIGINT,
      mtd_total_purchased_cust BIGINT,
      mtd_trips_stores BIGINT,
      mtd_trips_online BIGINT,
      mtd_trips BIGINT,
      mtd_trips_stores_gross_usd_amt DECIMAL(38,2),
      mtd_trips_online_gross_usd_amt DECIMAL(38,2),
      mtd_trips_gross_usd_amt DECIMAL(38,2),
      qtd_total_stores_purchased_cust BIGINT,
      qtd_total_online_purchased_cust BIGINT,
      qtd_total_purchased_cust BIGINT,
      qtd_trips_stores BIGINT,
      qtd_trips_online BIGINT,
      qtd_trips BIGINT,
      qtd_trips_stores_gross_usd_amt DECIMAL(38,2),
      qtd_trips_online_gross_usd_amt DECIMAL(38,2),
      qtd_trips_gross_usd_amt DECIMAL(38,2),
      week_end_total_stores_purchased_cust BIGINT,
      week_end_total_online_purchased_cust BIGINT,
      week_end_total_purchased_cust BIGINT,
      week_end_trips_stores BIGINT,
      week_end_trips_online BIGINT,
      week_end_trips BIGINT,
      week_end_trips_stores_gross_usd_amt DECIMAL(38,2),
      week_end_trips_online_gross_usd_amt DECIMAL(38,2),
      week_end_trips_trips_gross_usd_amt DECIMAL(38,2),
      month_end_total_stores_purchased_cust BIGINT,
      month_end_total_online_purchased_cust BIGINT,
      month_end_total_purchased_cust BIGINT,
      month_end_trips_stores BIGINT,
      month_end_trips_online BIGINT,
      month_end_trips BIGINT,
      month_end_trips_stores_gross_usd_amt DECIMAL(38,2),
      month_end_trips_online_gross_usd_amt DECIMAL(38,2),
      month_end_trips_trips_gross_usd_amt DECIMAL(38,2),
      quarter_end_total_stores_purchased_cust BIGINT,
      quarter_end_total_online_purchased_cust BIGINT,
      quarter_end_total_purchased_cust BIGINT, 
      quarter_end_trips_stores BIGINT,
      quarter_end_trips_online BIGINT,
      quarter_end_trips BIGINT,
      quarter_end_trips_stores_gross_usd_amt DECIMAL(38,2),
      quarter_end_trips_online_gross_usd_amt DECIMAL(38,2),
      quarter_end_trips_trips_gross_usd_amt DECIMAL(38,2)
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(activity_date_partition		,
channel		,
experience		,
engagement_cohort,
week_start_day_date		,
week_end_day_date		,
month_start_day_date		,
month_end_day_date		,
quarter_start_day_date		,
quarter_end_day_date		,
fiscal_day_num		,
fiscal_week_num		,
fiscal_month_num		,
fiscal_quarter_num		,
fiscal_year_num		,
recognized_in_period		,
new_recognized_in_period		,
new_in_period		,
bounced_in_period		,
        holdout_experimentname,
        holdout_variationname)
partition by RANGE_N(activity_date_partition BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.leapfrog_holdout_performance IS 'the final table that helps to agg daily level data to feed dashboard leapfrog holdout performance dashboard';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
