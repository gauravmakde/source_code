/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_leapfrog_segmented_audience_trips_11521_ACE_ENG;
     Task_Name=ddl_leapfrog_segmented_audience_trips;'
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

create multiset table {deg_t2_schema}.leapfrog_segmented_audience_trips 
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    ( 
         activity_date_partition	date	,
channel	varchar(200) character set unicode not casespecific	,
experience	varchar(200) character set unicode not casespecific	,
engagement_cohort	varchar(200) character set unicode not casespecific	,
week_start_day_date	date	,
week_end_day_date	date	,
month_start_day_date	date	,
month_end_day_date	date	,
quarter_start_day_date	date	,
quarter_end_day_date	date	,
fiscal_day_num	integer	,
fiscal_week_num	integer	,
fiscal_month_num	integer	,
fiscal_quarter_num	integer	,
fiscal_year_num	integer	,
recognized_in_period	integer	,
new_recognized_in_period	integer	,
new_in_period	integer	,
bounced_in_period	integer	,
holdout_experimentname	varchar(200) character set unicode not casespecific	,
holdout_variationname	varchar(200) character set unicode not casespecific	,
daily_total_stores_purchased_cust	bigint	,
daily_total_online_purchased_cust	bigint	,
daily_total_purchased_cust	bigint	,
daily_trips_stores	bigint	,
daily_trips_online	bigint	,
daily_trips	bigint	,
daily_trips_stores_gross_usd_amt	numeric(38,2)	,
daily_trips_online_gross_usd_amt	numeric(38,2)	,
daily_trips_gross_usd_amt	numeric(38,2)	,
wtd_total_stores_purchased_cust	bigint	,
wtd_total_online_purchased_cust	bigint	,
wtd_total_purchased_cust	bigint	,
wtd_trips_stores	bigint	,
wtd_trips_online	bigint	,
wtd_trips	bigint	,
wtd_trips_stores_gross_usd_amt	numeric(38,2)	,
wtd_trips_online_gross_usd_amt	numeric(38,2)	,
wtd_trips_gross_usd_amt	numeric(38,2)	,
mtd_total_stores_purchased_cust	bigint	,
mtd_total_online_purchased_cust	bigint	,
mtd_total_purchased_cust	bigint	,
mtd_trips_stores	bigint	,
mtd_trips_online	bigint	,
mtd_trips	bigint	,
mtd_trips_stores_gross_usd_amt	numeric(38,2)	,
mtd_trips_online_gross_usd_amt	numeric(38,2)	,
mtd_trips_gross_usd_amt	numeric(38,2)	,
qtd_total_stores_purchased_cust	bigint	,
qtd_total_online_purchased_cust	bigint	,
qtd_total_purchased_cust	bigint	,
qtd_trips_stores	bigint	,
qtd_trips_online	bigint	,
qtd_trips	bigint	,
qtd_trips_stores_gross_usd_amt	numeric(38,2)	,
qtd_trips_online_gross_usd_amt	numeric(38,2)	,
qtd_trips_gross_usd_amt	numeric(38,2)	,
week_end_total_stores_purchased_cust	bigint	,
week_end_total_online_purchased_cust	bigint	,
week_end_total_purchased_cust	bigint	,
week_end_trips_stores	bigint	,
week_end_trips_online	bigint	,
week_end_trips	bigint	,
week_end_trips_stores_gross_usd_amt	numeric(38,2)	,
week_end_trips_online_gross_usd_amt	numeric(38,2)	,
week_end_trips_trips_gross_usd_amt	numeric(38,2)	,
month_end_total_stores_purchased_cust	bigint	,
month_end_total_online_purchased_cust	bigint	,
month_end_total_purchased_cust	bigint	,
month_end_trips_stores	bigint	,
month_end_trips_online	bigint	,
month_end_trips	bigint	,
month_end_trips_stores_gross_usd_amt	numeric(38,2)	,
month_end_trips_online_gross_usd_amt	numeric(38,2)	,
month_end_trips_trips_gross_usd_amt	numeric(38,2)	,
quarter_end_total_stores_purchased_cust	bigint	,
quarter_end_total_online_purchased_cust	bigint	,
quarter_end_total_purchased_cust	bigint	,
quarter_end_trips_stores	bigint	,
quarter_end_trips_online	bigint	,
quarter_end_trips	bigint	,
quarter_end_trips_stores_gross_usd_amt	numeric(38,2)	,
quarter_end_trips_online_gross_usd_amt	numeric(38,2)	,
quarter_end_trips_trips_gross_usd_amt	numeric(38,2)	
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(activity_date_partition,channel,experience,engagement_cohort,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname,holdout_variationname)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.leapfrog_segmented_audience_trips IS 'The table that showed segemented user level agg trips metrics';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;