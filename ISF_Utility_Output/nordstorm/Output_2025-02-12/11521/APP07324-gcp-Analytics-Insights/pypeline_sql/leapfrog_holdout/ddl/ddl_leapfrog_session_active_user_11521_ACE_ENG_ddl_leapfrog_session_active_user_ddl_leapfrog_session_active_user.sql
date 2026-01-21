/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_leapfrog_session_active_user_11521_ACE_ENG;
     Task_Name=ddl_leapfrog_session_active_user;'
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

create multiset table T2DL_DAS_DIGENG.leapfrog_session_active_user
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    activity_date_partition	date	,
channel	varchar(150) character set unicode not casespecific	,
experience	varchar(150) character set unicode not casespecific	,
week_start_day_date	date	,
week_end_day_date	date	,
month_start_day_date	date	,
month_end_day_date	date	,
quarter_start_day_date	date	,
quarter_end_day_date	date	,
fiscal_day_num	bigint	,
fiscal_week_num	bigint	,
fiscal_month_num	bigint	,
fiscal_quarter_num	bigint	,
fiscal_year_num	bigint	,
recognized_in_period	integer	,
new_recognized_in_period	integer	,
new_in_period	integer	,
bounced_in_period	integer	,
            holdout_experimentname varchar(150) character set unicode not casespecific,
            holdout_variationname varchar(150) character set unicode not casespecific,
daily_active_user	bigint	,
daily_ordering_active_user bigint,
daily_total_sessions	bigint	,
daily_total_demand	numeric(12,2)	,
daily_total_orders	bigint	,
wtd_active_user	bigint	,
wtd_ordering_active_user bigint,
wtd_total_sessions	bigint	,
wtd_total_demand	numeric(12,2)	,
wtd_total_orders	bigint	,
mtd_active_user	bigint	,
mtd_ordering_active_user bigint,
mtd_total_sessions	bigint	,
mtd_total_demand	numeric(12,2)	,
mtd_total_orders	bigint	,
qtd_active_user	bigint	,
qtd_ordering_active_user bigint,
qtd_total_sessions	bigint	,
qtd_total_demand	numeric(12,2)	,
qtd_total_orders	bigint	,
week_end_active_user	bigint	,
week_end_ordering_active_user bigint,
week_end_total_sessions	bigint	,
week_end_total_demand	numeric(12,2)	,
week_end_total_orders	bigint	,
month_end_active_user	bigint	,
month_end_ordering_active_user bigint,
month_end_total_sessions	bigint	,
month_end_total_demand	numeric(12,2)	,
month_end_total_orders	bigint	,
quarter_end_active_user	bigint	,
quarter_end_ordering_active_user bigint,
quarter_end_total_sessions	bigint	,
quarter_end_total_demand	numeric(12,2)	,
quarter_end_total_orders	bigint
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname,holdout_variationname)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

-- Table Comment (STANDARD)
COMMENT ON  T2DL_DAS_DIGENG.leapfrog_session_active_user IS 'The table that showed user level agg info based on SPEED';


/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;