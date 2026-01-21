/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_segmented_audience_trips_11521_ACE_ENG;
     Task_Name=ddl_leapfrog_session_active_audience_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{deg_t2_schema}', 'leapfrog_session_active_audience_ldg', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.leapfrog_session_active_audience_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    activity_date_partition	date	,
channel	varchar(150) character set unicode not casespecific	,
experience	varchar(150) character set unicode not casespecific	,
engagement_cohort	varchar(150) character set unicode not casespecific	,
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
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;