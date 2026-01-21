/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_segmented_audience_trips_11521_ACE_ENG;
     Task_Name=leapfrog_session_active_audience;'
     FOR SESSION VOLATILE;


/*


T2/Table Name: leapfrog_session_active_audience
Team/Owner: Jin Liu
Date Created/Modified: 2024-01-12

Note:
-- What is the the purpose of the table : to replicate the dashboard we built for session active user but only flag the user who were exposed to leapfrog experiments
-- What is the update cadence/lookback window: daily between current - 9 and current -3

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
 
*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname, holdout_variationname), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on T2DL_DAS_DIGENG.leapfrog_session_active_audience_ldg
;

delete
from    T2DL_DAS_DIGENG.leapfrog_session_active_audience
where    activity_date_partition between '2024-05-06' and '2024-05-14'
;

insert into T2DL_DAS_DIGENG.leapfrog_session_active_audience
select  
 activity_date_partition		,
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
        holdout_variationname,
daily_active_user		,
daily_ordering_active_user ,
daily_total_sessions		,
daily_total_demand		,
daily_total_orders		,
wtd_active_user		,
wtd_ordering_active_user ,
wtd_total_sessions		,
wtd_total_demand		,
wtd_total_orders		,
mtd_active_user		,
mtd_ordering_active_user ,
mtd_total_sessions		,
mtd_total_demand		,
mtd_total_orders		,
qtd_active_user		,
qtd_ordering_active_user ,
qtd_total_sessions		,
qtd_total_demand		, 
qtd_total_orders		,
week_end_active_user		,
week_end_ordering_active_user ,
week_end_total_sessions		,
week_end_total_demand		, 
week_end_total_orders		,
month_end_active_user		,
month_end_ordering_active_user ,
month_end_total_sessions		,
month_end_total_demand		,
month_end_total_orders		,
quarter_end_active_user		,
quarter_end_ordering_active_user ,
quarter_end_total_sessions		,
quarter_end_total_demand		,
quarter_end_total_orders				
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from    T2DL_DAS_DIGENG.leapfrog_session_active_audience_ldg
where   activity_date_partition between '2024-05-06' and '2024-05-14'
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname, holdout_variationname), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on T2DL_DAS_DIGENG.leapfrog_session_active_audience
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;