/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=session_active_user_11521_ACE_ENG;
     Task_Name=session_active_user;'
     FOR SESSION VOLATILE;


/*


T2/Table Name: session_active_user
Team/Owner: Jin Liu
Date Created/Modified: 2023-08-07

Note:
-- What is the the purpose of the table : using SPEED source to create agg XTD user count for reporting purpose
-- What is the update cadence/lookback window: refresh everyday between current-5 and current -4

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on {deg_t2_schema}.session_active_user_ldg
;

delete
from    {deg_t2_schema}.session_active_user
where    activity_date_partition between {start_date} and {end_date}
;

insert into {deg_t2_schema}.session_active_user
select  
 activity_date_partition		,
channel		,
experience		,
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
from    {deg_t2_schema}.session_active_user_ldg
where   activity_date_partition between {start_date} and {end_date}
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on {deg_t2_schema}.session_active_user
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
 