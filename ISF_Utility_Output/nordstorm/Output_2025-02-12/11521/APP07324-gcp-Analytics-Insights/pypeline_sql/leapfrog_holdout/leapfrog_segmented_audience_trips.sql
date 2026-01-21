/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_segmented_audience_trips_11521_ACE_ENG;
     Task_Name=leapfrog_segmented_audience_trips;'
     FOR SESSION VOLATILE;



/*


T2/Table Name: leapfrog_segmented_audience_trips
Team/Owner: Jin Liu
Date Created/Modified: 2024-01-12

Note:
-- What is the the purpose of the table : to add trips metrics to leapfrog reporting for the user who were exposed to leapfrog experiments
-- What is the update cadence/lookback window: daily between current - 9 and current -3

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname, holdout_variationname), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on {deg_t2_schema}.leapfrog_segmented_audience_trips_ldg
;
 
delete
from    {deg_t2_schema}.leapfrog_segmented_audience_trips
where    activity_date_partition between {start_date} and {end_date}
;

insert into {deg_t2_schema}.leapfrog_segmented_audience_trips
select  
activity_date_partition	,
channel	,
experience	,
engagement_cohort	,
week_start_day_date	,
week_end_day_date	,
month_start_day_date	,
month_end_day_date	,
quarter_start_day_date	,
quarter_end_day_date	,
fiscal_day_num	,
fiscal_week_num	,
fiscal_month_num	,
fiscal_quarter_num	,
fiscal_year_num	,
recognized_in_period	,
new_recognized_in_period	,
new_in_period	,
bounced_in_period	,
holdout_experimentname	,
holdout_variationname	,
daily_total_stores_purchased_cust	,
daily_total_online_purchased_cust	,
daily_total_purchased_cust	,
daily_trips_stores	,
daily_trips_online	,
daily_trips	,
daily_trips_stores_gross_usd_amt	,
daily_trips_online_gross_usd_amt	,
daily_trips_gross_usd_amt	,
wtd_total_stores_purchased_cust	,
wtd_total_online_purchased_cust	,
wtd_total_purchased_cust	,
wtd_trips_stores	,
wtd_trips_online	,
wtd_trips	,
wtd_trips_stores_gross_usd_amt	,
wtd_trips_online_gross_usd_amt	,
wtd_trips_gross_usd_amt	,
mtd_total_stores_purchased_cust	,
mtd_total_online_purchased_cust	,
mtd_total_purchased_cust	,
mtd_trips_stores	,
mtd_trips_online	,
mtd_trips	,
mtd_trips_stores_gross_usd_amt	,
mtd_trips_online_gross_usd_amt	,
mtd_trips_gross_usd_amt	,
qtd_total_stores_purchased_cust	,
qtd_total_online_purchased_cust	,
qtd_total_purchased_cust	,
qtd_trips_stores	,
qtd_trips_online	,
qtd_trips	,
qtd_trips_stores_gross_usd_amt	,
qtd_trips_online_gross_usd_amt	,
qtd_trips_gross_usd_amt	,
week_end_total_stores_purchased_cust	,
week_end_total_online_purchased_cust	,
week_end_total_purchased_cust	,
week_end_trips_stores	,
week_end_trips_online	,
week_end_trips	,
week_end_trips_stores_gross_usd_amt	,
week_end_trips_online_gross_usd_amt	,
week_end_trips_trips_gross_usd_amt	,
month_end_total_stores_purchased_cust	,
month_end_total_online_purchased_cust	,
month_end_total_purchased_cust	,
month_end_trips_stores	,
month_end_trips_online	,
month_end_trips	,
month_end_trips_stores_gross_usd_amt	,
month_end_trips_online_gross_usd_amt	,
month_end_trips_trips_gross_usd_amt	,
quarter_end_total_stores_purchased_cust	,
quarter_end_total_online_purchased_cust	,
quarter_end_total_purchased_cust	,
quarter_end_trips_stores	,
quarter_end_trips_online	,
quarter_end_trips	,
quarter_end_trips_stores_gross_usd_amt	,
quarter_end_trips_online_gross_usd_amt	, 
quarter_end_trips_trips_gross_usd_amt				
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from    {deg_t2_schema}.leapfrog_segmented_audience_trips_ldg
where   activity_date_partition between {start_date} and {end_date}
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname, holdout_variationname), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on {deg_t2_schema}.leapfrog_segmented_audience_trips
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;