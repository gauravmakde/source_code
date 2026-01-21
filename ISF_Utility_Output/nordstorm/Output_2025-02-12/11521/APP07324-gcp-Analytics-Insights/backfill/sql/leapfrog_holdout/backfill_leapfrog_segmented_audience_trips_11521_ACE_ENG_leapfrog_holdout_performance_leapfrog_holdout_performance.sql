/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_segmented_audience_trips_11521_ACE_ENG;
     Task_Name=leapfrog_holdout_performance;'
     FOR SESSION VOLATILE;


/*
T2/Table Name:leapfrog_holdout_performance
Team/Owner: Jin Liu
Date Created/Modified: 2024-01-16 

Note:
-- What is the the purpose of the table: to create the final output table for leapfrog holdout performance
-- What is the update cadence/lookback window: daily between current - 9 and current -3

*/



/*
Temp table notes here if applicable
*/
CREATE MULTISET VOLATILE TABLE temp_table_name AS (


select a.*
,daily_total_stores_purchased_cust
,daily_total_online_purchased_cust
,daily_total_purchased_cust
,daily_trips_stores
,daily_trips_online
,daily_trips
,daily_trips_stores_gross_usd_amt
,daily_trips_online_gross_usd_amt
,daily_trips_gross_usd_amt
,wtd_total_stores_purchased_cust
,wtd_total_online_purchased_cust
,wtd_total_purchased_cust
,wtd_trips_stores
,wtd_trips_online
,wtd_trips
,wtd_trips_stores_gross_usd_amt
,wtd_trips_online_gross_usd_amt
,wtd_trips_gross_usd_amt
,mtd_total_stores_purchased_cust
,mtd_total_online_purchased_cust
,mtd_total_purchased_cust
,mtd_trips_stores
,mtd_trips_online
,mtd_trips
,mtd_trips_stores_gross_usd_amt
,mtd_trips_online_gross_usd_amt
,mtd_trips_gross_usd_amt
,qtd_total_stores_purchased_cust
,qtd_total_online_purchased_cust
,qtd_total_purchased_cust
,qtd_trips_stores
,qtd_trips_online
,qtd_trips
,qtd_trips_stores_gross_usd_amt
,qtd_trips_online_gross_usd_amt
,qtd_trips_gross_usd_amt
,week_end_total_stores_purchased_cust
,week_end_total_online_purchased_cust
,week_end_total_purchased_cust
,week_end_trips_stores
,week_end_trips_online
,week_end_trips
,week_end_trips_stores_gross_usd_amt
,week_end_trips_online_gross_usd_amt
,week_end_trips_trips_gross_usd_amt
,month_end_total_stores_purchased_cust
,month_end_total_online_purchased_cust
,month_end_total_purchased_cust
,month_end_trips_stores
,month_end_trips_online
,month_end_trips
,month_end_trips_stores_gross_usd_amt
,month_end_trips_online_gross_usd_amt
,month_end_trips_trips_gross_usd_amt
,quarter_end_total_stores_purchased_cust
,quarter_end_total_online_purchased_cust
,quarter_end_total_purchased_cust
,quarter_end_trips_stores
,quarter_end_trips_online
,quarter_end_trips
,quarter_end_trips_stores_gross_usd_amt
,quarter_end_trips_online_gross_usd_amt
,quarter_end_trips_trips_gross_usd_amt
from T2DL_DAS_DIGENG.leapfrog_session_active_audience a 
left join T2DL_DAS_DIGENG.leapfrog_segmented_audience_trips b 
on a.activity_date_partition =b.activity_date_partition 
and a.channel =b.channel 
and a.experience  = b.experience 
and COALESCE (a.engagement_cohort,'cold') = COALESCE (b.engagement_cohort,'cold')
and a.recognized_in_period =b.recognized_in_period 
and a.new_recognized_in_period = b.new_recognized_in_period 
and a.new_in_period =b.new_in_period 
and a.bounced_in_period = b.bounced_in_period 
and a.holdout_experimentname =b.holdout_experimentname 
and a.holdout_variationname =b.holdout_variationname 
where a.activity_date_partition between '2024-05-06' and '2024-05-14'

) 
WITH DATA
PRIMARY INDEX(activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname, holdout_variationname)
ON COMMIT PRESERVE ROWS
;



/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    T2DL_DAS_DIGENG.leapfrog_holdout_performance
WHERE   activity_date_partition >= '2024-05-06'
AND     activity_date_partition <= '2024-05-14'
;


INSERT INTO T2DL_DAS_DIGENG.leapfrog_holdout_performance
SELECT  activity_date_partition
,channel
,experience
,engagement_cohort
,week_start_day_date
,week_end_day_date
,month_start_day_date
,month_end_day_date
,quarter_start_day_date
,quarter_end_day_date
,fiscal_day_num
,fiscal_week_num
,fiscal_month_num
,fiscal_quarter_num
,fiscal_year_num
,recognized_in_period
,new_recognized_in_period
,new_in_period
,bounced_in_period
,holdout_experimentname
,holdout_variationname
,daily_active_user
,daily_ordering_active_user
,daily_total_sessions
,daily_total_demand
,daily_total_orders
,wtd_active_user
,wtd_ordering_active_user
,wtd_total_sessions
,wtd_total_demand
,wtd_total_orders
,mtd_active_user
,mtd_ordering_active_user
,mtd_total_sessions
,mtd_total_demand
,mtd_total_orders
,qtd_active_user
,qtd_ordering_active_user
,qtd_total_sessions
,qtd_total_demand
,qtd_total_orders
,week_end_active_user
,week_end_ordering_active_user
,week_end_total_sessions
,week_end_total_demand
,week_end_total_orders
,month_end_active_user
,month_end_ordering_active_user
,month_end_total_sessions
,month_end_total_demand
,month_end_total_orders
,quarter_end_active_user
,quarter_end_ordering_active_user
,quarter_end_total_sessions
,quarter_end_total_demand
,quarter_end_total_orders
,daily_total_stores_purchased_cust
,daily_total_online_purchased_cust
,daily_total_purchased_cust
,daily_trips_stores
,daily_trips_online
,daily_trips
,daily_trips_stores_gross_usd_amt
,daily_trips_online_gross_usd_amt
,daily_trips_gross_usd_amt
,wtd_total_stores_purchased_cust
,wtd_total_online_purchased_cust
,wtd_total_purchased_cust
,wtd_trips_stores
,wtd_trips_online
,wtd_trips
,wtd_trips_stores_gross_usd_amt
,wtd_trips_online_gross_usd_amt
,wtd_trips_gross_usd_amt
,mtd_total_stores_purchased_cust
,mtd_total_online_purchased_cust
,mtd_total_purchased_cust
,mtd_trips_stores
,mtd_trips_online
,mtd_trips
,mtd_trips_stores_gross_usd_amt
,mtd_trips_online_gross_usd_amt
,mtd_trips_gross_usd_amt
,qtd_total_stores_purchased_cust
,qtd_total_online_purchased_cust
,qtd_total_purchased_cust
,qtd_trips_stores
,qtd_trips_online
,qtd_trips
,qtd_trips_stores_gross_usd_amt
,qtd_trips_online_gross_usd_amt
,qtd_trips_gross_usd_amt
,week_end_total_stores_purchased_cust
,week_end_total_online_purchased_cust
,week_end_total_purchased_cust
,week_end_trips_stores
,week_end_trips_online
,week_end_trips
,week_end_trips_stores_gross_usd_amt
,week_end_trips_online_gross_usd_amt
,week_end_trips_trips_gross_usd_amt
,month_end_total_stores_purchased_cust
,month_end_total_online_purchased_cust
,month_end_total_purchased_cust
,month_end_trips_stores
,month_end_trips_online
,month_end_trips
,month_end_trips_stores_gross_usd_amt
,month_end_trips_online_gross_usd_amt
,month_end_trips_trips_gross_usd_amt
,quarter_end_total_stores_purchased_cust
,quarter_end_total_online_purchased_cust
,quarter_end_total_purchased_cust
,quarter_end_trips_stores
,quarter_end_trips_online
,quarter_end_trips
,quarter_end_trips_stores_gross_usd_amt
,quarter_end_trips_online_gross_usd_amt
,quarter_end_trips_trips_gross_usd_amt
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM	temp_table_name
WHERE   activity_date_partition >= '2024-05-06'
AND     activity_date_partition <= '2024-05-14'
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition,channel,experience,recognized_in_period,new_recognized_in_period,new_in_period, holdout_experimentname, holdout_variationname), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on T2DL_DAS_DIGENG.leapfrog_holdout_performance;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
 