
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=customer_cohort_trips_11521_ACE_ENG;
     Task_Name=leapfrog_segmented_audience_trips_load;'
     FOR SESSION VOLATILE;
    

create
or replace temp view cal as
SELECT
        DISTINCT TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(week_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) week_start_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) week_end_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(month_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) month_start_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) month_end_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) quarter_start_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) quarter_end_day_date,
        fiscal_day_num,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num
FROM
        object_model.day_cal_454_dim b
WHERE
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) between  '2024-05-06' and '2024-05-14';
        


create or replace temp view cohort as
select distinct
a.tran_date,
case when a.source_platform_code = 'MOW' then 'MOBILE_WEB'
 when a.source_platform_code = 'WEB' then 'DESKTOP_WEB'
  when a.source_platform_code = 'IOS' then 'IOS_APP'
   when a.source_platform_code = 'ANDROID' then 'ANDROID_APP'
   end source_platform_code,
case when a.source_channel_code = 'FULL_LINE' then 'NORDSTROM'
 when a.source_channel_code = 'RACK' then 'NORDSTROM_RACK'
   end source_channel_code,
b.inception_session_id,
last_value(a.engagement_cohort) over (partition by a.tran_date,b.inception_session_id order by a.cohort_rank desc) engagement_cohort,
holdout_experimentname,
holdout_variationname,
coalesce (trips_stores,0) trips_stores,
coalesce (trips_online,0) trips_online,
coalesce (trips_stores+trips_online,0) as trips,
coalesce (trips_stores_gross_usd_amt,0) trips_stores_gross_usd_amt,
coalesce (trips_online_gross_usd_amt,0) trips_online_gross_usd_amt,
coalesce (trips_gross_usd_amt,0) trips_gross_usd_amt
from
(
select
	a.acp_id,
	a.tran_date,
	a.source_platform_code,
	a.SOURCE_channel_CODE,
	a.engagement_cohort,
	b.cohort_rank,
	coalesce (b.trips_stores,0) trips_stores,
	coalesce (a.trips_online,0) trips_online,
	coalesce (b.trips_stores_gross_usd_amt,0) trips_stores_gross_usd_amt,
	coalesce (a.trips_online_gross_usd_amt,0) trips_online_gross_usd_amt,
	coalesce (b.trips_stores_gross_usd_amt,0)+coalesce (a.trips_online_gross_usd_amt,0) as trips_gross_usd_amt
	from ace_etl.customer_cohort_trips a
	left join
	(
		select
			acp_id,
			tran_date,
			SOURCE_channel_CODE,
			engagement_cohort,
            case
                when engagement_cohort='Highly-Engaged' then 1
                when engagement_cohort='Moderately-Engaged' then 2
                when engagement_cohort='Lightly-Engaged' then 3
                when engagement_cohort='Acquire & Activate' then 4
                when engagement_cohort='Acquired Mid-Qtr' then 5
                else 6
            end as cohort_rank,
			trips_stores,
			trips_stores_gross_usd_amt
		from ace_etl.customer_cohort_trips
		where tran_type_code = 'SALE'
		and tran_date between (select min(TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                ))) quarter_start_day_date from cal ) and '2024-05-14'
		and source_platform_code = 'INSTORE'
	) b
	on a.acp_id = b.acp_id
	and a.tran_date = b.tran_date
	and a.SOURCE_channel_CODE = b.SOURCE_channel_CODE
	and a.engagement_cohort = b.engagement_cohort
	where a.tran_type_code = 'SALE'
	and a.tran_date between (select min(TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                ))) quarter_start_day_date from cal ) and '2024-05-14'
	and a.source_platform_code in ('IOS','MOW','ANDROID','WEB')
) a
inner join
(
select
acp_id,
activity_date_partition,
max(inception_session_id) as inception_session_id
from acp_event_intermediate.session_user_lookup_parquet
where activity_date_partition between (select min(TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                ))) quarter_start_day_date from cal ) and '2024-05-14'
group by 1,2
) b
on a.acp_id = b.acp_id
and a.tran_date = b.activity_date_partition
inner join
(
select
            distinct
            channel,
            platform,
            holdout_experimentname,
            holdout_variationname,
            inception_session_id
        from
            ace_etl.leapfrog_holdout
        where
            holdout_flag + leapfrogeligible_flag = 1
            and activity_date_partition between (select min(TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                ))) quarter_start_day_date from cal ) and '2024-05-14'
) c
on b.inception_session_id = c.inception_session_id
and a.SOURCE_channel_CODE = c.channel
and a.source_platform_code = c.platform;




       





create
or replace temp view daily as
select 
        day_date, 
        channel,
        experience,
        engagement_cohort,
        week_start_day_date,
        week_end_day_date,
        month_start_day_date,
        month_end_day_date, 
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_day_num,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        new_cust,
        bounced_cust,
        holdout_experimentname,
        holdout_variationname,
        count(distinct case when trips_stores > 0 then inception_session_id end ) as total_stores_purchased_cust,
        				count(distinct case when trips_online > 0 then inception_session_id end ) as total_online_purchased_cust,
                        count(distinct inception_session_id) as total_purchased_cust,
                        sum(trips_stores) trips_stores,
                        sum(trips_online) trips_online,
                        sum(trips) trips,
                        sum(trips_stores_gross_usd_amt) trips_stores_gross_usd_amt,
                        sum(trips_online_gross_usd_amt) trips_online_gross_usd_amt,
                        sum(trips_gross_usd_amt) trips_gross_usd_amt
FROM
        (
                select distinct 
                        day_date,
                        week_start_day_date,
                        week_end_day_date,
                        month_start_day_date,
                        month_end_day_date,
                        quarter_start_day_date,
                        quarter_end_day_date,
                        fiscal_day_num,
                        fiscal_week_num,
                        fiscal_month_num,
                        fiscal_quarter_num,
                        fiscal_year_num,
                        a.activity_date_partition,
                        session_channel AS channel,
                        session_experience AS experience,
                        a.inception_session_id,
                        engagement_cohort,
                        holdout_experimentname,
            			holdout_variationname,
                        0 as new_cust,
                        1 AS recognized_in_period,
                        max(
                                CASE
                                        WHEN new_recognized = 1 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                day_date
                        ) AS new_recognized_in_period,
                        0 AS bounced_cust,
                        trips_stores,
                        trips_online,
                        trips,
                        trips_stores_gross_usd_amt,
                        trips_online_gross_usd_amt,
                        trips_gross_usd_amt
                FROM
                        acp_event_intermediate.session_user_attributes_parquet a
                        inner join cohort
                        on a.inception_session_id = cohort.inception_session_id
                        and a.activity_date_partition = cohort.tran_date
                        and a.session_experience = cohort.source_platform_code
                        and a.session_channel = cohort.source_channel_code
                        JOIN cal ON a.activity_date_partition = day_date
                WHERE
                        session_experience IN(
                                'ANDROID_APP',
                                'DESKTOP_WEB',
                                'IOS_APP',
                                'MOBILE_WEB'
                        )
                        AND session_channel IN ('NORDSTROM', 'NORDSTROM_RACK')
                        AND bounced IN (0, 1)
                        )
group by
        day_date,
        channel,
        experience,
         engagement_cohort,
        week_start_day_date,
        week_end_day_date,
        month_start_day_date,
        month_end_day_date,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_day_num,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        new_cust,
                holdout_experimentname,
        holdout_variationname,
        bounced_cust;






create
or replace temp view wtd as
select
        day_date,
        channel,
        experience,
         engagement_cohort,
        week_start_day_date,
        week_end_day_date,
        month_start_day_date,
        month_end_day_date,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        new_cust,
        bounced_cust,
                holdout_experimentname,
        holdout_variationname,
                count(distinct case when trips_stores > 0 then inception_session_id end ) as wtd_total_stores_purchased_cust,
        				count(distinct case when trips_online > 0 then inception_session_id end ) as wtd_total_online_purchased_cust,
                        count(distinct inception_session_id) as wtd_total_purchased_cust,
                        sum(trips_stores) wtd_trips_stores,
                        sum(trips_online) wtd_trips_online,
                        sum(trips) wtd_trips,
                        sum(trips_stores_gross_usd_amt) wtd_trips_stores_gross_usd_amt,
                        sum(trips_online_gross_usd_amt) wtd_trips_online_gross_usd_amt,
                        sum(trips_gross_usd_amt) wtd_trips_gross_usd_amt
from
        (
                select distinct 
                        day_date,
                        week_start_day_date,
                        week_end_day_date,
                        month_start_day_date,
                        month_end_day_date,
                        quarter_start_day_date,
                        quarter_end_day_date,
                        fiscal_week_num,
                        fiscal_month_num,
                        fiscal_quarter_num,
                        fiscal_year_num,
                        session_channel as channel,
                        session_experience as experience,
                        engagement_cohort,
                        a.inception_session_id,
                        0 as new_cust,
                        holdout_experimentname,
        				holdout_variationname,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                week_start_day_date
                        ) AS ordered_in_period,
                        1 as recognized_in_period,
                        max(
                                case
                                        when new_recognized = 1 then 1
                                        else 0
                                end
                        ) over (
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                week_start_day_date
                        ) as new_recognized_in_period,
                        0 as bounced_cust,
                        trips_stores,
                        trips_online,
                        trips,
                        trips_stores_gross_usd_amt,
                        trips_online_gross_usd_amt,
                        trips_gross_usd_amt
                from
                        acp_event_intermediate.session_user_attributes_parquet a
                        inner join cohort
                        on a.inception_session_id = cohort.inception_session_id
                        and a.activity_date_partition = cohort.tran_date
                        and a.session_experience = cohort.source_platform_code
                        and a.session_channel = cohort.source_channel_code
                        join cal on a.activity_date_partition >= week_start_day_date
                        and a.activity_date_partition <= day_date
                where
                        session_experience in(
                                'ANDROID_APP',
                                'DESKTOP_WEB',
                                'IOS_APP',
                                'MOBILE_WEB'
                        )
                        and session_channel in ('NORDSTROM', 'NORDSTROM_RACK')
                        and bounced in (0, 1)
        )
group by
        day_date,
        channel,
        experience,
         engagement_cohort,
        week_start_day_date,
        week_end_day_date,
        month_start_day_date,
        month_end_day_date,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        bounced_cust,
                holdout_experimentname,
        holdout_variationname,
        new_cust;





create
or replace temp view mtd as
select
        day_date,
        channel,
        experience,
         engagement_cohort,
        month_start_day_date,
        month_end_day_date,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        new_cust,
        bounced_cust,
                holdout_experimentname,
        holdout_variationname,
        count(distinct case when trips_stores > 0 then inception_session_id end ) as mtd_total_stores_purchased_cust,
        				count(distinct case when trips_online > 0 then inception_session_id end ) as mtd_total_online_purchased_cust,
                        count(distinct inception_session_id) as mtd_total_purchased_cust,
                        sum(trips_stores) mtd_trips_stores,
                        sum(trips_online) mtd_trips_online,
                        sum(trips) mtd_trips,
                        sum(trips_stores_gross_usd_amt) mtd_trips_stores_gross_usd_amt,
                        sum(trips_online_gross_usd_amt) mtd_trips_online_gross_usd_amt,
                        sum(trips_gross_usd_amt) mtd_trips_gross_usd_amt
from
        (
                select distinct 
                        day_date,
                        month_start_day_date,
                        month_end_day_date,
                        quarter_start_day_date,
                        quarter_end_day_date,
                        fiscal_month_num,
                        fiscal_quarter_num,
                        fiscal_year_num,
                        session_channel as channel,
                        session_experience as experience,
         engagement_cohort,
                        a.inception_session_id,
                        0 as new_cust,
                                holdout_experimentname,
        holdout_variationname,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                month_start_day_date
                        ) AS ordered_in_period,
                        1 as recognized_in_period,
                        max(
                                case
                                        when new_recognized = 1 then 1
                                        else 0
                                end
                        ) over (
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                month_start_day_date
                        ) as new_recognized_in_period,
                        0 as bounced_cust,
                        trips_stores,
                        trips_online,
                        trips,
                        trips_stores_gross_usd_amt,
                        trips_online_gross_usd_amt,
                        trips_gross_usd_amt
                from
                        acp_event_intermediate.session_user_attributes_parquet a
                        inner join cohort
                        on a.inception_session_id = cohort.inception_session_id
                        and a.activity_date_partition = cohort.tran_date
                        and a.session_experience = cohort.source_platform_code
                        and a.session_channel = cohort.source_channel_code
                        join cal on a.activity_date_partition >= month_start_day_date
                        and a.activity_date_partition <= day_date
                where
                        session_experience in(
                                'ANDROID_APP',
                                'DESKTOP_WEB',
                                'IOS_APP',
                                'MOBILE_WEB'
                        )
                        and session_channel in ('NORDSTROM', 'NORDSTROM_RACK')
                        and bounced in (0, 1)
        )
group by
        day_date,
        channel,
        experience,
         engagement_cohort,
        month_start_day_date,
        month_end_day_date,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        bounced_cust,
                holdout_experimentname,
        holdout_variationname,
        new_cust;





create
or replace temp view qtd as
select
        day_date,
        channel,
        experience,
         engagement_cohort,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        new_cust,
        bounced_cust,
                holdout_experimentname,
        holdout_variationname,
        count(distinct case when trips_stores > 0 then inception_session_id end ) as qtd_total_stores_purchased_cust,
        				count(distinct case when trips_online > 0 then inception_session_id end ) as qtd_total_online_purchased_cust,
                        count(distinct inception_session_id) as qtd_total_purchased_cust,
                        sum(trips_stores) qtd_trips_stores,
                        sum(trips_online) qtd_trips_online,
                        sum(trips) qtd_trips,
                        sum(trips_stores_gross_usd_amt) qtd_trips_stores_gross_usd_amt,
                        sum(trips_online_gross_usd_amt) qtd_trips_online_gross_usd_amt,
                        sum(trips_gross_usd_amt) qtd_trips_gross_usd_amt
from
        (
                select distinct 
                        day_date,
                        quarter_start_day_date,
                        quarter_end_day_date,
                        fiscal_quarter_num,
                        fiscal_year_num,
                        session_channel as channel,
                        session_experience as experience,
         engagement_cohort,
                        a.inception_session_id,
                        0 as new_cust,
                        holdout_experimentname,
        				holdout_variationname,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                quarter_start_day_date
                        ) AS ordered_in_period,
                        1 as recognized_in_period,
                        max(
                                case
                                        when new_recognized = 1 then 1
                                        else 0
                                end
                        ) over (
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                quarter_start_day_date
                        ) as new_recognized_in_period,
                        0 as bounced_cust,
                        trips_stores,
                        trips_online,
                        trips,
                        trips_stores_gross_usd_amt,
                        trips_online_gross_usd_amt,
                        trips_gross_usd_amt
                from
                        acp_event_intermediate.session_user_attributes_parquet a
                        inner join cohort
                        on a.inception_session_id = cohort.inception_session_id
                        and a.activity_date_partition = cohort.tran_date
                        and a.session_experience = cohort.source_platform_code
                        and a.session_channel = cohort.source_channel_code              
                        join cal on a.activity_date_partition >= quarter_start_day_date
                        and a.activity_date_partition <= day_date
                where
                        session_experience in(
                                'ANDROID_APP',
                                'DESKTOP_WEB',
                                'IOS_APP',
                                'MOBILE_WEB'
                        )
                        and session_channel in ('NORDSTROM', 'NORDSTROM_RACK')
                        and bounced in (0, 1)
        )
group by
        day_date,
        channel,
        experience,
         engagement_cohort,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        bounced_cust,
                holdout_experimentname,
        holdout_variationname,
        new_cust;





create
or replace temp view final as
with engagement_cohorts AS (
    SELECT 'Acquire & Activate' AS engagement_cohort
    UNION ALL SELECT 'Acquired Mid-Qtr'
    UNION ALL SELECT 'Highly-Engaged'
    UNION ALL SELECT 'Lightly-Engaged'
    UNION ALL SELECT 'Moderately-Engaged'
    UNION ALL SELECT NULL
),
holdout_experimentnames AS (
    SELECT 
    				 'ios_leapfrog_holdout' AS holdout_experimentname, 'IOS_APP' as experience,'NORDSTROM' AS channel
    UNION ALL SELECT 'desktop_leapfrog_holdout'AS holdout_experimentname, 'DESKTOP_WEB' as experience,'NORDSTROM' AS channel
    UNION ALL SELECT 'sbn_leapfrog_holdout'AS holdout_experimentname, 'MOBILE_WEB' as experience,'NORDSTROM' AS channel
    UNION ALL SELECT 'ios_leapfrog_holdout_rack'AS holdout_experimentname, 'IOS_APP' as experience,'NORDSTROM_RACK' AS channel
),
holdout_variationnames AS (
    SELECT 'leapfrogeligible' AS holdout_variationname
    UNION ALL SELECT 'default'
),
com as (
    SELECT d.*, he.channel, he.experience, ec.engagement_cohort,
           f1.flag AS recognized_in_period, f2.flag AS new_recognized_in_period,
           f3.flag AS new_cust, f4.flag AS bounced_cust,
           he.holdout_experimentname, hv.holdout_variationname
    FROM cal d
    CROSS JOIN engagement_cohorts ec
    CROSS JOIN (SELECT 0 AS flag UNION ALL SELECT 1) f1
    CROSS JOIN (SELECT 0 AS flag UNION ALL SELECT 1) f2
    CROSS JOIN (SELECT 0 AS flag) f3
    CROSS JOIN (SELECT 0 AS flag UNION ALL SELECT 1) f4
    CROSS JOIN holdout_experimentnames he
    CROSS JOIN holdout_variationnames hv
)
SELECT
        com.day_date as activity_date_partition,
        com.channel,
        com.experience,
        com.engagement_cohort,
        com.week_start_day_date,
        com.week_end_day_date,
        com.month_start_day_date,
        com.month_end_day_date,
        com.quarter_start_day_date,
        com.quarter_end_day_date,
        com.fiscal_day_num,
        com.fiscal_week_num,
        com.fiscal_month_num,
        com.fiscal_quarter_num,
        com.fiscal_year_num,
        com.recognized_in_period,
        com.new_recognized_in_period,
        com.new_cust as new_in_period,
        com.bounced_cust AS bounced_in_period,
        com.holdout_experimentname,
        com.holdout_variationname,
        
        coalesce (a.total_stores_purchased_cust,0) daily_total_stores_purchased_cust,
        coalesce (a.total_online_purchased_cust,0) daily_total_online_purchased_cust,
        coalesce (a.total_purchased_cust, 0) daily_total_purchased_cust,
        coalesce (a.trips_stores, 0) daily_trips_stores,
        coalesce (a.trips_online, 0) daily_trips_online,
        coalesce (a.trips, 0) daily_trips,
        coalesce (a.trips_stores_gross_usd_amt, 0) daily_trips_stores_gross_usd_amt,
        coalesce (a.trips_online_gross_usd_amt, 0) daily_trips_online_gross_usd_amt,
        coalesce (a.trips_gross_usd_amt, 0) daily_trips_gross_usd_amt,
        
        coalesce (wtd_total_stores_purchased_cust,0) wtd_total_stores_purchased_cust,
        coalesce (wtd_total_online_purchased_cust,0) wtd_total_online_purchased_cust,        
        coalesce (wtd_total_purchased_cust, 0) wtd_total_purchased_cust,
        coalesce (wtd_trips_stores, 0) wtd_trips_stores,
        coalesce (wtd_trips_online, 0)  wtd_trips_online,
        coalesce (wtd_trips, 0)  wtd_trips,
        coalesce (wtd_trips_stores_gross_usd_amt, 0)  wtd_trips_stores_gross_usd_amt,
        coalesce (wtd_trips_online_gross_usd_amt, 0)  wtd_trips_online_gross_usd_amt,
        coalesce (wtd_trips_gross_usd_amt, 0)  wtd_trips_gross_usd_amt,

        coalesce (mtd_total_stores_purchased_cust,0) mtd_total_stores_purchased_cust,
        coalesce (mtd_total_online_purchased_cust,0) mtd_total_online_purchased_cust,               
        coalesce (mtd_total_purchased_cust, 0) mtd_total_purchased_cust,
        coalesce (mtd_trips_stores, 0) mtd_trips_stores,
        coalesce (mtd_trips_online, 0)  mtd_trips_online,
        coalesce (mtd_trips, 0)  mtd_trips,
        coalesce (mtd_trips_stores_gross_usd_amt, 0)  mtd_trips_stores_gross_usd_amt,
        coalesce (mtd_trips_online_gross_usd_amt, 0)  mtd_trips_online_gross_usd_amt,
        coalesce (mtd_trips_gross_usd_amt, 0)  mtd_trips_gross_usd_amt,
        
        coalesce (qtd_total_stores_purchased_cust,0) qtd_total_stores_purchased_cust,
        coalesce (qtd_total_online_purchased_cust,0) qtd_total_online_purchased_cust,               
        coalesce (qtd_total_purchased_cust, 0) qtd_total_purchased_cust,
        coalesce (qtd_trips_stores, 0) qtd_trips_stores,
        coalesce (qtd_trips_online, 0)  qtd_trips_online,
        coalesce (qtd_trips, 0)  qtd_trips,
        coalesce (qtd_trips_stores_gross_usd_amt, 0)  qtd_trips_stores_gross_usd_amt,
        coalesce (qtd_trips_online_gross_usd_amt, 0)  qtd_trips_online_gross_usd_amt,
        coalesce (qtd_trips_gross_usd_amt, 0)  qtd_trips_gross_usd_amt,
        
       
        case 
                when com.day_date = b.week_end_day_date then coalesce (wtd_total_stores_purchased_cust,0)
        end as week_end_total_stores_purchased_cust,
        case
                when com.day_date = b.week_end_day_date then coalesce (wtd_total_online_purchased_cust,0)
        end as week_end_total_online_purchased_cust,
        
         case
                when com.day_date = b.week_end_day_date then coalesce (wtd_total_purchased_cust,0)
        end as week_end_total_purchased_cust,
        case
                when com.day_date = b.week_end_day_date then coalesce (wtd_trips_stores,0)
        end as week_end_trips_stores,
        case
                when com.day_date = b.week_end_day_date then coalesce (wtd_trips_online,0)
        end as week_end_trips_online,
        
        case
                when com.day_date = b.week_end_day_date then coalesce (wtd_trips,0)
        end as week_end_trips,
        case
                when com.day_date = b.week_end_day_date then coalesce (wtd_trips_stores_gross_usd_amt,0)
        end as week_end_trips_stores_gross_usd_amt,
        case
                when com.day_date =  b.week_end_day_date then coalesce (wtd_trips_online_gross_usd_amt,0)
        end as week_end_trips_online_gross_usd_amt,
        case when com.day_date = b.week_end_day_date then coalesce (wtd_trips_gross_usd_amt,0)
        end as week_end_trips_trips_gross_usd_amt,
        
        
        
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_total_stores_purchased_cust,0)
        end as month_end_total_stores_purchased_cust,
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_total_online_purchased_cust,0)
        end as month_end_total_online_purchased_cust,        
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_total_purchased_cust,0)
        end as month_end_total_purchased_cust,        
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_trips_stores,0)
        end as month_end_trips_stores,
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_trips_online,0)
        end as month_end_trips_online,
        
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_trips,0)
        end as month_end_trips,
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_trips_stores_gross_usd_amt,0)
        end as month_end_trips_stores_gross_usd_amt,
        case
                when com.day_date = m.month_end_day_date then coalesce (mtd_trips_online_gross_usd_amt,0)
        end as month_end_trips_online_gross_usd_amt,
        case when com.day_date = m.month_end_day_date then coalesce (mtd_trips_gross_usd_amt,0)
        end as month_end_trips_trips_gross_usd_amt,
        
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_total_stores_purchased_cust,0)
        end as quarter_end_total_stores_purchased_cust,
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_total_online_purchased_cust,0)
        end as quarter_end_total_online_purchased_cust,   
         case
                when com.day_date =q.quarter_end_day_date then coalesce (qtd_total_purchased_cust,0)
        end as quarter_end_total_purchased_cust,
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_trips_stores,0)
        end as quarter_end_trips_stores,
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_trips_online,0)
        end as quarter_end_trips_online,
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_trips,0)
        end as quarter_end_trips,
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_trips_stores_gross_usd_amt,0)
        end as quarter_end_trips_stores_gross_usd_amt,
        case
                when com.day_date = q.quarter_end_day_date then coalesce (qtd_trips_online_gross_usd_amt,0)
        end as quarter_end_trips_online_gross_usd_amt,
        case when com.day_date = q.quarter_end_day_date then coalesce (qtd_trips_gross_usd_amt,0)
        end as quarter_end_trips_trips_gross_usd_amt
from  com 

left join daily a 
on a.channel = com.channel
        and a.experience = com.experience
        and coalesce (a.engagement_cohort,'Cold-Start') = coalesce (com.engagement_cohort,'Cold-Start')
        and a.recognized_in_period = com.recognized_in_period
        and a.new_recognized_in_period = com.new_recognized_in_period
        and a.new_cust = com.new_cust
        and a.bounced_cust = com.bounced_cust
        and a.day_date = com.day_date
        and a.holdout_experimentname = com.holdout_experimentname
        and a.holdout_variationname=com.holdout_variationname
        
       left outer join wtd b 
        on com.channel = b.channel
        and com.experience = b.experience
        and coalesce (com.engagement_cohort,'Cold-Start') = coalesce (b.engagement_cohort,'Cold-Start')
        and com.recognized_in_period = b.recognized_in_period
        and com.new_recognized_in_period = b.new_recognized_in_period
        and com.new_cust = b.new_cust
        and com.bounced_cust = b.bounced_cust
        and com.day_date = b.day_date
        and com.week_start_day_date = b.week_start_day_date
        and com.holdout_experimentname = b.holdout_experimentname
        and com.holdout_variationname=b.holdout_variationname
        
        left outer join mtd m 
        on com.channel = m.channel
        and com.experience = m.experience
        and coalesce (com.engagement_cohort,'Cold-Start') = coalesce (m.engagement_cohort,'Cold-Start')
        and com.recognized_in_period = m.recognized_in_period
        and com.new_recognized_in_period = m.new_recognized_in_period
        and com.new_cust = m.new_cust
        and com.bounced_cust = m.bounced_cust
        and com.day_date = m.day_date
        and com.holdout_experimentname = m.holdout_experimentname
        and com.holdout_variationname=m.holdout_variationname
        
left outer join  qtd q 
        on com.channel = q.channel
        and com.experience = q.experience
        and coalesce (com.engagement_cohort,'Cold-Start') = coalesce (q.engagement_cohort,'Cold-Start')
        and com.recognized_in_period = q.recognized_in_period
        and com.new_recognized_in_period = q.new_recognized_in_period
        and com.new_cust = q.new_cust
        and com.bounced_cust = q.bounced_cust
        and com.day_date = q.day_date
       	and com.holdout_experimentname = q.holdout_experimentname
        and com.holdout_variationname=q.holdout_variationname;



-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert
        overwrite table leapfrog_segmented_audience_trips_output
select
        *
from
        final;

