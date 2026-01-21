
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_segmented_audience_trips_11521_ACE_ENG;
     Task_Name=leapfrog_session_active_audience_load;'
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
        fiscal_year_num,
        cast(quarter_idnt as int) as quarter_idnt
FROM
        object_model.day_cal_454_dim b
WHERE
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) between {start_date} and {end_date};
       

create or replace temp view cohort as 
select distinct
activity_date_partition,
inception_session_id,
last_value(engagement_cohort) over (partition by activity_date_partition,inception_session_id order by cohort_rank desc) engagement_cohort
from 
(
select 
a.acp_id,
activity_date_partition,
engagement_cohort,
case
    when engagement_cohort='Highly-Engaged' then 1
    when engagement_cohort='Moderately-Engaged' then 2
    when engagement_cohort='Lightly-Engaged' then 3
    when engagement_cohort='Acquire & Activate' then 4
    when engagement_cohort='Acquired Mid-Qtr' then 5
    else 6
end as cohort_rank,
max(inception_session_id) as inception_session_id
from acp_event_intermediate.session_user_lookup_parquet a 
left join cal 
on a.activity_date_partition = cal.day_date
inner join {hive_schema}.customer_cohort b 
on a.acp_id = b.acp_id
and cal.quarter_idnt = execution_qtr
where activity_date_partition between (select min(TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                ))) quarter_start_day_date from cal )
                and {end_date}
group by 1,2,3
);
  
CREATE OR replace temp VIEW hold_out_exp_sessions AS 
with q as 
(select min(TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                ))) quarter_start_day_date from cal 
                
)
SELECT 
            holdout_experimentname,
            holdout_variationname,
            activity_date_partition,
            session_id
from
    (
        select
            distinct     
            channelcountry,
            channel,
            platform,
            holdout_experimentname,
            holdout_variationname,
            inception_session_id,
            activity_date_partition,
            session_id,
            max(holdout_flag) over (
                partition by inception_session_id,
                channel,
                platform,
                holdout_experimentname
            ) holdout_flag,
            max(leapfrogeligible_flag) over (
                partition by inception_session_id,
                channel,
                platform,
                holdout_experimentname
            ) leapfrogeligible_flag,
            min(holdout_first_exposure_time_pst) over (
                partition by inception_session_id,
                channel,
                platform,
                holdout_experimentname
            ) holdout_first_exposure_time_pst
        from
            {hive_schema}.leapfrog_holdout
        where
            holdout_flag + leapfrogeligible_flag = 1
    ) a
where
    holdout_flag + leapfrogeligible_flag = 1
and activity_date_partition between (select quarter_start_day_date from q ) and {end_date};


set spark.sql.legacy.allowNonEmptyLocationInCTAS=true;

create or replace temp view daily_base
as select
                        a.activity_date_partition,
                        session_channel,
                        session_experience,
                        a.inception_session_id,
                        engagement_cohort,
                        a.session_id,
                        s.holdout_experimentname,
            			s.holdout_variationname,
                        0 as new_cust,
						order_event_cnt,
						max_session_acp_id,
						new_recognized,
						bounced,
                        order_current_price
                FROM
                        acp_event_intermediate.session_user_attributes_parquet a
                        inner join hold_out_exp_sessions S
                        on A.SESSION_ID = S.SESSION_ID
                        and a.activity_date_partition = s.activity_date_partition
                        left join cohort
                        on a.inception_session_id = cohort.inception_session_id
                        and a.activity_date_partition = cohort.activity_date_partition
                WHERE
                        session_experience IN(
                                'ANDROID_APP',
                                'DESKTOP_WEB',
                                'IOS_APP',
                                'MOBILE_WEB'
                        )
                        AND session_channel IN ('NORDSTROM', 'NORDSTROM_RACK')
                        AND bounced IN (0, 1);

drop table if exists {hive_schema}.leapfrog_daily_base;

create table {hive_schema}.leapfrog_daily_base
using parquet
location 's3://{s3_bucket_root_var}/leapfrog_daily_base/'
as select * from daily_base;

cache lazy table {hive_schema}.leapfrog_daily_base;

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
        count(DISTINCT inception_session_id) AS daily_active_user,
        count(DISTINCT CASE WHEN ordered_in_period = 1 THEN inception_session_id end) AS daily_ordering_active_user,
        count(DISTINCT session_id) AS daily_total_sessions,
        sum(order_current_price) AS daily_total_demand,
        sum(order_event_cnt) daily_total_orders
FROM
        (
                select
                        TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        ) day_date,
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
                        activity_date_partition,
                        session_channel AS channel,
                        session_experience AS experience,
                        inception_session_id,
                        engagement_cohort,
                        session_id,
                        holdout_experimentname,
            			holdout_variationname,
                        0 as new_cust,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                day_date
                        ) AS ordered_in_period,
                        max(
                                CASE
                                        WHEN NULLIF(max_session_acp_id, '') IS NOT NULL THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                day_date
                        ) AS recognized_in_period,
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
                        min(bounced) OVER(
                                PARTITION BY session_channel,
                                session_experience,
                                a.inception_session_id,
                                day_date
                        ) AS bounced_cust,
                        order_current_price,
                        order_event_cnt
                FROM
                        {hive_schema}.leapfrog_daily_base a
                        JOIN cal ON a.activity_date_partition = TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
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
        count(DISTINCT inception_session_id) AS wtd_active_user,
        count(DISTINCT CASE WHEN ordered_in_period = 1 THEN inception_session_id end) AS wtd_ordering_active_user,
        count(DISTINCT session_id) AS wtd_total_sessions,
        sum(order_current_price) AS wtd_total_demand,
        sum(order_event_cnt) wtd_total_orders
from
        (
                select
                        TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        ) day_date,
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
                        inception_session_id,
                        session_id,
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
                        max(
                                case
                                        when nullif(max_session_acp_id, '') is not null then 1
                                        else 0
                                end
                        ) over (
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                week_start_day_date
                        ) as recognized_in_period,
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
                        min(bounced) over(
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                week_start_day_date
                        ) as bounced_cust,
                        order_current_price,
                        order_event_cnt
                from
                        {hive_schema}.leapfrog_daily_base a
                        join cal on a.activity_date_partition >= TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(week_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
                        and a.activity_date_partition <= TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
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
        count(DISTINCT inception_session_id) AS mtd_active_user,
        count(DISTINCT CASE WHEN ordered_in_period = 1 THEN inception_session_id end) AS mtd_ordering_active_user,
        count(DISTINCT session_id) AS mtd_total_sessions,
        sum(order_current_price) AS mtd_total_demand,
        sum(order_event_cnt) mtd_total_orders
from
        (
                select
                        TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        ) day_date,
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
                        inception_session_id,
                        session_id,
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
                        max(
                                case
                                        when nullif(max_session_acp_id, '') is not null then 1
                                        else 0
                                end
                        ) over (
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                month_start_day_date
                        ) as recognized_in_period,
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
                        min(bounced) over(
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                month_start_day_date
                        ) as bounced_cust,
                        order_current_price,
                        order_event_cnt
                from
                        {hive_schema}.leapfrog_daily_base a
                        join cal on a.activity_date_partition >= TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(month_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
                        and a.activity_date_partition <= TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
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
        count(DISTINCT inception_session_id) AS qtd_active_user,
        count(DISTINCT CASE WHEN ordered_in_period = 1 THEN inception_session_id end) AS qtd_ordering_active_user,
        count(DISTINCT session_id) AS qtd_total_sessions,
        sum(order_current_price) AS qtd_total_demand,
        sum(order_event_cnt) qtd_total_orders
from
        (
                select
                        TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        ) day_date,
                        quarter_start_day_date,
                        quarter_end_day_date,
                        fiscal_quarter_num,
                        fiscal_year_num,
                        session_channel as channel,
                        session_experience as experience,
                        engagement_cohort,
                        inception_session_id,
                        session_id,
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
                        max(
                                case
                                        when nullif(max_session_acp_id, '') is not null then 1
                                        else 0
                                end
                        ) over (
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                quarter_start_day_date
                        ) as recognized_in_period,
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
                        min(bounced) over(
                                partition by session_channel,
                                session_experience,
                                a.inception_session_id,
                                quarter_start_day_date
                        ) as bounced_cust,
                        order_current_price,
                        order_event_cnt
                from
                        {hive_schema}.leapfrog_daily_base a
                        join cal on a.activity_date_partition >= TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
                        and a.activity_date_partition <= TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
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
    UNION ALL SELECT 'off'
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
        
        coalesce (a.daily_active_user, 0) daily_active_user,
        coalesce (a.daily_ordering_active_user, 0) daily_ordering_active_user,
        coalesce (a.daily_total_sessions, 0) daily_total_sessions,
        coalesce (a.daily_total_demand, 0) daily_total_demand,
        coalesce (a.daily_total_orders, 0) daily_total_orders,
        coalesce (wtd_active_user, 0) wtd_active_user,
        coalesce (wtd_ordering_active_user, 0) wtd_ordering_active_user,
        coalesce (wtd_total_sessions, 0) wtd_total_sessions,
        coalesce (wtd_total_demand, 0) wtd_total_demand,
        coalesce (wtd_total_orders, 0) wtd_total_orders,
        coalesce (mtd_active_user, 0) mtd_active_user,
        coalesce (mtd_ordering_active_user, 0) mtd_ordering_active_user,
        coalesce (mtd_total_sessions, 0) mtd_total_sessions,
        coalesce (mtd_total_demand, 0) mtd_total_demand,
        coalesce (mtd_total_orders, 0) mtd_total_orders,
        coalesce (qtd_active_user, 0) qtd_active_user,
        coalesce (qtd_ordering_active_user, 0) qtd_ordering_active_user,
        coalesce (qtd_total_sessions, 0) qtd_total_sessions,
        coalesce (qtd_total_demand, 0) qtd_total_demand,
        coalesce (qtd_total_orders, 0) qtd_total_orders,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (wtd_active_user, 0)
        end as week_end_active_user,
        case 
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (wtd_ordering_active_user, 0) 
        end as week_end_ordering_active_user,
        
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (wtd_total_sessions, 0)
        end as week_end_total_sessions,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (wtd_total_demand, 0)
        end as week_end_total_demand,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (wtd_total_orders, 0)
        end as week_end_total_orders,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (mtd_active_user, 0)
        end as month_end_active_user,
        
		case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (mtd_ordering_active_user, 0)
        end as month_end_ordering_active_user,
        
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (mtd_total_sessions, 0)
        end as month_end_total_sessions,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (mtd_total_demand, 0)
        end as month_end_total_demand,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (mtd_total_orders, 0)
        end as month_end_total_orders,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (qtd_active_user, 0)
        end as quarter_end_active_user,
        
		case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (qtd_ordering_active_user, 0)
        end as quarter_end_ordering_active_user,
        
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (qtd_total_sessions, 0)
        end as quarter_end_total_sessions,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (qtd_total_demand, 0)
        end as quarter_end_total_demand,
        case
                when com.day_date = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then coalesce (qtd_total_orders, 0)
        end as quarter_end_total_orders
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
        overwrite table leapfrog_session_active_audience_output
select
        *
from
        final;