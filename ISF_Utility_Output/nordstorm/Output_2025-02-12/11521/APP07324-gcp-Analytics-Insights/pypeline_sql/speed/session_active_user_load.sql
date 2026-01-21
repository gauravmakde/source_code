/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=session_user_active_11521_ACE_ENG;
     Task_Name=session_user_active_load;'
     FOR SESSION VOLATILE;

create
or replace temp view cal as
SELECT
        DISTINCT day_date,
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
        fiscal_year_num
FROM
        object_model.day_cal_454_dim b
WHERE
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) between {start_date} and {end_date};

create
or replace temp view daily as
select
        activity_date_partition,
        channel,
        experience,
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
                        a.activity_date_partition,
                        session_channel AS channel,
                        session_experience AS experience,
                        inception_session_id,
                        a.session_id,
                        0 as new_cust,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                inception_session_id,
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
                                inception_session_id,
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
                                inception_session_id,
                                day_date
                        ) AS new_recognized_in_period,
                        min(bounced) OVER(
                                PARTITION BY session_channel,
                                session_experience,
                                inception_session_id,
                                day_date
                        ) AS bounced_cust,
                        order_current_price,
                        order_event_cnt
                FROM
                        acp_event_intermediate.session_user_attributes_parquet a
                        JOIN cal ON a.activity_date_partition = TO_DATE(
                                CAST(
                                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                                )
                        )
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
        activity_date_partition,
        channel,
        experience,
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
        bounced_cust;

create
or replace temp view wtd as
select
        day_date,
        channel,
        experience,
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
                        inception_session_id,
                        a.session_id,
                        0 as new_cust,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                inception_session_id,
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
                                inception_session_id,
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
                                inception_session_id,
                                week_start_day_date
                        ) as new_recognized_in_period,
                        min(bounced) over(
                                partition by session_channel,
                                session_experience, 
                                inception_session_id,
                                week_start_day_date
                        ) as bounced_cust,
                        order_current_price,
                        order_event_cnt
                from
                        acp_event_intermediate.session_user_attributes_parquet a
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
        new_cust;

create
or replace temp view mtd as
select
        day_date,
        channel,
        experience,
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
                        inception_session_id,
                        a.session_id,
                        0 as new_cust,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                inception_session_id,
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
                                inception_session_id,
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
                                inception_session_id,
                                month_start_day_date
                        ) as new_recognized_in_period,
                        min(bounced) over(
                                partition by session_channel,
                                session_experience,
                                inception_session_id,
                                month_start_day_date
                        ) as bounced_cust,
                        order_current_price,
                        order_event_cnt
                from
                        acp_event_intermediate.session_user_attributes_parquet a
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
        new_cust;

create
or replace temp view qtd as
select
        day_date,
        channel,
        experience,
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        new_cust,
        bounced_cust,
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
                        inception_session_id,
                        a.session_id,
                        0 as new_cust,
                        max(
                                CASE
                                        WHEN order_event_cnt>0 THEN 1
                                        ELSE 0
                                END
                        ) OVER (
                                PARTITION BY session_channel,
                                session_experience,
                                inception_session_id,
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
                                inception_session_id,
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
                                inception_session_id,
                                quarter_start_day_date
                        ) as new_recognized_in_period,
                        min(bounced) over(
                                partition by session_channel,
                                session_experience,
                                inception_session_id,
                                quarter_start_day_date
                        ) as bounced_cust,
                        order_current_price,
                        order_event_cnt
                from
                        acp_event_intermediate.session_user_attributes_parquet a
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
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_quarter_num,
        fiscal_year_num,
        recognized_in_period,
        new_recognized_in_period,
        bounced_cust,
        new_cust;

create
or replace temp view final as
SELECT
        a.activity_date_partition,
        a.channel,
        a.experience,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(a.week_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) week_start_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(a.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) week_end_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(a.month_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) month_start_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(a.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) month_end_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(a.quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) quarter_start_day_date,
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(a.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) quarter_end_day_date,
        a.fiscal_day_num,
        a.fiscal_week_num,
        a.fiscal_month_num,
        a.fiscal_quarter_num,
        a.fiscal_year_num,
        a.recognized_in_period,
        a.new_recognized_in_period,
        a.new_cust as new_in_period,
        a.bounced_cust AS bounced_in_period,
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
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then wtd_active_user
        end as week_end_active_user,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then wtd_ordering_active_user
        end as week_end_ordering_active_user,
        
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then wtd_total_sessions
        end as week_end_total_sessions,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then wtd_total_demand
        end as week_end_total_demand,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(b.week_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then wtd_total_orders
        end as week_end_total_orders,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then mtd_active_user
        end as month_end_active_user,
        
		case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then mtd_ordering_active_user
        end as month_end_ordering_active_user,
        
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then mtd_total_sessions
        end as month_end_total_sessions,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then mtd_total_demand
        end as month_end_total_demand,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(m.month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then mtd_total_orders
        end as month_end_total_orders,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then qtd_active_user
        end as quarter_end_active_user,
        
		case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then qtd_ordering_active_user
        end as quarter_end_ordering_active_user,
        
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then qtd_total_sessions
        end as quarter_end_total_sessions,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then qtd_total_demand
        end as quarter_end_total_demand,
        case
                when a.activity_date_partition = TO_DATE(
                        CAST(
                                UNIX_TIMESTAMP(q.quarter_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                        )
                ) then qtd_total_orders
        end as quarter_end_total_orders
from
        daily a
        left join wtd b on a.channel = b.channel
        and a.experience = b.experience
        and a.recognized_in_period = b.recognized_in_period
        and a.new_recognized_in_period = b.new_recognized_in_period
        and a.bounced_cust = b.bounced_cust
        and a.activity_date_partition = b.day_date
        join mtd m on a.channel = m.channel
        and a.experience = m.experience
        and a.recognized_in_period = m.recognized_in_period
        and a.new_recognized_in_period = m.new_recognized_in_period
        and a.bounced_cust = m.bounced_cust
        and a.activity_date_partition = m.day_date
        join qtd q on a.channel = q.channel
        and a.experience = q.experience
        and a.recognized_in_period = q.recognized_in_period
        and a.new_recognized_in_period = q.new_recognized_in_period
        and a.bounced_cust = q.bounced_cust
        and a.activity_date_partition = q.day_date;

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert
        overwrite table session_active_user_output
select
        *
from
        final;
 