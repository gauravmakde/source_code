--SQL script must begin QUERY_BAND SETTINGS
SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=leapfrog_holdout_11521_ACE_ENG;
     Task_Name=leapfrog_holdout;'
     FOR SESSION VOLATILE;

--FILE CREATION DATE - 06/28/2023
--CREATED BY - Jin Liu
--DataSource - SPEED
--Cadence - Daily (incremental)
create table if not exists ace_etl.leapfrog_holdout (
    shopper_id string,
    channelcountry string,
    channel string,
    platform string,
    holdout_experimentname string,
    holdout_variationname string,
    holdout_first_exposure_time timestamp,
    holdout_first_exposure_time_pst timestamp,
    experimentname string,
    variationname string,
    first_exposure_time timestamp,
    first_exposure_time_pst timestamp,
    min_first_exposure_time timestamp,
    min_first_exposure_date_pst date,
    min_first_exposure_time_pst timestamp,
    holdout_flag int,
    leapfrogeligible_flag int,
    missing_from_holdout int,
    missing_from_eligible int,
    inception_session_id string,
    recognized_in_date int,
    max_session_acp_id string,
    max_session_shopper_id string,
    unique_acp_id_min string,
    unique_entcust_id_min string,
    unique_experiment_id_min string,
    unique_loyalty_id_min string,
    speed_shopper_id string,
    activity_date_partition date,
    session_id string,
    session_endtime_pst timestamp,
    session_channel string,
    session_experience string,
    click_occurred int,
    order_event_cnt bigint,
    order_current_price decimal(38, 6),
    order_reg_price decimal(38, 6),
    day_date date,
    fiscal_day_num string,
    day_date_last_year string,
    day_date_last_year_realigned string,
    fiscal_week_num string,
    week_num_of_fiscal_month string,
    week_start_day_date string,
    week_end_day_date string,
    fiscal_month_num string,
    month_start_day_date string,
    month_end_day_date string,
    fiscal_quarter_num string,
    quarter_start_day_date string,
    quarter_end_day_date string,
    fiscal_year_num string,
    day_num_of_fiscal_month string,
    lly_day_date string,
    lly_day_date_realigned string
) using ORC location 's3://ace-etl/leapfrog_holdout' partitioned BY (activity_date_partition);

create
or replace temp view base as
select
    distinct coalesce(holdout_shopper_id, shopper_id) as shopper_id,
    coalesce(holdout_channelcountry, channelcountry) as channelcountry,
    coalesce(holdout_channel, channel) as channel,
    coalesce(holdout_platform, platform) as platform,
    holdout_experimentname,
    holdout_variationname,
    holdout_first_exposure_time,
    from_utc_timestamp(holdout_first_exposure_time, 'US/Pacific') as holdout_first_exposure_time_pst,
    experimentname,
    variationname,
    first_exposure_time,
    from_utc_timestamp(first_exposure_time, 'US/Pacific') as first_exposure_time_pst,
    least(holdout_first_exposure_time, first_exposure_time) as min_first_exposure_time,
    cast(from_utc_timestamp(
        least(holdout_first_exposure_time, first_exposure_time),
        'US/Pacific'
    ) as date) as min_first_exposure_date_pst,
    from_utc_timestamp(
        least(holdout_first_exposure_time, first_exposure_time),
        'US/Pacific'
    ) as min_first_exposure_time_pst,
    coalesce (holdout_flag, 0) holdout_flag,
    coalesce(leapfrogeligible_flag, 0) leapfrogeligible_flag,
    case
        when holdout_shopper_id is null then 1
        else 0
    end as missing_from_holdout,
    case
        when holdout_shopper_id is not null
        and shopper_id is null then 1
        else 0
    end as missing_from_eligible
from
    (
        select
            distinct source.channelcountry as holdout_channelcountry,
            source.channel as holdout_channel,
            source.platform as holdout_platform,
            experimentname as holdout_experimentname,
            lower(variationname) as holdout_variationname,
            max(
                case
                    when lower(variationname) = 'default' then 1
                    else 0
                end
            ) over (
                partition by customer.id,
                source.platform,
                source.channel
            ) as holdout_flag,
            max(
                case
                    when lower(variationname) = 'leapfrogeligible' then 1
                    else 0
                end
            ) over (
                partition by customer.id,
                source.platform,
                source.channel
            ) as leapfrogeligible_flag,
            customer.id as holdout_shopper_id,
            min(
                cast(
                    from_unixtime(
                        cast(cast(headers ['SystemTime'] as string) as BIGINT) * 0.001
                    ) as timestamp
                )
            ) over (
                partition by source.channelcountry,
                experimentname,
                variationname,
                customer.id,
                source.platform,
                source.channel
            ) holdout_first_exposure_time
        from
            acp_event.customer_experiment_variation_exposed_parquet a
        where
            lower(experimentname) like '%leapfrog_holdout%'
            and source.channel in ('FULL_LINE', 'RACK')
            and source.channelcountry in ('US')
            and a.SOURCE.platform IN ('WEB', 'MOW', 'IOS', 'ANDROID')
            and cast(headers ['Nord-Load'] as string) is null
            and cast(headers ['nord-load'] as string) is null
            and cast(headers ['Nord-Test'] as string) is null
            and cast(headers ['nord-test'] as string) is null
            and cast(headers ['Sretest'] as string) is null
            and coalesce(cast(headers ['identified-bot'] as string), 'XXX') <> 'True'
    ) holdout full
    outer join (
        select
            distinct source.channelcountry,
            source.channel,
            source.platform,
            experimentname,
            lower(variationname) variationname,
            customer.id as shopper_id,
            min(
                cast(
                    from_unixtime(
                        cast(cast(headers ['SystemTime'] as string) as BIGINT) * 0.001
                    ) as timestamp
                )
            ) first_exposure_time
        from
            acp_event.customer_experiment_variation_exposed_parquet a
        where
            (lower(experimentname) like '%leapfrog%' or lower(experimentname) in (
                'pdp_cta_treatment_fla_v2' ,
                'pdp_cta_treatment_rack_v2',
                'pdp_dash_hudson_social_gallery_below_2nd_rec_tray_fla',
                'pdp_dash_hudson_social_gallery_below_2nd_rec_tray_rack',
                'pdp_buypack_redesign_rack' , 
                'productdetails_buypack_redesign',
                'pdp_non_designer_360_videos_rack' , 
                'pdp_non_designer_360_videos_fla',
                'pdp_ncom_card_offer_before_bnpl',
                'pdp_leapfrog_cj6_rcom_card_offer_first'
            )) 
            and lower(experimentname) not like '%leapfrog_holdout%'
            and source.channel in ('FULL_LINE', 'RACK')
            and source.channelcountry in ('US')
            and a.SOURCE.platform IN ('WEB', 'MOW', 'IOS', 'ANDROID')
            and cast(headers ['Nord-Load'] as string) is null
            and cast(headers ['nord-load'] as string) is null
            and cast(headers ['Nord-Test'] as string) is null
            and cast(headers ['nord-test'] as string) is null
            and cast(headers ['Sretest'] as string) is null
            and coalesce(cast(headers ['identified-bot'] as string), 'XXX') <> 'True'
        group by
            1,
            2,
            3,
            4,
            5,
            6
    ) eligible on holdout.holdout_channel = eligible.channel
    and holdout.holdout_platform = eligible.platform
    and holdout.holdout_shopper_id = eligible.shopper_id
where
    coalesce(holdout_shopper_id, shopper_id) <> '';

create
or replace temp view lookup as
select
    a.inception_session_id,
    max(
        case
            when nullif(a.max_session_acp_id, '') is not null then 1
            else 0
        end
    ) over (
        partition by a.inception_session_id,
        a.activity_date_partition
    ) as recognized_in_date,
    a.max_session_acp_id,
    a.max_session_shopper_id,
    a.unique_acp_id_min,
    a.unique_entcust_id_min,
    a.unique_experiment_id_min,
    a.unique_loyalty_id_min,
    a.shopper_id speed_shopper_id,
    a.activity_date_partition,
    a.session_id,
    c.session_min_time_pst as session_endtime_pst,
    case
        when a.session_channel = 'NORDSTROM_RACK' then 'RACK'
        when a.session_channel = 'NORDSTROM' then 'FULL_LINE'
    end as session_channel,
    case
        when a.session_experience = 'IOS_APP' then 'IOS'
        when a.session_experience = 'MOBILE_WEB' then 'MOW'
        when a.session_experience = 'DESKTOP_WEB' then 'WEB'
        else a.session_experience
    end as session_experience,
    c.click_occurred,
    b.order_event_cnt,
    b.order_current_price,
    b.order_reg_price
from
    acp_event_intermediate.session_user_lookup_parquet a
    inner join acp_event_intermediate.session_user_attributes_parquet b on a.session_id = b.session_id
    and a.activity_date_partition = b.activity_date_partition
    left join acp_event_intermediate.session_fact_attributes_parquet c on a.session_id = c.session_id
    and a.activity_date_partition = c.activity_date_partition
where
    a.activity_date_partition between date'2024-01-20'
    and date'2024-03-01';

create
or replace temp view cal as
select
    distinct TO_DATE(
        CAST(
            UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
        )
    ) day_date,
    fiscal_day_num,
    day_date_last_year,
    day_date_last_year_realigned,
    fiscal_week_num,
    week_num_of_fiscal_month,
    week_start_day_date,
    week_end_day_date,
    fiscal_month_num,
    month_start_day_date,
    month_end_day_date,
    fiscal_quarter_num,
    quarter_start_day_date,
    quarter_end_day_date,
    fiscal_year_num,
    day_num_of_fiscal_month,
    lly_day_date,
    lly_day_date_realigned
from
    object_model.day_cal_454_dim
where
    TO_DATE(
        CAST(
            UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
        )
    ) between date'2024-01-20'
    and date'2024-03-01';

create
or replace temp view final as
select
    shopper_id,
    channelcountry,
    channel,
    platform,
    holdout_experimentname,
    holdout_variationname,
    holdout_first_exposure_time,
    holdout_first_exposure_time_pst,
    experimentname,
    variationname,
    first_exposure_time,
    first_exposure_time_pst,
    min_first_exposure_time,
    min_first_exposure_date_pst,
    min_first_exposure_time_pst,
    holdout_flag,
    leapfrogeligible_flag,
    missing_from_holdout,
    missing_from_eligible,
    inception_session_id,
    recognized_in_date,
    max_session_acp_id,
    max_session_shopper_id,
    unique_acp_id_min,
    unique_entcust_id_min,
    unique_experiment_id_min,
    unique_loyalty_id_min,
    speed_shopper_id,
    session_id,
    session_endtime_pst,
    session_channel,
    session_experience,
    click_occurred,
    order_event_cnt,
    order_current_price,
    order_reg_price,
    day_date,
    fiscal_day_num,
    day_date_last_year,
    day_date_last_year_realigned,
    fiscal_week_num,
    week_num_of_fiscal_month,
    week_start_day_date,
    week_end_day_date,
    fiscal_month_num,
    month_start_day_date,
    month_end_day_date,
    fiscal_quarter_num,
    quarter_start_day_date,
    quarter_end_day_date,
    fiscal_year_num,
    day_num_of_fiscal_month,
    lly_day_date, 
    lly_day_date_realigned,
    activity_date_partition
from
    base
    left join lookup on base.shopper_id = lookup.speed_shopper_id
    and base.min_first_exposure_time_pst <= lookup.session_endtime_pst
    and base.channel = lookup.session_channel
    and base.platform = lookup.session_experience
    left join cal on lookup.activity_date_partition = cal.day_date;

insert
    OVERWRITE TABLE ace_etl.leapfrog_holdout PARTITION (activity_date_partition)
select
    /*+ REPARTITION(100) */
    *
from
    final;

MSCK REPAIR table ace_etl.leapfrog_holdout;


