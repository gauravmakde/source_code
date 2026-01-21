/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_events;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: IDF_EVENTS
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all loyalty and regular event data by WEEK
 
*/
DELETE FROM {ip_forecast_t2_schema}.IDF_EVENTS
WHERE (week_start_date >= {START_DATE} AND week_start_date <= {END_DATE});
   
INSERT INTO {ip_forecast_t2_schema}.IDF_EVENTS
with wk as (
    SELECT distinct
        week_idnt as week_num, 
        cast(day_date as date) as day_date,  
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  {START_DATE} and {END_DATE}
),
event as ( 
    select
        'NORDSTROM_RACK' as channel_brand,
        'ONLINE' as selling_channel,
        wk.week_num,
        wk.week_start_date,
        cast(
            max(
                case
                    when events.rcom_event_name = 'null' then null
                    else events.rcom_event_name
                end
            ) as varchar(100)
        ) as regular_event_name,
        cast(
            max(
                case
                    when events.rcom_loyalty_event = '0' then null
                    else events.rcom_loyalty_event
                end
            ) as varchar(100)
        ) as loyalty_event
    from
        T2DL_DAS_STORE_SALES_FORECASTING.loyalty_events_sales_shipping as events
        join wk on events.event_date = wk.day_date
    group by
        1,
        2,
        3,
        4
    union
    select
        'NORDSTROM_RACK' as channel_brand,
        'STORE' as selling_channel,
        wk.week_num,
        wk.week_start_date,
        cast(
            max(
                case
                    when events.rs_event_name = 'null' then null
                    else events.rs_event_name
                end
            ) as varchar(100)
        ) as regular_event_name,
        cast(
            max(
                case
                    when events.rs_loyalty_event = '0' then null
                    else events.rs_loyalty_event
                end
            ) as varchar(100) 
        ) as loyalty_event
    from
        T2DL_DAS_STORE_SALES_FORECASTING.loyalty_events_sales_shipping as events
        join wk on events.event_date = wk.day_date
    group by
        1,
        2, 
        3,
        4
    union
    select
        'NORDSTROM' as channel_brand,
        'ONLINE' as selling_channel,
        wk.week_num,
        wk.week_start_date,
        cast(
            max(
                case
                    when events.ncom_event_name = 'null' then null
                    else events.ncom_event_name
                end
            ) as varchar(100)
        ) as regular_event_name,
        cast(
            max(
                case
                    when events.ncom_loyalty_event = '0' then null
                    else events.ncom_loyalty_event
                end
            ) as varchar(100)
        ) as loyalty_event
    from
        T2DL_DAS_STORE_SALES_FORECASTING.loyalty_events_sales_shipping as events
        join wk on events.event_date = wk.day_date
    group by
        1,
        2,
        3,
        4
    union
    select
        'NORDSTROM' as channel_brand,
        'STORE' as selling_channel,
        wk.week_num,
        wk.week_start_date,
        cast(
            max(
                case
                    when events.fls_event_name = 'null' then null
                    else events.fls_event_name
                end
            ) as varchar(100)
        ) as regular_event_name,
        cast(
            max(
                case
                    when events.fls_loyalty_event = '0' then null
                    else events.fls_loyalty_event
                end
            ) as varchar(100)
        ) as loyalty_event
    from
        T2DL_DAS_STORE_SALES_FORECASTING.loyalty_events_sales_shipping as events
        join wk on event_date = wk.day_date
    group by
        1,
        2,
        3,
        4
)
select
    channel_brand,
    selling_channel,
    week_num,
    week_start_date,
    cast(regular_event_name as varchar(100)) as regular_event_name,
    cast(loyalty_event as varchar(100)) as loyalty_event,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    event;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel) ,
                    column (week_start_date, channel_brand, selling_channel)
on {ip_forecast_t2_schema}.IDF_EVENTS;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;