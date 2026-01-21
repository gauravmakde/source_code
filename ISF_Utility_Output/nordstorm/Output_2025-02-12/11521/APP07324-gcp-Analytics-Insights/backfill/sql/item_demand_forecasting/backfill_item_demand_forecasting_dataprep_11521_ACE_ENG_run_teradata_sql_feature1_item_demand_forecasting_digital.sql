/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_digital;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: IDF_DIGITAL
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all digital data by EPM_CHOICE_NUM X WEEK
Includes product views, averagerating, add to bag

*/ 

 
DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.IDF_DIGITAL
WHERE (week_start_date >= '2021-06-13' AND week_start_date <= '2021-06-27');

INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.IDF_DIGITAL
with wk as (
    SELECT distinct
        week_idnt as week_num, 
        cast(day_date as date) as day_date, 
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  '2021-06-13' and '2021-06-27'
),
sku as (
    select distinct
            epm_choice_num,
            web_style_num as web_style_id
    from
            PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_HIST
    where
            channel_country = 'US' and epm_choice_num is not null and web_style_id is not null
)
select
    CASE
        WHEN pfd.channel = 'FULL_LINE' THEN 'NORDSTROM'
        WHEN pfd.channel = 'RACK' THEN 'NORDSTROM_RACK'
        ELSE null
    END as channel_brand,
    'ONLINE' as selling_channel,
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,    
    cast(
        avg(
            case
                when cast(pfd.averagerating as varchar(1)) = '*'
                    or pfd.averagerating = 0 then NULL
                else pfd.averagerating
            end
        ) as Double PRECISION
    ) as averagerating,
    cast(SUM(pfd.product_views) as Double PRECISION) as product_views,
    cast(SUM(pfd.add_to_bag_quantity) as Double PRECISION) as add_to_bag,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  
    wk 
    JOIN T2DL_DAS_PRODUCT_FUNNEL.product_funnel_daily as pfd ON pfd.event_date_pacific = wk.day_date
    JOIN sku ON sku.web_style_id = pfd.style_id
WHERE
    pfd.channelcountry = 'US'
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    10;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel), 
                    column (week_start_date, epm_choice_num, channel_brand, selling_channel)
on T2DL_DAS_INV_POSITION_FORECAST.IDF_DIGITAL;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/ 
SET QUERY_BAND = NONE FOR SESSION;