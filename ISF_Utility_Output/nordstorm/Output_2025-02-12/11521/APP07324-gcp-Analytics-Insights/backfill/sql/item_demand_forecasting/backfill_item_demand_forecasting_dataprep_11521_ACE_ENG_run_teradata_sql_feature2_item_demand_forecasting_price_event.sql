/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_price_event;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: IDF_PRICE_EVENT
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all pricing event data by EPM_CHOICE_NUM X WEEK 
Includes anniversary flag by CC


*/

DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.IDF_PRICE_EVENT
WHERE (week_start_date >= '2021-06-13' AND week_start_date <= '2021-06-27');

INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.IDF_PRICE_EVENT
with wk as (
    SELECT distinct
        week_idnt as week_num, 
        CAST(week_start_day_date as date) as week_start_date,
        CAST(week_end_day_date as date) as week_end_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  '2021-06-13' and '2021-06-27'
), 
sku as ( 
    select
    	distinct 
        rms_sku_num,
        epm_choice_num
    from
        PRD_NAP_USR_VWS.PRODUCT_SKU_DIM
    where
       channel_country = 'US'
),
all_data as ( SELECT
    price_fcst.channel_brand,
    price_fcst.selling_channel, 
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    cast(
        trim(max(coalesce(price_fcst.event_id, 0))) as varchar(50) 
        ) as current_price_event,
   CAST(MAX(price_fcst.enticement_tags) as varchar(100)) as event_tags 
FROM
    wk
		join PRD_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM as price_fcst
    on
    	wk.week_start_date <= price_fcst.eff_end_tmstp 
        and wk.week_end_date >= price_fcst.eff_begin_tmstp 
    join sku
    on
    	price_fcst.rms_sku_num = sku.rms_sku_num
where
    sku.epm_choice_num is not null
    and price_fcst.channel_country = 'US'
    and price_fcst.selling_channel in ('ONLINE', 'STORE')
    and price_fcst.channel_brand in ('NORDSTROM', 'NORDSTROM_RACK')
group by
    1,
    2,
    3,
    4,
    5,
    6)
select b.* from
(select distinct 
channel_brand,
selling_channel,
epm_choice_num,
week_start_date
 from T2DL_DAS_INV_POSITION_FORECAST.IDF_ONLINE_INV
union
select distinct channel_brand,
selling_channel,
epm_choice_num,
week_start_date
from T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_INV) a
join all_data  as b
on a.channel_brand=b.channel_brand 
and a.selling_channel=b.selling_channel
and a.epm_choice_num=b.epm_choice_num 
and a.week_start_date=b.week_start_date; 
  
COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel) 
on T2DL_DAS_INV_POSITION_FORECAST.IDF_PRICE_EVENT;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;