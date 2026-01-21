/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_store_inventory;'
     FOR SESSION VOLATILE;

 
/*
T2/Table Name: IDF_STORE_INV
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all store inventory data by EPM_CHOICE_NUM X WEEK

*/
DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_INV
WHERE (week_start_date >= '2021-06-13' AND week_start_date <= '2021-06-27');
  
INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_INV
with store as (
    select
        store_num,
        CASE
            WHEN channel_num in (110) THEN 'NORDSTROM'
            WHEN channel_num in (210) THEN 'NORDSTROM_RACK'
        END AS channel_brand,
        'STORE' AS selling_channel
    from
        PRD_NAP_USR_VWS.STORE_DIM
    where
        channel_num in (110, 210)
),
wk as (
   SELECT  
        distinct week_idnt as week_num, 
       CAST(week_start_day_date as date) as week_start_date,
       CAST(week_start_day_date as date) - INTERVAL '1' DAY as prev_week_end_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between '2021-06-13' and '2021-06-27'
),
sku as (  
    select distinct 
        psd.rms_sku_num,
        psd.epm_choice_num,
        price_fcst.regular_price_amt
    from
        PRD_NAP_USR_VWS.PRODUCT_SKU_DIM as psd
        left join (
            select
                rms_sku_num,
                max(regular_price_amt) as regular_price_amt
            from
                PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM
            where
                channel_country = 'US' 
            group by
                1
        ) as price_fcst on psd.rms_sku_num = price_fcst.rms_sku_num
    where psd.channel_country = 'US'
)
select
    store.channel_brand,
    store.selling_channel,
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    store.store_num,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    cast(max(sku.regular_price_amt) as Double PRECISION) as regular_price_amt,
    sum(inv.stock_on_hand_qty) as boh,
    count(distinct inv.rms_sku_id) as boh_sku_ct,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    wk
    join PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT as inv on inv.snapshot_date = wk.prev_week_end_date
    join store on inv.location_id = store.store_num
    join sku on inv.rms_sku_id = sku.rms_sku_num
where
    inv.stock_on_hand_qty > 0
    and sku.epm_choice_num is not null
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    11;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_INV;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
