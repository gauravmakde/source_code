/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_online_inventory;'
     FOR SESSION VOLATILE;
 


/*
T2/Table Name: IDF_ONLINE_INV
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all online inventory data by EPM_CHOICE_NUM X WEEK

*/
DELETE FROM {ip_forecast_t2_schema}.IDF_ONLINE_INV
WHERE (week_start_date >= {START_DATE} AND week_start_date <= {END_DATE});

INSERT INTO {ip_forecast_t2_schema}.IDF_ONLINE_INV
with sku as (
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
    where
         psd.channel_country = 'US'
),
store as (
    select
        store_num,
        CASE
            WHEN channel_num in (110, 120) THEN 'NORDSTROM'
            WHEN channel_num in (210, 250) THEN 'NORDSTROM_RACK'
        END AS channel_brand,
        CASE
            WHEN channel_num in (110, 210) THEN 'STORE'
            WHEN channel_num in (120, 250) THEN 'ONLINE'
        END AS selling_channel
    from
        PRD_NAP_USR_VWS.STORE_DIM
    where
        channel_num in (110, 210, 120, 250)
),
wk as (
    SELECT distinct
        week_idnt as week_num, 
        cast(day_date as date) as day_date, 
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  {START_DATE} and {END_DATE}
),
liveonsite as (
    SELECT 
        popid.channel_brand,
        sku.epm_choice_num, 
        wk_start_end.week_num,
        wk_start_end.week_start_date,
        COUNT(distinct popid.rms_sku_num) as sku_ct,
        MAX(sku.regular_price_amt) as regular_price_amt
    FROM (
        select
            week_num, 
            min(day_date) as week_start_date,
            max(day_date) as week_end_date
        from
            wk
        where
            day_date >= '2021-06-13' --Date represents when the data switches over from historical table to current live on site table
        group by
            1
        ) as wk_start_end
        join PRD_NAP_USR_VWS.PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM AS popid
            on wk_start_end.week_start_date <= popid.eff_end_tmstp
            and wk_start_end.week_end_date >= popid.eff_begin_tmstp
        INNER JOIN sku ON popid.rms_sku_num = sku.rms_sku_num
    WHERE
        popid.is_online_purchasable = 'Y'
        AND popid.channel_brand IN ('NORDSTROM', 'NORDSTROM_RACK')
        AND popid.channel_country = 'US'
        AND sku.epm_choice_num is not null
    group by
        1,
        2,
        3,
        4
),
liveonsite_history as (
    SELECT
        t2los.channel_brand,
        sku.epm_choice_num,
        wk1.week_num,
        wk1.week_start_date,
        COUNT(distinct t2los.rms_sku_num) as sku_ct,
        MAX(sku.regular_price_amt) as regular_price_amt
    FROM
        (
            select
                distinct week_num,
                 week_start_date,
                 day_date
            from
                wk
            where
                day_date >= '2018-11-25'
                and day_date <= '2021-06-12'
        ) as wk1
        join T2DL_DAS_SITE_MERCH.live_on_site_historical AS t2los on wk1.day_date = t2los.day_date
        INNER JOIN sku ON t2los.rms_sku_num = sku.rms_sku_num
    WHERE
        t2los.is_online_purchasable = 'Y'
        AND t2los.channel_brand IN ('NORDSTROM', 'NORDSTROM_RACK')
        AND t2los.channel_country = 'US'
        AND sku.epm_choice_num is not null
        AND wk1.day_date >= '2018-11-25'
        and wk1.day_date <= '2021-06-12'
    group by
        1,
        2,
        3,
        4
      ),
onlineinv as (
    select
        store.channel_brand,
        sku.epm_choice_num,
        wk1.week_num,
        wk1.week_start_date,
        sum(inv.stock_on_hand_qty) as boh,
        count(distinct inv.rms_sku_id) as boh_sku_ct,
        sum(
            case
                when store.selling_channel = 'STORE' then stock_on_hand_qty
                else 0
            end
        ) as boh_store,
        count(distinct
            case
                when store.selling_channel = 'STORE' then rms_sku_id
                else NULL
            end
        ) as boh_store_sku_ct,
        count(distinct
            case
                when store.selling_channel = 'STORE' then location_id
                else NULL
            end
        ) as boh_store_ct
    from
        (
            select
                week_num,
                min(day_date) - INTERVAL '1' DAY as prev_week_end_date,
                week_start_date
            from
                wk
            group by
                1,3
        ) as wk1
        join PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT as inv
            on inv.snapshot_date = wk1.prev_week_end_date
        join sku on inv.rms_sku_id = sku.rms_sku_num
        join store on inv.location_id = store.store_num
    where
        inv.stock_on_hand_qty > 0
        and sku.epm_choice_num is not null
    group by
        1,
        2,
        3,
        4
)
select
    los.channel_brand,
    'ONLINE' as selling_channel,
    los.epm_choice_num,
    los.week_num,
    los.week_start_date,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    los.sku_ct,
    cast(los.regular_price_amt as Double PRECISION) regular_price_amt,
    coalesce(onlineinv.boh, 0) as boh,
    coalesce(onlineinv.boh_sku_ct, 0) as boh_sku_ct,
    coalesce(onlineinv.boh_store, 0) as boh_store,
    coalesce(onlineinv.boh_store_sku_ct, 0) as boh_store_sku_ct,
    coalesce(onlineinv.boh_store_ct, 0) as boh_store_ct,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    (
        select
            *
        from
            liveonsite
        union
        select
            *
        from
            liveonsite_history
    ) as los
    left join onlineinv on los.channel_brand = onlineinv.channel_brand
    and los.epm_choice_num = onlineinv.epm_choice_num
    and los.week_num = onlineinv.week_num;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)
on {ip_forecast_t2_schema}.IDF_ONLINE_INV;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;   