/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_dropship;'
     FOR SESSION VOLATILE;


/* 
T2/Table Name: IDF_DROPSHIP_INV
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all dropship inventory data by EPM_CHOICE_NUM X WEEK
   
*/
DELETE FROM {ip_forecast_t2_schema}.IDF_DROPSHIP
WHERE (week_start_date >= {START_DATE} AND week_start_date <= {END_DATE});

INSERT INTO {ip_forecast_t2_schema}.IDF_DROPSHIP
with wk as (
   SELECT 
        distinct week_idnt as week_num, 
       CAST(week_start_day_date as date) as week_start_date,
       CAST(week_start_day_date as date) - INTERVAL '1' DAY as prev_week_end_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between {START_DATE} and {END_DATE}  
),
sku as (
    select distinct
        rms_sku_num,
        epm_choice_num
    from
        PRD_NAP_USR_VWS.PRODUCT_SKU_DIM
    where
      channel_country = 'US'
)
select
    inv.channel_brand,
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    cast(sum(inv.stock_on_hand_qty) as double PRECISION) as dropship_boh,
    cast(count(distinct inv.rms_sku_id) as double PRECISION) as dropship_sku_ct,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp    
from
    wk
    join ( 
        select
            case
                when location_type = 'DS_OP' then 'NORDSTROM_RACK'
                else 'NORDSTROM'
            end as channel_brand,
            snapshot_date,
            rms_sku_id,
            sum(stock_on_hand_qty) as stock_on_hand_qty
        from
            PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT
        where
            location_type IN ('DS_OP', 'DS')
        group by
            1,
            2,
            3
    ) as inv on inv.snapshot_date = wk.prev_week_end_date
    join sku on inv.rms_sku_id = sku.rms_sku_num
where
    sku.epm_choice_num is not null
group by 
    1,
    2, 
    3,
    4,
    5,
    8;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    column (week_start_date, epm_choice_num, channel_brand)
on {ip_forecast_t2_schema}.IDF_DROPSHIP;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;