/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_store_sales;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: IDF_STORE_SALE
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table all store sales data by EPM_CHOICE_NUM X WEEK 

*/ 

DELETE FROM {ip_forecast_t2_schema}.IDF_STORE_SALE
WHERE (week_start_date >= {START_DATE} AND week_start_date <= {END_DATE});
   
INSERT INTO {ip_forecast_t2_schema}.IDF_STORE_SALE
with store as (
    select
        store_num,
        case 
            when channel_num in (110) then 'NORDSTROM' 
            when channel_num in (210) then 'NORDSTROM_RACK' 
        end as channel_brand,
        'STORE' AS selling_channel
    from
        PRD_NAP_USR_VWS.STORE_DIM
    where
        channel_num in (110, 210)
),
wk as (
    SELECT distinct
        week_idnt as week_num, 
        cast(day_date as date) as day_date, 
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  {START_DATE} and {END_DATE}
), 
sku as ( 
    select
        distinct 
        rms_sku_num, 
        epm_choice_num
    from
        PRD_NAP_USR_VWS.PRODUCT_SKU_DIM
    where channel_country = 'US'
)
select
    store.channel_brand,
    store.selling_channel,
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    store.store_num,        
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    cast(sum(dem.demand_units) as double precision) as demand_quantity,
    cast(
        sum(
            case 
                when dem.line_item_fulfillment_type = 'StoreTake' then dem.demand_units 
                else 0 
            end
        ) as double precision
    ) as store_take_demand,
    cast(
        sum(
            case 
                when dem.line_item_fulfillment_type not like '%DropShip' then dem.demand_units 
                else 0 
            end
        ) as double precision
    ) as owned_demand,
    cast(
        sum(
            case 
                when trim(dem.employee_discount_ind) = 'Y' then dem.demand_units 
                else 0 
            end
        ) as double precision
    ) as employee_demand,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    wk
    inner join PRD_NAP_USR_VWS.JWN_DEMAND_METRIC_VW as dem
    on
        wk.day_date = dem.demand_date
    inner join sku
    on
        dem.rms_sku_num = sku.rms_sku_num
    inner join store
    on
        dem.intent_store_num = store.store_num
where 
    dem.channel_country='US' 
    and dem.channel = 'STORE'
    and trim(dem.jwn_reported_demand_ind) = 'Y'    
    and trim(dem.line_item_activity_type_code) = 'S' -- SALE line items, including EXCHANGES 
group by
    1,
    2,
    3, 
    4,
    5,
    6,
    7,
    12;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on {ip_forecast_t2_schema}.IDF_STORE_SALE;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;