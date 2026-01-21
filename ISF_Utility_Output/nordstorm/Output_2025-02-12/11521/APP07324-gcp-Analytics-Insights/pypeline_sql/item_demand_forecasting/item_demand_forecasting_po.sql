/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_po;'
     FOR SESSION VOLATILE;



/*
T2/Table Name: IDF_PO
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all PO (Purchase Orders) data by EPM_CHOICE_NUM X WEEK for both online and store

*/
DELETE FROM {ip_forecast_t2_schema}. IDF_PO
WHERE (week_start_date >= {START_DATE} AND week_start_date <= {END_DATE});
 
INSERT INTO {ip_forecast_t2_schema}. IDF_PO
with store as (
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
    store.channel_brand,
    'ONLINE' as selling_channel,
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    sum(
        case
            when store.selling_channel = 'ONLINE' then alloc.allocated_qty
            else 0
        end
    ) as allocated_qty_online,
    cast(
        sum(
            case
                when store.selling_channel = 'ONLINE' then alloc.received_qty
                else 0
            end
        ) as Double PRECISION
    ) as received_qty_online,
    sum(
        case
            when store.selling_channel = 'STORE' then alloc.allocated_qty
            else 0
        end
    ) as allocated_qty_store,
    cast(
        sum(
            case
                when store.selling_channel = 'STORE' then alloc.received_qty
                else 0
            end
        ) as Double PRECISION
    ) as received_qty_store,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_DISTRIBUTELOCATION_FACT as alloc --- PO & sku, distribution_location_id, allocated_qty, unit_cost_amt
    join PRD_NAP_USR_VWS.PURCHASE_ORDER_FACT pof on alloc.purchase_order_num = pof.purchase_order_num
    join wk on pof.open_to_buy_endofweek_date = wk.day_date
    join sku on alloc.rms_sku_num = sku.rms_sku_num
    join store on alloc.distribute_location_id = store.store_num
where
    pof.status <> 'Worksheet' -- represent a pending PO might not get approved
    and pof.start_ship_date is not null -- filter out those fake PO typically with start_ship_date is null, similar to ORIG_APPROVAL_DT IS NOT NULL
    and alloc.unit_cost_amt > 0
    and alloc.unit_cost_amt is not null -- filter out those fake PO typically with unit_cost_amt<=0
    and alloc.distribution_id <> 0 --remove order without any allocation
    and sku.epm_choice_num is not null
group by
    1,
    2,
    3,
    4,
    5,
    6,
    11;


COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on {ip_forecast_t2_schema}.IDF_PO;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;