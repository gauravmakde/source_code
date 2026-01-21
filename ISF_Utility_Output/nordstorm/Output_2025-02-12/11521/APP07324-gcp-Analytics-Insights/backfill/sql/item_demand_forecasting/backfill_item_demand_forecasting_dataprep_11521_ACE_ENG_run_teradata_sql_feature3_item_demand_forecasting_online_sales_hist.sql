/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_online_sales_hist;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: IDF_ONLINE_SALE_HIST
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table all online sales data by EPM_CHOICE_NUM X WEEK (historical data - not from clarity tables)
clarity tables are not reliable before 2021
 
*/  
DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.IDF_ONLINE_SALE_HIST
WHERE (week_start_date >= '2021-06-13' AND week_start_date <= '2021-06-27');

INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.IDF_ONLINE_SALE_HIST
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
        rms_sku_num
        , epm_choice_num
    from  PRD_NAP_USR_VWS.PRODUCT_SKU_DIM
    where
    channel_country='US'
)
SELECT
    CASE
        WHEN oldf.source_channel_code = 'FULL_LINE' THEN 'NORDSTROM'
        WHEN oldf.source_channel_code = 'RACK' THEN 'NORDSTROM_RACK'
        ELSE null
    END as channel_brand
    , 'ONLINE' as selling_channel
    , sku.epm_choice_num
    , wk.week_num
    , wk.week_start_date
    , current_timestamp(0) at time zone 'gmt' as last_updated_utc
    , cast(dma.us_dma_code as INTEGER) as dma_cd
    , SUM(oldf.order_line_quantity) as order_line_quantity
    , cast(SUM(oldf.order_line_amount_usd) as Double PRECISION) as order_line_amount_usd
    , SUM(case WHEN oldf.LAST_RELEASED_NODE_TYPE_CODE = 'DS' THEN oldf.order_line_quantity else 0 end)                                            AS dropship_order_line_quantity
    , cast(SUM(case WHEN oldf.LAST_RELEASED_NODE_TYPE_CODE = 'DS' THEN oldf.order_line_amount_usd else 0 end) as Double PRECISION)                AS dropship_order_line_amount_usd
    , SUM(case WHEN oldf.PROMISE_TYPE_CODE like '%PICKUP%' THEN oldf.order_line_quantity else 0 end)                                              AS bopus_order_line_quantity
    , cast(SUM(case WHEN oldf.PROMISE_TYPE_CODE like '%PICKUP%' THEN oldf.order_line_amount_usd else 0 end) as Double PRECISION)                  AS bopus_order_line_amount_usd
    , SUM(case WHEN oldf.CANCEL_REASON_CODE = 'MERCHANDISE_NOT_AVAILABLE' THEN oldf.order_line_quantity else 0 end)                               AS MERCHANDISE_NOT_AVAILABLE_order_line_quantity
    , cast(SUM(case WHEN oldf.CANCEL_REASON_CODE = 'MERCHANDISE_NOT_AVAILABLE' THEN oldf.order_line_amount_usd else 0 end) as Double PRECISION)   AS MERCHANDISE_NOT_AVAILABLE_order_line_amount_usd
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp      
FROM wk
INNER JOIN prd_nap_usr_vws.ORDER_LINE_DETAIL_FACT as oldf
    ON oldf.order_date_pacific = wk.day_date
INNER JOIN sku
    ON oldf.rms_sku_num = sku.rms_sku_num
LEFT JOIN PRD_NAP_USR_VWS.ORG_US_ZIP_DMA as dma
    ON trim(cast(coalesce(oldf.destination_zip_code, oldf.bill_zip_code) AS varchar(10))) = dma.us_zip_code
WHERE (oldf.CANCEL_REASON_CODE = 'MERCHANDISE_NOT_AVAILABLE' or oldf.CANCEL_REASON_CODE is null or oldf.CANCEL_REASON_CODE = '')
    AND oldf.source_channel_country_code='US'
    AND oldf.ORDER_DATE_PACIFIC <> '4444-04-04' -- excluding orders WHERE NAP received partial data only. eg: canceled event only
GROUP BY
	channel_brand
	, selling_channel
    , sku.epm_choice_num
    , wk.week_num
    , wk.week_start_date
    , last_updated_utc
    , dma_cd
    , 16  
    HAVING sku.epm_choice_num is not null; 

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on T2DL_DAS_INV_POSITION_FORECAST.IDF_ONLINE_SALE_HIST;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;