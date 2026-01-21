/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_online_sales;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: IDF_ONLINE_SALE
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2024-02

Note:
This table all online sales data by EPM_CHOICE_NUM X WEEK 
 
*/  
DELETE FROM {ip_forecast_t2_schema}.IDF_ONLINE_SALE  
WHERE (week_start_date >= {START_DATE} AND week_start_date <= {END_DATE});

INSERT INTO {ip_forecast_t2_schema}.IDF_ONLINE_SALE
with wk as (
    SELECT distinct
        week_idnt as week_num, 
        cast(day_date as date) as day_date,  
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  {START_DATE} and {END_DATE}
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
    dem.banner as channel_brand
    , 'ONLINE' as selling_channel
    , sku.epm_choice_num
    , wk.week_num
    , wk.week_start_date
    , current_timestamp(0) at time zone 'gmt' as last_updated_utc
    , cast(dma.us_dma_code as INTEGER) as dma_cd
    , SUM(dem.demand_units) as order_line_quantity
    , cast(SUM(dem.jwn_reported_demand_usd_amt) as Double PRECISION) as order_line_amount_usd
    , SUM(case WHEN dem.fulfilled_from_location_type like '%Vendor%' THEN dem.demand_units else 0 end)                                        AS dropship_order_line_quantity
    , cast(SUM(case WHEN dem.fulfilled_from_location_type like '%Vendor%' THEN dem.jwn_reported_demand_usd_amt else 0 end) as Double PRECISION)                AS dropship_order_line_amount_usd
    , SUM(case WHEN dem.delivery_method = 'BOPUS' THEN dem.demand_units else 0 end)                                              AS bopus_order_line_quantity
    , cast(SUM(case WHEN dem.delivery_method = 'BOPUS' THEN dem.jwn_reported_demand_usd_amt else 0 end) as Double PRECISION)                  AS bopus_order_line_amount_usd
    , SUM(case WHEN dem.fulfilled_from_method = 'FC Filled' THEN dem.demand_units else 0 end)                                              AS fc_fill_order_line_quantity
    , cast(SUM(case WHEN dem.fulfilled_from_method = 'FC Filled' THEN dem.jwn_reported_demand_usd_amt else 0 end) as Double PRECISION)                  AS fc_fill_order_line_amount_usd
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM wk
INNER JOIN prd_nap_usr_vws.JWN_DEMAND_METRIC_VW as dem
    ON dem.demand_date = wk.day_date
INNER JOIN sku
    ON dem.rms_sku_num = sku.rms_sku_num
LEFT JOIN PRD_NAP_USR_VWS.ORG_US_ZIP_DMA as dma
    ON trim(cast(coalesce(dem.ship_zip_code, dem.bill_zip_code) AS varchar(10))) = dma.us_zip_code
WHERE dem.channel_country='US'        
    AND dem.channel = 'DIGITAL'        
    AND trim(dem.jwn_reported_demand_ind) = 'Y'    
GROUP BY
	channel_brand
	, selling_channel
	, sku.epm_choice_num
    , wk.week_num
    , wk.week_start_date
    , last_updated_utc
    , dma_cd
    , dw_sys_load_tmstp 
  HAVING sku.epm_choice_num is not null ; 

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on {ip_forecast_t2_schema}.IDF_ONLINE_SALE;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;