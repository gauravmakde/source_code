/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_store_sales_merch;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: IDF_STORE_SALE_MERCH
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2024-02

Note:
This table contains all store sales data based on Merch in-season team's standard by EPM_CHOICE_NUM X WEEK 

*/
DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_SALE_MERCH
WHERE (week_start_date >= '2021-06-13' AND week_start_date <= '2021-06-27');
 
 
INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_SALE_MERCH
with wk as (
    SELECT distinct
        week_idnt as week_num, 
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  '2021-06-13' and '2021-06-27'
),
store as (  
   SELECT DISTINCT
         store_num
        ,channel_num
        ,channel_desc
        ,channel_brand 
        ,selling_channel
    FROM prd_nap_usr_vws.price_store_dim_vw  
    WHERE 1=1 
      AND channel_country = 'US'
      AND selling_channel <> 'UNKNOWN'
      AND channel_num IN ('110','210') --110 is FULL LINE STORES, 210 is RACK STORES
)
   SELECT DISTINCT 
        store.channel_brand
        ,store.selling_channel
        ,h.epm_choice_num
        ,s.week_num
        , wk.week_start_date       
        ,s.store_num                
        ,SUM(s.gross_sales_tot_units) AS gross_sls_ttl_u   
        ,SUM(s.gross_sales_tot_regular_units) AS gross_sls_ttl_reg_u
        ,CURRENT_TIMESTAMP as dw_sys_load_tmstp           
    FROM prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
    JOIN store
        ON store.store_num = s.store_num
    JOIN wk 
        ON s.week_num = wk.week_num
    JOIN t2dl_das_in_season_margin_forecasts.inseason_hierarchy AS h 
        ON h.sku_idnt = s.rms_sku_num
    GROUP BY 1,2,3,4,5,6,9;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on T2DL_DAS_INV_POSITION_FORECAST.IDF_STORE_SALE_MERCH;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;