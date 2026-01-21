SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_forecast_11521_ACE_ENG;
     Task_Name=remote_sell_forecast;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6,2024
*/

/*
Remote Sell Forecast - staging to prod table
*/

DELETE 	{remote_selling_t2_schema}.remote_sell_forecast
FROM 	{remote_selling_t2_schema}.remote_sell_forecast_ldg AS ldg
WHERE 	{remote_selling_t2_schema}.remote_sell_forecast.day_date = ldg.day_date
;
-----
INSERT INTO {remote_selling_t2_schema}.remote_sell_forecast
SELECT
    fiscal_week
    , day_date
    , digital_sales
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {remote_selling_t2_schema}.remote_sell_forecast_ldg ldg
;

COLLECT STATISTICS COLUMN(day_date) ON {remote_selling_t2_schema}.remote_sell_forecast;
-- drop staging table
DROP TABLE {remote_selling_t2_schema}.remote_sell_forecast_ldg;

SET QUERY_BAND = NONE FOR SESSION;
