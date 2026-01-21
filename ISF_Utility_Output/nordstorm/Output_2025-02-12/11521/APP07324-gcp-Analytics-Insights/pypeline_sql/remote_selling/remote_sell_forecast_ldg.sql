SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_forecast_11521_ACE_ENG;
     Task_Name=remote_sell_forecast_ldg;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6,2024
*/

--Load csv to datalab table whenever updates are made to file in S3, depends on file sensor
CREATE OR REPLACE TEMPORARY VIEW remote_sell_forecast_data_view_csv
(
    
    fiscal_week         int,
    day_date            date,
    digital_sales  string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/remotesellingplan.csv",
    sep ",",
    header "true"
)
;

-------------------
INSERT INTO TABLE remote_sell_forecast_data_ldg_output
SELECT
    fiscal_week
    , day_date
    , digital_sales
FROM remote_sell_forecast_data_view_csv
;



