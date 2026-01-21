SET QUERY_BAND = 'App_ID=APP08743;
     DAG_ID=mta_finance_forecast_11521_ACE_ENG;
     Task_Name=mta_finance_forecast_load;'
     FOR SESSION VOLATILE;

-- Reading data from S3
create or replace temporary view mta_finance_forecast_csv
        (
        `date`                  date 
        , `box`                 string
        , marketing_type        string
        , cost                  decimal(18,2)
        , traffic_udv           decimal(18,2)
        , orders                decimal(18,2)
        , gross_sales           decimal(18,2)
        , net_sales             decimal(18,2)
        , `sessions`            decimal(18,2)
        , session_orders        decimal(18,2)
        )
USING csv
OPTIONS (path "s3://analytics-insights-triggers/Finance_Forecast/US_Daily_MTA_Finance_forecast.csv",
        sep ",",
        header "true",
        dateformat "M/d/yyyy");

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table finance_forecast_ldg_output
select * from mta_finance_forecast_csv
;