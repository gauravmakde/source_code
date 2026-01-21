SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_rn_nsg_wifi_coef_11521_ACE_ENG;
     Task_Name=fls_rn_nsg_wifi_coef_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_fls_rn_nsg_wifi_coef
	(
	store_number			INTEGER
    , intercept		        DECIMAL(20,15)
    , rn_wifi_slope         DECIMAL(20,15)
    , holiday_dt            DECIMAL(20,15)
    , weekend_dt            DECIMAL(20,15)
    , holiday_rn_wifi_slope DECIMAL(20,15)
    , weekend_rn_wifi_slope DECIMAL(20,15)
    , r_squared             DECIMAL(20,15)
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/fls_rn_nsg_wifi_coef.csv",
    sep ",",
	header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table fls_rn_nsg_wifi_coef_output
select * from temp_fls_rn_nsg_wifi_coef
;