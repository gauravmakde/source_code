SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=wifi_config_change_correction_11521_ACE_ENG;
     Task_Name=wifi_config_change_correction_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_wifi_config_change_correction
	(
	store_number			    INTEGER
    , correction_start_date		DATE
    , level_correction          DECIMAL(20,2)
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/wifi_config_change_correction.csv",
    sep ",",
	header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table wifi_config_change_correction_output
select * from temp_wifi_config_change_correction
;