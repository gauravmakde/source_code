SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_rn_nsg_wifi_event_dt_11521_ACE_ENG;
     Task_Name=fls_rn_nsg_wifi_event_dt_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_fls_rn_nsg_wifi_event_dt
	(
	wifi_event_date DATE
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/fls_rn_nsg_wifi_event_dt.csv",
    sep ",",
	header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table fls_rn_nsg_wifi_event_dt_output
select * from temp_fls_rn_nsg_wifi_event_dt
;