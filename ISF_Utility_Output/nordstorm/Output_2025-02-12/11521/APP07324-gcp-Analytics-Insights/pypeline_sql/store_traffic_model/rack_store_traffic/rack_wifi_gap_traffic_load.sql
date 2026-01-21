SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_wifi_gap_traffic_11521_ACE_ENG;
     Task_Name=rack_wifi_gap_traffic_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view rack_wifi_gap_traffic_vw_csv 
	(
	store_number 		INTEGER
    , day_dt 			DATE
    , wifi_count 		DECIMAL(20,5)
    , traffic 			DECIMAL(20,5)
	)
USING csv 
OPTIONS (path "s3://store-traffic/napbi_store_traffic/rack_wifi_gap_traffic.csv",
		sep ",",
		header "true",
		dateFormat "M/d/yyyy");
 
create
or replace temporary view rack_wifi_gap_traffic_vw_csv_2 as
select store_number
     , day_dt
     , wifi_count
     , traffic
from rack_wifi_gap_traffic_vw_csv
where day_dt between {start_date} and {end_date}
;

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table rack_wifi_gap_traffic_output
select * from rack_wifi_gap_traffic_vw_csv_2
;
 