SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_local_covid_traffic_adjustment_11521_ACE_ENG;
     Task_Name=rack_local_covid_traffic_adjustment_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view rack_local_covid_traffic_adjustment_vw_csv 
	(
	store_number 		INTEGER
    , region 			STRING
    , store_type 		STRING
    , intercept 		DECIMAL(20,5)
    , slope 			DECIMAL(20,5)
    , start_date 		DATE
    , end_date 			DATE
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/rack_local_covid_traffic_adjustment.csv",
		sep ",",
		header "true",
		dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table rack_local_covid_traffic_adjustment_output
select * from rack_local_covid_traffic_adjustment_vw_csv 
; 
