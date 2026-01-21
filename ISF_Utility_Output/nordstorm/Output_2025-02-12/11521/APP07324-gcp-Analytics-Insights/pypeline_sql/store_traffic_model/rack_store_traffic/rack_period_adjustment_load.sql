SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_period_adjustment_11521_ACE_ENG;
     Task_Name=rack_period_adjustment_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view rack_period_adjustment_vw_csv
	(
	period_description 		STRING
    , period_type 			STRING
    , event_type 			STRING
    , region 				STRING
    , intercept 			DECIMAL(20,5)
    , slope 				DECIMAL(20,5)
    , start_date 			DATE
    , end_date 				DATE
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/rack_period_adjustment.csv",
		sep ",",
		header "true",
		dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table rack_period_adjustment_output
select * from rack_period_adjustment_vw_csv
; 
