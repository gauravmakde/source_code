SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_unplanned_closures_11521_ACE_ENG;
     Task_Name=fls_unplanned_closures_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view fls_unplanned_closures_vw_csv
	(
	closure_date			DATE
        , all_store_flag		INTEGER
        , store_number		        INTEGER
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/fls_unplanned_closure.csv",
	sep ",",
	header "true",
	dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table fls_unplanned_closures_output
select * from fls_unplanned_closures_vw_csv
; 
