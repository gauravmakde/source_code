SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_local_covid_traffic_adjustment_11521_ACE_ENG;
     Task_Name=fls_local_covid_traffic_adjustment_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view fls_local_covid_traffic_adjustment_vw_csv
	(
    store_number 		INTEGER,
    covid_mltplr 		DECIMAL(20,15),
    start_date 			DATE,
    end_date 			DATE
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/fls_local_covid_traffic_adjustment.csv",
	sep ",",
	header "true",
	dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table fls_local_covid_traffic_adjustment_output
select * from fls_local_covid_traffic_adjustment_vw_csv
; 
