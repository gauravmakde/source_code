SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_tran_to_trip_estimate_coeff_11521_ACE_ENG;
     Task_Name=fls_tran_to_trip_estimate_coeff_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_fls_tran_to_trip_estimate_coeff
	(
	store_number			INTEGER
    , time_period_type		STRING
    , trans_to_trips_mltplr DECIMAL(20,15)
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/fls_tran_to_trip_estimate_coeff.csv",
    sep ",",
	header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table fls_tran_to_trip_estimate_coeff_output
select * from temp_fls_tran_to_trip_estimate_coeff
;