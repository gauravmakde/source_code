SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_camera_store_details_11521_ACE_ENG;
     Task_Name=fls_camera_store_details_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_fls_camera_store_details
	(
	store_number			INTEGER
    , imputation_flag		INTEGER
    , intercept             DECIMAL(20,15)
    , slope                 DECIMAL(20,15)
    , start_date            DATE
    , end_date              DATE
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/fls_camera_store_details.csv",
    sep ",",
	header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table fls_camera_store_details_output
select * from temp_fls_camera_store_details
;