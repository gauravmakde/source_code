SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=camera_store_details_11521_ACE_ENG;
     Task_Name=camera_store_details_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view camera_store_details_vw_csv
	(
	store_number			INTEGER
        , imputation_flag		INTEGER
        , intercept		        DECIMAL(20,15)
        , slope				DECIMAL(20,15)
        , start_date 			DATE
        , end_date 			DATE
        , traffic_source 		STRING
        , store_type 			STRING
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/camera_store_details.csv",
	sep ",",
	header "true",
	dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table camera_store_details_output
select * from camera_store_details_vw_csv
; 
