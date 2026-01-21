SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=corrected_modeled_traffic_11521_ACE_ENG;
     Task_Name=corrected_modeled_traffic_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view corrected_modeled_traffic_vw_csv
	(
    store_number              INTEGER 
    , day_date                DATE
    , corrected_wifi          INTEGER
    , corrected_traffic       DECIMAL(20,2) 
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/corrected_modeled_traffic.csv",
	sep ",",
	header "true",
	dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table corrected_modeled_traffic_output
select * from corrected_modeled_traffic_vw_csv
; 
