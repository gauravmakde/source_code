SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_region_mapping_11521_ACE_ENG;
     Task_Name=rack_region_mapping_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view rack_region_mapping_vw_csv
	(
	region_group 		STRING
    , region 			STRING
    , clbr_start_date 	DATE
    , clbr_end_date 	DATE
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/rack_region_mapping.csv",
		sep ",",
		header "true",
		dateFormat "M/d/yyyy");
 

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table rack_region_mapping_output
select * from rack_region_mapping_vw_csv
;
 