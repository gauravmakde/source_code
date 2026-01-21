SET QUERY_BAND = 'App_ID=APP09037; 
     DAG_ID=mothership_ets_rsb_lookup_11521_ACE_ENG;
     Task_Name=ets_rsb_lookup_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_ets_rsb_lookup_csv
	(
  Brand                   STRING,
  Strategic_Brand          STRING
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/mothership/strategic_brands_lookup.csv",
    sep ",",
	  header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table ets_rsb_lookup_output
select * from temp_ets_rsb_lookup_csv
;