SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=store_covid_reopen_dt_11521_ACE_ENG;
     Task_Name=store_covid_reopen_dt_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view store_covid_reopen_dt_vw_csv
	(
    store_number 		STRING,
    reopen_dt 			DATE
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/store_covid_reopen_dt.csv",
	sep ",",
	header "true",
	dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table store_covid_reopen_dt_output
select * from store_covid_reopen_dt_vw_csv
;
 