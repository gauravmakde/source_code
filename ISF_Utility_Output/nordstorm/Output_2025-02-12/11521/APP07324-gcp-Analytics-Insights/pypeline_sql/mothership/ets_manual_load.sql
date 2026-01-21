SET QUERY_BAND = 'App_ID=APP09037;  
     DAG_ID=mothership_ets_manual_11521_ACE_ENG;
     Task_Name=ets_manual_load;'
     FOR SESSION VOLATILE; 


-- Reading data from S3
create or replace temporary view temp_ets_manual_csv
	(
    box                   STRING,
    metric                STRING,
    fiscal_year           DECIMAL(4,0), 
    time_period           STRING,
    actuals               DECIMAL(25,5),
    plan                  DECIMAL(25,5)
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/mothership/ets_manual.csv",
    sep ",",
	  header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table ets_manual_output
select * from temp_ets_manual_csv
;