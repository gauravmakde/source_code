SET QUERY_BAND = 'App_ID=APP09037; 
     DAG_ID=mothership_ets_buyerflow_hist_11521_ACE_ENG;
     Task_Name=ets_buyerflow_hist_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_ets_buyerflow_hist_csv
	(
    fiscal_year              DECIMAL(4,0),
    channel                  STRING,
    total_customers_ly       DECIMAL(9,0), 
    total_trips_ly           DECIMAL(9,0), 
    acquired_ly              DECIMAL(9,0)
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/mothership/buyerflow_hist.csv",
    sep ",",
	  header "true");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table ets_buyerflow_hist_output
select * from temp_ets_buyerflow_hist_csv
;