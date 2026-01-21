SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_bot_traffic_fix_11521_ACE_ENG;
     Task_Name=bot_traffic_fix_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view temp_bot_traffic_fix_csv
	(
	day_date                       DATE,
  box                            STRING,
  device_type                    STRING,
  suspicious_visitors            DECIMAL(8,0),
  suspicious_viewing_visitors    DECIMAL(8,0),
  suspicious_adding_visitors     DECIMAL(8,0)
	)
USING csv
OPTIONS (path "s3://analytics-insights-triggers/mothership/bot_traffic_restatement_post_rocket.csv",
    sep ",",
	  header "true",
	  dateFormat "M/d/yyyy");


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table bot_traffic_fix_output
select * from temp_bot_traffic_fix_csv
;