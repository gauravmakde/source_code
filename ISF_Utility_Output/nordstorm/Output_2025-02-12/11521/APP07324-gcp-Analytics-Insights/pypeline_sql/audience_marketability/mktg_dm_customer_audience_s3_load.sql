

/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=mktg_dm_customer_audience_11521_ACE_ENG;
     Task_Name=mktg_dm_customer_audience_s3_load;'
     FOR SESSION VOLATILE;



-- Reading data from S3
create or replace temporary view mktg_dm_customer_audience_acp_s3
(
    acp_id                          string
)
USING csv OPTIONS (path "s3://audience-marketability/ray_dm_marketable.csv");

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table  mktg_dm_customer_audience_output
select * from mktg_dm_customer_audience_acp_s3
;


