/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08300;
     DAG_ID=mta_email_campaign_correction_11521_ACE_ENG;
     Task_Name=mta_email_campaign_category_correction_load;'
     FOR SESSION VOLATILE;



-- Reading data from S3
create or replace temporary view email_cat_correction_csv
USING csv
OPTIONS (path "s3://analytics-insights-triggers/email_performance/email_campaign_category_correction.csv",
        sep ",",
        header "true");

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table mta_email_campaign_category_correction_ldg_output
select * from email_cat_correction_csv
;



