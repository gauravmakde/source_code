/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09035;
     DAG_ID=promo_event_desc_historical_data_11521_ACE_ENG;
     Task_Name=promo_event_desc_historical_data_s3_load;'
     FOR SESSION VOLATILE;



--- This is for TPT
-- Reading data from S3
create or replace temporary view promo_event_desc_hist_s3
(
    tbl_source                          string
    ,event_id                           integer
    ,event_name                         string
    ,promo_id                           integer
    ,promo_name                         string 
)
USING csv 
OPTIONS (path "s3://audience-marketability/promo_event_desc_hist.csv",
         sep ",",
         header "true");

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table PROMOTION_EVENT_HISTORICAL_DATA_ldg_output
select * from promo_event_desc_hist_s3
;

