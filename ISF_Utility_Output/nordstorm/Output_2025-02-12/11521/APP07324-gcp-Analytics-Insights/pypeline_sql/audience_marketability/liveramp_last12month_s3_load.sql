/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=liveramp_last12month_11521_ACE_ENG;
     Task_Name=liveramp_last12month_s3_load;'
     FOR SESSION VOLATILE;

-- Reading data from S3
create or replace temporary view liveramp_table_s3
(
    acp_id                string
    ,l1y_purchase_idnt    integer
    ,l2y_purchase_idnt    integer
    ,l4y_purchase_idnt    integer
    ,directmail_idnt      integer
    ,site_visited_channel string
    ,site_visited_experience string
    ,site_visited_sessions integer
    ,site_visited_pageviews integer
    ,site_visited_atb       integer
    ,site_converted_orders  integer
    ,site_converted_demand  decimal(12,2) 
    ,paid_idnt            integer
    ,paid_marketable_idnt integer
    ,email_idnt           integer
    ,op_email_marketable_idnt integer
    ,fp_email_marketable_idnt integer
    ,email_reached_idnt   integer
    ,email_purchased_idnt integer
    ,is_loyalty_member    integer
    ,is_cardmember        integer
    ,loyalty_status       string
    ,member_enroll_country_code  string
    ,loyalty_level         string
    ,anniversary_flag       string
    ,engagement_cohort      string
    ,historic_shopping_channel  string
    ,aare                   string
    ,ltm_gross_spend        decimal(20,2)
    ,ltm_total_trips        string
    ,lr_custom_column1      integer
    ,lr_custom_column2      integer
    ,lr_custom_column3      integer
)
USING csv OPTIONS (path "s3://audience-marketability/liveramp_data.csv",
                        sep ",",
                        header = 'true');


-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table liveramp_table_last12month_output
select * from liveramp_table_s3
;

