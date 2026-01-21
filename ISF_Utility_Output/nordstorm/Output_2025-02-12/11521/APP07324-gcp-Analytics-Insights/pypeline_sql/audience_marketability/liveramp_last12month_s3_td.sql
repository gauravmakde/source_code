
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=liveramp_last12month_11521_ACE_ENG;
     Task_Name=liveramp_last12month_s3_td;'
     FOR SESSION VOLATILE;


/*


T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.liveramp_table_last12month
Team/Owner: Nicole Miao
Date Created/Modified: Dec 6,2023

Note:
-- What is the the purpose of the table: The purpose of this table is to create an one-stop view, containing marketable/reachable information for four marketing channels (Paid, Email, DM, and Site), as well as customer attributes on the ACP_ID level. 
-- What is the update cadence/lookback window: Monthly update, 12 months look back window

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    {mktg_audience_t2_schema}.liveramp_table_last12month
;

insert into  {mktg_audience_t2_schema}.liveramp_table_last12month
select  acp_id   
    ,l1y_purchase_idnt   
    ,l2y_purchase_idnt   
    ,l4y_purchase_idnt    
    ,directmail_idnt    
    ,site_visited_channel 
    ,site_visited_experience 
    ,site_visited_sessions
    ,site_visited_pageviews 
    ,site_visited_atb   
    ,site_converted_orders  
    ,site_converted_demand  
    ,paid_idnt         
    ,paid_marketable_idnt  
    ,email_idnt  
    ,op_email_marketable_idnt
    ,fp_email_marketable_idnt     
    ,email_reached_idnt   
    ,email_purchased_idnt
    ,is_loyalty_member  
    ,is_cardmember   
    ,loyalty_status    
    ,member_enroll_country_code
    ,loyalty_level   
    ,anniversary_flag  
    ,engagement_cohort   
    ,historic_shopping_channel
    ,aare
    ,ltm_gross_spend
    ,ltm_total_trips
    ,lr_custom_column1      
    ,lr_custom_column2      
    ,lr_custom_column3 
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {mktg_audience_t2_schema}.liveramp_table_last12month_ldg
;


-- drop staging table
drop table {mktg_audience_t2_schema}.liveramp_table_last12month_ldg
;



collect statistics column (acp_id)
on {mktg_audience_t2_schema}.liveramp_table_last12month
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
