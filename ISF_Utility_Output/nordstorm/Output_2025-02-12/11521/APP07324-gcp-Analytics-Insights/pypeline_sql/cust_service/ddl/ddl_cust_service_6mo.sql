SET QUERY_BAND = 'App_ID=APP08159;
     DAG_ID=ddl_cust_service_6mo_11521_ACE_ENG;
     Task_Name=ddl_cust_service_6mo;'
     FOR SESSION VOLATILE;

/*
Table definition for {service_eng_t2_schema}.cust_service_6mo: 6-Month Level Service Engagement by Customer and 6 Month Ending
Team：Customer Analytics - Styling & Strategy
Date Created: Mar. 12th 2023

Note:
-- Update Cadence: Monthly (after end of each fiscal month)

*/

create multiset table {service_eng_t2_schema}.cust_service_6mo
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (    
    acp_id varchar(50)
   ,six_mo_ending decimal(6)
   ,service_name varchar(35)
   ,customer_qualifier integer compress
   ,gross_usd_amt_whole decimal(20,2) compress
   ,net_usd_amt_whole decimal(20,2) compress
   ,gross_usd_amt_split decimal(20,2) compress
   ,net_usd_amt_split decimal(20,2) compress
   ,private_style integer compress
   ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ) 
primary index (acp_id,six_mo_ending,service_name) 
;

-- Table Comment (STANDARD)
COMMENT ON  {service_eng_t2_schema}.cust_service_6mo IS '6-Month Level Service Engagement by Customer and 6 Month Ending';


SET QUERY_BAND = NONE FOR SESSION;