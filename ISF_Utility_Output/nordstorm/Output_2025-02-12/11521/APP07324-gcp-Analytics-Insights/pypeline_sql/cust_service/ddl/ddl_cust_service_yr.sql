SET QUERY_BAND = 'App_ID=APP08159;
     DAG_ID=ddl_cust_service_yr_11521_ACE_ENG;
     Task_Name=ddl_cust_service_yr;'
     FOR SESSION VOLATILE;

/*
Table definition for {service_eng_t2_schema}.cust_service_yr: Year Level Service Engagement by Customer and Year Ending
Team：Customer Analytics - Styling & Strategy
Date Created: Mar. 12th 2023

Note:
-- Update Cadence: Monthly (after end of each fiscal month)

*/

create multiset table {service_eng_t2_schema}.cust_service_yr
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (    
    acp_id varchar(50)
   ,year_ending decimal(6)
   ,service_name varchar(35)
   ,customer_qualifier integer compress
   ,gross_usd_amt_whole decimal(20,2) compress
   ,net_usd_amt_whole decimal(20,2) compress
   ,gross_usd_amt_split decimal(20,2) compress
   ,net_usd_amt_split decimal(20,2) compress
   ,private_style integer compress
   ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ) 
primary index (acp_id,year_ending,service_name) 
;

-- Table Comment (STANDARD)
COMMENT ON  {service_eng_t2_schema}.cust_service_yr IS 'Year Level Service Engagement by Customer and Year Ending';


SET QUERY_BAND = NONE FOR SESSION;