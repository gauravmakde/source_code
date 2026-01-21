/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09058;
     DAG_ID=ddl_weekly_buyerflow_agg_11521_ACE_ENG;
     Task_Name=ddl_weekly_buyerflow_agg;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_EA_BFL.WEEKLY_BUYERFLOW_AGG
Team/Owner: Engagement Analytics - Amanda Wolfman + Elisa Olvera
Date Created/Modified: January 19,2023.

Note:
-- What is the the purpose of the table - run buyerflow counts weekly
-- What is the update cadence/lookback window - it will run the past week + 2 weeks ago

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'weekly_buyerflow_agg', OUT_RETURN_MSG);

create multiset table {bfl_t2_schema}.weekly_buyerflow_agg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     report_period integer
    ,report_type varchar(30)
    ,box varchar(10)
    ,predicted_segment varchar(16)
    ,country varchar(10)
    ,loyalty_status varchar(30)
    ,nordy_level varchar(30)
    ,total_customer integer
    ,total_gross_spend decimal(14,2)
    ,total_net_spend decimal(14,2)
    ,total_trips decimal(14,2)
    ,total_items decimal(14,2)
    ,retained_customer integer
    ,total_retained_spend decimal(14,2)
    ,net_retained_spend decimal(14,2)
    ,retained_trips decimal(14,2)
    ,retained_items decimal(14,2)
    ,new_customer integer
    ,total_new_spend decimal(14,2)
    ,net_new_spend decimal(14,2)
    ,new_trips decimal(14,2)
    ,new_items decimal(14,2)
    ,react_customer integer
    ,total_react_spend decimal(14,2)
    ,net_react_spend decimal(14,2)
    ,react_trips decimal(14,2)
    ,react_items decimal(14,2)
    ,activated_customer integer
    ,total_activated_spend decimal(14,2)
    ,net_activated_spend decimal(14,2)
    ,activated_trips decimal(14,2)
    ,activated_items decimal(14,2)
    ,eng_customer integer
    ,total_eng_spend decimal(14,2)
    ,net_eng_spend decimal(14,2)
    ,eng_trips decimal(14,2)
    ,eng_items decimal(14,2)
    ,update_tmstp timestamp   
    )
primary index(report_period,box)

;

-- Table Comment (STANDARD)
COMMENT ON  {bfl_t2_schema}.weekly_buyerflow_agg IS 'Weekly buyerflow data run in ISF';



/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

