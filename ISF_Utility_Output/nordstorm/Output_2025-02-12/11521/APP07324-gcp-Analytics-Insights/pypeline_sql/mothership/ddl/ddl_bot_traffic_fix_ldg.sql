SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_bot_traffic_fix_11521_ACE_ENG;
     Task_Name=ddl_bot_traffic_fix_ldg;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_mothership.bot_traffic_fix_ldg
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Created/Modified: 09/18/2023

Notes:
creates table with estimated bot traffic during known bot attacks. 
This table then subtracted from visitor_funnel_fact traffic numbers
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'bot_traffic_fix_ldg', OUT_RETURN_MSG);
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mothership_t2_schema}', 'bot_traffic_fix_ldg', OUT_RETURN_MSG);

create multiset table {mothership_t2_schema}.bot_traffic_fix_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    day_date                       DATE,
    box                            VARCHAR(5),
    device_type                    VARCHAR(5),
    suspicious_visitors            DECIMAL(8,0),
    suspicious_viewing_visitors    DECIMAL(8,0),
    suspicious_adding_visitors     DECIMAL(8,0)
    )
PRIMARY INDEX (day_date, box, device_type)
;

SET QUERY_BAND = NONE FOR SESSION;