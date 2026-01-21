SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=ddl_mothership_bot_traffic_fix_11521_ACE_ENG;
     Task_Name=ddl_bot_traffic_fix;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_mothership.bot_traffic_fix
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Created/Modified: 09/18/2023

Notes:
creates table with estimated bot traffic during known bot attacks. 
This table then subtracted from visitor_funnel_fact traffic numbers
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'bot_traffic_fix', OUT_RETURN_MSG);


create multiset table {mothership_t2_schema}.bot_traffic_fix
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    day_date              DATE,
    box                   VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
    device_type           VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
    suspicious_visitors            DECIMAL(8,0),
    suspicious_viewing_visitors    DECIMAL(8,0),
    suspicious_adding_visitors     DECIMAL(8,0),
    dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (day_date, box, device_type)
;

-- Table Comment (STANDARD)
COMMENT ON {mothership_t2_schema}.bot_traffic_fix IS 'estimated bot traffic to be subtracted from visitor_funnel_fact';
SET QUERY_BAND = NONE FOR SESSION;