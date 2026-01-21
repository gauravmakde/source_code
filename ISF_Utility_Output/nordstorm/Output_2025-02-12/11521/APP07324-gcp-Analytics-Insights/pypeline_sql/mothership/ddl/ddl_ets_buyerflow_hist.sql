SET QUERY_BAND = 'App_ID=APP09037; 
     DAG_ID=ddl_mothership_ets_buyerflow_hist_11521_ACE_ENG;
     Task_Name=ddl_ets_buyerflow_hist;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_mothership.ets_buyerflow_hist
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Created/Modified: 09/13/2023

ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates table for 2022 buyerflow data excluding canada (a long and complex calculation that doesn't need to run every time the code runs)
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'ets_buyerflow_hist', OUT_RETURN_MSG);


create multiset table {mothership_t2_schema}.ets_buyerflow_hist
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    fiscal_year              INTEGER,
    channel                  VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
    total_customers_ly       INTEGER, 
    total_trips_ly           INTEGER,
    acquired_ly              INTEGER,
    dw_sys_load_tmstp        TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (fiscal_year, channel)
;

-- Table Comment (STANDARD)
COMMENT ON {mothership_t2_schema}.ets_buyerflow_hist IS 'table for 2022 buyerflow data excluding canada';
SET QUERY_BAND = NONE FOR SESSION;