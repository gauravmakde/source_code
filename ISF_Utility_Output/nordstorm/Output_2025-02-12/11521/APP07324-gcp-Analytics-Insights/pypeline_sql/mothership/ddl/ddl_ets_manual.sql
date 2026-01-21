SET QUERY_BAND = 'App_ID=APP09037;
     DAG_ID=ddl_mothership_ets_manual_11521_ACE_ENG;
     Task_Name=ddl_ets_manual;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_mothership.ets_manual_actuals
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Created/Modified: 07/19/2023

Notes:
ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates table of data from stakeholders with complex manual (non-automatable in NAP) calculations

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'ets_manual', OUT_RETURN_MSG);


create multiset table {mothership_t2_schema}.ets_manual
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    box                   VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
    metric                VARCHAR(70) CHARACTER SET UNICODE NOT CASESPECIFIC,
    fiscal_year           INTEGER,
    time_period           VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    actuals               DECIMAL(25,5),
    plan                  DECIMAL(25,5),
    dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (box, metric, time_period)
;

-- Table Comment (STANDARD)
COMMENT ON {mothership_t2_schema}.ets_manual IS 'data from stakeholders with complex manual (non-automatable in NAP) calculations';
SET QUERY_BAND = NONE FOR SESSION;