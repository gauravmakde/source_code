SET QUERY_BAND = 'App_ID=APP09037; 
     DAG_ID=ddl_mothership_ets_rsb_lookup_11521_ACE_ENG;
     Task_Name=ddl_ets_rsb_lookup;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_mothership.ets_rsb_lookup
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Created/Modified: 07/19/2023

ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates table for rack strategic brands (RSB) to use in sales calculations
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'ets_rsb_lookup', OUT_RETURN_MSG);


create multiset table {mothership_t2_schema}.ets_rsb_lookup
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    Brand                 VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Strategic_Brand       VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (Brand)
;

-- Table Comment (STANDARD)
COMMENT ON {mothership_t2_schema}.ets_rsb_lookup IS 'rack strategic brands (RSB) to use in sales calculations for Executive Telemetry Scorecard (ETS)';
SET QUERY_BAND = NONE FOR SESSION;