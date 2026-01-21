SET QUERY_BAND = 'App_ID=APP09037;
     DAG_ID=ddl_mothership_ets_primary_11521_ACE_ENG;
     Task_Name=ddl_ets_primary;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_mothership.ets_primary
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Modified: 07/19/2023

Notes:
ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates intermediate data table to make dashboard run quickly, the primary table used to power the dashboard
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'ets_primary', OUT_RETURN_MSG);


CREATE MULTISET TABLE {mothership_t2_schema}.ets_primary,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
        box                	        VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
        period_type                	VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
        period_value                VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
        feature_name                VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
		    feature_value               DECIMAL(33,6) DEFAULT 0.000000 COMPRESS 0.000000,
		    dw_sys_load_tmstp           TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
     PRIMARY INDEX (box, period_type, period_value, feature_name);


-- table comment
COMMENT ON  {mothership_t2_schema}.ets_primary IS 'primary table used to power the Executive Telemetry Scorecard (ETS) dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views';

SET QUERY_BAND = NONE FOR SESSION;