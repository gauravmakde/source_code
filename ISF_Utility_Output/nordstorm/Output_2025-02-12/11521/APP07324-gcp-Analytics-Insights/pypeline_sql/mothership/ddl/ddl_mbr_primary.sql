SET QUERY_BAND = 'App_ID=APP08717;
     DAG_ID=ddl_mothership_mbr_primary_11521_ACE_ENG;
     Task_Name=ddl_mbr_primary;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_mothership.mbr_primary
Team/Owner: tech_ffp_analytics/Matthew Bond/Charlie Taylor
Date Modified: 09/27/2023

Notes:
MBR = finance Monthly Business Review tableau dashboard: https://tableau.nordstrom.com/#/site/AS/views/MBRdash/DigitalMBRPriorMonths?:iid=2
creates intermediate data table to make dashboard run quickly, the primary table is used to power the dashboard
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'mbr_primary', OUT_RETURN_MSG);

CREATE MULTISET TABLE {mothership_t2_schema}.mbr_primary,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
        month_num                    DECIMAL(6,0) DEFAULT 0.000000, 
        business_unit_desc          VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
        feature_name                VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
		    feature_value               DECIMAL(33,6) DEFAULT 0.000000 COMPRESS 0.000000,
		    dw_sys_load_tmstp           TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
     PRIMARY INDEX (month_num, business_unit_desc, feature_name);


-- table comment
COMMENT ON  {mothership_t2_schema}.mbr_primary IS 'primary table used to power the finance Monthly Business Review tableau dashboard: https://tableau.nordstrom.com/#/site/AS/views/MBRdash/DigitalMBRPriorMonths?:iid=2';

SET QUERY_BAND = NONE FOR SESSION;