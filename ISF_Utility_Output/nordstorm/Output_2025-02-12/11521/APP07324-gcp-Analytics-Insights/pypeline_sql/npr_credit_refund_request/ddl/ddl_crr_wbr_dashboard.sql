/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_crr_wbr_dashboard_11521_ACE_ENG;
     Task_Name=ddl_crr_wbr_dashboard;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {ccr_t2_schema}.crr_wbr_dashboard
Team/Owner: Deboleena Ganguly (deboleena.ganguly@nordstrom.com)
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{ccr_t2_schema}', 'crr_wbr_dashboard', OUT_RETURN_MSG);

CREATE MULTISET TABLE {ccr_t2_schema}.crr_wbr_dashboard ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      cal_date DATE FORMAT 'YY/MM/DD',
      fiscal_week SMALLINT,
      fiscal_month VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      fiscal_year SMALLINT,
      denied_claims INTEGER,
      approved_claims INTEGER,
      denied_amt FLOAT,
      approved_amt FLOAT,
      insults INTEGER,
      total_claim_amt FLOAT,
      total_claims INTEGER,
      reviewed_claims_count INTEGER,
      reviewed_claims_amount FLOAT,
      insult_dollars FLOAT,
      net_savings FLOAT,
      channel_brand VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC,
      which_table VARCHAR(10) CHARACTER SET UNICODE CASESPECIFIC,
      denied_claims_cnt INTEGER,
      approved_claims_cnt INTEGER,
      reviewed_claims_cnt INTEGER,
      dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( cal_date, channel_brand );
;

-- Table Comment (STANDARD)
COMMENT ON  {ccr_t2_schema}.crr_wbr_dashboard IS 'CRR WBR source table.';
-- Column comments (OPTIONAL)
--COMMENT ON  {ccr_t2_schema}.final_table_name.column_1 IS 'column description';

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
