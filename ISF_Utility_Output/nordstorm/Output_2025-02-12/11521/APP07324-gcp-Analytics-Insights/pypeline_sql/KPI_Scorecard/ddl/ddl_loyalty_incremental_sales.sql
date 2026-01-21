SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=ddl_loyalty_incremental_sales_11521_ACE_ENG;
     Task_Name=ddl_loyalty_incremental_sales;'
     FOR SESSION VOLATILE;



--DROP TABLE {kpi_scorecard_t2_schema}.loyalty_incremental_sales ;

CREATE MULTISET TABLE {kpi_scorecard_t2_schema}.loyalty_incremental_sales ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
    report_year INTEGER,
    report_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    banner  VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    card_sales FLOAT,
	member_sales FLOAT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(report_year,report_month,banner)
;

-- Table Comment (STANDARD)
COMMENT ON  {kpi_scorecard_t2_schema}.loyalty_incremental_sales IS 'LOYALTY INCREMENTAL SALES CODE FOR CUSTOMERS DATA';

SET QUERY_BAND = NONE FOR SESSION;

