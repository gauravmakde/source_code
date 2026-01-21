SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_reporting_year_shopped_lkp_11521_ACE_ENG;
     Task_Name=ddl_reporting_year_shopped_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.reporting_year_shopped_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/05/2023

Note:
-- DDL for Reporting Year Shopped Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.reporting_year_shopped_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    reporting_year_shopped_desc VARCHAR(32)
    ,reporting_year_shopped_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (reporting_year_shopped_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.reporting_year_shopped_lkp IS 'Reporting Year Shopped Look-up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;