SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_division_lkp_11521_ACE_ENG;
     Task_Name=ddl_division_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.division_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Note:
-- DDL for Division Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.division_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    div_num INTEGER
    ,div_desc VARCHAR(32)
    ,subdiv_num INTEGER
    ,subdiv_desc VARCHAR(32)
    ,dept_num INTEGER
    ,dept_desc VARCHAR(50)
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (div_num, subdiv_num, dept_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.division_lkp IS 'Division Look Up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;