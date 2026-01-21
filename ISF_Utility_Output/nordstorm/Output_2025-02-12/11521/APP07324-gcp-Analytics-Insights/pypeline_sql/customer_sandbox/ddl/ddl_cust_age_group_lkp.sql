SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_cust_age_group_lkp_11521_ACE_ENG;
     Task_Name=ddl_cust_age_group_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_age_group_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/05/2023

Note:
-- DDL for Age Group Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.cust_age_group_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    cust_age_group_desc VARCHAR(32)
    ,cust_age_group_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (cust_age_group_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.cust_age_group_lkp IS 'Age Group Look-up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;