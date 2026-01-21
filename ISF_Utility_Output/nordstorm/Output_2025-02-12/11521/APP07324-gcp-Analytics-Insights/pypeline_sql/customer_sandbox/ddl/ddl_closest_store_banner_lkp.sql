SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_closest_store_banner_lkp_11521_ACE_ENG;
     Task_Name=ddl_closest_store_banner_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.closest_store_banner_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/13/2023

Note:
-- DDL for Closest Store Banner Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.closest_store_banner_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    closest_store_banner_desc VARCHAR(32)
    ,closest_store_banner_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (closest_store_banner_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.closest_store_banner_lkp IS 'Closest Store Banner Look Up for Customer Sandbox';


SET QUERY_BAND = NONE FOR SESSION;