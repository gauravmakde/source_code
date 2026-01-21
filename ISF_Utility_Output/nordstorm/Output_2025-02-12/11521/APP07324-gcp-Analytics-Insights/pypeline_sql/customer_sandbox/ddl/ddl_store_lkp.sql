SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_store_lkp_11521_ACE_ENG;
     Task_Name=ddl_store_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.store_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Note:
-- DDL for Store Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.store_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    store_num INTEGER
    ,store_desc VARCHAR(170)
    ,store_dma_desc VARCHAR(100)
    ,store_dma_num INTEGER
    ,store_region_desc VARCHAR(32)
    ,store_region_num INTEGER
    ,channel_desc VARCHAR(20)
    ,channel_num INTEGER
    ,banner_desc VARCHAR(10)
    ,banner_num INTEGER
    ,channel_country_desc VARCHAR(7)
    ,channel_country_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (store_num, store_dma_desc, store_region_num, channel_num, channel_country_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.store_lkp IS 'Store Look Up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;