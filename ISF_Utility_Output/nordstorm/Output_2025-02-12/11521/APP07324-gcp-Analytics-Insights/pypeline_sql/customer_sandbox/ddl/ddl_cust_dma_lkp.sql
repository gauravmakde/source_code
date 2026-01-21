SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_cust_dma_lkp_11521_ACE_ENG;
     Task_Name=ddl_cust_dma_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_dma_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Note:
-- DDL for Customer DMA Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.cust_dma_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    cust_dma_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,cust_dma_num INTEGER
    ,cust_region_desc VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,cust_region_num INTEGER
    ,cust_country_desc VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,cust_country_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (cust_dma_num,cust_dma_desc, cust_region_num, cust_country_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.cust_dma_lkp IS 'Customer DMA Look Up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;