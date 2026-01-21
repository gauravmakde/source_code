SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_cust_nord_buyer_flow_lkp_11521_ACE_ENG;
     Task_Name=ddl_cust_nord_buyer_flow_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_nord_buyer_flow_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 05/22/2023

Note:
-- DDL for Customer Nordstrom Banner Buyerflow Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.cust_nord_buyer_flow_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    cust_nord_buyer_flow_desc VARCHAR(30)
    ,cust_nord_buyer_flow_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (cust_nord_buyer_flow_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.cust_nord_buyer_flow_lkp IS 'Nordstrom Banner Buyerflow Lookup';



SET QUERY_BAND = NONE FOR SESSION;