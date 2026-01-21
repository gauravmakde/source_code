SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_item_delivery_method_lkp_11521_ACE_ENG;
     Task_Name=ddl_item_delivery_method_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.item_delivery_method_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/05/2023

Note:
-- DDL for Item Delivery Method Look-up table for microstrategy customer sandbox

*/

create multiset table {str_t2_schema}.item_delivery_method_lkp,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    item_delivery_method_desc VARCHAR(32)
    ,item_delivery_method_num INTEGER
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (item_delivery_method_num)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.item_delivery_method_lkp IS 'Item Delivery Method Type Look-up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;