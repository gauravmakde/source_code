SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=ddl_customer_store_distance_buckets_11521_ACE_ENG;
     Task_Name=ddl_customer_store_distance_buckets;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.customer_store_distance_buckets
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/13/2023

Note:
-- DDL for Customer Store Distance Fact table to support store distance look ups for sandbox

*/

create multiset table {str_t2_schema}.customer_store_distance_buckets,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO

    (
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,billing_postal_code VARCHAR(10)
    ,closest_store INTEGER
    ,closest_store_banner VARCHAR(4)
    ,closest_store_distance FLOAT
    ,closest_store_dist_bucket VARCHAR(9)
    ,nord_loyalty_store INTEGER
    ,nord_sol_distance FLOAT
    ,nord_sol_dist_bucket VARCHAR(9)
    ,rack_loyalty_store INTEGER
    ,rack_sol_distance FLOAT
    ,rack_sol_dist_bucket VARCHAR(9)
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (acp_id)
;

-- Table Comment (STANDARD)
COMMENT ON {str_t2_schema}.customer_store_distance_buckets IS 'Age Group Look-up for Customer Sandbox';

SET QUERY_BAND = NONE FOR SESSION;