/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_node_week_sku_inventory_twist_fact_11521_ACE_ENG;
     Task_Name=ddl_node_week_sku_inventory_twist_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_usl.node_week_sku_inventory_twist_fact
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: Jun. 28th 2023

Note:
-- Purpose of the table: Fact table to directly get Sku / Store / Week level info regarding inventory and twist
-- Update Cadence: Weekly on Sunday

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'node_week_sku_inventory_twist_fact', OUT_RETURN_MSG);

create multiset table {usl_t2_schema}.node_week_sku_inventory_twist_fact
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     store_number INTEGER
    -- ,channel VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,week_num INTEGER
    ,month_num INTEGER
    ,year_num SMALLINT
    ,rms_sku_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    -- ,rms_style_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,color_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,selection_cc VARCHAR(21) CHARACTER SET UNICODE NOT CASESPECIFIC
    -- ,mpg_category VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    -- ,merch_role VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,stock_on_hand_qty INTEGER compress
    ,in_transit_qty INTEGER compress
    ,instock_traffic DECIMAL(12,4) compress
    ,allocated_traffic DECIMAL(12,4) compress
    ,twist FLOAT compress
    ,dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(store_number, week_num, rms_sku_num);

-- Table Comment (STANDARD)
COMMENT ON {usl_t2_schema}.node_week_sku_inventory_twist_fact IS 'Fact table to directly get Sku / Store / Week level info regarding inventory and twist';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

