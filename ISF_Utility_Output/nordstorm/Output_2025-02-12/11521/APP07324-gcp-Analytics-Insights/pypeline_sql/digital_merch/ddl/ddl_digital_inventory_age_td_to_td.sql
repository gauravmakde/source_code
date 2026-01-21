/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=ddl_digital_inventory_age_table_11521_ACE_ENG;
     Task_Name=ddl_digital_inventory_age_td_to_td;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_digitalmerch_flash
Team/Owner: Nicole Miao, Sean Larkin, Rae Ann Boswell
Date Created/Modified: 01/09/2024
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'digital_inventory_age', OUT_RETURN_MSG);

create multiset table {digital_merch_t2_schema}.digital_inventory_age
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
        channel_brand varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,sku_idnt     varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,div_desc     varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,grp_desc     varchar(40) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,current_price_type  varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,subdiv_cat      varchar(40) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,week_num              integer
        ,cohortweek            integer compress 
        ,inventory_age         integer compress
        ,cc_count              integer compress
        ,new_flag_count        integer compress
        ,rp_flag_count         integer compress
        ,sales_dollars         numeric(20,2) compress
        ,demand_dollars        numeric(20,2) compress
        ,order_quantity        integer compress
        ,product_views         numeric(30,2) compress
        ,net_sales_reg_units   integer compress
        ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(sku_idnt,week_num)
partition by RANGE_N (cast(week_num as integer) between 202104 and 205552 each 1)
;

-- Table Comment (STANDARD)
COMMENT ON  {digital_merch_t2_schema}.digital_inventory_age IS 'A table containing the inventory age for all live-on-site SKUs';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
