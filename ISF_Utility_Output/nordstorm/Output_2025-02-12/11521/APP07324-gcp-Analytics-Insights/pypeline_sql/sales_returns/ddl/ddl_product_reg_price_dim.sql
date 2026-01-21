SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=ddl_product_reg_price_dim_11521_ACE_ENG;
     Task_Name=ddl_product_reg_price_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name:T2DL_DAS_SALES_RETURNS.product_reg_price_dim
Team/Owner: AE
Date Created/Modified: 11/17/2022

Note:
-- What is the the purpose of the table: Migrate NAPBI to ISF

*/


create multiset table {sales_returns_t2_schema}.product_reg_price_dim
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     rms_sku_num varchar(10)
    ,price_store_num varchar(11)
    ,regular_price_amt float
    ,pricing_start_date date format 'yyyy-mm-dd'
    ,pricing_end_date date format 'yyyy-mm-dd'
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(rms_sku_num, price_store_num, pricing_start_date, pricing_end_date)
partition by case_n(price_store_num = '1',
     price_store_num = '808',
     price_store_num = '338',
     price_store_num = '828',
     price_store_num = '835',
     price_store_num = '844',
     price_store_num = '867',
     no case, unknown); 

-- Table Comment (STANDARD)
COMMENT ON  {sales_returns_t2_schema}.product_reg_price_dim IS 'PRODUCT_REG_PRICE_DIM using ISF';

SET QUERY_BAND = NONE FOR SESSION;