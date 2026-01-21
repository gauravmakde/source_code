-- April 2024  add columns for Marketplace

SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=ddl_append_sales_and_returns_fact_base_11521_ACE_ENG;
     Task_Name=ddl_append_sales_and_returns_fact_base;'
     FOR SESSION VOLATILE;
 
ALTER TABLE {sales_returns_t2_schema}.sales_and_returns_fact_base 
ADD source_store_num INTEGER Compress, 
ADD marketplace_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress;

SET QUERY_BAND = NONE FOR SESSION;

 