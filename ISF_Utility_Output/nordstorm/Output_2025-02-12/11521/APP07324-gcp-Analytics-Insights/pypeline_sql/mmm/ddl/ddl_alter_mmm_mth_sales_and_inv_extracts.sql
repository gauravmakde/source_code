SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_alter_mmm_mth_sales_and_inv_extracts_11521_ACE_ENG;
     Task_Name=ddl_alter_mmm_mth_sales_and_inv_extracts_11521_ACE_ENG;'
     FOR SESSION VOLATILE;

/*
ALTER to add dw_sys_load_tmstp to MMM sales and inv tables */
 
ALTER TABLE {mmm_t2_schema}.mmm_mth_sales_extract
ADD   dw_sys_load_tmstp  timestamp(6) default current_timestamp(6) not null;

ALTER TABLE {mmm_t2_schema}.mmm_mth_inv_extract
ADD   dw_sys_load_tmstp  timestamp(6) default current_timestamp(6) not null;

SET QUERY_BAND = NONE FOR SESSION;