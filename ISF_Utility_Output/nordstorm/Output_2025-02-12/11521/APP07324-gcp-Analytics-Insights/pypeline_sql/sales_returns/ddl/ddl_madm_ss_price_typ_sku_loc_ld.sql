SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=ddl_madm_ss_price_typ_sku_loc_ld_11521_ACE_ENG;
     Task_Name=ddl_madm_ss_price_typ_sku_loc_ld;'
     FOR SESSION VOLATILE;

/* 
t2/table name:t2dl_das_sales_returns.madm_ss_price_typ_sku_loc_ld
team/owner: ae
date created/modified: Sep 2023

note:
-- what is the the purpose of the table:

price history in nap incomplete or missing prior to 2021, so current price_type sql
pulls older history from MADM.  MADM is being sunset in 2024,
so need to pull and store static snapshot of price types from madm.  
note: price_typ_sku_loc_ld is the MADM table name, and so the extracted snapshot (ss)
is named madm_ss_price_typ_sku_loc_ld.   It contains history from jan 2014 - Sep 2023.
*/

 /*CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'madm_ss_price_typ_sku_loc_ld', OUT_RETURN_MSG); */
 
create multiset table {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     loc_idnt char(4) character set unicode not casespecific 
    ,sku_idnt varchar(10) character set unicode not casespecific 
    ,start_dt date format 'yyyy-mm-dd'
    ,end_dt date format 'yyyy-mm-dd'
    ,price_typ_cd char(2) character set unicode not casespecific compress ('cl','pm')
	,clearance_soh_ind char(1) character set unicode not casespecific compress ('Y','N')
    ,unit_rtl_amt decimal(20,4) default 0.0000  compress 0.0000 
    ,dw_sys_load_tmstp  timestamp(6) default current_timestamp(6) not null
    )
primary index(loc_idnt, sku_idnt, start_dt, end_dt)
partition by case_n(loc_idnt = '1',
     loc_idnt = '808',
     loc_idnt = '338',
     loc_idnt = '828',
     loc_idnt = '835',
     loc_idnt = '844',
     loc_idnt = '867',
     no case, unknown); 
	 
-- table comment
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld IS 'snapshot of MADM price_typ_sku_loc_ld as of Sep 2023 has clear and promo (no reg)';
--Column comments
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.loc_idnt IS 'selling store number';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.sku_idnt IS 'rms_sku_num';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.start_dt IS 'start date of the off price sale for the sku';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.end_dt IS 'end date of the off price sale for the sku';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.price_typ_cd IS 'two possible values - CL for clearance, PM for promotion';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.clearance_soh_ind IS 'if stock is owned as clearance will be Y. Used to override PM ';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.unit_rtl_amt IS 'the reduced price that was charged for the sku';
COMMENT ON  {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld.dw_sys_load_tmstp IS 'day time of snapshot initialization';

SET QUERY_BAND = NONE FOR SESSION;
