SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_mmm_mth_sales_extract_11521_ACE_ENG;
     Task_Name=ddl_mmm_mth_sales_extract;'
     FOR SESSION VOLATILE;

/*
Table definition for t2dl_das_mmm.mmm_mth_sales_extract

column names specific to neustar mmm model

Team/Owner: AE
Date Created/Modified: 11/23/2022
update Oct 2023 add dw_sys_load_tmstp
*/

/*CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'mmm_mth_sales_extract', OUT_RETURN_MSG); */
 
create multiset table {mmm_t2_schema}.mmm_mth_sales_extract ,FALLBACK , 
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(month_idnt integer ,
day_dt date format 'yyyy-mm-dd',
division varchar(60) character set unicode not casespecific,
store integer,
channel varchar(50) character set unicode not casespecific
compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA' ) , 
reg_revenue decimal(38,2),
reg_qty decimal(38,0),
promo_revenue decimal(38,2),
promo_qty decimal(38,0),
clr_revenue decimal(38,2),
clr_qty decimal(38,0),
dw_sys_load_tmstp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))    
primary index ( day_dt, division, store );

-- Table Comment (STANDARD)
COMMENT ON  {mmm_t2_schema}.mmm_mth_sales_extract IS 'MMM sales extract';

SET QUERY_BAND = NONE FOR SESSION;