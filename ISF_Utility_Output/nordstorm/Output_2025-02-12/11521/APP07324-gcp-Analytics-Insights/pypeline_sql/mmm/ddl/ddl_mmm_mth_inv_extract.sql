SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_mmm_mth_inv_extract_11521_ACE_ENG;
     Task_Name=ddl_mmm_mth_inv_extract;'
     FOR SESSION VOLATILE;

/*
T2DL_DAS_MMM.MMM_MTH_INV_EXTRACT

Team/Owner: AE
Date Created/Modified: 11/23/2022
update Oct 2023 add dw_sys_load_tmstp
*/

/*CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'mmm_mth_inv_extract', OUT_RETURN_MSG); */

 CREATE MULTISET TABLE {mmm_t2_schema}.mmm_mth_inv_extract ,FALLBACK , 
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      month_idnt INTEGER,
      wk_start_dt DATE FORMAT 'YYYY-MM-DD',
      wk_end_dt DATE FORMAT 'YYYY-MM-DD',
      division VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      chain_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      loc_idnt VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      reg_qty INTEGER,
      reg_amt FLOAT,
      reg_sku_cnt INTEGER,
      promo_qty INTEGER,
      promo_amt DECIMAL(38,4),
      promo_sku_cnt INTEGER,
      clr_qty INTEGER,
      clr_amt DECIMAL(38,4),
      clr_sku_cnt INTEGER,
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( wk_start_dt, wk_end_dt ,division , chain_desc, loc_idnt );

-- Table Comment (STANDARD)
COMMENT ON  {mmm_t2_schema}.mmm_mth_inv_extract IS 'MMM inventory extract';

SET QUERY_BAND = NONE FOR SESSION;