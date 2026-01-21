   /*
Purpose:        Creates empty tables in {{environment_schema}} for live on site daily aggregated
					los_daily_agg

Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING 
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Mayank Sarwa
Date Created/Modified: 11/19/2024

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'los_daily_agg{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.los_daily_agg{env_suffix} ,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
	  ty_ly_ind VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      fiscal_year INTEGER,
      fiscal_week INTEGER,
      day_num INTEGER,
      day_date DATE FORMAT 'YY/MM/DD',
      channel_country VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_brand VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      division VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdivision VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      department VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      "class" VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subclass VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      quantrix_category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ccs_category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      category_role VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_type_1 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_type_2 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      price_band_one VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      price_band_two VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      style_group_num VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cc VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_ind INTEGER,
      rp_ind INTEGER,
      ds_ind INTEGER,
      mp_ind INTEGER,
      cl_ind INTEGER,
      pm_ind INTEGER,
      reg_ind INTEGER,
      flash_ind INTEGER,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (day_date, channel_country, channel_brand, subclass, cc);

GRANT SELECT ON {environment_schema}.los_daily_agg{env_suffix} TO PUBLIC;
