SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_live_on_site_daily_11521_ACE_ENG;
     Task_Name=ddl_live_on_site_daily;'
     FOR SESSION VOLATILE;

 CREATE MULTISET TABLE {live_on_site_t2_schema}.live_on_site_daily, FALLBACK,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (fiscal_year SMALLINT,
       fiscal_week SMALLINT,
       day_num SMALLINT,
       day_date DATE FORMAT 'YY/MM/DD',
       channel_country VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
       channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
       sku_id VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
       price_band_one VARCHAR(12) CHARACTER SET UNICODE NOT CASESPECIFIC,
       price_band_two VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC,
       rp_ind BYTEINT,
       ds_ind BYTEINT,
       cl_ind BYTEINT,
       pm_ind BYTEINT,
       reg_ind BYTEINT,
       flash_ind BYTEINT,
       dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
 PRIMARY INDEX (sku_id, channel_country, day_date);

SET QUERY_BAND = NONE FOR SESSION;