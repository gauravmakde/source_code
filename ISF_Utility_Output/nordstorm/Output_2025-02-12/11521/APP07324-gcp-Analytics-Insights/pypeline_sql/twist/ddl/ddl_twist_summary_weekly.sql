SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=ddl_twist_summary_weekly_11521_ACE_ENG;
     Task_Name=ddl_twist_summary_weekly;'
     FOR SESSION VOLATILE;

/*
T2/Table Name:T2DL_DAS_TWIST.twist_summary_weekly
Team/Owner:Meghan Hickey (meghan.d.hickey@nordstrom.com)
Date Created/Modified: 04/03/2024

Updates

     * replace 873 with 5629
     * replace market_instock_traffic with fc_instock_traffic

Date Created/Modified: 05/07/2024

Updates

     * add 873 back for visibility

GOAL: Roll up in-stock rates for summary dashboard in tableau
*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'twist_summary_weekly', OUT_RETURN_MSG);

CREATE MULTISET TABLE {twist_t2_schema}.twist_summary_weekly ,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      wk_num BYTEINT,
      wk_end_day_dt DATE FORMAT 'YY/MM/DD',
      WK_IDNT INTEGER,
      WK_LABEL VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
      mth_num BYTEINT,
      fiscal_mth_label VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      qtr_num BYTEINT,
      QTR_LABEL VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      yr_num INTEGER,
      banner CHAR(4)  CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORD','RACK'),
      country CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('CA','US'),
      channel VARCHAR(46) character set unicode not casespecific compress ('110, FULL LINE STORES','210, RACK STORES','250, OFFPRICE ONLINE', '120, N.COM','121, N.CA','FULL LINE CANADA', 'RACK CANADA' )                            ,
      location VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC,
      market INTEGER,
      dma_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      rp_product_ind INTEGER compress (0,1),
      rp_desc VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('RP','NRP'),
      mp_ind VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Y','N'),
      anchor_brand_ind INTEGER compress (0,1),
      rack_strategic_brand_ind INTEGER compress (0,1),
      div_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdiv_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC compress ('Y','N'),
      supp_npg_desc VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('NPG','NON-NPG'),
      price_type     varchar(1) character set unicode not casespecific compress ('R','C','P'),
      traffic DECIMAL(38,4) compress (0.0000),
      instock_traffic DECIMAL(38,4) compress (0.0000),
      fc_instock_traffic DECIMAL(38,4) compress (0.0000),
      hist_items INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      share_fp DECIMAL(38,6) compress (0.000000),
      share_op DECIMAL(38,6) compress (0.000000),
      sku_count INTEGER compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),
      instock_489 DECIMAL(38,4) compress (0.0000),
      instock_568 DECIMAL(38,4) compress (0.0000),
      instock_584 DECIMAL(38,4) compress (0.0000),
      instock_599 DECIMAL(38,4) compress (0.0000),
      instock_659 DECIMAL(38,4) compress (0.0000),
      instock_873 DECIMAL(38,4) compress (0.0000),
      instock_5629 DECIMAL(38,4) compress (0.0000),
      instock_881 DECIMAL(38,4) compress (0.0000),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( WK_IDNT ,location ,dept_desc, supplier)
PARTITION BY RANGE_N(wk_idnt  BETWEEN 201901 AND 202552 EACH 1);

COMMENT ON {twist_t2_schema}.twist_summary_weekly IS 'Roll up in-stock rates for summary dashboard in tableau';
SET QUERY_BAND = NONE FOR SESSION;
