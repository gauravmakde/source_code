SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=ddl_twist_item_weekly_11521_ACE_ENG;
     Task_Name=ddl_twist_item_weekly;'
     FOR SESSION VOLATILE;

/*
T2/Table Name:T2DL_DAS_TWIST.twist_item_weekly
Team/Owner:Meghan Hickey (meghan.d.hickey@nordstrom.com)
Date Created/Modified: 04/03/2024

Updates

     * replace eoh with asoh
     * replace 873 with 5629

GOAL: Roll up in-stock rates for RP item dashboard in tableau
*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'twist_item_weekly', OUT_RETURN_MSG);

CREATE MULTISET TABLE {twist_t2_schema}.twist_item_weekly ,
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
      rp_product_ind INTEGER compress (0,1),
      rp_desc VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('RP','NRP'),
      anchor_brand_ind INTEGER compress (0,1),
      rack_strategic_brand_ind INTEGER compress (0,1),
      div_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdiv_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC compress ('Y','N'),
      supp_npg_desc VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('NPG','NON-NPG'),
      brand_name VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subclass_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      vpn VARCHAR(263) CHARACTER SET UNICODE NOT CASESPECIFIC,
      color VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cc VARCHAR(263) CHARACTER SET UNICODE NOT CASESPECIFIC,
      price_type     varchar(1) character set unicode not casespecific compress ('R','C','P'),
      traffic DECIMAL(38,4) compress (0.0000),

      ncom_traffic DECIMAL(38,4) compress (0.0000),
      ncom_instock_traffic DECIMAL(38,4) compress (0.0000),
      ncom_fc_instock_traffic DECIMAL(38,4) compress (0.0000),

      fls_traffic DECIMAL(38,4) compress (0.0000),
      fls_instock_traffic DECIMAL(38,4) compress (0.0000),
      fls_fc_instock_traffic DECIMAL(38,4) compress (0.0000),

      rack_traffic DECIMAL(38,4) compress (0.0000),
      rack_instock_traffic DECIMAL(38,4) compress (0.0000),
      rack_fc_instock_traffic DECIMAL(38,4) compress (0.0000),

      rcom_traffic DECIMAL(38,4) compress (0.0000),
      rcom_instock_traffic DECIMAL(38,4) compress (0.0000),
      rcom_fc_instock_traffic DECIMAL(38,4) compress (0.0000),

      demand DECIMAL(38,4) compress (0.0000),
      fls_demand DECIMAL(38,4) compress (0.0000),
      ncom_demand DECIMAL(38,4) compress (0.0000),
      rack_demand DECIMAL(38,4) compress (0.0000),
      rcom_demand DECIMAL(38,4) compress (0.0000),

      items INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      fls_items INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      ncom_items INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      rack_items INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      rcom_items INTEGER compress (0,1,2,3,4,5,6,7,8,9),

      eoh_mc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      eoh_fc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_mc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_fc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      
      fls_eoh INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      fls_asoh INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      
      ncom_eoh_mc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      ncom_eoh_fc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      ncom_asoh_mc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      ncom_asoh_fc INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      
      rack_eoh INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      rack_asoh INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      
      rcom_eoh INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      rcom_asoh INTEGER compress (0,1,2,3,4,5,6,7,8,9),

      asoh_489 INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_568 INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_584 INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_599 INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_659 INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_5629 INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      asoh_881 INTEGER compress (0,1,2,3,4,5,6,7,8,9),

      instock_489 DECIMAL(38,4) compress (0.0000),
      instock_568 DECIMAL(38,4) compress (0.0000),
      instock_584 DECIMAL(38,4) compress (0.0000),
      instock_599 DECIMAL(38,4) compress (0.0000),
      instock_659 DECIMAL(38,4) compress (0.0000),
      instock_5629 DECIMAL(38,4) compress (0.0000),
      instock_881 DECIMAL(38,4) compress (0.0000),

      oo_4wk_units INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      oo_4wk_dollars DECIMAL(38,4) compress (0.0000),
      fls_oo_4wk_units INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      ncom_oo_4wk_units INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      rack_oo_4wk_units INTEGER compress (0,1,2,3,4,5,6,7,8,9),
      rcom_oo_4wk_units INTEGER compress (0,1,2,3,4,5,6,7,8,9),

      quartile DECIMAL(10,4) compress (0.0000),
      share_fp DECIMAL(38,6) compress (0.000000),
      share_op DECIMAL(38,6) compress (0.000000),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( WK_IDNT ,cc)
PARTITION BY RANGE_N(wk_idnt  BETWEEN 201901 AND 202552 EACH 1);

COMMENT ON {twist_t2_schema}.twist_item_weekly IS 'Roll up in-stock rates for RP item dashboard in tableau';
SET QUERY_BAND = NONE FOR SESSION;
