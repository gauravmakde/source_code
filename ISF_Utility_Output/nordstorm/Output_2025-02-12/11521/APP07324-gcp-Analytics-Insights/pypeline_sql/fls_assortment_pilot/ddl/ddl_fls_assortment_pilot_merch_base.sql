SET QUERY_BAND = 'App_ID=APP07324;
    DAG_ID=ddl_fls_assortment_pilot_11521_ACE_ENG;
    Task_Name=ddl_fls_assortment_pilot_merch_base;'
    FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.fls_assortment_pilot_merch_base
Team/Owner: Merch Insights / Thomas Peterson
Date Created/Modified: 6/19/2024
*/

-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'fls_assortment_pilot_merch_base', OUT_RETURN_MSG);


CREATE MULTISET TABLE {shoe_categories_t2_schema}.fls_assortment_pilot_merch_base 
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      cc_num VARCHAR(149) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_num INTEGER,
      channel_num INTEGER,
      week_num INTEGER,
      month_num INTEGER,
      rp_flag BYTEINT,
      sales_flag BYTEINT,
      inv_flag BYTEINT,
      rcpt_flag BYTEINT,
      twist_flag BYTEINT,
      sff_flag BYTEINT,
      trans_flag BYTEINT,
      sales_retail DECIMAL(38,4),
      sales_cost DECIMAL(38,4),
      sales_units INTEGER,
      sales_reg_retail DECIMAL(38,4),
      sales_reg_cost DECIMAL(38,4),
      sales_reg_units INTEGER,
      sales_pro_retail DECIMAL(38,4),
      sales_pro_cost DECIMAL(38,4),
      sales_pro_units INTEGER,
      sales_clr_retail DECIMAL(38,4),
      sales_clr_cost DECIMAL(38,4),
      sales_clr_units INTEGER,
      sales_ds_retail DECIMAL(38,4),
      sales_ds_cost DECIMAL(38,4),
      sales_ds_units INTEGER,
      sales_gross_retail DECIMAL(38,4),
      sales_gross_cost DECIMAL(38,4),
      sales_gross_units INTEGER,
      return_retail DECIMAL(38,4),
      return_cost DECIMAL(38,4),
      return_units INTEGER,
      return_ds_retail DECIMAL(38,4),
      return_ds_cost DECIMAL(38,4),
      return_ds_units INTEGER,
      boh_retail DECIMAL(38,4),
      boh_cost DECIMAL(38,4),
      boh_units DECIMAL(38,0),
      eoh_retail DECIMAL(38,4),
      eoh_cost DECIMAL(38,4),
      eoh_units DECIMAL(38,0),
      -- ttl_inv_retail DECIMAL(38,4),
      -- ttl_inv_cost DECIMAL(38,4),
      -- ttl_inv_units DECIMAL(38,0),
      rcpt_po_retail DECIMAL(38,4),
      rcpt_po_cost DECIMAL(38,4),
      rcpt_po_units DECIMAL(38,0),
      rcpt_ds_retail DECIMAL(38,4),
      rcpt_ds_cost DECIMAL(38,4),
      rcpt_ds_units DECIMAL(38,0),
      tsfr_rs_in_retail DECIMAL(38,4),
      tsfr_rs_in_cost DECIMAL(38,4),
      tsfr_rs_in_units DECIMAL(38,0),
      tsfr_rack_in_retail DECIMAL(38,4),
      tsfr_rack_in_cost DECIMAL(38,4),
      tsfr_rack_in_units DECIMAL(38,0),
      tsfr_pah_in_retail DECIMAL(38,4),
      tsfr_pah_in_cost DECIMAL(38,4),
      tsfr_pah_in_units DECIMAL(38,0),
      twist_instock_traffic DECIMAL(38,4),
      twist_total_traffic DECIMAL(38,4),
      sff_units DECIMAL(38,0),
      bopus_units DECIMAL(38,0),
      dsr_units DECIMAL(38,0),
    --   ending_age DECIMAL(38,4),
      aur_dim DECIMAL(18,5),
      qual_ats_units DECIMAL(38,0),
      anniv_flag SMALLINT,
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( cc_num ,store_num ,week_num );

SET QUERY_BAND = NONE FOR SESSION;