/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=ddl_inventory_position_master_11521_ACE_ENG;
     Task_Name=ddl_inventory_position_master;'
     FOR SESSION VOLATILE;

/*
T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'inventory_position_master_table', OUT_RETURN_MSG);

create multiset table {ip_forecast_t2_schema}.inventory_position_master_table
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
      week_num INTEGER,
      week_start_date DATE FORMAT 'YY/MM/DD',
      channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      selling_channel VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_country VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      epm_choice_num BIGINT,
      total_sku_ct INTEGER,
      total_size1_ct INTEGER,
      rms_style_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      color_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      manufacturer_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      prmy_supp_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sbclass_num INTEGER,
      class_num INTEGER,
      dept_num INTEGER,
      grp_num INTEGER,
      div_num INTEGER,
      style_desc VARCHAR(800) CHARACTER SET UNICODE NOT CASESPECIFIC,
      nord_display_color VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      return_disposition_code VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      live_start_date DATE FORMAT 'YY/MM/DD',
      selling_channel_eligibility_list VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      drop_ship_eligible_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      anniversary_flag BYTEINT,
      sku_ct INTEGER,
      regular_sku_ct INTEGER,
      skuday_ct INTEGER,
      regular_skuday_ct INTEGER,
      clearance_skuday_ct INTEGER,
      promotion_skuday_ct INTEGER,
      regular_price_amt DECIMAL(15,2),
      current_price_amt DECIMAL(15,2),
      inv_ncom_location_ct INTEGER,
      inv_nca_location_ct INTEGER,
      inv_nrhl_location_ct INTEGER,
      inv_usfp_location_ct INTEGER,
      inv_usop_location_ct INTEGER,
      dma_cd INTEGER,
      order_line_quantity INTEGER,
      order_line_amount_usd DECIMAL(38,6),
      bopus_order_line_quantity INTEGER,
      bopus_order_line_amount_usd DECIMAL(38,6),
      dropship_order_line_quantity INTEGER,
      dropship_order_line_amount_usd DECIMAL(38,6),
      MERCHANDISE_NOT_AVAILABLE_order_line_quantity INTEGER,
      MERCHANDISE_NOT_AVAILABLE_order_line_amount_usd DECIMAL(38,6),
      review_count FLOAT,
      product_views DECIMAL(38,2),
      fp_event_name VARCHAR(22) CHARACTER SET UNICODE NOT CASESPECIFIC,
      op_event_name VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC,
      rp_ind BYTEINT)
PRIMARY INDEX ( channel_brand ,selling_channel ,epm_choice_num ,
class_num ,dept_num );

-- Table Comment (STANDARD)
COMMENT ON  {ip_forecast_t2_schema}.inventory_position_master_table IS 'Master table for IP - unconstrained demand forecasting' ;
-- Column comments (OPTIONAL)

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
