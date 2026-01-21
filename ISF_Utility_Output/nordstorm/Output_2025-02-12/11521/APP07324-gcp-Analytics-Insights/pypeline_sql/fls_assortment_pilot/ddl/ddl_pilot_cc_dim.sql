SET QUERY_BAND = 'App_ID=APP07324;
    DAG_ID=ddl_fls_assortment_pilot_11521_ACE_ENG;
    Task_Name=ddl_pilot_cc_dim;'
    FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.pilot_cc_dim
Team/Owner: Merch Insights / Thomas Peterson
Date Created/Modified: 6/19/2024
*/

-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'pilot_cc_dim', OUT_RETURN_MSG);

CREATE MULTISET TABLE {shoe_categories_t2_schema}.pilot_cc_dim
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      cc_num VARCHAR(149) CHARACTER SET UNICODE NOT CASESPECIFIC,
      style_desc VARCHAR(5000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      style_group_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      web_style_num BIGINT,
      color_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      color_desc VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_num INTEGER,
      div_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sdiv_num INTEGER,
      sdiv_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_num INTEGER,
      dept_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_num INTEGER,
      class_label VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sbclass_num INTEGER,
      sbclass_label VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      anchor_brand_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      quantrix_category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
PRIMARY INDEX ( cc_num );

SET QUERY_BAND = NONE FOR SESSION;