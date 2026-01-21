SET QUERY_BAND = 'App_ID=APP07324;
    DAG_ID=ddl_fls_assortment_pilot_11521_ACE_ENG;
    Task_Name=ddl_pilot_store_dim;'
    FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.pilot_store_dim
Team/Owner: Merch Insights / Thomas Peterson
Date Created/Modified: 6/19/2024
*/

-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'pilot_store_dim', OUT_RETURN_MSG);

CREATE MULTISET TABLE {shoe_categories_t2_schema}.pilot_store_dim 
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      store_num INTEGER,
      store_label VARCHAR(53) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_num_nyc INTEGER,
      store_label_nyc VARCHAR(53) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_num INTEGER,
      channel_label VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC,
      selling_channel VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      banner VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_type_code VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_type_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_address_state CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_address_state_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_postal_code VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_country_code CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_dma_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_region VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_location_latitude FLOAT,
      store_location_longitude FLOAT,
      comp_status_code VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      eligibility_types VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      active_store_flag BYTEINT,
      gross_square_footage INTEGER,
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( store_num );

SET QUERY_BAND = NONE FOR SESSION;