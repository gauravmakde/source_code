SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_store_dim_11521_ACE_ENG;
     Task_Name=ddl_store_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.store_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Store-level lookup table containing attributes specific to a store (including location, size, & demographics of the surrounding area).
-- What is the update cadence/lookback window: weekly refreshment, run every Monday at 8am UTC
*/

-- For testing the table. Comment out before final merge.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'store_dim', OUT_RETURN_MSG);

CREATE SET TABLE {usl_t2_schema}.store_dim ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
     --  smart_store_id INTEGER,
     --  smart_business_unit VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_number INTEGER,
      store_name VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_unit VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_id VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_journey VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_segment VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_open_date DATE FORMAT 'YY/MM/DD',
      store_close_date DATE FORMAT 'YY/MM/DD',
      store_location_areatype VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_location_centertype VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_location_malltype VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_location_tradeareatype VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_location_qualityscore INTEGER,
      store_city VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_msa VARCHAR(60) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_dma VARCHAR(80) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_state CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_region VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      country VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_latitude FLOAT,
      store_longitude FLOAT,
      store_layout_sqft INTEGER,
      store_layout_floors INTEGER,
      store_layout_maxoccupancy FLOAT,
      store_layout_entries INTEGER,
      store_commute_drivetime FLOAT,
      store_commute_drivedistance FLOAT,
      store_proximity_fls FLOAT,
      store_proximity_rack FLOAT,
      store_population_people INTEGER,
      store_population_households INTEGER,
      store_population_density FLOAT,
      store_population_students INTEGER,
      store_population_avghhsize INTEGER,
      store_population_growth INTEGER,
      store_income_median INTEGER,
      store_income_75k INTEGER,
      store_income_100k INTEGER,
      store_income_growth INTEGER,
      store_apparelspend_index FLOAT,
      store_population_density_segment VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_commute_drivetime_segment VARCHAR(29) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_commute_drivedistance_segment VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_proximity_fls_segment VARCHAR(28) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_proximity_rack_segment VARCHAR(29) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_layout_maxoccupancy_segment VARCHAR(38) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_segment_id CHAR(32) CHARACTER SET LATIN NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( store_number );
COMMENT ON  {usl_t2_schema}.store_dim IS 'Store-level lookup table containing attributes specific to a store (including location, size, & demographics of the surrounding area).';

SET QUERY_BAND = NONE FOR SESSION;