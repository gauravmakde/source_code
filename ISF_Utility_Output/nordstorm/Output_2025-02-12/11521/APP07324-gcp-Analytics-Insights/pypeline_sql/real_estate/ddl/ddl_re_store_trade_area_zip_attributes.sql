SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_re_store_trade_area_zip_attributes_11521_ACE_ENG;
     Task_Name=ddl_re_store_trade_area_zip_attributes;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_trare_store_trade_area_zip_attributesde_area_zip_attributes
Owner: Rujira Achawanantakun
Modified: 2023-05-09
Note:
- This ddl is used to create re_store_trade_area_zip_attributes table structure into tier 2 datalab.

*/
CREATE MULTISET TABLE {real_estate_t2_schema}.re_store_trade_area_zip_attributes
	 ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
    (
	 store_number                  int not null
	,data_year                     int not null
	,zip_code                      varchar(7) not null
	,drivedistance                 float
	,drivetime                     float
	,nearest_fls_number            int
	,drivedistance_to_nearest_fls  float
	,nearest_rack_number           int
	,drivedistance_to_nearest_rack float
	,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_number, data_year, zip_code)
;

-- Table Comment
COMMENT ON  {real_estate_t2_schema}.re_store_trade_area_zip_attributes IS 'Store and trade area zip code';
SET QUERY_BAND = NONE FOR SESSION;