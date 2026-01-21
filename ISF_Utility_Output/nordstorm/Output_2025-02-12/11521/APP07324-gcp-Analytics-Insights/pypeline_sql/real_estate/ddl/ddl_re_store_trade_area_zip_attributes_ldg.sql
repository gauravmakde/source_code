SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=re_store_trade_area_zip_attributes_11521_ACE_ENG;
     Task_Name=ddl_re_store_trade_area_zip_attributes_ldg;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_trade_area_zip_attributes_ldg
Owner: Rujira Achawanantakun
Modified: 2023-05-09
Note:
- This landing table is created as part of the re_store_trade_area_zip_attributes job that loads
data from S3 to teradata. The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{real_estate_t2_schema}', 're_store_trade_area_zip_attributes_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {real_estate_t2_schema}.re_store_trade_area_zip_attributes_ldg
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
    )
PRIMARY INDEX(store_number, data_year, zip_code)
;
SET QUERY_BAND = NONE FOR SESSION;