SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=re_store_trade_area_zip_attributes_11521_ACE_ENG;
     Task_Name=re_store_trade_area_zip_attributes;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_trade_area_zip_attributes
Owner: Rujira Achawanantakun
Modified: 2023-05-09
Note:
- This ddl is used to create re_store_trade_area_zip_attributes table structure into tier 2 datalab

*/
-- add zero padding to zip code that are numeric
update {real_estate_t2_schema}.re_store_trade_area_zip_attributes_ldg
set zip_code = LPAD(TRIM(zip_code), 5, '0')
where REGEXP_SIMILAR(TRIM(zip_code), '[0-9]+') = 1;

-- delete data from a landing table
delete
from {real_estate_t2_schema}.re_store_trade_area_zip_attributes
;

-- insert data from a staging table to a landing table
insert into {real_estate_t2_schema}.re_store_trade_area_zip_attributes
    select
    store_number
    ,data_year
    ,zip_code
    ,drivedistance
    ,drivetime
    ,nearest_fls_number
    ,drivedistance_to_nearest_fls
    ,nearest_rack_number
    ,drivedistance_to_nearest_rack
	,CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from {real_estate_t2_schema}.re_store_trade_area_zip_attributes_ldg
;
-- drop staging table
drop table {real_estate_t2_schema}.re_store_trade_area_zip_attributes_ldg;

SET QUERY_BAND = NONE FOR SESSION;