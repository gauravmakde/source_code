SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=re_store_attributes_11521_ACE_ENG;
     Task_Name=re_store_attributes;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_attributes
Owner: Rujira Achawanantakun
Modified: 2023-05-01
Note:
- This ddl is used to create re_store_attributes table structure into tier 2 datalab

*/
-- add zero padding to zip code that are numeric
update {real_estate_t2_schema}.re_store_attributes_ldg
set zip_code = LPAD(TRIM(zip_code), 5, '0')
where REGEXP_SIMILAR(TRIM(zip_code), '[0-9]+') = 1;

-- delete data from a landing table
delete
from {real_estate_t2_schema}.re_store_attributes
;

-- insert data from a staging table to a landing table
insert into {real_estate_t2_schema}.re_store_attributes
    select
    store_number
    ,store_status
    ,store_type
    ,store_name
    ,address
    ,city
    ,state
    ,zip_code
    ,country
    ,region
    ,district_number
    ,district
    ,msa
    ,msa_code
    ,dma
    ,dma_code
    ,longitude
    ,latitude
    ,open_date
    ,relocation_date
    ,remodel_date
    ,close_date
    ,covid19_open_date
    ,floors
    ,sqft_gross
    ,sqft_sales
    ,sqft_net_sales
    ,max_occupancy_load
    ,exterior_entries
    ,mall_entries
    ,location_quality_score
    ,area_type
    ,center_type
    ,mall_type
    ,mall_name
    ,nearest_fls_number
    ,nearest_fls_drivedistance
    ,nearest_rack_number
    ,nearest_rack_drivedistance
    ,trade_area_type
	,CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from {real_estate_t2_schema}.re_store_attributes_ldg
;
-- drop staging table
drop table {real_estate_t2_schema}.re_store_attributes_ldg;

SET QUERY_BAND = NONE FOR SESSION;