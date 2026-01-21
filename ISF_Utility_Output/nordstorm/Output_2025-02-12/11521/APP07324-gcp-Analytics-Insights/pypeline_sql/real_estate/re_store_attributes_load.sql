SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=re_store_attributes_11521_ACE_ENG;
     Task_Name=re_store_attributes_load;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_attributes_ldg
Owner: Rujira Achawanantakun
Modified: 2023-05-01
Note:
- This sql is used to read re_store_attributes_ldg data from S3 and load it into
the staging table in tie 2 datalab.

*/

-- load data from s3 to a temp table
create or replace temporary view re_store_attributes_ldg_csv USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/real_estate/RE_Store_Attributes_latest.csv",
    sep ",",
    header "true"
)
;

-- load data from a temp view to a staging table
insert into table re_store_attributes_ldg_temp
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
	from re_store_attributes_ldg_csv
;
