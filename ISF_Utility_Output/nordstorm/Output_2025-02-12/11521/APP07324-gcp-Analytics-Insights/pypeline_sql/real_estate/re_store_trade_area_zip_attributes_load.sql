SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=re_store_trade_area_zip_attributes_11521_ACE_ENG;
     Task_Name=re_store_trade_area_zip_attributes_load;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_trade_area_zip_attributes_ldg
Owner: Rujira Achawanantakun
Modified: 2023-05-09
Note:
- This sql is used to read re_store_trade_area_zip_attributes_ldg data from S3 and load it into
the staging table in tie 2 datalab.

*/

-- load data from s3 to a temp table
create or replace temporary view re_store_trade_area_zip_attributes_ldg_csv USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/real_estate/RE_Store_Trade_Area_Zip_Attributes_latest.csv",
    sep ",",
    header "true"
)
;

-- load data from a temp view to a staging table
insert into table re_store_trade_area_zip_attributes_ldg_temp
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
	from re_store_trade_area_zip_attributes_ldg_csv
;