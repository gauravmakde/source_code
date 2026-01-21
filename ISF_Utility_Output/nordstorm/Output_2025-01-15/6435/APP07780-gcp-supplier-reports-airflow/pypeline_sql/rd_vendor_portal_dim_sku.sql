SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_SKU_DATA;'
FOR SESSION VOLATILE;

--- Read the sku data in csv format from hive DB.
create temporary view temp_sku_data as select * from vndr_portal_sku;

insert overwrite table vndr_portal_sku SELECT * FROM temp_sku_data;



