SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_SUPP_DATA;'
FOR SESSION VOLATILE;

--- Read the supplier data in csv format from hive DB.
create temporary view temp_supp_data as select * from vndr_portal_supp;

insert overwrite table vndr_portal_supp SELECT * FROM temp_supp_data;



