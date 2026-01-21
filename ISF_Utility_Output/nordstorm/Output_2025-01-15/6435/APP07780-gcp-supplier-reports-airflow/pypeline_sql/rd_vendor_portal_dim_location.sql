SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_LOC_DATA;'
FOR SESSION VOLATILE;

--- Read the location data in csv format from hive DB.
create temporary view temp_loc_data as select * from vndr_portal_loc;

insert overwrite table vndr_portal_loc SELECT * FROM temp_loc_data;



