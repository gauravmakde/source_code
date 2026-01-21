SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_STYLE_DATA;'
FOR SESSION VOLATILE;

--- Read the style data in csv format from hive DB.
create temporary view temp_style_data as select * from vndr_portal_style;

insert overwrite table vndr_portal_style SELECT * FROM temp_style_data;



