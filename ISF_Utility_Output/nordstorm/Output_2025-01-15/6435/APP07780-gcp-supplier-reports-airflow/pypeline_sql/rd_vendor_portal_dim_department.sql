SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_DEPT_DATA;'
FOR SESSION VOLATILE;

--- Read the dept dimension data in csv format from hive DB.
create temporary view temp_dept_data as select * from vndr_portal_dept;

insert overwrite table vndr_portal_dept SELECT * FROM temp_dept_data;



