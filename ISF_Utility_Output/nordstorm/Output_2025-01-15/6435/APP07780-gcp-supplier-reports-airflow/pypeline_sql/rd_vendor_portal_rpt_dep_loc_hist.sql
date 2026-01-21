SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extracts2;
Task_Name=READ_DEPT_LOC_HIST_DATA;'
FOR SESSION VOLATILE;

--- Read the week range data in csv format from hive DB.
create temporary view temp_dept_loc_hst_data as select * from vndr_portal_dept_loc_hst;

insert overwrite table  vndr_portal_dept_loc_hst partition(wk_idnt) SELECT * FROM temp_dept_loc_hst_data;



