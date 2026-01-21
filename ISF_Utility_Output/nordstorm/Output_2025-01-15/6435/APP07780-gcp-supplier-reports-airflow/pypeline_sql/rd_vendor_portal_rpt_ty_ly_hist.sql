SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extracts2;
Task_Name=READ_TY_LY_HIST_DATA;'
FOR SESSION VOLATILE;

--- Read the ty ly hst data in csv format from hive DB.
create temporary view temp_ty_ly_hst_data as select * from vndr_portal_dept_loc_ty_ly_hst;

--insert overwrite table vndr_portal_dept_loc_ty_ly_hst select '4444-04-04' as batch_date;

insert overwrite table vndr_portal_dept_loc_ty_ly_hst partition(wk_idnt) SELECT * FROM temp_ty_ly_hst_data;



