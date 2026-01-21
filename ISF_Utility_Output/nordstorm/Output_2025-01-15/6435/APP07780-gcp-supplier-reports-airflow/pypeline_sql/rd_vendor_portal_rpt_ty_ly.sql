SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_TY_LY_DATA;'
FOR SESSION VOLATILE;

--- Read the week range data in csv format from hive DB.
create temporary view temp_ty_ly_data as select * from vndr_portal_dept_loc_ty_ly;

create temporary view temp_ty_ly_hst_data as select * from vndr_portal_dept_loc_ty_ly_hst;

insert overwrite table vndr_portal_dept_loc_ty_ly partition(wk_idnt) SELECT * FROM temp_ty_ly_hst_data;

insert into vndr_portal_dept_loc_ty_ly partition(wk_idnt) SELECT * FROM temp_ty_ly_data;