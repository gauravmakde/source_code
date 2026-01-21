SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extracts2;
Task_Name=READ_COLOR_SIZE_HIST_DATA;'
FOR SESSION VOLATILE;

--- Read the colr hist data in csv format from hive DB.
create temporary view temp_color_hist_data as select * from vndr_portal_color_size_hst;

insert overwrite table  vndr_portal_color_size_hst partition(wk_idnt) SELECT * FROM temp_color_hist_data;



