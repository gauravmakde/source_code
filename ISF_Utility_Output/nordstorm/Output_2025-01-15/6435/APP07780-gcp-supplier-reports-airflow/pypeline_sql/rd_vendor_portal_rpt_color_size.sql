SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_COLOR_SIZE_DATA;'
FOR SESSION VOLATILE;

--- Read the color size data in csv format from hive DB.
create temporary view temp_color_size_data as select * from vndr_portal_color_size;

create temporary view temp_color_hist_data as select * from vndr_portal_color_size_hst;

insert overwrite table  vndr_portal_color_size partition(wk_idnt) SELECT * FROM temp_color_hist_data;

insert into vndr_portal_color_size partition(wk_idnt) SELECT * FROM temp_color_size_data;







