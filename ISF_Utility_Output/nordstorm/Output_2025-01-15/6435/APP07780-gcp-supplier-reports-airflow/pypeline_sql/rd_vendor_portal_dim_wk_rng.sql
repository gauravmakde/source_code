SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_WK_RNG_DATA;'
FOR SESSION VOLATILE;

--- Read the week range data in csv format from hive DB.
create temporary view temp_wr_rng_data as select * from week_range;

insert overwrite table week_range SELECT * FROM temp_wr_rng_data;



