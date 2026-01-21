SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extracts2;
Task_Name=READ_VPN_HIST_DATA;'
FOR SESSION VOLATILE;

--- Read the vpn hist data in csv format from hive DB.
create temporary view temp_vpn_hist_data as select * from vndr_portal_vpn_hst;

insert overwrite table  vndr_portal_vpn_hst partition(start_week_idnt) SELECT * FROM temp_vpn_hist_data;



