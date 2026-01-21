SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=READ_VPN_DATA;'
FOR SESSION VOLATILE;

--- Read the vpn data in csv format from hive DB.
create temporary view temp_vpn_data as select * from vndr_portal_vpn;

create temporary view temp_vpn_hist_data as select * from vndr_portal_vpn_hst;

insert overwrite table  vndr_portal_vpn partition(wk_idnt) SELECT * FROM temp_vpn_hist_data;

insert into vndr_portal_vpn partition(wk_idnt) SELECT * FROM temp_vpn_data;