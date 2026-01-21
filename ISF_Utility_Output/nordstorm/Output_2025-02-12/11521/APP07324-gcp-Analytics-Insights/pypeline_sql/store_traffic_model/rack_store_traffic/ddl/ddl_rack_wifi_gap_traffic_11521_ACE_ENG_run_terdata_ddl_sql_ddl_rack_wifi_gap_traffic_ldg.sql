SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_wifi_gap_traffic_11521_ACE_ENG;
     Task_Name=ddl_rack_wifi_gap_traffic_ldg;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_wifi_gap_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/30/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_FLS_TRAFFIC_MODEL', 'rack_wifi_gap_traffic_ldg', OUT_RETURN_MSG);

create multiset table T2DL_DAS_FLS_TRAFFIC_MODEL.rack_wifi_gap_traffic_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    store_number INTEGER,
    day_dt date,
    wifi_count DECIMAL(20,5) DEFAULT 0.00 COMPRESS 0.00,
    traffic DECIMAL(20,5) DEFAULT 0.00 COMPRESS 0.00
) UNIQUE PRIMARY INDEX (store_number, day_dt);


SET QUERY_BAND = NONE FOR SESSION;