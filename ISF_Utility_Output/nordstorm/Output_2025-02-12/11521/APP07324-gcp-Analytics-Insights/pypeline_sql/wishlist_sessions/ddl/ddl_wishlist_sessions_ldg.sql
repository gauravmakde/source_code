SET QUERY_BAND = 'App_ID=APP08750;
     DAG_ID=wishlist_sessions_11521_ACE_ENG;
     Task_Name=ddl_wishlist_sessions_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_WISHLIST_DIGITAL_EXP.wishlist_sessions_daily_ldg
Team/Owner: Sachin Goyal
Date Created/Modified: 02/03/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{wishlist_t2_schema}', 'wishlist_sessions_daily_ldg', OUT_RETURN_MSG);

create multiset table {wishlist_t2_schema}.wishlist_sessions_daily_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
      activity_date  date
    , channelcountry varchar(2)
    , channel        varchar(16)
    , experience     varchar(16)
    , pdv_sessions   integer compress
    , atwl_sessions  integer compress
    , pscv_sessions  integer compress
    , atb_sessions   integer compress
    , ord_sessions   integer compress
    , demand         numeric(12,2) compress
    )
primary index(channelcountry, channel, experience)
PARTITION BY RANGE_N(activity_date BETWEEN DATE '2022-01-30' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

SET QUERY_BAND = NONE FOR SESSION;