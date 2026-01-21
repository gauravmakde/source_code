SET QUERY_BAND = 'App_ID=APP08750;
     DAG_ID=ddl_wishlist_sessions_11521_ACE_ENG;
     Task_Name=ddl_wishlist_sessions;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_WISHLIST_DIGITAL_EXP.wishlist_sessions_daily
Team/Owner: Sachin Goyal
Date Created/Modified: 02/06/2023

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window: Daily

*/

create multiset table {wishlist_t2_schema}.wishlist_sessions_daily
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
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(channelcountry, channel, experience)
PARTITION BY RANGE_N(activity_date BETWEEN DATE '2022-01-30' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;


COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily IS 'Wishlist Funnel Sessions and Demand';

COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.activity_date IS 'pst date of session';
COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.pdv_sessions IS 'count of sessions with >= 1 product detail viewed event';
COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.atwl_sessions IS 'count of sessions with >= 1 item added to a wishlist';
COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.pscv_sessions IS 'count of sessions with a wishlist product summary collection viewed event';
COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.atb_sessions IS 'count of sessions with >= 1 item added to bag from a wishlist';
COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.ord_sessions IS 'count of sessions with an item added to bag from wishlist and purchased within the same session';
COMMENT ON {wishlist_t2_schema}.wishlist_sessions_daily.demand IS 'sum of wishlist item demand where item was added from a list and purchased within the same session';

SET QUERY_BAND = NONE FOR SESSION;