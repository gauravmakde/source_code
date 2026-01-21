SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=ddl_speed_session_fact_daily_11521_ACE_ENG;
     Task_Name=ddl_speed_session_fact_daily;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_SESSIONS.SPEED_SESSION_FACT_DAILY
Team/Owner: Digital Optimization/Sheeba Philip
Date Created/Modified: 04/19/2023 

Note:
SPEED SESSION FACT does common transformations and outputs basic metrics and dimensions at the session_id level
this table rolls them up to a daily grain

*/

create multiset table {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    activity_date_pacific date,
   channel VARCHAR(32),
   experience VARCHAR(32),
   channelcountry VARCHAR(8),
   landing_page VARCHAR(100),
   exit_page VARCHAR(100),
   mrkt_type varchar(50),
   finance_rollup varchar(50),
   finance_detail varchar(50),
   bounce_ind integer,
   acp_id_populated integer,
   loyalty_id_populated integer,
   total_pages_visited bigint,
   total_unique_pages_visited bigint,
   session_count bigint,
   session_duration_seconds decimal(38, 3),
   hp_sessions bigint,
   hp_engaged_sessions bigint,
   hp_views bigint,
   pdp_sessions bigint,
   pdp_engaged_sessions bigint,
   pdp_views bigint,
   browse_sessions bigint,
   browse_engaged_sessions bigint,
   browse_views bigint,
   search_sessions bigint,
   search_engaged_sessions bigint,
   search_views bigint,
   sbn_sessions bigint,
   sbn_engaged_sessions bigint,
   sbn_views bigint,
   wishlist_sessions bigint,
   wishlist_engaged_sessions bigint,
   wishlist_views bigint,
   checkout_sessions bigint,
   checkout_engaged_sessions bigint,
   checkout_views bigint,
   shopb_sessions bigint,
   shopb_engaged_sessions bigint,
   shopb_views bigint,
   atb_sessions bigint,
   atb_views bigint,
   atb_events bigint,
   ordered_sessions bigint,
   sales_qty bigint,
   web_orders bigint,
   web_demand_usd decimal(38, 6),
   dw_batch_date date not null,
   dw_sys_load_tmstp  TIMESTAMP NOT NULL
    )
primary index(activity_date_pacific, channelcountry, channel, experience, mrkt_type, finance_rollup, finance_detail)
partition by range_n(activity_date_pacific BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

-- Table Comment (STANDARD)
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY IS 'Aggregated table built off of dior_session_fact, with the focus on count of sessions daily. DIOR sessions tables bring in additional dimensions and metrics about each session from different domains';
-- Column comments (OPTIONAL)
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.activity_date_pacific IS 'The date when the session occurred, converted to Pacific time';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.channel IS 'Banner for the session, either NORDSTROM or NORDSTROM_RACK';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.experience IS 'Platform for the sessionDESKTOP_WEB, MOBILE_WEB, or IOS_APP';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.channelcountry IS 'Country where session was initiated, US';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.landing_page IS 'Page type for first page view of session, aka entry page';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.exit_page IS 'Page type for last page view of the session, aka exit page';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.mrkt_type IS 'Highest view of marketing hierarchy, whether the session was BASE (non marketing driven), or driven by PAID or UNPAID marketing. Derived from referrer and utm_channel in the url';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.finance_rollup IS 'More granular view of which marketing channel drove a customer to their digital experience with Nordstrom';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.finance_detail IS 'Most granular view of which marketing channel drove a customer to their digital experience with Nordstrom';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.bounce_ind IS 'Whether the session bounced (only had one page view/interaction upon site arrival)';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.acp_id_populated IS 'A flag which shows whether an acp_id is associated with sessions where available, the session associated with an acp_id';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.loyalty_id_populated IS 'A flag which shows whether a Loyalty id associated to the session';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.total_pages_visited IS 'Total pages visited during the session';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.total_unique_pages_visited IS 'Total Unique pages visisted during a session';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.session_duration_seconds IS 'Difference between session start and end times';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.session_count IS 'Count of sessions, the focus metric of this table';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.hp_sessions IS 'Unique count of session with a homepage page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.hp_engaged_sessions IS 'Unique count of session with ahomepage page engagements';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.hp_views IS 'Total number of homepage visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.pdp_sessions IS  'Unique count of session with a pdpd page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.pdp_engaged_sessions IS  'Unique count of session with apdp page engagements';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.pdp_views is 'Total number of pdp page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.browse_sessions IS 'Unique count of session with a brose page visit';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.browse_engaged_sessions IS 'Unique count of session with browse page engagements';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.browse_views IS 'Total number of browse page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.search_sessions IS 'Unique count of session with a search page visit';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.search_engaged_sessions IS 'Unique count of session with search page engagements';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.search_views 'Total number of search page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.sbn_sessions IS 'Unique count of session with a search and browse page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.sbn_engaged_sessions IS 'Unique count of session with a search and browse page engagement';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.sbn_views IS 'Total number of search and browse visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.wishlist_sessions IS 'Unique count of session with a wishlist page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.wishlist_engaged_sessions IS 'Unique count of session with a wishlist page engagement';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.wishlist_views IS 'Total number of wishlist visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.checkout_sessions IS 'Unique count of session with a checkout page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.checkout_engaged_sessions IS 'Unique count of session with a checkout page engagement';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.checkout_views IS 'Total number of checkout visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.shopb_sessions IS 'Unique count of session with a shopping bag page visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.shopb_engaged_sessions IS 'Unique count of session with a shopping bag page engagement';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.shopb_views IS 'Total number of shopping bag visits';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.atb_sessions IS 'Unique count of session with an added to bag event';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.atb_events IS 'Total added to bag events';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.atb_views IS 'Total atb count on page';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.ordered_sessions IS 'Unique count of sessions with an ordersubmitted event';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.sales_qty IS 'Total items ordered in a session';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.web_orders IS 'Total number of orders in a session';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.web_demand_usd IS 'Sum of demand from orders placed, using web events. Denominated in US dollars';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.dw_batch_date IS 'Date that the record was added to the table';
COMMENT ON  {mwp_t2_schema}.SPEED_SESSION_FACT_DAILY.dw_sys_load_tmstp IS 'Timestamp that the record was added to the table';
SET QUERY_BAND = NONE FOR SESSION;