SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=speed_session_fact_daily_11521_ACE_ENG;
     Task_Name=ddl_speed_session_fact_daily_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_DIGENG.SPEED_SESSION_FACT_DAILY_LDG
Team/Owner: Digital Optimization/Sheeba Philip
Date Created/Modified: 04/19/2023   
Note:
SPEED SESSION FACT does common transformations and outputs basic metrics and dimensions at the session_id level
this table rolls them up to a daily grain. 


This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mwp_t2_schema}', 'speed_session_fact_daily_ldg', OUT_RETURN_MSG);

create multiset table {mwp_t2_schema}.speed_session_fact_daily_ldg
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

)PRIMARY INDEX (activity_date_pacific)
partition by range_n(activity_date_pacific BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;
SET QUERY_BAND = NONE FOR SESSION;