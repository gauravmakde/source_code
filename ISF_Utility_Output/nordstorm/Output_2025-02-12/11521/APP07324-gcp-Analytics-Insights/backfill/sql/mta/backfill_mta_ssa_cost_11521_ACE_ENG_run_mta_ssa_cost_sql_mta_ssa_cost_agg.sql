SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_ssa_cost_11521_ACE_ENG;
     Task_Name=mta_ssa_cost_agg;'
     FOR SESSION VOLATILE;

/******************************************************************************
T2DL_DAS_MTA.MTA_SSA_COST_AGG
  - sources three tables
    - mkt_utm_agg
    - mta_paid_campaign_daily
    - affiliates_cost_daily_fact
    - finance_forecast (mta)
*******************************************************************************/


/*=======================================
    MTA and SSA
 =======================================*/
-- drop table mkt_utm_agg_optimized;
CREATE MULTISET VOLATILE TABLE mkt_utm_agg_optimized
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
    activity_date_pacific DATE,
    order_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('RACK.COM','FULL LINE', 'N.COM','N.CA','RACK CANADA','RACK','FULL LINE CANADA'),
    channelcountry  CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('US', 'CA'),
    arrived_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','R.COM','NULL'),
    marketing_type VARCHAR(12) CHARACTER SET UNICODE NOT CASESPECIFIC,
    finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    funnel_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Up', 'Low', 'Mid'),
    nmn_flag VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('YES','NO'),
    attributed_demand FLOAT,
    attributed_pred_net FLOAT,
    attributed_units FLOAT,
    attributed_orders FLOAT,
    session_orders FLOAT,
	session_demand FLOAT,
    sessions FLOAT,
    bounced_sessions FLOAT
    )
PRIMARY INDEX(activity_date_pacific)
PARTITION BY RANGE_N(activity_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
    ON COMMIT PRESERVE ROWS
;

INSERT INTO mkt_utm_agg_optimized
select
    mua.activity_date_pacific
    , CASE mua.channel
        WHEN 'N.COM' THEN 'NCOM'
        WHEN 'R.COM' THEN 'RCOM'
        WHEN 'RACK.COM' THEN 'RCOM'
        WHEN 'FULL LINE' THEN 'FLS'
        WHEN 'N.CA' THEN 'NCA'
        WHEN 'FULL LINE CANADA' THEN 'FLS CANADA'
        WHEN 'RACK CANADA' THEN 'RACK CANADA'
        ELSE mua.channel END  as order_channel
    , mua.channelcountry
    , CASE mua.arrived_channel
        WHEN 'N.COM' THEN 'NCOM'
        WHEN 'R.COM' THEN 'RCOM'
        WHEN 'RACK.COM' THEN 'RCOM'
        WHEN 'FULL LINE' THEN 'FLS'
        WHEN 'N.CA' THEN 'NCA'
        WHEN 'FULL LINE CANADA' THEN 'FLS CANADA'
        WHEN 'RACK CANADA' THEN 'RACK CANADA'
        ELSE mua.arrived_channel END arrived_channel
    , trim(COALESCE(mua.marketing_type, 'BASE')) as marketing_type
    , mua.finance_rollup
    , mua.finance_detail
    , upper(COALESCE(CASE WHEN mua.finance_rollup IN ('BASE','UNATTRIBUTED') THEN 'UNKNOWN' ELSE ucl.funnel_type END,'UNKNOWN')) funnel_type
    , CASE WHEN mua.utm_channel LIKE '%_EX_%' THEN 'YES' ELSE 'NO' END AS nmn_flag
    , sum(mua.gross) as attributed_demand
    , sum(mua.net_sales) as attributed_pred_net
    , sum(mua.units) as attributed_units
    , sum(mua.orders) as attributed_orders
    , sum(mua.session_orders) as session_orders
	, sum(mua.session_demand) as session_demand
    , sum(mua.sessions) as sessions
    , sum(mua.bounced_sessions) as bounced_sessions
    from T2DL_DAS_MTA.mkt_utm_agg mua
    LEFT JOIN T2DL_DAS_MTA.utm_channel_lookup ucl
        ON ucl.utm_mkt_chnl =mua.utm_channel
where mua.activity_date_pacific between '2024-05-01' and '2024-05-27'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
;

COLLECT STATISTICS COLUMN (ACTIVITY_DATE_PACIFIC,CHANNELCOUNTRY, ARRIVED_CHANNEL ,FINANCE_DETAIL) ON mkt_utm_agg_optimized;


/*=======================================
    Affiliates Cost
    - nmn flag is no
    - banner join to arrived_channel
 =======================================*/
-- drop table aff_optimized;
--EXPLAIN
CREATE MULTISET VOLATILE TABLE aff_optimized AS (
    select
        stats_date aff_date
        , CASE aff.banner
            WHEN 'Nord' THEN 'NCOM'
            WHEN 'Rack' THEN 'RCOM'
            ELSE UPPER(aff.banner) END arrived_channel
        , country
        , 'LOW' as funnel_type
        , sum(mid_funnel_daily_cost) as mid_funnel_daily_cost
        , sum(low_funnel_daily_cost) as low_funnel_daily_cost
    from T2DL_DAS_FUNNEL_IO.AFFILIATES_COST_DAILY_FACT aff
    where stats_date between '2024-05-01' and '2024-05-27'
    group by 1, 2, 3, 4
)  with data
   primary index(aff_date, arrived_channel, country)
   on commit preserve rows
;

/*=======================================
    Paid Campaigns - MTA and Cost
 =======================================*/
-- DROP TABLE paid_camp_optimized;
-- EXPLAIN
CREATE MULTISET VOLATILE TABLE paid_camp_optimized AS (
    select
        activity_date_pacific
        , 'PAID' as marketing_type
        , CASE
	        WHEN finance_detail = 'PAID_SHOPPING' THEN 'SHOPPING' 
	        WHEN finance_detail in ('PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED') THEN 'PAID_SEARCH'
	        WHEN finance_detail = 'DISPLAY' THEN 'DISPLAY'
	        WHEN finance_detail = 'VIDEO' THEN 'PAID_OTHER'
	        WHEN finance_detail = 'SOCIAL_PAID' THEN 'SOCIAL'
            END as finance_rollup
        , CASE finance_detail WHEN 'PAID_SHOPPING' THEN 'SHOPPING' ELSE finance_detail
            END as finance_detail
        , CASE order_channel
            WHEN 'N.COM' THEN 'NCOM'
            WHEN 'R.COM' THEN 'RCOM'
            WHEN 'RACK.COM' THEN 'RCOM'
            WHEN 'FULL LINE' THEN 'FLS'
            WHEN 'N.CA' THEN 'NCA'
            WHEN 'FULL LINE CANADA' THEN 'FLS CANADA'
            WHEN 'RACK CANADA' THEN 'RACK CANADA'
        ELSE order_channel END as order_channel
        , CASE arrived_channel
            WHEN 'N.COM' THEN 'NCOM'
            WHEN 'R.COM' THEN 'RCOM'
            WHEN 'RACK.COM' THEN 'RCOM'
            WHEN 'FULL LINE' THEN 'FLS'
            WHEN 'N.CA' THEN 'NCA'
            WHEN 'FULL LINE CANADA' THEN 'FLS CANADA'
            WHEN 'RACK CANADA' THEN 'RACK CANADA'
        ELSE arrived_channel END arrived_channel
        , country
        , upper(funnel_type) funnel_type
        , upper(nmn_flag) nmn_flag
        , sum(cost) cost
    from T2DL_DAS_MTA.MTA_PAID_CAMPAIGN_DAILY
    where activity_date_pacific between '2024-05-01' and '2024-05-27'
        AND country IN ('US', 'CA')
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)  with data
   primary index(activity_date_pacific, order_channel, finance_detail)
   on commit preserve rows
;

COLLECT STATS COLUMN(country), COLUMN(nmn_flag) ON paid_camp_optimized;

/*=======================================
    MTA Finance Forecast
 =======================================*/
-- drop table mta_forecast_optimized;
CREATE MULTISET VOLATILE TABLE mta_forecast_optimized AS (
    SELECT
         fcst."date" as fcst_date
        , CASE box
            WHEN 'N.COM' THEN 'NCOM'
            WHEN 'R.COM' THEN 'RCOM'
            WHEN 'RACK.COM' THEN 'RCOM'
            WHEN 'FULL LINE' THEN 'FLS'
         ELSE box END as order_channel
        , CASE box
            when 'FLS' then 'NCOM'
            WHEN 'N.COM' THEN 'NCOM'
            WHEN 'R.COM' THEN 'RCOM'
            WHEN 'RACK.COM' THEN 'RCOM'
            WHEN 'FULL LINE' THEN 'NCOM'
        ELSE box END as  arrived_channel
        , trim(marketing_type) marketing_type
        , 'US' as  channelcountry
        -- forecast
        , sum(fcst."cost") as fcst_cost
        , sum(fcst.traffic_udv) as fcst_traffic
        , sum(fcst.orders) as fcst_attributed_orders
        , sum(fcst.gross_sales) as fcst_attributed_demand
        , sum(fcst.net_sales)  as fcst_attributed_pred_net
        , sum(fcst.sessions) as fcst_sessions
        , sum(fcst.session_orders) as fcst_session_orders
    FROM T2DL_DAS_MTA.FINANCE_FORECAST fcst
    WHERE fcst."date" between '2024-05-01' and '2024-05-27'
    GROUP BY 1, 2, 3, 4, 5
)  with data
   primary index(fcst_date, order_channel, marketing_type)
   on commit preserve rows
;

COLLECT STATS COLUMN(fcst_date), COLUMN(order_channel) ON mta_forecast_optimized;



/*=======================================
    ROW COUNT all sources
 =======================================*/
-- DROP TABLE mta_ssa_cost_fcst;
CREATE MULTISET VOLATILE TABLE mta_ssa_cost_fcst AS (
select
      COALESCE(mua.activity_date_pacific, cost.activity_date_pacific, aff.aff_date, fcst.fcst_date) activity_date_pacific
    , COALESCE(mua.order_channel, cost.order_channel, fcst.order_channel) as order_channel
    , COALESCE(mua.channelcountry, cost.country, aff.country, fcst.channelcountry) as channelcountry
    , COALESCE(mua.arrived_channel, cost.arrived_channel, aff.arrived_channel, fcst.arrived_channel) arrived_channel
    , COALESCE(mua.marketing_type, cost.marketing_type, fcst.marketing_type) marketing_type
    , COALESCE(mua.finance_rollup, cost.finance_rollup) finance_rollup
    , COALESCE(mua.finance_detail, cost.finance_detail) finance_detail
    , COALESCE(mua.funnel_type, cost.funnel_type, 'UNKNOWN') as funnel_type
    , COALESCE(mua.nmn_flag, cost.nmn_flag, 'UNKNOWN') as nmn_flag
    , mua.attributed_demand
    , mua.attributed_pred_net
    , mua.attributed_units
    , mua.attributed_orders
    , mua.session_orders as session_orders
	, mua.session_demand as session_demand
    , mua.sessions as sessions
    , mua.bounced_sessions as bounced_sessions
    , cost.cost as paid_campaign_cost
    , aff.low_funnel_daily_cost as affiliates_cost
    -- forecast
    , fcst.fcst_cost
    , fcst.fcst_traffic
    , fcst.fcst_attributed_orders
    , fcst.fcst_attributed_demand
    , fcst.fcst_attributed_pred_net
    , fcst.fcst_sessions
    , fcst.fcst_session_orders
    -- row numbers
    , ROW_NUMBER() OVER(PARTITION BY
        mua.finance_detail
        , mua.order_channel
        , mua.arrived_channel
        , mua.channelcountry
        , mua.activity_date_pacific
        , mua.nmn_flag
        , mua.funnel_type
        , mua.marketing_type
        , mua.finance_rollup
        order by mua.order_channel ASC
        ) AS mua_row_num
    , ROW_NUMBER() OVER(PARTITION BY
        cost.finance_detail
        , cost.arrived_channel
        , cost.order_channel
        , cost.country
        , cost.activity_date_pacific
        , cost.nmn_flag
        , cost.funnel_type
        order by cost.arrived_channel ASC
        ) AS cost_row_num
    , ROW_NUMBER() OVER(PARTITION BY
         aff.arrived_channel
        , aff.country
        , aff.aff_date
        order by aff.arrived_channel ASC
        ) AS aff_row_num
    , ROW_NUMBER() OVER(PARTITION BY
        fcst.order_channel
        , fcst.arrived_channel
        , fcst.channelcountry
        , fcst.marketing_type
        , fcst.fcst_date
        order by fcst.order_channel ASC
        ) AS fcst_row_num
    from mkt_utm_agg_optimized mua
    -- PAID CAMPAIGN COST
    full outer join paid_camp_optimized cost
        on mua.finance_detail = cost.finance_detail
        and mua.finance_rollup = cost.finance_rollup
        and mua.arrived_channel = cost.arrived_channel
        and mua.order_channel = cost.order_channel
        and mua.channelcountry = cost.country
        and mua.activity_date_pacific = cost.activity_date_pacific
        and mua.nmn_flag = cost.nmn_flag
        and mua.marketing_type = cost.marketing_type
        and mua.funnel_type = cost.funnel_type
--         and mua.arrived_channel = mua.order_channel
    -- AFFILIATES COST
    full outer join aff_optimized aff
        on mua.arrived_channel = aff.arrived_channel
        and mua.channelcountry = aff.country
        and mua.activity_date_pacific = aff.aff_date
        and mua.finance_detail = 'AFFILIATES'
        and mua.nmn_flag = 'No'
        and mua.funnel_type = 'LOW'
        and mua.funnel_type = mua.funnel_type
        and mua.arrived_channel = mua.order_channel
    -- FORECAST
    full outer join mta_forecast_optimized fcst
        on mua.order_channel = fcst.order_channel
        and mua.arrived_channel = fcst.arrived_channel
        and mua.marketing_type = fcst.marketing_type
        and mua.activity_date_pacific = fcst.fcst_date
        and mua.channelcountry = fcst.channelcountry
        and mua.channelcountry = 'US'
)  with data
   primary index(activity_date_pacific, order_channel, finance_detail)
   on commit preserve rows
;

COLLECT STATS COLUMN(activity_date_pacific), COLUMN(order_channel) ON mta_ssa_cost_fcst;

/*=======================================
    DELETE any records from run dates
 =======================================*/
delete from T2DL_DAS_MTA.MTA_SSA_COST_AGG
where activity_date_pacific between '2024-05-01' and '2024-05-27';

/*=======================================
    INSERT to final table
 =======================================*/
insert into T2DL_DAS_MTA.MTA_SSA_COST_AGG
-- EXPLAIN
select
    activity_date_pacific
    , order_channel
    , channelcountry
    , arrived_channel
    , marketing_type
    , finance_rollup
    , finance_detail
    , funnel_type
    , nmn_flag
     -- mta
    , CASE WHEN mua_row_num = 1 THEN attributed_demand ELSE NULL END AS attributed_demand
    , CASE WHEN mua_row_num = 1 THEN attributed_pred_net ELSE NULL END AS attributed_pred_net
    , CASE WHEN mua_row_num = 1 THEN attributed_units ELSE NULL END AS attributed_units
    , CASE WHEN mua_row_num = 1 THEN attributed_orders ELSE NULL END AS attributed_orders
     -- ssa
    , CASE WHEN mua_row_num = 1 THEN session_orders ELSE NULL END AS session_orders
	, CASE WHEN mua_row_num = 1 THEN session_demand ELSE NULL END AS session_demand
    , CASE WHEN mua_row_num = 1 THEN sessions ELSE NULL END AS sessions
    , CASE WHEN mua_row_num = 1 THEN bounced_sessions ELSE NULL END AS bounced_sessions
     -- cost
    , CASE WHEN cost_row_num = 1 THEN paid_campaign_cost ELSE NULL END AS paid_campaign_cost
    , CASE WHEN aff_row_num = 1 THEN affiliates_cost ELSE NULL END AS affiliates_cost
    , CASE WHEN cost_row_num = 1 OR aff_row_num = 1 THEN coalesce(paid_campaign_cost, affiliates_cost) ELSE NULL END AS cost
     -- forecast
    , CASE WHEN fcst_row_num = 1 THEN fcst_cost ELSE NULL END AS fcst_cost
    , CASE WHEN fcst_row_num = 1 THEN fcst_traffic ELSE NULL END AS fcst_traffic
    , CASE WHEN fcst_row_num = 1 THEN fcst_attributed_orders ELSE NULL END AS fcst_attributed_orders
    , CASE WHEN fcst_row_num = 1 THEN fcst_attributed_demand ELSE NULL END AS fcst_attributed_demand
    , CASE WHEN fcst_row_num = 1 THEN fcst_attributed_pred_net ELSE NULL END AS fcst_attributed_pred_net
    , CASE WHEN fcst_row_num = 1 THEN fcst_sessions ELSE NULL END AS fcst_sessions
    , CASE WHEN fcst_row_num = 1 THEN fcst_session_orders ELSE NULL END AS fcst_session_orders
    , current_Timestamp(6) as dw_sys_load_tmstp
from mta_ssa_cost_fcst
;

collect statistics column(partition) on T2DL_DAS_MTA.MTA_SSA_COST_AGG;

SET QUERY_BAND = NONE FOR SESSION;





