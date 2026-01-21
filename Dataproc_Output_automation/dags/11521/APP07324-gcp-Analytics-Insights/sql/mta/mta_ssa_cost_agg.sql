BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08047;
DAG_ID=mta_ssa_cost_11521_ACE_ENG;
---     Task_Name=mta_ssa_cost_agg;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS mkt_utm_agg_optimized (
activity_date_pacific DATE,
order_channel STRING,
channelcountry STRING,
arrived_channel STRING,
marketing_type STRING,
finance_rollup STRING,
finance_detail STRING,
funnel_type STRING,
nmn_flag STRING,
attributed_demand FLOAT64,
attributed_pred_net FLOAT64,
attributed_units FLOAT64,
attributed_orders FLOAT64,
session_orders FLOAT64,
session_demand FLOAT64,
sessions FLOAT64,
bounced_sessions FLOAT64
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;






INSERT INTO mkt_utm_agg_optimized
(SELECT mua.activity_date_pacific,
   CASE
   WHEN LOWER(mua.channel) = LOWER('N.COM')
   THEN 'NCOM'
   WHEN LOWER(mua.channel) = LOWER('R.COM')
   THEN 'RCOM'
   WHEN LOWER(mua.channel) = LOWER('RACK.COM')
   THEN 'RCOM'
   WHEN LOWER(mua.channel) = LOWER('FULL LINE')
   THEN 'FLS'
   WHEN LOWER(mua.channel) = LOWER('N.CA')
   THEN 'NCA'
   WHEN LOWER(mua.channel) = LOWER('FULL LINE CANADA')
   THEN 'FLS CANADA'
   WHEN LOWER(mua.channel) = LOWER('RACK CANADA')
   THEN 'RACK CANADA'
   ELSE mua.channel
   END AS order_channel,
  mua.channelcountry,
   CASE
   WHEN LOWER(mua.arrived_channel) = LOWER('N.COM')
   THEN 'NCOM'
   WHEN LOWER(mua.arrived_channel) = LOWER('R.COM')
   THEN 'RCOM'
   WHEN LOWER(mua.arrived_channel) = LOWER('RACK.COM')
   THEN 'RCOM'
   WHEN LOWER(mua.arrived_channel) = LOWER('FULL LINE')
   THEN 'FLS'
   WHEN LOWER(mua.arrived_channel) = LOWER('N.CA')
   THEN 'NCA'
   WHEN LOWER(mua.arrived_channel) = LOWER('FULL LINE CANADA')
   THEN 'FLS CANADA'
   WHEN LOWER(mua.arrived_channel) = LOWER('RACK CANADA')
   THEN 'RACK CANADA'
   ELSE mua.arrived_channel
   END AS arrived_channel,
  TRIM(COALESCE(mua.marketing_type, 'BASE')) AS marketing_type,
  mua.finance_rollup,
  mua.finance_detail,
  UPPER(COALESCE(CASE
     WHEN LOWER(mua.finance_rollup) IN (LOWER('BASE'), LOWER('UNATTRIBUTED'))
     THEN 'UNKNOWN'
     ELSE ucl.funnel_type
     END, 'UNKNOWN')) AS funnel_type,
   CASE
   WHEN LOWER(mua.utm_channel) LIKE LOWER('%_EX_%')
   THEN 'YES'
   ELSE 'NO'
   END AS nmn_flag,
  SUM(mua.gross) AS attributed_demand,
  SUM(mua.net_sales) AS attributed_pred_net,
  SUM(mua.units) AS attributed_units,
  SUM(mua.orders) AS attributed_orders,
  SUM(mua.session_orders) AS session_orders,
  SUM(mua.session_demand) AS session_demand,
  SUM(mua.sessions) AS sessions,
  SUM(mua.bounced_sessions) AS bounced_sessions
 FROM `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mkt_utm_agg AS mua
  LEFT JOIN t2dl_das_mta.utm_channel_lookup AS ucl ON LOWER(ucl.utm_mkt_chnl) = LOWER(mua.utm_channel)
 WHERE mua.activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 GROUP BY mua.activity_date_pacific,
  order_channel,
  mua.channelcountry,
  arrived_channel,
  marketing_type,
  mua.finance_rollup,
  mua.finance_detail,
  funnel_type,
  nmn_flag);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (ACTIVITY_DATE_PACIFIC,CHANNELCOUNTRY, ARRIVED_CHANNEL ,FINANCE_DETAIL) ON mkt_utm_agg_optimized;
BEGIN
SET _ERROR_CODE  =  0;








CREATE TEMPORARY TABLE IF NOT EXISTS aff_optimized
AS
SELECT stats_date AS aff_date,
  CASE
  WHEN LOWER(banner) = LOWER('Nord')
  THEN 'NCOM'
  WHEN LOWER(banner) = LOWER('Rack')
  THEN 'RCOM'
  ELSE UPPER(banner)
  END AS arrived_channel,
 country,
 'LOW' AS funnel_type,
 SUM(mid_funnel_daily_cost) AS mid_funnel_daily_cost,
 SUM(low_funnel_daily_cost) AS low_funnel_daily_cost
FROM t2dl_das_funnel_io.affiliates_cost_daily_fact AS aff
WHERE stats_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY aff_date,
 arrived_channel,
 country,
 funnel_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS paid_camp_optimized
AS
SELECT activity_date_pacific,
 'PAID' AS marketing_type,
  CASE
  WHEN LOWER(finance_detail) = LOWER('PAID_SHOPPING')
  THEN 'SHOPPING'
  WHEN LOWER(finance_detail) IN (LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED'))
  THEN 'PAID_SEARCH'
  WHEN LOWER(finance_detail) = LOWER('DISPLAY')
  THEN 'DISPLAY'
  WHEN LOWER(finance_detail) = LOWER('VIDEO')
  THEN 'PAID_OTHER'
  WHEN LOWER(finance_detail) = LOWER('SOCIAL_PAID')
  THEN 'SOCIAL'
  ELSE NULL
  END AS finance_rollup,
  CASE
  WHEN LOWER(finance_detail) = LOWER('PAID_SHOPPING')
  THEN 'SHOPPING'
  ELSE finance_detail
  END AS finance_detail,
  CASE
  WHEN LOWER(order_channel) = LOWER('N.COM')
  THEN 'NCOM'
  WHEN LOWER(order_channel) = LOWER('R.COM')
  THEN 'RCOM'
  WHEN LOWER(order_channel) = LOWER('RACK.COM')
  THEN 'RCOM'
  WHEN LOWER(order_channel) = LOWER('FULL LINE')
  THEN 'FLS'
  WHEN LOWER(order_channel) = LOWER('N.CA')
  THEN 'NCA'
  WHEN LOWER(order_channel) = LOWER('FULL LINE CANADA')
  THEN 'FLS CANADA'
  WHEN LOWER(order_channel) = LOWER('RACK CANADA')
  THEN 'RACK CANADA'
  ELSE order_channel
  END AS order_channel,
  CASE
  WHEN LOWER(arrived_channel) = LOWER('N.COM')
  THEN 'NCOM'
  WHEN LOWER(arrived_channel) = LOWER('R.COM')
  THEN 'RCOM'
  WHEN LOWER(arrived_channel) = LOWER('RACK.COM')
  THEN 'RCOM'
  WHEN LOWER(arrived_channel) = LOWER('FULL LINE')
  THEN 'FLS'
  WHEN LOWER(arrived_channel) = LOWER('N.CA')
  THEN 'NCA'
  WHEN LOWER(arrived_channel) = LOWER('FULL LINE CANADA')
  THEN 'FLS CANADA'
  WHEN LOWER(arrived_channel) = LOWER('RACK CANADA')
  THEN 'RACK CANADA'
  ELSE arrived_channel
  END AS arrived_channel,
 country,
 UPPER(funnel_type) AS funnel_type,
 UPPER(nmn_flag) AS nmn_flag,
 SUM(cost) AS cost
FROM t2dl_das_mta.mta_paid_campaign_daily
WHERE activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(country) IN (LOWER('US'), LOWER('CA'))
GROUP BY activity_date_pacific,
 marketing_type,
 finance_rollup,
 finance_detail,
 order_channel,
 arrived_channel,
 country,
 funnel_type,
 nmn_flag;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS COLUMN(country), COLUMN(nmn_flag) ON paid_camp_optimized;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS mta_forecast_optimized
AS
SELECT date AS fcst_date,
  CASE
  WHEN LOWER(box) = LOWER('N.COM')
  THEN 'NCOM'
  WHEN LOWER(box) = LOWER('R.COM')
  THEN 'RCOM'
  WHEN LOWER(box) = LOWER('RACK.COM')
  THEN 'RCOM'
  WHEN LOWER(box) = LOWER('FULL LINE')
  THEN 'FLS'
  ELSE box
  END AS order_channel,
  CASE
  WHEN LOWER(box) = LOWER('FLS')
  THEN 'NCOM'
  WHEN LOWER(box) = LOWER('N.COM')
  THEN 'NCOM'
  WHEN LOWER(box) = LOWER('R.COM')
  THEN 'RCOM'
  WHEN LOWER(box) = LOWER('RACK.COM')
  THEN 'RCOM'
  WHEN LOWER(box) = LOWER('FULL LINE')
  THEN 'NCOM'
  ELSE box
  END AS arrived_channel,
 TRIM(marketing_type) AS marketing_type,
 'US' AS channelcountry,
 SUM(cost) AS fcst_cost,
 SUM(traffic_udv) AS fcst_traffic,
 SUM(orders) AS fcst_attributed_orders,
 SUM(gross_sales) AS fcst_attributed_demand,
 SUM(net_sales) AS fcst_attributed_pred_net,
 SUM(sessions) AS fcst_sessions,
 SUM(session_orders) AS fcst_session_orders
FROM t2dl_das_mta.finance_forecast AS fcst
WHERE date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY fcst_date,
 order_channel,
 arrived_channel,
 marketing_type,
 channelcountry;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS COLUMN(fcst_date), COLUMN(order_channel) ON mta_forecast_optimized;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS mta_ssa_cost_fcst
AS
SELECT COALESCE(mua.activity_date_pacific, cost.activity_date_pacific, aff.aff_date, fcst.fcst_date) AS
 activity_date_pacific,
 COALESCE(mua.order_channel, cost.order_channel, fcst.order_channel) AS order_channel,
 COALESCE(mua.channelcountry, cost.country, aff.country, fcst.channelcountry) AS channelcountry,
 COALESCE(mua.arrived_channel, cost.arrived_channel, aff.arrived_channel, fcst.arrived_channel) AS arrived_channel,
 COALESCE(mua.marketing_type, cost.marketing_type, fcst.marketing_type) AS marketing_type,
 COALESCE(mua.finance_rollup, cost.finance_rollup) AS finance_rollup,
 COALESCE(mua.finance_detail, cost.finance_detail) AS finance_detail,
 COALESCE(mua.funnel_type, cost.funnel_type, 'UNKNOWN') AS funnel_type,
 COALESCE(mua.nmn_flag, cost.nmn_flag, 'UNKNOWN') AS nmn_flag,
 mua.attributed_demand,
 mua.attributed_pred_net,
 mua.attributed_units,
 mua.attributed_orders,
 mua.session_orders,
 mua.session_demand,
 mua.sessions,
 mua.bounced_sessions,
 cost.cost AS paid_campaign_cost,
 aff.low_funnel_daily_cost AS affiliates_cost,
 fcst.fcst_cost,
 fcst.fcst_traffic,
 fcst.fcst_attributed_orders,
 fcst.fcst_attributed_demand,
 fcst.fcst_attributed_pred_net,
 fcst.fcst_sessions,
 fcst.fcst_session_orders,
 ROW_NUMBER() OVER (PARTITION BY mua.finance_detail, mua.order_channel, mua.arrived_channel, mua.channelcountry, mua.activity_date_pacific
    , mua.nmn_flag, mua.funnel_type, mua.marketing_type, mua.finance_rollup ORDER BY mua.order_channel) AS mua_row_num,
 ROW_NUMBER() OVER (PARTITION BY cost.finance_detail, cost.arrived_channel, cost.order_channel, cost.country, cost.activity_date_pacific
    , cost.nmn_flag, cost.funnel_type ORDER BY cost.arrived_channel) AS cost_row_num,
 ROW_NUMBER() OVER (PARTITION BY aff.arrived_channel, aff.country, aff.aff_date ORDER BY aff.arrived_channel) AS
 aff_row_num,
 ROW_NUMBER() OVER (PARTITION BY fcst.order_channel, fcst.arrived_channel, fcst.channelcountry, fcst.marketing_type,
    fcst.fcst_date ORDER BY fcst.order_channel) AS fcst_row_num
FROM mkt_utm_agg_optimized AS mua
 FULL JOIN paid_camp_optimized AS cost ON LOWER(mua.finance_detail) = LOWER(cost.finance_detail) AND LOWER(mua.finance_rollup
           ) = LOWER(cost.finance_rollup) AND LOWER(mua.arrived_channel) = LOWER(cost.arrived_channel) AND LOWER(mua.order_channel
         ) = LOWER(cost.order_channel) AND LOWER(mua.channelcountry) = LOWER(cost.country) AND mua.activity_date_pacific
       = cost.activity_date_pacific AND LOWER(mua.nmn_flag) = LOWER(cost.nmn_flag) AND LOWER(mua.marketing_type) = LOWER(cost
     .marketing_type) AND LOWER(mua.funnel_type) = LOWER(cost.funnel_type)
 FULL JOIN aff_optimized AS aff ON LOWER(mua.arrived_channel) = LOWER(aff.arrived_channel) AND LOWER(mua.channelcountry
          ) = LOWER(aff.country) AND mua.activity_date_pacific = aff.aff_date AND LOWER(mua.finance_detail) = LOWER('AFFILIATES'
        ) AND LOWER(mua.nmn_flag) = LOWER('No') AND LOWER(mua.funnel_type) = LOWER('LOW') AND LOWER(mua.arrived_channel
     ) = LOWER(mua.order_channel) AND LOWER(mua.funnel_type) IS NOT NULL
 FULL JOIN mta_forecast_optimized AS fcst ON LOWER(mua.order_channel) = LOWER(fcst.order_channel) AND LOWER(mua.arrived_channel
        ) = LOWER(fcst.arrived_channel) AND LOWER(mua.marketing_type) = LOWER(fcst.marketing_type) AND mua.activity_date_pacific
      = fcst.fcst_date AND LOWER(mua.channelcountry) = LOWER(fcst.channelcountry) AND LOWER(mua.channelcountry) = LOWER('US'
    );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS COLUMN(activity_date_pacific), COLUMN(order_channel) ON mta_ssa_cost_fcst;
BEGIN
SET _ERROR_CODE  =  0;




DELETE FROM `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_ssa_cost_agg
WHERE activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;





INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_ssa_cost_agg
(SELECT activity_date_pacific,
  order_channel,
  SUBSTR(channelcountry, 1, 2) AS channelcountry,
  arrived_channel,
  marketing_type,
  finance_rollup,
  finance_detail,
  funnel_type,
  SUBSTR(nmn_flag, 1, 3) AS nmn_flag,
   CASE
   WHEN mua_row_num = 1
   THEN attributed_demand
   ELSE NULL
   END AS attributed_demand,
   CASE
   WHEN mua_row_num = 1
   THEN attributed_pred_net
   ELSE NULL
   END AS attributed_pred_net,
   CASE
   WHEN mua_row_num = 1
   THEN attributed_units
   ELSE NULL
   END AS attributed_units,
   CASE
   WHEN mua_row_num = 1
   THEN attributed_orders
   ELSE NULL
   END AS attributed_orders,
   CASE
   WHEN mua_row_num = 1
   THEN session_orders
   ELSE NULL
   END AS session_orders,
   CASE
   WHEN mua_row_num = 1
   THEN session_demand
   ELSE NULL
   END AS session_demand,
   CASE
   WHEN mua_row_num = 1
   THEN sessions
   ELSE NULL
   END AS sessions,
   CASE
   WHEN mua_row_num = 1
   THEN bounced_sessions
   ELSE NULL
   END AS bounced_sessions,
  CAST(CASE
    WHEN cost_row_num = 1
    THEN paid_campaign_cost
    ELSE NULL
    END AS FLOAT64) AS paid_campaign_cost,
   CASE
   WHEN aff_row_num = 1
   THEN affiliates_cost
   ELSE NULL
   END AS affiliates_cost,
  CAST(CASE
    WHEN cost_row_num = 1 OR aff_row_num = 1
    THEN COALESCE(paid_campaign_cost, affiliates_cost)
    ELSE NULL
    END AS FLOAT64) AS cost,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_cost
    ELSE NULL
    END AS FLOAT64) AS fcst_cost,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_traffic
    ELSE NULL
    END AS FLOAT64) AS fcst_traffic,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_attributed_orders
    ELSE NULL
    END AS FLOAT64) AS fcst_attributed_orders,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_attributed_demand
    ELSE NULL
    END AS FLOAT64) AS fcst_attributed_demand,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_attributed_pred_net
    ELSE NULL
    END AS FLOAT64) AS fcst_attributed_pred_net,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_sessions
    ELSE NULL
    END AS FLOAT64) AS fcst_sessions,
  CAST(CASE
    WHEN fcst_row_num = 1
    THEN fcst_session_orders
    ELSE NULL
    END AS FLOAT64) AS fcst_session_orders,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM mta_ssa_cost_fcst);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column(partition) on t2dl_das_mta.MTA_SSA_COST_AGG;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;