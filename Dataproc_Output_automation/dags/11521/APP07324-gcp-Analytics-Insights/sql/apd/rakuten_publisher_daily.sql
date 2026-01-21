-- SET QUERY_BAND = 'App_ID=APP08629;
--      DAG_ID=rakuten_publisher_daily_11521_ACE_ENG;
--      Task_Name=rakuten_publisher_daily;'
--      FOR SESSION VOLATILE;
--T2/Table Name: {apd_t2_schema}.rakuten_publisher_daily
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2023-09-27
--Note:
-- This table supports the 4 BOX Affiliates Performance Dashboard-Consolidated Version.
CREATE TEMPORARY TABLE IF NOT EXISTS start_varibale 
AS
SELECT MIN(day_date) AS start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.start_date}});


CREATE TEMPORARY TABLE IF NOT EXISTS end_varibale
AS
SELECT MAX(day_date) AS end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.end_date}});


CREATE TEMPORARY TABLE IF NOT EXISTS _variables2
AS SELECT * FROM start_varibale
 INNER JOIN end_varibale ON TRUE;


CREATE TEMPORARY TABLE IF NOT EXISTS apd_4_box_mta_temp 
AS
SELECT mta_publisher_daily.day_dt,
 mta_publisher_daily.utm_campaign,
 mta_publisher_daily.publisher,
 mta_publisher_daily.publisher_group,
 mta_publisher_daily.publisher_subgroup,
 mta_publisher_daily.banner,
 SUM(mta_publisher_daily.attributed_demand) AS attributed_demand,
 SUM(mta_publisher_daily.attributed_orders) AS attributed_orders,
 SUM(mta_publisher_daily.attributed_net_sales) AS attributed_net_sales,
 SUM(mta_publisher_daily.acquired_attributed_orders) AS acquired_attributed_orders,
 SUM(mta_publisher_daily.retained_attributed_orders) AS retained_attributed_orders,
 SUM(mta_publisher_daily.acquired_attributed_demand) AS acquired_attributed_demand
FROM `{{params.gcp_project_id}}`.{{params.apd_t2_schema}}.mta_publisher_daily
 INNER JOIN `_variables2` ON TRUE
WHERE mta_publisher_daily.day_dt
 between `_variables2`.start_date
 AND `_variables2`.end_date
GROUP BY mta_publisher_daily.day_dt,
 mta_publisher_daily.utm_campaign,
 mta_publisher_daily.publisher,
 mta_publisher_daily.publisher_group,
 mta_publisher_daily.publisher_subgroup,
 mta_publisher_daily.banner;


CREATE TEMPORARY TABLE IF NOT EXISTS aff_sessions
AS
SELECT mta_utm_session_agg.activity_date_pacific AS day_dt,
  CASE
  WHEN LOWER(mta_utm_session_agg.channel) = LOWER('NORDSTROM') THEN 'NORDSTROM'
  WHEN LOWER(mta_utm_session_agg.channel) = LOWER('NORDSTROM_RACK') THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
 UPPER(mta_utm_session_agg.utm_campaign) AS utm_campaign,
 SUM(CAST(mta_utm_session_agg.total_sessions AS NUMERIC)) AS sessions
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mta_utm_session_agg
 INNER JOIN `_variables2` ON mta_utm_session_agg.activity_date_pacific BETWEEN `_variables2`.start_date AND `_variables2`
  .end_date
WHERE LOWER(mta_utm_session_agg.channel) IN (LOWER('NORDSTROM'), LOWER('NORDSTROM_RACK'))
 AND LOWER(mta_utm_session_agg.channelcountry) = LOWER('US')
 AND LOWER(mta_utm_session_agg.finance_rollup) = LOWER('AFFILIATES')
 AND LOWER(mta_utm_session_agg.session_type) = LOWER('ARRIVED')
GROUP BY day_dt,
 banner,
 utm_campaign;


CREATE TEMPORARY TABLE IF NOT EXISTS daily_cost_split 
AS
SELECT CASE
  WHEN LOWER(banner) = LOWER('Rack') THEN 'NORDSTROM RACK'
  ELSE 'NORDSTROM'
  END AS banner,
 UPPER(encrypted_id) AS utm_campaign,
 start_day_date,
 end_day_date,
  CASE WHEN end_day_date <> start_day_date THEN mf_total_spend / DATE_DIFF(end_day_date, start_day_date, DAY)
  ELSE mf_total_spend END AS daily_cost,
  CASE WHEN end_day_date <> start_day_date THEN lf_paid_placement / DATE_DIFF(end_day_date, start_day_date, DAY)
  ELSE lf_paid_placement END AS paid_placement
FROM `{{params.gcp_project_id}}`.t2dl_das_funnel_io.affiliates_campaign_cost AS tl
WHERE LOWER(country) = LOWER('US');


CREATE TEMPORARY TABLE IF NOT EXISTS paid_placement 
 AS
SELECT cal.day_date AS day_dt,
 splt.banner,
 splt.utm_campaign,
 SUM(splt.daily_cost) AS daily_cost,
 SUM(splt.paid_placement) AS paid_placement
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS cal
 INNER JOIN daily_cost_split AS splt ON TRUE
 INNER JOIN `_variables2` ON TRUE
WHERE cal.day_date
 BETWEEN splt.start_day_date
 AND splt.end_day_date 
 and cal.day_date
 BETWEEN `_variables2`.start_date
 AND `_variables2`.end_date
GROUP BY day_dt,
 splt.banner,
 splt.utm_campaign;




CREATE TEMPORARY TABLE IF NOT EXISTS rakuten_tbl 
AS
SELECT r.stats_date AS day_dt,
 UPPER(r.platform_id) AS utm_campaign,
 'NORDSTROM RACK' AS banner,
 aff.return_rate AS aff_return_rate,
 COALESCE(SUM(r.clicks), 0) AS clicks,
 COALESCE(SUM(r.gross_commissions), 0) AS rakuten_gross_commissions,
 COALESCE(SUM(r.estimated_net_total_cost), 0) AS estimated_net_total_cost,
 COALESCE(SUM(r.gross_sales), 0) AS rakuten_gross_sales,
 COALESCE(SUM(r.sales), 0) AS rakuten_net_sales
FROM `{{params.gcp_project_id}}`.t2dl_das_funnel_io.rack_funnel_cost_fact AS r
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_funnel_io.affiliates_return_rate AS aff ON TRUE
 INNER JOIN `_variables2` ON TRUE
WHERE r.stats_date
 BETWEEN `_variables2`.start_date
 AND `_variables2`.end_date
 and stats_date 
 BETWEEN aff.start_day_date
 AND aff.end_day_date
 AND LOWER(r.currency) = LOWER('USD')
 AND LOWER(r.sourcetype) LIKE LOWER('rakuten%')
 AND LOWER(r.file_name) = LOWER('rack_rakuten')
GROUP BY day_dt,
 utm_campaign,
 banner,
 aff_return_rate
UNION ALL
SELECT s.stats_date AS day_dt,
 UPPER(s.platform_id) AS utm_campaign,
 'NORDSTROM' AS banner,
 aff_1.return_rate AS aff_return_rate,
 COALESCE(SUM(s.clicks), 0) AS clicks,
 COALESCE(SUM(s.gross_commissions), 0) AS rakuten_gross_commissions,
 COALESCE(SUM(s.estimated_net_total_cost), 0) AS estimated_net_total_cost,
 COALESCE(SUM(s.gross_sales), 0) AS rakuten_gross_sales,
 COALESCE(SUM(s.sales), 0) AS rakuten_net_sales
FROM `{{params.gcp_project_id}}`.t2dl_das_funnel_io.fp_funnel_cost_fact AS s
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_funnel_io.affiliates_return_rate AS aff_1 ON TRUE
 INNER JOIN `_variables2` AS `_variables20` ON TRUE
WHERE s.stats_date
 BETWEEN `_variables20`.start_date
 AND `_variables20`.end_date
 and stats_date
 BETWEEN aff_1.start_day_date
 AND aff_1.end_day_date
 AND LOWER(s.currency) = LOWER('USD')
 AND LOWER(s.sourcetype) LIKE LOWER('rakuten%')
 AND LOWER(s.file_name) = LOWER('fp_rakuten')
GROUP BY day_dt,
 utm_campaign,
 banner,
 aff_return_rate;


CREATE TEMPORARY TABLE IF NOT EXISTS rakuten_tbl_2
AS
SELECT COALESCE(a.day_dt, aff_sessions.day_dt, d.day_dt, rakuten_tbl.day_dt) AS day_dt,
 COALESCE(a.utm_campaign, aff_sessions.utm_campaign, d.utm_campaign, rakuten_tbl.utm_campaign) AS utm_campaign,
 COALESCE(a.publisher, g.publisher_name, 'Unknown') AS publisher,
 COALESCE(a.publisher_group, g.publisher_group, 'Unknown') AS publisher_group,
 COALESCE(a.publisher_subgroup, g.publisher_subgroup, 'Unknown') AS publisher_subgroup,
 COALESCE(a.banner, aff_sessions.banner, d.banner, rakuten_tbl.banner) AS banner,
 COALESCE(rakuten_tbl.aff_return_rate, 0) AS aff_return_rate,
 SUM(a.attributed_demand) AS attributed_demand,
 SUM(a.attributed_net_sales) AS attributed_net_sales,
 SUM(a.attributed_orders) AS attributed_orders,
 SUM(a.acquired_attributed_orders) AS acquired_attributed_orders,
 SUM(a.retained_attributed_orders) AS retained_attributed_orders,
 SUM(a.acquired_attributed_demand) AS acquired_attributed_demand,
 COALESCE(SUM(rakuten_tbl.clicks), 0) AS clicks,
 COALESCE(SUM(aff_sessions.sessions), 0) AS sessions,
 COALESCE(SUM(d.paid_placement ), 0) AS paid_placement,
 COALESCE(SUM(d.daily_cost), 0) AS daily_cost,
 COALESCE(SUM(rakuten_tbl.rakuten_gross_commissions), 0) AS rakuten_gross_commissions,
 COALESCE(SUM(rakuten_tbl.estimated_net_total_cost), 0) AS estimated_net_total_cost,
 COALESCE(SUM(rakuten_tbl.rakuten_gross_sales), 0) AS rakuten_gross_sales,
 COALESCE(SUM(CASE
    WHEN a.day_dt = rakuten_tbl.day_dt AND LOWER(a.utm_campaign) = LOWER(rakuten_tbl.utm_campaign) AND LOWER(a.banner) =
      LOWER(rakuten_tbl.banner)
    THEN a.attributed_orders
    ELSE 0
    END), 0) AS rakuten_orders,
 COALESCE(SUM(rakuten_tbl.rakuten_net_sales), 0) AS rakuten_net_sales,
 COALESCE(SUM(a.acquired_attributed_orders), 0) AS rakuten_acquired_attributed_orders,
 COALESCE(SUM(a.acquired_attributed_demand), 0) AS rakuten_acquired_attributed_demand
FROM apd_4_box_mta_temp AS a
 FULL JOIN aff_sessions ON a.day_dt = aff_sessions.day_dt AND LOWER(a.utm_campaign) = LOWER(aff_sessions.utm_campaign)
  AND LOWER(a.banner) = LOWER(aff_sessions.banner)
 FULL JOIN (SELECT paid_placement.day_dt,
  paid_placement.utm_campaign ,
   paid_placement.banner,
   paid_placement.paid_placement,
   paid_placement.daily_cost
  FROM paid_placement AS paid_placement
   INNER JOIN `_variables2` ON TRUE
  WHERE paid_placement.day_dt <= `_variables2`.end_date) AS d ON a.day_dt = d.day_dt AND 
  LOWER(a.utm_campaign) = LOWER(d
     .utm_campaign) AND LOWER(a.banner) = LOWER(d.banner)
 FULL JOIN rakuten_tbl ON a.day_dt = rakuten_tbl.day_dt AND LOWER(a.utm_campaign) = LOWER(rakuten_tbl.utm_campaign) AND
   LOWER(a.banner) = LOWER(rakuten_tbl.banner)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_funnel_io.affiliate_publisher_mapping AS g ON LOWER(COALESCE(aff_sessions.utm_campaign, d.utm_campaign
    , rakuten_tbl.utm_campaign)) = LOWER(g.encrypted_id)
GROUP BY day_dt,
 utm_campaign,
 publisher,
 publisher_group,
 publisher_subgroup,
 banner,
 aff_return_rate;


CREATE TEMPORARY TABLE IF NOT EXISTS apd_4_box_rakuten
AS
SELECT day_dt,
 banner,
 utm_campaign,
 publisher,
 publisher_group,
 publisher_subgroup,
 MAX(aff_return_rate) AS aff_return_rate,
 SUM(attributed_demand) AS attributed_demand,
 SUM(attributed_orders) AS attributed_orders,
 SUM(attributed_net_sales) AS attributed_net_sales,
 SUM(acquired_attributed_demand) AS acquired_attributed_demand,
 SUM(acquired_attributed_orders) AS acquired_attributed_orders,
 SUM(retained_attributed_orders) AS retained_attributed_orders,
 SUM(clicks) AS clicks,
 SUM(sessions) AS sessions,
 SUM(paid_placement) AS paid_placement,
 SUM(daily_cost) AS daily_cost,
 SUM(rakuten_gross_commissions) AS rakuten_gross_commissions,
 SUM(estimated_net_total_cost) AS estimated_net_total_cost,
 SUM(rakuten_gross_sales) AS rakuten_gross_sales,
 SUM(rakuten_orders) AS rakuten_orders,
 SUM(rakuten_net_sales) AS rakuten_net_sales,
 SUM(rakuten_acquired_attributed_orders) AS rakuten_acquired_attributed_orders,
 SUM(rakuten_acquired_attributed_demand) AS rakuten_acquired_attributed_demand
FROM rakuten_tbl_2 AS a
GROUP BY day_dt,
 banner,
 utm_campaign,
 publisher,
 publisher_group,
 publisher_subgroup;


DELETE FROM `{{params.gcp_project_id}}`.{{params.apd_t2_schema}}.rakuten_publisher_daily
WHERE day_dt BETWEEN (SELECT start_date
  FROM `_variables2`) AND (SELECT end_date
  FROM `_variables2`);


INSERT INTO `{{params.gcp_project_id}}`.{{params.apd_t2_schema}}.rakuten_publisher_daily
(SELECT day_dt,
  banner,
  utm_campaign,
  publisher,
  publisher_group,
  publisher_subgroup,
  aff_return_rate,
  attributed_demand,
  attributed_orders,
  attributed_net_sales,
  acquired_attributed_demand,
  acquired_attributed_orders,
  retained_attributed_orders,
  CAST(clicks AS BIGNUMERIC) AS clicks,
  sessions,
  paid_placement,
  daily_cost,
  rakuten_gross_commissions,
  estimated_net_total_cost,
  rakuten_gross_sales,
  rakuten_orders,
  rakuten_net_sales,
  rakuten_acquired_attributed_orders,
  rakuten_acquired_attributed_demand,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM apd_4_box_rakuten AS a);