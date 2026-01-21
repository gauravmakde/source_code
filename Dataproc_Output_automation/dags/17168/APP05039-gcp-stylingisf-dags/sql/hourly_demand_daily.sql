--{autocommit_on};



-- SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH_DEV;Job_Name=hourly_demand_daily_v1;Data_Plane=Customer;Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=HOURLY_DEMAND_DAILY_FACT;Conn_Type=JDBC;DAG_ID=hourly_demand_daily_v1_17168_styling_styling_insights_kpi;Task_Name=job_0;' FOR SESSION VOLATILE;





CREATE TEMPORARY TABLE IF NOT EXISTS cust_hourly_demand
CLUSTER BY ent_cust_id
AS
SELECT CAST(order_tmstp_pacific AS DATE) AS order_date,
  SUBSTR(SUBSTR(CAST(order_tmstp_pacific AS STRING), 1, 20), 1, 13) || ':00:00' AS order_date_hour,
 source_channel_country_code AS channel_country_code,
 source_channel_code AS channel_code,
 ent_cust_id,
 source_platform_code AS platform_code,
 SUM(order_line_amount) AS demand,
 COUNT(DISTINCT order_num) AS orders
FROM `{{params.gcp_project_id}}`.{{params.usr_view_schema}}.order_line_detail_fact
WHERE CAST(order_tmstp_pacific AS DATE) BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 15 DAY) AND (DATE_SUB(CURRENT_DATE('PST8PDT'),
    INTERVAL 1 DAY))
 AND LOWER(COALESCE(fraud_cancel_ind, '9999')) <> LOWER('Y')
 AND LOWER(source_platform_code) <> LOWER('POS')
GROUP BY order_date,
 order_date_hour,
 channel_country_code,
 channel_code,
 ent_cust_id,
 platform_code;


CREATE TEMPORARY TABLE IF NOT EXISTS cust_hourly_demand_xref
CLUSTER BY acp_id
AS
SELECT hdr.order_date,
 hdr.order_date_hour,
 hdr.channel_country_code,
 hdr.channel_code,
 hdr.ent_cust_id,
 t1.acp_id,
 hdr.platform_code,
 hdr.demand,
 hdr.orders
FROM cust_hourly_demand AS hdr
 LEFT JOIN (SELECT cust_id,
   acp_id,
   dw_sys_updt_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.usr_view_schema}}.acp_analytical_cust_xref
  WHERE LOWER(cust_source) = LOWER('icon')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY dw_sys_updt_tmstp DESC)) = 1) AS t1 ON LOWER(hdr.ent_cust_id
   ) = LOWER(t1.cust_id);


CREATE TEMPORARY TABLE IF NOT EXISTS loyalty_hourly_demand
CLUSTER BY order_date
AS
SELECT xref.order_date,
 xref.order_date_hour,
 xref.channel_country_code,
 xref.channel_code,
 xref.ent_cust_id,
 xref.acp_id,
 xref.platform_code,
 xref.demand,
 xref.orders,
  CASE
  WHEN LOWER(loyalty.nordy_level) = LOWER('ICON')
  THEN '1Icon'
  WHEN LOWER(loyalty.nordy_level) = LOWER('AMBASSADOR')
  THEN '2Ambassador'
  WHEN LOWER(loyalty.nordy_level) = LOWER('INFLUENCER')
  THEN '3Influencer'
  WHEN LOWER(loyalty.nordy_level) = LOWER('INSIDER')
  THEN '4Insider'
  WHEN LOWER(loyalty.nordy_level) = LOWER('MEMBER')
  THEN '5Member'
  WHEN LOWER(loyalty.nordy_level) = LOWER('UNKNOWN')
  THEN '6Non_Member'
  WHEN loyalty.nordy_level IS NULL
  THEN '7Non_Loyalty'
  ELSE NULL
  END AS nordylevel,
 loyalty.acp_card_member_enroll_status_flag AS cardmember_ind
FROM cust_hourly_demand_xref AS xref
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.usr_view_schema}}.analytical_customer AS loyalty ON LOWER(xref.acp_id) = LOWER(loyalty.acp_id);


DELETE FROM `{{params.gcp_project_id}}`.{{params.fact_schema}}.hourly_demand_daily_fact
WHERE order_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 15 DAY) AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY));


INSERT INTO `{{params.gcp_project_id}}`.{{params.fact_schema}}.hourly_demand_daily_fact
(SELECT order_date,
  CAST(order_date_hour AS DATETIME) AS order_date_hour,
  channel_country_code,
  channel_code,
  COALESCE(SUBSTR(nordylevel, 2, 20), 'Non_Loyalty') AS nordylevel,
  COALESCE(cardmember_ind, 'N') AS cardmember_ind,
  platform_code,
  ROUND(CAST(SUM(demand) AS NUMERIC), 2) AS demand,
  SUM(orders) AS orders
 FROM loyalty_hourly_demand
 GROUP BY order_date,
  order_date_hour,
  channel_country_code,
  channel_code,
  nordylevel,
  cardmember_ind,
  platform_code);


--COLLECT STATISTICS COLUMN (PARTITION , order_date) ON `{{params.gcp_project_id}}`.{{params.fact_schema}}.HOURLY_DEMAND_DAILY_FACT


--COLLECT STATISTICS COLUMN (PARTITION) ON `{{params.gcp_project_id}}`.{{params.fact_schema}}.HOURLY_DEMAND_DAILY_FACT


-- SET QUERY_BAND = NONE FOR SESSION;
