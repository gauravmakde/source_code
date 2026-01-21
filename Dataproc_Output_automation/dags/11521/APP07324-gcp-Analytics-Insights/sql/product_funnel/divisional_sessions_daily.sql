
CREATE TEMPORARY TABLE IF NOT EXISTS atb_sessions
AS
SELECT DISTINCT session_id,
 activity_date_pacific,
  CASE
  WHEN LOWER(channel) = LOWER('NORDSTROM_RACK')
  THEN 'RACK'
  WHEN LOWER(channel) = LOWER('NORDSTROM')
  THEN 'FULL_LINE'
  ELSE NULL
  END AS channel,
  CASE
  WHEN LOWER(experience) = LOWER('MOBILE_WEB')
  THEN 'MOW'
  WHEN LOWER(experience) = LOWER('DESKTOP_WEB')
  THEN 'WEB'
  WHEN LOWER(experience) = LOWER('IOS_APP')
  THEN 'IOS'
  WHEN LOWER(experience) = LOWER('ANDROID_APP')
  THEN 'ANDROID'
  WHEN LOWER(experience) = LOWER('POINT_OF_SALE')
  THEN 'POS'
  WHEN LOWER(experience) = LOWER('UNKNOWN')
  THEN 'UNKNOWN'
  WHEN LOWER(experience) = LOWER('CARE_PHONE')
  THEN 'CSR_PHONE'
  WHEN LOWER(experience) = LOWER('THIRD_PARTY_VENDOR')
  THEN 'VENDOR'
  ELSE NULL
  END AS platform,
 channelcountry,
 product_idtype,
 product_id AS sku_id,
 productstyle_id AS style_id,
 productstyle_idtype,
 NULL AS product_views,
 product_cart_add_units,
 NULL AS product_order_units,
 NULL AS product_order_value
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_style_sku_fact AS csssf
WHERE activity_date_pacific BETWEEN  {{params.start_date}} AND  {{params.end_date}};


CREATE TEMPORARY TABLE IF NOT EXISTS order_sessions
AS
SELECT DISTINCT session_id,
 activity_date_pacific,
  CASE
  WHEN LOWER(channel) = LOWER('NORDSTROM_RACK')
  THEN 'RACK'
  WHEN LOWER(channel) = LOWER('NORDSTROM')
  THEN 'FULL_LINE'
  ELSE NULL
  END AS channel,
  CASE
  WHEN LOWER(experience) = LOWER('MOBILE_WEB')
  THEN 'MOW'
  WHEN LOWER(experience) = LOWER('DESKTOP_WEB')
  THEN 'WEB'
  WHEN LOWER(experience) = LOWER('IOS_APP')
  THEN 'IOS'
  WHEN LOWER(experience) = LOWER('ANDROID_APP')
  THEN 'ANDROID'
  WHEN LOWER(experience) = LOWER('POINT_OF_SALE')
  THEN 'POS'
  WHEN LOWER(experience) = LOWER('UNKNOWN')
  THEN 'UNKNOWN'
  WHEN LOWER(experience) = LOWER('CARE_PHONE')
  THEN 'CSR_PHONE'
  WHEN LOWER(experience) = LOWER('THIRD_PARTY_VENDOR')
  THEN 'VENDOR'
  ELSE NULL
  END AS platform,
 channelcountry,
 product_idtype,
 product_id AS sku_id,
 productstyle_id AS style_id,
 productstyle_idtype,
 NULL AS product_views,
 NULL AS product_cart_add_units,
 product_order_units,
 product_order_value
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_style_sku_fact AS csssf
WHERE activity_date_pacific BETWEEN  {{params.start_date}} AND  {{params.end_date}};


CREATE TEMPORARY TABLE IF NOT EXISTS product_sessions
AS
SELECT DISTINCT product.session_id,
 pfd.event_date_pacific AS activity_date_pacific,
 pfd.channel,
 pfd.platform,
 pfd.channelcountry,
 'RMS' AS productid_type,
 pfd.rms_sku_num AS sku_id,
 SUBSTR(CAST(pfd.web_style_num AS STRING), 1, 10) AS style_id,
 product.productstyle_idtype,
 product.product_views,
 product.product_cart_add_units,
 product.product_order_units,
 product.product_order_value
FROM (SELECT DISTINCT session_id,
   activity_date_pacific,
    CASE
    WHEN LOWER(channel) = LOWER('NORDSTROM_RACK')
    THEN 'RACK'
    WHEN LOWER(channel) = LOWER('NORDSTROM')
    THEN 'FULL_LINE'
    ELSE NULL
    END AS channel,
    CASE
    WHEN LOWER(experience) = LOWER('MOBILE_WEB')
    THEN 'MOW'
    WHEN LOWER(experience) = LOWER('DESKTOP_WEB')
    THEN 'WEB'
    WHEN LOWER(experience) = LOWER('IOS_APP')
    THEN 'IOS'
    WHEN LOWER(experience) = LOWER('ANDROID_APP')
    THEN 'ANDROID'
    WHEN LOWER(experience) = LOWER('POINT_OF_SALE')
    THEN 'POS'
    WHEN LOWER(experience) = LOWER('UNKNOWN')
    THEN 'UNKNOWN'
    WHEN LOWER(experience) = LOWER('CARE_PHONE')
    THEN 'CSR_PHONE'
    WHEN LOWER(experience) = LOWER('THIRD_PARTY_VENDOR')
    THEN 'VENDOR'
    ELSE NULL
    END AS platform,
   channelcountry,
   productstyle_id,
   productstyle_idtype,
   product_views,
   NULL AS product_cart_add_units,
   NULL AS product_order_units,
   NULL AS product_order_value
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_style_fact
  WHERE activity_date_pacific BETWEEN  {{params.start_date}} AND  {{params.end_date}}) AS product
 RIGHT JOIN `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS pfd ON LOWER(SUBSTR(CAST(pfd.web_style_num AS STRING)
        , 1, 10)) = LOWER(product.productstyle_id) AND LOWER(pfd.platform) = LOWER(product.platform) AND LOWER(pfd.channel
      ) = LOWER(product.channel) AND product.activity_date_pacific = pfd.event_date_pacific AND LOWER(pfd.channelcountry
    ) = LOWER(product.channelcountry)
WHERE pfd.event_date_pacific BETWEEN  {{params.start_date}} AND  {{params.end_date}};


CREATE TEMPORARY TABLE IF NOT EXISTS sessions_fact
AS
SELECT session_id,
 activity_date_pacific,
 channel,
 platform,
 channelcountry,
 productid_type,
 sku_id,
 style_id,
 productstyle_idtype,
 product_views,
 cast(trunc(product_cart_add_units) as integer)AS product_cart_add_units,
 cast(trunc(product_order_units) as integer)AS product_order_units,
  cast(trunc(product_order_value) as integer)AS product_order_value
FROM product_sessions
UNION ALL
SELECT session_id,
 activity_date_pacific,
 channel,
 platform,
 channelcountry,
 product_idtype,
 sku_id,
 style_id,
 productstyle_idtype,
 cast(trunc(product_views) as integer)AS product_views,
 product_cart_add_units,
 cast(trunc(product_order_units) as integer)AS product_order_units,
 cast(trunc(product_order_value) as integer)AS product_order_value
 FROM atb_sessions
UNION ALL
SELECT session_id,
 activity_date_pacific,
 channel,
 platform,
 channelcountry,
 product_idtype,
 sku_id,
 style_id,
 productstyle_idtype,
 cast(trunc(product_views) as integer) AS product_views,
 cast(trunc(product_cart_add_units) as integer) AS product_cart_add_units,
 product_order_units,
 product_order_value
FROM order_sessions;


CREATE TEMPORARY TABLE IF NOT EXISTS ppfd
AS
SELECT event_date_pacific,
 channelcountry,
 channel,
 platform,
 web_style_num AS style_id,
 rms_sku_num AS sku_id,
 order_quantity,
 order_demand,
 add_to_bag_quantity,
 product_views,
 current_price_type,
 current_price_amt,
 regular_price_amt
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS ppfd
WHERE event_date_pacific BETWEEN  {{params.start_date}} AND  {{params.end_date}};

-- Create a temporary table in BigQuery
CREATE TEMPORARY TABLE IF NOT EXISTS sku_base AS
WITH sku_base_data AS (
  SELECT
    sku.rms_sku_num,
    sku.channel_country,
    CAST(sku.web_style_num AS STRING) AS web_style_num,
    sty.style_group_num,
    CONCAT(CAST(sku.div_num AS STRING), ', ', sku.div_desc) AS division,
    CONCAT(CAST(sku.grp_num AS STRING), ', ', sku.grp_desc) AS subdiv,
    CONCAT(CAST(sku.dept_num AS STRING), ', ', sku.dept_desc) AS department,
    CONCAT(CAST(sku.class_num AS STRING), ', ', sku.class_desc) AS class,
    CONCAT(CAST(sku.sbclass_num AS STRING), ', ', sku.sbclass_desc) AS subclass,
    UPPER(sku.brand_name) AS brand_name,
    UPPER(supp.vendor_name) AS supplier,
    UPPER(sty.type_level_1_desc) AS product_type_1,
    UPPER(sty.style_desc) AS style_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim sty
    ON sku.epm_style_num = sty.epm_style_num
    AND sku.channel_country = sty.channel_country
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
    ON sku.prmy_supp_num = supp.vendor_num
  WHERE NOT sku.dept_num IN (584, 585, 523)
    AND NOT (sku.div_num = 340 AND sku.class_num = 90)
)
SELECT * FROM sku_base_data;



CREATE TEMPORARY TABLE IF NOT EXISTS div_sessions_daily AS
WITH sf AS (
  SELECT
    session_id,
    activity_date_pacific,
    sf.sku_id,
    sf.style_id,
    sf.channel,
    sf.platform,
    sf.channelcountry,
    ppfd.current_price_type,
    ppfd.regular_price_amt,
    ppfd.current_price_amt,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id, ppfd.style_id, ppfd.channel, ppfd.platform, activity_date_pacific ORDER BY activity_date_pacific) = 1
      THEN ppfd.product_views ELSE 0
    END AS pfd_product_views,
    sf.product_views,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id, ppfd.style_id, ppfd.channel, ppfd.platform, activity_date_pacific ORDER BY activity_date_pacific) = 1
      THEN add_to_bag_quantity ELSE 0
    END AS pfd_add_to_bag_quantity,
    sf.product_cart_add_units,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id, ppfd.style_id, ppfd.channel, ppfd.platform, activity_date_pacific ORDER BY activity_date_pacific) = 1
      THEN order_quantity ELSE 0
    END AS pfd_order_quantity,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id, ppfd.style_id, ppfd.channel, ppfd.platform, activity_date_pacific ORDER BY activity_date_pacific) = 1
      THEN order_demand ELSE 0
    END AS pfd_order_demand,
    sf.product_order_units
  FROM sessions_fact sf
  LEFT JOIN ppfd ppfd
    ON sf.activity_date_pacific = ppfd.event_date_pacific
    AND sf.sku_id = ppfd.sku_id
    AND cast(sf.style_id as int64) = ppfd.style_id
    AND sf.channel = ppfd.channel
    AND sf.platform = ppfd.platform
    AND sf.channelcountry = ppfd.channelcountry
),
final_data AS (
  SELECT
    session_id,
    activity_date_pacific,
    sf.channel,
    sf.platform,
    sf.channelcountry,
    division,
    subdiv,
    department,
    sf.regular_price_amt,
    sf.current_price_amt,
    sf.current_price_type,
    SUM(sf.pfd_product_views) AS product_views,
    COUNT(DISTINCT CASE WHEN sf.product_views > 0 THEN session_id END) AS product_viewed_session,
    SUM(sf.pfd_add_to_bag_quantity) AS add_to_bag_quantity,
    COUNT(DISTINCT CASE WHEN sf.product_cart_add_units > 0 THEN session_id END) AS cart_add_session,
    SUM(sf.pfd_order_demand) AS order_demand,
    SUM(sf.pfd_order_quantity) AS order_quantity,
    COUNT(DISTINCT CASE WHEN sf.product_order_units > 0 THEN session_id END) AS web_order_session,
    CURRENT_datetime('PST8PDT') AS dw_sys_load_tmstp
  FROM sf
  INNER JOIN sku_base psdv
    ON psdv.rms_sku_num = sf.sku_id
    AND psdv.web_style_num = sf.style_id
    AND psdv.channel_country = sf.channelcountry
  GROUP BY
    session_id,
    activity_date_pacific,
    sf.channel,
    sf.platform,
    sf.channelcountry,
    division,
    subdiv,
    department,
    sf.regular_price_amt,
    sf.current_price_amt,
    sf.current_price_type
)
SELECT * FROM final_data;




DELETE FROM `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.divisional_sessions_daily
WHERE activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}};



insert into `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.divisional_sessions_daily
select * from div_sessions_daily;
