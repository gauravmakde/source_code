BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=campaign_attributed_funnel_fact_11521_ACE_ENG;
---     Task_Name=campaign_attributed_funnel_fact;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS prod_hier AS
SELECT DISTINCT sku.channel_country,
 sku.web_style_num,
 sku.dept_num,
 sku.grp_num AS sdiv_num,
 sku.brand_name,
 v.vendor_name AS supplier
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(sku.prmy_supp_num) = LOWER(v.vendor_num)
WHERE LOWER(sku.gwp_ind) <> LOWER('Y')
 AND LOWER(sku.smart_sample_ind) <> LOWER('Y');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nmn_funnel_fact_temp AS
SELECT sesh.activity_date_pacific,
 sesh.channelcountry AS channel_country,
 sesh.channel AS channel_banner,
 CAST(TRUNC(CAST(CASE
   WHEN LOWER(sesh.channel) = LOWER('NORDSTROM') AND LOWER(sesh.channelcountry) = LOWER('US')
   THEN 120
   WHEN LOWER(sesh.channel) = LOWER('NORDSTROM') AND LOWER(sesh.channelcountry) = LOWER('CA')
   THEN 121
   WHEN LOWER(sesh.channel) = LOWER('NORDSTROM_RACK') AND LOWER(sesh.channelcountry) = LOWER('US')
   THEN 250
   ELSE NULL
   END AS FLOAT64)) AS INTEGER) AS channel_num,
 sma.mrkt_type AS mktg_type,
 sma.finance_rollup,
 sma.finance_detail,
 sma.utm_source,
 TRIM(SUBSTR(sma.utm_channel, 0, 70)) AS utm_channel,
 TRIM(SUBSTR(sma.utm_campaign, 0, 90)) AS utm_campaign,
 TRIM(SUBSTR(sma.utm_term, 0, 90)) AS utm_term,
 TRIM(SUBSTR(sma.utm_content, 0, 300)) AS utm_content,
 skus.supplier AS supplier_name,
 skus.brand_name,
 skus.web_style_num,
 SUM(sesh.product_views) AS product_views,
 SUM(sesh.instock_product_views) AS instock_product_views,
 SUM(sesh.cart_add_units) AS cart_add_units,
 SUM(sesh.order_units) AS order_units
FROM (SELECT session_id,
    activity_date_pacific,
    channelcountry,
    channel,
    CAST(TRUNC(CAST(CASE
      WHEN CASE
        WHEN UPPER(productstyle_id) = LOWER(productstyle_id)
        THEN productstyle_id
        ELSE NULL
        END = ''
      THEN '0'
      ELSE CASE
       WHEN UPPER(productstyle_id) = LOWER(productstyle_id)
       THEN productstyle_id
       ELSE NULL
       END
      END AS FLOAT64)) AS INTEGER) AS productstyle_id,
    SUM(product_views) AS product_views,
    SUM(CASE
      WHEN LOWER(product_available_ind) = LOWER('Y')
      THEN product_views
      ELSE 0
      END) AS instock_product_views,
    0 AS cart_add_units,
    0 AS order_units
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_style_fact
   WHERE activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
   GROUP BY session_id,
    activity_date_pacific,
    channelcountry,
    channel,
    productstyle_id
   UNION DISTINCT
   SELECT session_id,
    activity_date_pacific,
    channelcountry,
    channel,
    CAST(TRUNC(CAST(CASE
      WHEN CASE
        WHEN UPPER(productstyle_id) = LOWER(productstyle_id)
        THEN productstyle_id
        ELSE NULL
        END = ''
      THEN '0'
      ELSE CASE
       WHEN UPPER(productstyle_id) = LOWER(productstyle_id)
       THEN productstyle_id
       ELSE NULL
       END
      END AS FLOAT64)) AS INTEGER) AS productstyle_id,
    0 AS product_views,
    0 AS instock_product_views,
    SUM(product_cart_add_units) AS cart_add_units,
    SUM(product_order_units) AS order_units
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_style_sku_fact
   WHERE activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
   GROUP BY session_id,
    activity_date_pacific,
    channelcountry,
    channel,
    productstyle_id) AS sesh
 LEFT JOIN prod_hier AS skus ON sesh.productstyle_id = skus.web_style_num AND LOWER(sesh.channelcountry) = LOWER(skus.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mta.ssa_mkt_attr_fact AS sma ON LOWER(sesh.session_id) = LOWER(sma.session_id)
GROUP BY sesh.activity_date_pacific,
 channel_country,
 channel_banner,
 channel_num,
 mktg_type,
 sma.finance_rollup,
 sma.finance_detail,
 sma.utm_source,
 utm_channel,
 utm_campaign,
 utm_term,
 utm_content,
 supplier_name,
 skus.brand_name,
 skus.web_style_num;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.campaign_attributed_funnel_fact
WHERE activity_date_pacific >= {{params.start_date}} AND activity_date_pacific <= {{params.end_date}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.campaign_attributed_funnel_fact
(SELECT activity_date_pacific,
  SUBSTR(channel_country, 1, 2) AS channel_country,
  channel_banner,
  channel_num,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  utm_content,
  supplier_name,
  brand_name,
  web_style_num,
  product_views,
  instock_product_views,
  cart_add_units,
  order_units,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM nmn_funnel_fact_temp
 WHERE activity_date_pacific >= {{params.start_date}}
  AND activity_date_pacific <= {{params.end_date}});

  
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (activity_date_pacific, web_style_num, channel_num, utm_channel), COLUMN (activity_date_pacific), COLUMN (channel_num, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, supplier_name, brand_name) ON `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
