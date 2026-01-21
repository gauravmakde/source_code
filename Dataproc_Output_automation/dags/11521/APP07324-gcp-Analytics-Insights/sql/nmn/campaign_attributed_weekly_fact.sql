BEGIN
DECLARE  _ERROR_CODE INT64;
DECLARE  _ERROR_MESSAGE STRING; 
/* SET QUERY_BAND = 'App_ID=APP08162; 
DAG_ID=campaign_attributed_weekly_fact_11521_ACE_ENG; 
---Task_Name=campaign_attributed_weekly_fact;'*/ 
---FOR SESSION VOLATILE;

BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS variable_start_end_week AS
SELECT
  MAX(CASE
      WHEN day_date < CURRENT_DATE('PST8PDT')
 THEN week_idnt
      ELSE NULL
  END ) AS start_week_num,
  MAX(CASE
      WHEN day_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY) THEN week_idnt
      ELSE NULL
  END ) AS end_week_num
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim; 

EXCEPTION WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;

END ;

BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS caf_ty AS
SELECT
  dd.week_idnt,
  caf.channel_num,
  caf.channel_country,
  caf.channel_banner,
  caf.mktg_type,
  caf.finance_rollup,
  caf.finance_detail,
  caf.utm_source,
  caf.utm_channel,
  caf.utm_campaign,
  caf.utm_term,
  CASE
    WHEN LOWER(caf.utm_channel) LIKE LOWER('%_EX_%') THEN caf.utm_content
    ELSE NULL
  END AS nmn_utm_content,
  caf.supplier_name,
  caf.brand_name,
  TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
  TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
  TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
  SUM(caf.product_views) AS ty_product_views,
  SUM(caf.instock_product_views) AS ty_instock_product_views,
  SUM(caf.cart_add_units) AS ty_cart_add_units,
  SUM(caf.order_units) AS ty_order_units,
  0 AS ly_product_views,
  0 AS ly_instock_product_views,
  0 AS ly_cart_add_units,
  0 AS ly_order_units
FROM
  `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_funnel_fact AS caf
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd
  ON
  caf.activity_date_pacific = dd.day_date
  LEFT JOIN (
  SELECT
    DISTINCT web_style_num,
    dept_num,
    div_num,
    grp_num,
    div_desc,
    grp_desc,
    dept_desc,
    channel_country
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS sku
  ON caf.web_style_num = sku.web_style_num
  AND LOWER(caf.channel_country ) = LOWER(sku.channel_country)
  WHERE dd.week_idnt BETWEEN ( SELECT  start_week_num  FROM variable_start_end_week)
  AND ( SELECT  end_week_num FROM variable_start_end_week)
GROUP BY
  dd.week_idnt,
  caf.channel_num,
  caf.channel_country,
  caf.channel_banner,
  caf.mktg_type,
  caf.finance_rollup,
  caf.finance_detail,
  caf.utm_source,
  caf.utm_channel,
  caf.utm_campaign,
  caf.utm_term,
  nmn_utm_content,
  caf.supplier_name,
  caf.brand_name,
  division,
  subdivision,
  department; 
  
EXCEPTION WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;

END  ;

BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS caf_ly AS
SELECT
  dd.week_idnt,
  caf.channel_num,
  caf.channel_country,
  caf.channel_banner,
  caf.mktg_type,
  caf.finance_rollup,
  caf.finance_detail,
  caf.utm_source,
  caf.utm_channel,
  caf.utm_campaign,
  caf.utm_term,
  CASE
    WHEN LOWER(caf.utm_channel) LIKE LOWER('%_EX_%') THEN caf.utm_content
    ELSE NULL
  END  AS nmn_utm_content,
  caf.supplier_name,
  caf.brand_name,
  TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
  TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
  TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
  0 AS ty_product_views,
  0 AS ty_instock_product_views,
  0 AS ty_cart_add_units,
  0 AS ty_order_units,
  SUM(caf.product_views) AS ly_product_views,
  SUM(caf.instock_product_views) AS ly_instock_product_views,
  SUM(caf.cart_add_units) AS ly_cart_add_units,
  SUM(caf.order_units) AS ly_order_units
FROM
  `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_funnel_fact AS caf
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd
ON
  caf.activity_date_pacific = dd.day_date_last_year_realigned
LEFT JOIN (
  SELECT
    DISTINCT web_style_num,
    dept_num,
    div_num,
    grp_num,
    div_desc,
    grp_desc,
    dept_desc,
    channel_country
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS sku
  ON
  caf.web_style_num = sku.web_style_num
  AND LOWER(caf.channel_country ) = LOWER(sku.channel_country)
  WHERE
  dd.week_idnt BETWEEN (SELECT start_week_num FROM variable_start_end_week)
  AND (SELECT end_week_num FROM variable_start_end_week)
GROUP BY
  dd.week_idnt,
  caf.channel_num,
  caf.channel_country,
  caf.channel_banner,
  caf.mktg_type,
  caf.finance_rollup,
  caf.finance_detail,
  caf.utm_source,
  caf.utm_channel,
  caf.utm_campaign,
  caf.utm_term,
  nmn_utm_content,
  caf.supplier_name,
  caf.brand_name,
  division,
  subdivision,
  department; 
  
EXCEPTION  WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END  ;


BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS caf_agg AS
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  ty_product_views,
  ty_instock_product_views,
  ty_cart_add_units,
  ty_order_units,
  0 AS ty_gross_sales_amt,
  0 AS ty_gross_sales_units,
  ly_product_views,
  ly_instock_product_views,
  ly_cart_add_units,
  ly_order_units,
  0 AS ly_gross_sales_amt,
  0 AS ly_gross_sales_units
FROM  caf_ty
UNION DISTINCT
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_term,
  utm_campaign,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  ty_product_views,
  ty_instock_product_views,
  ty_cart_add_units,
  ty_order_units,
  0 AS ty_gross_sales_amt,
  0 AS ty_gross_sales_units,
  ly_product_views,
  ly_instock_product_views,
  ly_cart_add_units,
  ly_order_units,
  0 AS ly_gross_sales_amt,
  0 AS ly_gross_sales_units
FROM  caf_ly; 

EXCEPTION  WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END ;


BEGIN
SET  _ERROR_CODE = 0; 
CREATE TEMPORARY TABLE IF NOT EXISTS cas_ty AS
SELECT
  dd.week_idnt,
  cas.channel_num,
  cas.channel_country,
  cas.channel_banner,
  cas.mktg_type,
  cas.finance_rollup,
  cas.finance_detail,
  cas.utm_source,
  cas.utm_channel,
  cas.utm_campaign,
  cas.utm_term,
  CASE
    WHEN LOWER(cas.utm_channel) LIKE LOWER('%_EX_%') THEN cas.utm_content
    ELSE NULL
  END  AS nmn_utm_content,
  cas.supplier_name,
  cas.brand_name,
  TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
  TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
  TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
  SUM(cas.gross_sales_amt_usd) AS ty_gross_sales_amt,
  SUM(cas.gross_sales_units) AS ty_gross_sales_units,
  0 AS ly_gross_sales_amt,
  0 AS ly_gross_sales_units
FROM
  `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_sales_fact AS cas
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd
ON
  cas.business_day_date = dd.day_date
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
ON
  LOWER(cas.sku_num) = LOWER(sku.rms_sku_num)
  AND LOWER(cas.channel_country ) = LOWER(sku.channel_country)
WHERE
  dd.week_idnt BETWEEN (SELECT start_week_num FROM variable_start_end_week)
  AND (SELECT end_week_num FROM variable_start_end_week)
  GROUP BY
    dd.week_idnt,
    cas.channel_num,
    cas.channel_country,
    cas.channel_banner,
    cas.mktg_type,
    cas.finance_rollup,
    cas.finance_detail,
    cas.utm_source,
    cas.utm_channel,
    cas.utm_campaign,
    cas.utm_term,
    nmn_utm_content,
    cas.supplier_name,
    cas.brand_name,
    division,
    subdivision,
    department; 

EXCEPTION  WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;

END ;

BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS cas_ly AS
SELECT
  dd.week_idnt,
  cas.channel_num,
  cas.channel_country,
  cas.channel_banner,
  cas.mktg_type,
  cas.finance_rollup,
  cas.finance_detail,
  cas.utm_source,
  cas.utm_channel,
  cas.utm_campaign,
  cas.utm_term,
  CASE
    WHEN LOWER(cas.utm_channel) LIKE LOWER('%_EX_%') THEN cas.utm_content
    ELSE NULL
  END  AS nmn_utm_content,
  cas.supplier_name,
  cas.brand_name,
  TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
  TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
  TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
  0 AS ty_gross_sales_amt,
  0 AS ty_gross_sales_units,
  SUM(cas.gross_sales_amt_usd) AS ly_gross_sales_amt,
  SUM(cas.gross_sales_units) AS ly_gross_sales_units
  FROM
    `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_sales_fact AS cas
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd
  ON cas.business_day_date = dd.day_date_last_year_realigned
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
  ON
    LOWER(cas.sku_num) = LOWER(sku.rms_sku_num)
    AND LOWER(cas.channel_country ) = LOWER(sku.channel_country)
  WHERE
  dd.week_idnt BETWEEN (SELECT start_week_num FROM variable_start_end_week)
  AND (SELECT end_week_num FROM variable_start_end_week)
GROUP BY
  dd.week_idnt,
  cas.channel_num,
  cas.channel_country,
  cas.channel_banner,
  cas.mktg_type,
  cas.finance_rollup,
  cas.finance_detail,
  cas.utm_source,
  cas.utm_channel,
  cas.utm_campaign,
  cas.utm_term,
  nmn_utm_content,
  cas.supplier_name,
  cas.brand_name,
  division,
  subdivision,
  department; 
  
EXCEPTION WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END  ;


BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS cas_agg AS
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  0 AS ty_product_views,
  0 AS ty_instock_product_views,
  0 AS ty_cart_add_units,
  0 AS ty_order_units,
  ty_gross_sales_amt,
  ty_gross_sales_units,
  0 AS ly_product_views,
  0 AS ly_instock_product_views,
  0 AS ly_cart_add_units,
  0 AS ly_order_units,
  ly_gross_sales_amt,
  ly_gross_sales_units
FROM
  cas_ty
UNION DISTINCT
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  0 AS ty_product_views,
  0 AS ty_instock_product_views,
  0 AS ty_cart_add_units,
  0 AS ty_order_units,
  ty_gross_sales_amt,
  ty_gross_sales_units,
  0 AS ly_product_views,
  0 AS ly_instock_product_views,
  0 AS ly_cart_add_units,
  0 AS ly_order_units,
  ly_gross_sales_amt,
  ly_gross_sales_units
FROM
  cas_ly; 
  
EXCEPTION  WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END ;


BEGIN
SET _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS caf_cas AS
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  SUM(ty_product_views) AS ty_product_views,
  SUM(ty_instock_product_views) AS ty_instock_product_views,
  SUM(ty_cart_add_units) AS ty_cart_add_units,
  SUM(ty_order_units) AS ty_order_units,
  SUM(ly_product_views) AS ly_product_views,
  SUM(ly_instock_product_views) AS ly_instock_product_views,
  SUM(ly_cart_add_units) AS ly_cart_add_units,
  SUM(ly_order_units) AS ly_order_units,
  0 AS ty_gross_sales_amt,
  0 AS ty_gross_sales_units,
  0 AS ly_gross_sales_amt,
  0 AS ly_gross_sales_units
FROM
  caf_agg
GROUP BY
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department
UNION DISTINCT
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  0 AS ty_product_views,
  0 AS ty_instock_product_views,
  0 AS ty_cart_add_units,
  0 AS ty_order_units,
  0 AS ly_product_views,
  0 AS ly_instock_product_views,
  0 AS ly_cart_add_units,
  0 AS ly_order_units,
  SUM(ty_gross_sales_amt) AS ty_gross_sales_amt,
  SUM(ty_gross_sales_units) AS ty_gross_sales_units,
  SUM(ly_gross_sales_amt) AS ly_gross_sales_amt,
  SUM(ly_gross_sales_units) AS ly_gross_sales_units
FROM
  cas_agg
GROUP BY
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department; 
  
EXCEPTION WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END ; 

--COLLECT STATISTICS COLUMN (BRAND_NAME) ON  caf_cas;

BEGIN
SET  _ERROR_CODE = 0; 

CREATE TEMPORARY TABLE IF NOT EXISTS caw_fact_stg  AS
SELECT
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department,
  SUM(ty_product_views) AS ty_product_views,
  SUM(ty_instock_product_views) AS ty_instock_product_views,
  SUM(ty_cart_add_units) AS ty_cart_add_units,
  SUM(ty_order_units) AS ty_order_units,
  SUM(ty_gross_sales_amt) AS ty_gross_sales_amt,
  SUM(ty_gross_sales_units) AS ty_gross_sales_units,
  SUM(ly_product_views) AS ly_product_views,
  SUM(ly_instock_product_views) AS ly_instock_product_views,
  SUM(ly_cart_add_units) AS ly_cart_add_units,
  SUM(ly_order_units) AS ly_order_units,
  SUM(ly_gross_sales_amt) AS ly_gross_sales_amt,
  SUM(ly_gross_sales_units) AS ly_gross_sales_units
FROM
  caf_cas
GROUP BY
  week_idnt,
  channel_num,
  channel_country,
  channel_banner,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  nmn_utm_content,
  supplier_name,
  brand_name,
  division,
  subdivision,
  department; 
  
EXCEPTION WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END  ;

BEGIN
SET  _ERROR_CODE = 0;

DELETE
FROM  `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_weekly_fact
WHERE
  week_idnt BETWEEN (SELECT start_week_num FROM variable_start_end_week)
  AND (SELECT end_week_num FROM variable_start_end_week); 
    
EXCEPTION  WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END  ;

BEGIN
SET  _ERROR_CODE = 0;

INSERT INTO  `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_weekly_fact (
  SELECT
    week_idnt,
    channel_num,
    channel_country,
    channel_banner,
    mktg_type,
    finance_rollup,
    finance_detail,
    utm_source,
    utm_channel,
    utm_campaign,
    utm_term,
    nmn_utm_content,
    supplier_name,
    brand_name,
    division,
    subdivision,
    department,
    CAST(ty_product_views AS INTEGER) AS ty_product_views,
    CAST(ty_instock_product_views AS INTEGER) AS ty_instock_product_views,
    CAST(ty_cart_add_units AS INTEGER) AS ty_cart_add_units,
    CAST(ty_order_units AS INTEGER) AS ty_order_units,
    ty_gross_sales_amt,
    ty_gross_sales_units,
    ly_product_views,
    ly_instock_product_views,
    ly_cart_add_units,
    ly_order_units,
    ly_gross_sales_amt,
    ly_gross_sales_units,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM
    caw_fact_stg AS a); 
    
EXCEPTION  WHEN ERROR THEN SET _ERROR_CODE = 1; 
SET _ERROR_MESSAGE = @@error.message;
END  ; 

--COLLECT STATISTICS COLUMN (WEEK_IDNT,
--   CHANNEL_NUM,
--   CHANNEL_COUNTRY,
--   CHANNEL_BANNER,
--   MKTG_TYPE,
--   FINANCE_ROLLUP,
--   FINANCE_DETAIL,
--   UTM_SOURCE,
--   UTM_CHANNEL,
--   utm_campaign,
--   utm_term,
--   NMN_UTM_CONTENT,
--   SUPPLIER_NAME,
--   BRAND_NAME,
--   DIVISION,
--   SUBDIVISION,
--   DEPARTMENT)
-- ON   t2dl_das_nmn.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT; --COLLECT STATISTICS COLUMN (WEEK_IDNT)
-- ON   t2dl_das_nmn.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT; --COLLECT STATISTICS COLUMN (BRAND_NAME, SUBDIVISION)
-- ON t2dl_das_nmn.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT; /*SET QUERY_BAND = NONE FOR SESSION;*/
END  ;