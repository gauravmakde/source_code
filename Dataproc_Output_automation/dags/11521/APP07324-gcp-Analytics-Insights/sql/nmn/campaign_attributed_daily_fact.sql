BEGIN

/*SET QUERY_BAND = 'App_ID=APP08162;
DAG_ID=campaign_attributed_daily_fact_11521_ACE_ENG;
Task_Name=campaign_attributed_daily_fact;'*/



CREATE TEMPORARY TABLE IF NOT EXISTS caf_ty

AS
SELECT dd.day_date,
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
 caf.utm_content,
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
 0 AS ly_order_units,
  CASE
  WHEN dd.day_date > DATE '2023-12-01'
  THEN CASE
   WHEN LOWER(caf.utm_term) LIKE LOWER('social@_%')
   THEN NULL
   WHEN LOWER(caf.utm_term) LIKE LOWER('%campaign%')
   THEN NULL
   WHEN LOWER(caf.utm_channel) = LOWER('low_ex_email_marketing')
   THEN REGEXP_SUBSTR(caf.utm_term, '[^_]+', 1, 1)
   WHEN LOWER(caf.utm_channel) LIKE LOWER('%@_ex@_%')
   THEN CASE
    WHEN LENGTH(caf.utm_term) - LENGTH(REPLACE(caf.utm_term, '_', '')) = 2
    THEN REGEXP_SUBSTR(caf.utm_term, '[^_]+', 1, 1)
    WHEN LENGTH(caf.utm_term) - LENGTH(REPLACE(caf.utm_term, '-', '')) = 2
    THEN REGEXP_SUBSTR(caf.utm_term, '[^-]+', 1, 1)
    WHEN LOWER(SUBSTR(caf.utm_term, 4 * -1)) = LOWER('pmax')
    THEN SUBSTR(caf.utm_term, 1, INSTR(caf.utm_term, '_') - 1)
    WHEN LENGTH(caf.utm_term) - LENGTH(REPLACE(caf.utm_term, '%5F', '')) >= 6
    THEN SUBSTR(caf.utm_term, 1, STRPOS(LOWER(caf.utm_term), LOWER('%5F')) - 1)
    ELSE NULL
    END
   ELSE NULL
   END
  ELSE NULL
  END AS campaign_id,
 TRIM(CASE
   WHEN dd.day_date > DATE '2023-12-01'
   THEN CASE
    WHEN LOWER(caf.utm_campaign) LIKE LOWER('%afterpay%')
    THEN '5188900'
    WHEN LOWER(caf.utm_channel) = LOWER('low_ex_email_marketing')
    THEN NULL
    WHEN LOWER(caf.utm_channel) LIKE LOWER('%@_ex@_%')
    THEN CASE
     WHEN LOWER(caf.utm_campaign) LIKE LOWER('%e012894%')
     THEN NULL
     WHEN LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf.utm_campaign, '_', '')) = 8 OR LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf
          .utm_campaign, '_', '')) = 5
     THEN SUBSTR(caf.utm_campaign, INSTR(caf.utm_campaign, '_', 1, 3) + 1, INSTR(caf.utm_campaign, '_', 1, 4) - INSTR(caf
         .utm_campaign, '_', 1, 3) - 1)
     WHEN LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf.utm_campaign, '_', '')) = 3 OR LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf
          .utm_campaign, '_', '')) = 7
     THEN SUBSTR(caf.utm_campaign, INSTR(caf.utm_campaign, '_', 1, 2) + 1, INSTR(caf.utm_campaign, '_', 1, 3) - INSTR(caf
         .utm_campaign, '_', 1, 2) - 1)
     ELSE NULL
     END
    ELSE NULL
    END
   ELSE NULL
   END) AS campaign_brand_id,
  CASE
  WHEN LOWER(caf.utm_campaign) LIKE LOWER('%n_202%')
  THEN SUBSTR(caf.utm_campaign, LENGTH(caf.utm_campaign) - STRPOS(LOWER(REVERSE(caf.utm_campaign)), LOWER('_')) + 2)
  WHEN LOWER(caf.utm_campaign) LIKE LOWER('%nr_202%')
  THEN SUBSTR(caf.utm_campaign, LENGTH(caf.utm_campaign) - STRPOS(LOWER(REVERSE(caf.utm_campaign)), LOWER('_')) + 2)
  ELSE NULL
  END AS placementsio_id
FROM `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_funnel_fact AS caf
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd ON caf.activity_date_pacific = dd.day_date
 LEFT JOIN (SELECT DISTINCT web_style_num,
   dept_num,
   div_num,
   grp_num,
   div_desc,
   grp_desc,
   dept_desc,
   channel_country
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS sku ON caf.web_style_num = sku.web_style_num AND LOWER(caf.channel_country
    ) = LOWER(sku.channel_country)
WHERE dd.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY dd.day_date,
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
 caf.utm_content,
 caf.supplier_name,
 caf.brand_name,
 division,
 subdivision,
 department;




CREATE TEMPORARY TABLE IF NOT EXISTS caf_ly

AS
SELECT dd.day_date,
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
 caf.utm_content,
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
 SUM(caf.order_units) AS ly_order_units,
  CASE
  WHEN dd.day_date > DATE '2023-12-01'
  THEN CASE
   WHEN LOWER(caf.utm_term) LIKE LOWER('social@_%')
   THEN NULL
   WHEN LOWER(caf.utm_term) LIKE LOWER('%campaign%')
   THEN NULL
   WHEN LOWER(caf.utm_channel) = LOWER('low_ex_email_marketing')
   THEN REGEXP_SUBSTR(caf.utm_term, '[^_]+', 1, 1)
   WHEN LOWER(caf.utm_channel) LIKE LOWER('%@_ex@_%')
   THEN CASE
    WHEN LENGTH(caf.utm_term) - LENGTH(REPLACE(caf.utm_term, '_', '')) = 2
    THEN REGEXP_SUBSTR(caf.utm_term, '[^_]+', 1, 1)
    WHEN LENGTH(caf.utm_term) - LENGTH(REPLACE(caf.utm_term, '-', '')) = 2
    THEN REGEXP_SUBSTR(caf.utm_term, '[^-]+', 1, 1)
    WHEN LOWER(SUBSTR(caf.utm_term, 4 * -1)) = LOWER('pmax')
    THEN SUBSTR(caf.utm_term, 1, INSTR(caf.utm_term, '_') - 1)
    WHEN LENGTH(caf.utm_term) - LENGTH(REPLACE(caf.utm_term, '%5F', '')) >= 6
    THEN SUBSTR(caf.utm_term, 1, STRPOS(LOWER(caf.utm_term), LOWER('%5F')) - 1)
    ELSE NULL
    END
   ELSE NULL
   END
  ELSE NULL
  END AS campaign_id,
 TRIM(CASE
   WHEN dd.day_date > DATE '2023-12-01'
   THEN CASE
    WHEN LOWER(caf.utm_campaign) LIKE LOWER('%afterpay%')
    THEN '5188900'
    WHEN LOWER(caf.utm_channel) LIKE LOWER('%@_ex@_%')
    THEN CASE
     WHEN LOWER(caf.utm_campaign) LIKE LOWER('%e012894%')
     THEN NULL
     WHEN LOWER(caf.utm_campaign) = LOWER('general_j009192')
     THEN NULL
     WHEN LOWER(caf.utm_campaign) LIKE LOWER('%j01%')
     THEN NULL
     WHEN LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf.utm_campaign, '_', '')) = 8 OR LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf
          .utm_campaign, '_', '')) = 5
     THEN SUBSTR(caf.utm_campaign, INSTR(caf.utm_campaign, '_', 1, 3) + 1, INSTR(caf.utm_campaign, '_', 1, 4) - INSTR(caf
         .utm_campaign, '_', 1, 3) - 1)
     WHEN LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf.utm_campaign, '_', '')) = 3 OR LENGTH(caf.utm_campaign) - LENGTH(REPLACE(caf
          .utm_campaign, '_', '')) = 7
     THEN SUBSTR(caf.utm_campaign, INSTR(caf.utm_campaign, '_', 1, 2) + 1, INSTR(caf.utm_campaign, '_', 1, 3) - INSTR(caf
         .utm_campaign, '_', 1, 2) - 1)
     ELSE NULL
     END
    ELSE NULL
    END
   ELSE NULL
   END) AS campaign_brand_id,
  CASE
  WHEN LOWER(caf.utm_campaign) LIKE LOWER('%n_202%')
  THEN SUBSTR(caf.utm_campaign, LENGTH(caf.utm_campaign) - STRPOS(LOWER(REVERSE(caf.utm_campaign)), LOWER('_')) + 2)
  WHEN LOWER(caf.utm_campaign) LIKE LOWER('%nr_202%')
  THEN SUBSTR(caf.utm_campaign, LENGTH(caf.utm_campaign) - STRPOS(LOWER(REVERSE(caf.utm_campaign)), LOWER('_')) + 2)
  ELSE NULL
  END AS placementsio_id
FROM `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_funnel_fact AS caf
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd ON caf.activity_date_pacific = dd.day_date_last_year_realigned
 LEFT JOIN (SELECT DISTINCT web_style_num,
   dept_num,
   div_num,
   grp_num,
   div_desc,
   grp_desc,
   dept_desc,
   channel_country
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS sku ON caf.web_style_num = sku.web_style_num AND LOWER(caf.channel_country
    ) = LOWER(sku.channel_country)
WHERE dd.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY dd.day_date,
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
 caf.utm_content,
 caf.supplier_name,
 caf.brand_name,
 division,
 subdivision,
 department;



CREATE TEMPORARY TABLE IF NOT EXISTS caf_agg

AS
SELECT day_date,
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
 utm_content,
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
 campaign_id,
 campaign_brand_id,
 placementsio_id,
 0 AS ly_gross_sales_amt,
 0 AS ly_gross_sales_units
FROM caf_ty
UNION ALL
SELECT day_date,
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
 utm_content,
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
 campaign_id,
 campaign_brand_id,
 placementsio_id,
 0 AS ly_gross_sales_amt,
 0 AS ly_gross_sales_units
FROM caf_ly;



CREATE TEMPORARY TABLE IF NOT EXISTS cas_ty

AS
SELECT dd.day_date,
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
 cas.utm_content,
 cas.supplier_name,
 cas.brand_name,
   TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
   TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
   TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
 SUM(cas.gross_sales_amt_usd) AS ty_gross_sales_amt,
 SUM(cas.gross_sales_units) AS ty_gross_sales_units,
 0 AS ly_gross_sales_amt,
 0 AS ly_gross_sales_units,
  CASE
  WHEN dd.day_date > DATE '2023-12-01'
  THEN CASE
   WHEN LOWER(cas.utm_term) LIKE LOWER('social@_%')
   THEN NULL
   WHEN LOWER(cas.utm_term) LIKE LOWER('%campaign%')
   THEN NULL
   WHEN LOWER(cas.utm_channel) = LOWER('low_ex_email_marketing')
   THEN REGEXP_SUBSTR(cas.utm_term, '[^_]+', 1, 1)
   WHEN LOWER(cas.utm_channel) LIKE LOWER('%@_ex@_%')
   THEN CASE
    WHEN LENGTH(cas.utm_term) - LENGTH(REPLACE(cas.utm_term, '_', '')) = 2
    THEN REGEXP_SUBSTR(cas.utm_term, '[^_]+', 1, 1)
    WHEN LENGTH(cas.utm_term) - LENGTH(REPLACE(cas.utm_term, '-', '')) = 2
    THEN REGEXP_SUBSTR(cas.utm_term, '[^-]+', 1, 1)
    WHEN LOWER(SUBSTR(cas.utm_term, 4 * -1)) = LOWER('pmax')
    THEN SUBSTR(cas.utm_term, 1, INSTR(cas.utm_term, '_') - 1)
    WHEN LENGTH(cas.utm_term) - LENGTH(REPLACE(cas.utm_term, '%5F', '')) >= 6
    THEN SUBSTR(cas.utm_term, 1, STRPOS(LOWER(cas.utm_term), LOWER('%5F')) - 1)
    ELSE NULL
    END
   ELSE NULL
   END
  ELSE NULL
  END AS campaign_id,
 TRIM(CASE
   WHEN dd.day_date > DATE '2023-12-01'
   THEN CASE
    WHEN LOWER(cas.utm_campaign) LIKE LOWER('%afterpay%')
    THEN '5188900'
    WHEN LOWER(cas.utm_channel) LIKE LOWER('%@_ex@_%')
    THEN CASE
     WHEN LOWER(cas.utm_campaign) LIKE LOWER('%e012894%')
     THEN NULL
     WHEN LOWER(cas.utm_campaign) = LOWER('general_j009192')
     THEN NULL
     WHEN LOWER(cas.utm_campaign) LIKE LOWER('%j01%')
     THEN NULL
     WHEN LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas.utm_campaign, '_', '')) = 8 OR LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas
          .utm_campaign, '_', '')) = 5
     THEN SUBSTR(cas.utm_campaign, INSTR(cas.utm_campaign, '_', 1, 3) + 1, INSTR(cas.utm_campaign, '_', 1, 4) - INSTR(cas
         .utm_campaign, '_', 1, 3) - 1)
     WHEN LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas.utm_campaign, '_', '')) = 3 OR LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas
          .utm_campaign, '_', '')) = 7
     THEN SUBSTR(cas.utm_campaign, INSTR(cas.utm_campaign, '_', 1, 2) + 1, INSTR(cas.utm_campaign, '_', 1, 3) - INSTR(cas
         .utm_campaign, '_', 1, 2) - 1)
     ELSE NULL
     END
    ELSE NULL
    END
   ELSE NULL
   END) AS campaign_brand_id,
  CASE
  WHEN LOWER(cas.utm_campaign) LIKE LOWER('%n_202%')
  THEN SUBSTR(cas.utm_campaign, LENGTH(cas.utm_campaign) - STRPOS(LOWER(REVERSE(cas.utm_campaign)), LOWER('_')) + 2)
  WHEN LOWER(cas.utm_campaign) LIKE LOWER('%nr_202%')
  THEN SUBSTR(cas.utm_campaign, LENGTH(cas.utm_campaign) - STRPOS(LOWER(REVERSE(cas.utm_campaign)), LOWER('_')) + 2)
  ELSE NULL
  END AS placementsio_id
FROM `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_sales_fact AS cas
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd ON cas.business_day_date = dd.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(cas.sku_num) = LOWER(sku.rms_sku_num) AND LOWER(cas.channel_country
    ) = LOWER(sku.channel_country)
WHERE dd.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY dd.day_date,
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
 cas.utm_content,
 cas.supplier_name,
 cas.brand_name,
 division,
 subdivision,
 department;



CREATE TEMPORARY TABLE IF NOT EXISTS cas_ly
AS
SELECT dd.day_date,
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
 cas.utm_content,
 cas.supplier_name,
 cas.brand_name,
   TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
   TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
   TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
 0 AS ty_gross_sales_amt,
 0 AS ty_gross_sales_units,
 SUM(cas.gross_sales_amt_usd) AS ly_gross_sales_amt,
 SUM(cas.gross_sales_units) AS ly_gross_sales_units,
  CASE
  WHEN dd.day_date > DATE '2023-12-01'
  THEN CASE
   WHEN LOWER(cas.utm_term) LIKE LOWER('social@_%')
   THEN NULL
   WHEN LOWER(cas.utm_term) LIKE LOWER('%campaign%')
   THEN NULL
   WHEN LOWER(cas.utm_channel) = LOWER('low_ex_email_marketing')
   THEN REGEXP_SUBSTR(cas.utm_term, '[^_]+', 1, 1)
   WHEN LOWER(cas.utm_channel) LIKE LOWER('%@_ex@_%')
   THEN CASE
    WHEN LENGTH(cas.utm_term) - LENGTH(REPLACE(cas.utm_term, '_', '')) = 2
    THEN REGEXP_SUBSTR(cas.utm_term, '[^_]+', 1, 1)
    WHEN LENGTH(cas.utm_term) - LENGTH(REPLACE(cas.utm_term, '-', '')) = 2
    THEN REGEXP_SUBSTR(cas.utm_term, '[^-]+', 1, 1)
    WHEN LOWER(SUBSTR(cas.utm_term, 4 * -1)) = LOWER('pmax')
    THEN SUBSTR(cas.utm_term, 1, INSTR(cas.utm_term, '_') - 1)
    WHEN LENGTH(cas.utm_term) - LENGTH(REPLACE(cas.utm_term, '%5F', '')) >= 6
    THEN SUBSTR(cas.utm_term, 1, STRPOS(LOWER(cas.utm_term), LOWER('%5F')) - 1)
    ELSE NULL
    END
   ELSE NULL
   END
  ELSE NULL
  END AS campaign_id,
 TRIM(CASE
   WHEN dd.day_date > DATE '2023-12-01'
   THEN CASE
    WHEN LOWER(cas.utm_campaign) LIKE LOWER('%afterpay%')
    THEN '5188900'
    WHEN LOWER(cas.utm_channel) = LOWER('low_ex_email_marketing')
    THEN NULL
    WHEN LOWER(cas.utm_channel) LIKE LOWER('%@_ex@_%')
    THEN CASE
     WHEN LOWER(cas.utm_campaign) LIKE LOWER('%e012894%')
     THEN NULL
     WHEN LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas.utm_campaign, '_', '')) = 8 OR LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas
          .utm_campaign, '_', '')) = 5
     THEN SUBSTR(cas.utm_campaign, INSTR(cas.utm_campaign, '_', 1, 3) + 1, INSTR(cas.utm_campaign, '_', 1, 4) - INSTR(cas
         .utm_campaign, '_', 1, 3) - 1)
     WHEN LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas.utm_campaign, '_', '')) = 3 OR LENGTH(cas.utm_campaign) - LENGTH(REPLACE(cas
          .utm_campaign, '_', '')) = 7
     THEN SUBSTR(cas.utm_campaign, INSTR(cas.utm_campaign, '_', 1, 2) + 1, INSTR(cas.utm_campaign, '_', 1, 3) - INSTR(cas
         .utm_campaign, '_', 1, 2) - 1)
     ELSE NULL
     END
    ELSE NULL
    END
   ELSE NULL
   END) AS campaign_brand_id,
  CASE
  WHEN LOWER(cas.utm_campaign) LIKE LOWER('%n_202%')
  THEN SUBSTR(cas.utm_campaign, LENGTH(cas.utm_campaign) - STRPOS(LOWER(REVERSE(cas.utm_campaign)), LOWER('_')) + 2)
  WHEN LOWER(cas.utm_campaign) LIKE LOWER('%nr_202%')
  THEN SUBSTR(cas.utm_campaign, LENGTH(cas.utm_campaign) - STRPOS(LOWER(REVERSE(cas.utm_campaign)), LOWER('_')) + 2)
  ELSE NULL
  END AS placementsio_id
FROM `{{params.gcp_project_id}}`.t2dl_das_nmn.campaign_attributed_sales_fact AS cas
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd ON cas.business_day_date = dd.day_date_last_year_realigned
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(cas.sku_num) = LOWER(sku.rms_sku_num) AND LOWER(cas.channel_country
    ) = LOWER(sku.channel_country)
WHERE dd.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY dd.day_date,
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
 cas.utm_content,
 cas.supplier_name,
 cas.brand_name,
 division,
 subdivision,
 department;



CREATE TEMPORARY TABLE IF NOT EXISTS cas_agg

AS
SELECT day_date,
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
 utm_content,
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
 campaign_id,
 campaign_brand_id,
 placementsio_id,
 ly_gross_sales_amt,
 ly_gross_sales_units
FROM cas_ty
UNION ALL
SELECT day_date,
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
 utm_content,
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
 campaign_id,
 campaign_brand_id,
 placementsio_id,
 ly_gross_sales_amt,
 ly_gross_sales_units
FROM cas_ly;



CREATE TEMPORARY TABLE IF NOT EXISTS caf_cas

AS
SELECT day_date,
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
 utm_content,
 supplier_name,
 brand_name,
 division,
 subdivision,
 department,
 campaign_id,
 campaign_brand_id,
 placementsio_id,
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
FROM caf_agg
GROUP BY day_date,
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
 utm_content,
 supplier_name,
 brand_name,
 division,
 subdivision,
 department,
 campaign_id,
 campaign_brand_id,
 placementsio_id
UNION ALL
SELECT day_date,
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
 utm_content,
 supplier_name,
 brand_name,
 division,
 subdivision,
 department,
 campaign_id,
 campaign_brand_id,
 placementsio_id,
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
FROM cas_agg
GROUP BY day_date,
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
 utm_content,
 supplier_name,
 brand_name,
 division,
 subdivision,
 department,
 campaign_id,
 campaign_brand_id,
 placementsio_id;



CREATE TEMPORARY TABLE IF NOT EXISTS cad_fact_stg

AS
SELECT day_date,
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
 utm_content,
 supplier_name,
 brand_name,
 division,
 subdivision,
 department,
 campaign_id,
 campaign_brand_id,
 placementsio_id,
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
FROM caf_cas
GROUP BY day_date,
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
 utm_content,
 supplier_name,
 brand_name,
 division,
 subdivision,
 department,
 campaign_id,
 campaign_brand_id,
 placementsio_id;



DELETE FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.campaign_attributed_daily_fact
WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}};



INSERT INTO `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.campaign_attributed_daily_fact 
SELECT day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, ty_product_views, ty_instock_product_views, ty_cart_add_units, ty_order_units, ty_gross_sales_amt, ty_gross_sales_units, ly_product_views,ly_instock_product_views, ly_cart_add_units, ly_order_units, ly_gross_sales_amt, ly_gross_sales_units, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM cad_fact_stg AS a;



--COLLECT STATISTICS COLUMN (DAY_DATE, CHANNEL_NUM, CHANNEL_COUNTRY, CHANNEL_BANNER, MKTG_TYPE, FINANCE_ROLLUP, FINANCE_DETAIL, UTM_SOURCE, UTM_CHANNEL,utm_campaign, utm_term, UTM_CONTENT, SUPPLIER_NAME, BRAND_NAME, DIVISION, SUBDIVISION, DEPARTMENT, campaign_id, campaign_brand_id, placementsio_id) ON t2dl_das_nmn.CAMPAIGN_ATTRIBUTED_DAILY_FACT;
--COLLECT STATISTICS COLUMN (DAY_DATE) ON t2dl_das_nmn.CAMPAIGN_ATTRIBUTED_DAILY_FACT;
--COLLECT STATISTICS COLUMN (BRAND_NAME ,SUBDIVISION) ON t2dl_das_nmn.CAMPAIGN_ATTRIBUTED_DAILY_FACT;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
