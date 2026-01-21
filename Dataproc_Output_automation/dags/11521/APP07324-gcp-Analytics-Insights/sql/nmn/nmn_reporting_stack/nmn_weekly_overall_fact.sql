-- SET QUERY_BAND = 'App_ID=APP08172;
--      DAG_ID=nmn_weekly_overall_fact_11521_ACE_ENG;
--      Task_Name=nmn_weekly_overall_fact;'
--      FOR SESSION VOLATILE;

/*

T2/Table Name: T2dl_DAS_NMN.nmn_weekly_overall_fact
Team/Owner: NMN
Date Created/Modified:

Note:
-- To Support the NMN Report view 1 
-- Weekly Refresh
*/
CREATE TEMPORARY TABLE IF NOT EXISTS variable_start_date
AS
SELECT week_idnt,
 MIN(day_date) AS day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date = {{params.start_date}}
GROUP BY week_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS variable_end_date
AS
SELECT week_idnt,
 MIN(day_date) AS day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date = {{params.end_date}}
GROUP BY week_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS ly_week_mapping
AS
SELECT DISTINCT TRIM(FORMAT('%11d', a.week_idnt)) AS week_idnt,
 TRIM(FORMAT('%11d', b.week_idnt)) AS last_year_wk_idnt,
 TRIM(FORMAT('%11d', a.month_idnt)) AS month_idnt,
 TRIM(FORMAT('%11d', a.quarter_idnt)) AS quarter_idnt,
 TRIM(FORMAT('%11d', a.fiscal_year_num)) AS fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
 LEFT JOIN (SELECT DISTINCT week_idnt,
   day_idnt,
   day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS b ON a.day_idnt_last_year = b.day_idnt
WHERE a.week_idnt BETWEEN 201901 AND 202552;


CREATE TEMPORARY TABLE IF NOT EXISTS sku_id_map
AS
SELECT DISTINCT TRIM(sku.rms_sku_num) AS rms_sku_num,
 sku.channel_country,
 TRIM(FORMAT('%11d', CAST(TRUNC(CAST(CONCAT(sku.div_num, ', ', sku.div_desc)AS FLOAT64)) AS INT64))) AS div_desc,
 sku.div_num,
 TRIM(FORMAT('%11d', CAST(TRUNC(CAST(CONCAT(sku.grp_num, ', ', sku.grp_desc)AS FLOAT64)) AS INT64))) AS grp_desc,
 sku.grp_num,
 TRIM(FORMAT('%11d', CAST(TRUNC(CAST(CONCAT(sku.dept_num, ', ', sku.dept_desc)AS FLOAT64)) AS INT64))) AS dept_desc,
 sku.dept_num,
 TRIM(FORMAT('%11d', CAST(TRUNC(CAST(CONCAT(sku.class_num, ', ', sku.class_desc)AS FLOAT64)) AS INT64))) AS class_desc,
 sku.class_num,
 sku.brand_name,
 v.npg_flag,
 v.vendor_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v 
 ON LOWER(sku.prmy_supp_num) = LOWER(v.vendor_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS vr 
 ON LOWER(sku.prmy_supp_num) = LOWER(vr.order_from_vendor_num
   )
WHERE LOWER(v.vendor_status_code) = LOWER('A')
 AND LOWER(vr.payto_vendor_status_code) = LOWER('A')
 AND LOWER(sku.channel_country) = LOWER('US');


CREATE TEMPORARY TABLE IF NOT EXISTS sales_units_week_level
AS
SELECT a.chnl_idnt,
 TRIM(FORMAT('%11d', a.week_idnt)) AS week_idnt,
 TRIM(lw.last_year_wk_idnt) AS last_year_wk_idnt,
 TRIM(lw.month_idnt) AS month_idnt,
 TRIM(lw.quarter_idnt) AS quarter_idnt,
 TRIM(lw.fiscal_year_num) AS fiscal_year_num,
 a.country,
 a.sku_idnt,
 SUM(a.sales_dollars) AS sales_dollars,
 SUM(a.return_dollars) AS return_dollars,
 SUM(a.sales_units) AS sales_units,
 SUM(a.return_units) AS return_units,
 SUM(a.eoh_units) AS eoh_units
FROM t2dl_das_in_season_management_reporting.wbr_merch_staging AS a
 LEFT JOIN ly_week_mapping AS lw ON a.week_idnt = CAST(lw.week_idnt AS FLOAT64)
WHERE a.week_idnt BETWEEN (SELECT week_idnt
   FROM variable_start_date) AND (SELECT week_idnt
   FROM variable_end_date)
 AND LOWER(a.country) = LOWER('US')
GROUP BY a.chnl_idnt,
 week_idnt,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 a.country,
 a.sku_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_units_final
AS
SELECT a.chnl_idnt,
 a.week_idnt,
 a.last_year_wk_idnt,
 a.month_idnt,
 a.quarter_idnt,
 a.fiscal_year_num,
 b.channel_country,
 b.div_desc,
 b.div_num,
 b.grp_desc,
 b.grp_num,
 b.dept_desc,
 b.dept_num,
 b.class_desc,
 b.class_num,
 b.brand_name,
 b.npg_flag,
 b.vendor_name,
 SUM(a.sales_dollars) AS sales_dollars,
 SUM(a.return_dollars) AS return_dollars,
 SUM(a.sales_units) AS sales_units,
 SUM(a.return_units) AS return_units,
 SUM(a.eoh_units) AS eoh_units
FROM sales_units_week_level AS a
 LEFT JOIN sku_id_map AS b ON LOWER(a.sku_idnt) = LOWER(b.rms_sku_num) AND LOWER(a.country) = LOWER(b.channel_country)
WHERE LOWER(a.country) = LOWER('US')
 AND CAST(TRUNC(CAST(a.week_idnt AS FLOAT64)) AS INTEGER) BETWEEN (SELECT week_idnt
   FROM variable_start_date) AND (SELECT week_idnt
   FROM variable_end_date)
GROUP BY a.chnl_idnt,
 a.week_idnt,
 a.last_year_wk_idnt,
 a.month_idnt,
 a.quarter_idnt,
 a.fiscal_year_num,
 b.channel_country,
 b.div_desc,
 b.div_num,
 b.grp_desc,
 b.grp_num,
 b.dept_desc,
 b.dept_num,
 b.class_desc,
 b.class_num,
 b.brand_name,
 b.npg_flag,
 b.vendor_name;


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_week_level
AS
SELECT a.rms_sku_num,
 a.channel_num,
 a.week_num,
 b.last_year_wk_idnt,
 COALESCE(SUM(a.receipts_regular_units + a.receipts_crossdock_regular_units), 0) AS receipt_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
 LEFT JOIN ly_week_mapping AS b ON a.week_num = CAST(b.week_idnt AS FLOAT64)
WHERE a.week_num BETWEEN (SELECT week_idnt
   FROM variable_start_date) AND (SELECT week_idnt
   FROM variable_end_date)
GROUP BY a.rms_sku_num,
 a.channel_num,
 a.week_num,
 b.last_year_wk_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_table_final
AS
SELECT a.channel_num,
 TRIM(FORMAT('%11d', a.week_num)) AS week_num,
 TRIM(a.last_year_wk_idnt) AS last_year_wk_idnt,
 TRIM(FORMAT('%11d', dd.month_idnt)) AS month_idnt,
 TRIM(FORMAT('%11d', dd.quarter_idnt)) AS quarter_idnt,
 TRIM(FORMAT('%11d', dd.fiscal_year_num)) AS fiscal_year_num,
 b.channel_country,
 b.div_desc,
 b.div_num,
 b.brand_name,
 b.npg_flag,
 b.grp_desc,
 b.grp_num,
 b.dept_desc,
 b.dept_num,
 b.class_desc,
 b.class_num,
 b.vendor_name,
 SUM(a.receipt_units) AS receipt_units
FROM receipts_week_level AS a
 LEFT JOIN (SELECT DISTINCT channel_num,
   store_country_code
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim) AS c ON a.channel_num = c.channel_num
 LEFT JOIN sku_id_map AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(c.store_country_code) = LOWER(b.channel_country
    )
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd ON a.week_num = dd.week_idnt
WHERE a.week_num BETWEEN (SELECT week_idnt
   FROM variable_start_date) AND (SELECT week_idnt
   FROM variable_end_date)
 AND LOWER(c.store_country_code) = LOWER('US')
GROUP BY a.channel_num,
 week_num,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 b.channel_country,
 b.div_desc,
 b.div_num,
 b.brand_name,
 b.npg_flag,
 b.grp_desc,
 b.grp_num,
 b.dept_desc,
 b.dept_num,
 b.class_desc,
 b.class_num,
 b.vendor_name;


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_sales_merged
AS
SELECT COALESCE(a.chnl_idnt, FORMAT('%11d', b.channel_num)) AS channel_num,
 COALESCE(a.channel_country, b.channel_country) AS channel_country,
 COALESCE(a.week_idnt, b.week_num) AS week_idnt,
 COALESCE(a.last_year_wk_idnt, b.last_year_wk_idnt) AS last_year_wk_idnt,
 COALESCE(a.month_idnt, b.month_idnt) AS month_idnt,
 COALESCE(a.quarter_idnt, b.quarter_idnt) AS quarter_idnt,
 COALESCE(a.fiscal_year_num, b.fiscal_year_num) AS fiscal_year_num,
 COALESCE(a.div_desc, b.div_desc) AS div_desc,
 COALESCE(a.div_num, b.div_num) AS div_num,
 COALESCE(a.grp_desc, b.grp_desc) AS grp_desc,
 COALESCE(a.grp_num, b.grp_num) AS grp_num,
 COALESCE(a.dept_desc, b.dept_desc) AS dept_desc,
 COALESCE(a.dept_num, b.dept_num) AS dept_num,
 COALESCE(a.class_desc, b.class_desc) AS class_desc,
 COALESCE(a.class_num, b.class_num) AS class_num,
 COALESCE(a.brand_name, b.brand_name) AS brand_name,
 COALESCE(a.npg_flag, b.npg_flag) AS npg_flag,
 COALESCE(a.vendor_name, b.vendor_name) AS vendor_name,
 COALESCE(SUM(a.sales_dollars), 0) AS sales_dollars,
 COALESCE(SUM(a.sales_units), 0) AS sales_units,
 COALESCE(SUM(a.return_dollars), 0) AS return_dollars,
 COALESCE(SUM(a.return_units), 0) AS return_units,
 COALESCE(SUM(b.receipt_units), 0) AS receipt_units,
 COALESCE(SUM(a.eoh_units), 0) AS eoh_units
FROM sales_units_final AS a
 FULL JOIN receipts_table_final AS b 
 ON CAST(a.chnl_idnt AS FLOAT64) = b.channel_num 
 AND LOWER(a.channel_country) = LOWER(b.channel_country) 
 AND LOWER(a.week_idnt) = LOWER(b.week_num) 
 AND LOWER(a.last_year_wk_idnt) = LOWER(b.last_year_wk_idnt) 
 AND LOWER(a.month_idnt) = LOWER(b.month_idnt) 
 AND LOWER(a.quarter_idnt) =  LOWER(b.quarter_idnt) 
 AND LOWER(a.fiscal_year_num) = LOWER(b.fiscal_year_num) AND a.div_num = b.div_num
 AND LOWER(a.div_desc) = LOWER(b.div_desc) AND LOWER(a.grp_desc) = LOWER(b.grp_desc) AND a.grp_num = b.grp_num
 AND LOWER(a.dept_desc) = LOWER(b.dept_desc) AND a.dept_num = b.dept_num AND LOWER(a.class_desc) = LOWER(b.class_desc
        ) AND a.class_num = b.class_num AND LOWER(a.brand_name) = LOWER(b.brand_name) AND LOWER(a.npg_flag) = LOWER(b.npg_flag
     ) AND LOWER(a.vendor_name) = LOWER(b.vendor_name)
WHERE LOWER(a.channel_country) = LOWER('US')
GROUP BY channel_num,
 channel_country,
 week_idnt,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 div_desc,
 div_num,
 grp_desc,
 grp_num,
 dept_desc,
 dept_num,
 class_desc,
 class_num,
 brand_name,
 npg_flag,
 vendor_name;


CREATE TEMPORARY TABLE IF NOT EXISTS pv_order_week_level
AS
SELECT CASE
  WHEN LOWER(b.channel) = LOWER('FULL_LINE')
  THEN '120'
  WHEN LOWER(b.channel) = LOWER('RACK')
  THEN '250'
  ELSE 'Others'
  END AS channel_num,
 TRIM(FORMAT('%11d', a.week_idnt)) AS week_idnt,
 TRIM(FORMAT('%11d', a.month_idnt)) AS month_idnt,
 TRIM(FORMAT('%11d', a.quarter_idnt)) AS quarter_idnt,
 TRIM(FORMAT('%11d', a.fiscal_year_num)) AS fiscal_year_num,
 b.sku_id,
 b.channelcountry,
 SUM(b.product_views) AS product_views,
 SUM(b.order_quantity) AS order_quantity
FROM t2dl_das_product_funnel.product_funnel_daily AS b
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a ON b.event_date_pacific = a.day_date
WHERE b.event_date_pacific BETWEEN (SELECT day_date
   FROM variable_start_date) AND (SELECT day_date
   FROM variable_end_date)
 AND LOWER(b.channelcountry) = LOWER('US')
GROUP BY channel_num,
 week_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 b.sku_id,
 b.channelcountry;


CREATE TEMPORARY TABLE IF NOT EXISTS pv_order_final
AS
SELECT a.channel_num,
 a.week_idnt,
 c.last_year_wk_idnt,
 a.month_idnt,
 a.quarter_idnt,
 a.fiscal_year_num,
 b.channel_country,
 b.div_desc,
 b.div_num,
 b.grp_desc,
 b.grp_num,
 b.dept_desc,
 b.dept_num,
 b.class_desc,
 b.class_num,
 b.brand_name,
 b.npg_flag,
 b.vendor_name,
 SUM(a.product_views) AS product_views,
 SUM(a.order_quantity) AS order_quantity
FROM pv_order_week_level AS a
 LEFT JOIN sku_id_map AS b ON LOWER(b.rms_sku_num) = LOWER(a.sku_id) AND LOWER(b.channel_country) = LOWER(a.channelcountry
    )
 LEFT JOIN ly_week_mapping AS c ON LOWER(c.week_idnt) = LOWER(a.week_idnt)
WHERE LOWER(a.channelcountry) = LOWER('US')
 AND CAST(TRUNC(CAST(a.week_idnt AS FLOAT64)) AS INTEGER) BETWEEN (SELECT week_idnt
   FROM variable_start_date) AND (SELECT week_idnt
   FROM variable_end_date)
GROUP BY a.channel_num,
 a.week_idnt,
 c.last_year_wk_idnt,
 a.month_idnt,
 a.quarter_idnt,
 a.fiscal_year_num,
 b.channel_country,
 b.div_desc,
 b.div_num,
 b.grp_desc,
 b.grp_num,
 b.dept_desc,
 b.dept_num,
 b.class_desc,
 b.class_num,
 b.brand_name,
 b.npg_flag,
 b.vendor_name;


CREATE TEMPORARY TABLE IF NOT EXISTS merged_table_ty
AS
SELECT CAST(TRUNC(CAST(COALESCE(a.channel_num, b.channel_num) AS FLOAT64)) AS INT64) AS channel_num,
 COALESCE(a.channel_country, b.channel_country) AS channel_country,
 CAST(TRUNC(CAST(COALESCE(a.week_idnt, b.week_idnt)AS FLOAT64)) AS INT64) AS week_idnt,
 CAST(TRUNC(CAST(COALESCE(a.last_year_wk_idnt, b.last_year_wk_idnt)AS FLOAT64)) AS INT64) AS last_year_wk_idnt,
 CAST(TRUNC(CAST(COALESCE(a.month_idnt, b.month_idnt)AS FLOAT64)) AS INT64) AS month_idnt,
 CAST(TRUNC(CAST(COALESCE(a.quarter_idnt, b.quarter_idnt)AS FLOAT64)) AS INT64) AS quarter_idnt,
 CAST(TRUNC(CAST(COALESCE(a.fiscal_year_num, b.fiscal_year_num)AS FLOAT64)) AS INT64)  AS fiscal_year_num,
 COALESCE(a.div_desc, b.div_desc) AS div_desc,
 COALESCE(a.div_num, b.div_num) AS div_num,
 COALESCE(a.grp_desc, b.grp_desc) AS grp_desc,
 COALESCE(a.grp_num, b.grp_num) AS grp_num,
 COALESCE(a.dept_desc, b.dept_desc) AS dept_desc,
 COALESCE(a.dept_num, b.dept_num) AS dept_num,
 COALESCE(a.class_desc, b.class_desc) AS class_desc,
 COALESCE(a.class_num, b.class_num) AS class_num,
 COALESCE(a.brand_name, b.brand_name) AS brand_name,
 COALESCE(a.npg_flag, b.npg_flag) AS npg_flag,
 COALESCE(a.vendor_name, b.vendor_name) AS vendor_name,
 COALESCE(SUM(a.sales_dollars), 0) AS sales_dollars,
 COALESCE(SUM(a.sales_units), 0) AS sales_units,
 COALESCE(SUM(a.return_dollars), 0) AS return_dollars,
 COALESCE(SUM(a.return_units), 0) AS return_units,
 COALESCE(SUM(a.receipt_units), 0) AS receipt_units,
 COALESCE(SUM(b.product_views), 0) AS product_views,
 COALESCE(SUM(b.order_quantity), 0) AS order_quantity,
 COALESCE(SUM(a.eoh_units), 0) AS eoh_units
FROM receipts_sales_merged AS a
 FULL JOIN pv_order_final AS b ON LOWER(a.channel_num) = LOWER(b.channel_num) AND LOWER(a.channel_country) = LOWER(b.channel_country
                    ) AND LOWER(a.week_idnt) = LOWER(b.week_idnt) AND LOWER(a.last_year_wk_idnt) = LOWER(b.last_year_wk_idnt
                  ) AND LOWER(a.month_idnt) = LOWER(b.month_idnt) AND LOWER(a.quarter_idnt) = LOWER(b.quarter_idnt) AND
              LOWER(a.fiscal_year_num) = LOWER(b.fiscal_year_num) AND a.div_num = b.div_num AND LOWER(a.div_desc) =
            LOWER(b.div_desc) AND LOWER(a.grp_desc) = LOWER(b.grp_desc) AND a.grp_num = b.grp_num AND LOWER(a.dept_desc
          ) = LOWER(b.dept_desc) AND a.dept_num = b.dept_num AND LOWER(a.class_desc) = LOWER(b.class_desc) AND a.class_num
       = b.class_num AND LOWER(a.brand_name) = LOWER(b.brand_name) AND LOWER(a.npg_flag) = LOWER(b.npg_flag) AND LOWER(a
    .vendor_name) = LOWER(b.vendor_name)
GROUP BY channel_num,
 channel_country,
 week_idnt,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 div_desc,
 div_num,
 grp_desc,
 grp_num,
 dept_desc,
 dept_num,
 class_desc,
 class_num,
 brand_name,
 npg_flag,
 vendor_name;

delete `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.nmn_weekly_overall_fact 
where week_idnt between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date);

INSERT INTO `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.nmn_weekly_overall_fact
select a.*,current_datetime('PST8PDT') as dw_sys_load_tmstp from merged_table_ty a;

-- COLLECT STATISTICS  
--                     COLUMN (channel_num), 
--                     COLUMN (channel_country),
--                     COLUMN (week_idnt),
--                     COLUMN (div_num),
--                     COLUMN (grp_num),
--                     COLUMN (dept_num),
--                     COLUMN (class_num),
--                     COLUMN (brand_name),
--                     COLUMN (supplier_name)
-- on t2dl_das_nmn.nmn_weekly_overall_fact;
-- SET QUERY_BAND = NONE FOR SESSION;
