BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08629;
DAG_ID=mta_publisher_daily_11521_ACE_ENG;
---     Task_Name=mta_publisher_daily;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS start_varibale AS
SELECT MIN(day_date) AS start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.start_date}} );

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS end_varibale AS
SELECT MAX(day_date) AS end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.end_date}});

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS _variables AS
SELECT start_varibale.start_date,
 end_varibale.end_date
FROM start_varibale
 INNER JOIN end_varibale ON TRUE;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup AS
SELECT day_cal.day_date AS day_dt,
 day_cal.last_year_day_date AS ly_day_dt,
 DATE_SUB(MIN(CASE
    WHEN day_cal.last_year_day_date_realigned IS NULL
    THEN DATE_SUB(day_cal.day_date, INTERVAL 364 DAY)
    ELSE day_cal.last_year_day_date_realigned
    END), INTERVAL 1 DAY) AS ly_start_dt,
 DATE_SUB(MIN(day_cal.day_date), INTERVAL 1 DAY) AS ly_end_dt,
 MAX(day_cal.day_date) AS ty_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
 INNER JOIN `_variables` ON TRUE
WHERE day_cal.day_date between `_variables`.start_date AND `_variables`.end_date
GROUP BY day_dt,
 ly_day_dt;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS acquired_cust AS
SELECT DISTINCT a.acp_id,
 a.aare_status_date AS day_date,
 a.aare_chnl_code AS box,
 'NTN' AS aare_status_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS a
 INNER JOIN `_variables` ON TRUE
WHERE a.aare_status_date   between `_variables`.start_date AND `_variables`.end_date;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS activated_cust AS
SELECT DISTINCT a.acp_id,
 a.activated_date AS day_date,
 a.activated_channel AS box,
 'A' AS aare_status_code
FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.customer_activation AS a
 INNER JOIN `_variables` ON TRUE
WHERE a.activated_date  between `_variables`.start_date AND `_variables`.end_date;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly_stg_1 AS
SELECT DISTINCT dtl.acp_id,
 dl.ly_start_dt AS ly_dl_start_dt,
 dl.ly_end_dt AS ly_dl_end_dt,
 dl.ty_end_dt AS ty_dl_end_dt,
 COALESCE(dtl.order_date, dtl.tran_date) AS sale_date,
  CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS box
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
 ON dtl.intent_store_num = str.store_num
 LEFT JOIN date_lookup AS dl ON COALESCE(dtl.order_date, dtl.tran_date) = dl.day_dt
WHERE COALESCE(dtl.order_date, dtl.tran_date) BETWEEN (SELECT MIN(ly_start_dt) FROM date_lookup) AND (SELECT MAX(ty_end_dt) FROM date_lookup)
 AND LOWER(dtl.line_item_net_amt_currency_code) IN (LOWER('USD'))
 AND dtl.line_net_usd_amt > 0
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'))
 AND dtl.acp_id IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly_test_mapping AS
SELECT ly_stg_1.acp_id,
 MIN(ly_stg_1.ly_dl_start_dt) AS ly_dl_start_dt
FROM ly_stg_1
 INNER JOIN `_variables` ON TRUE
WHERE ly_stg_1.sale_date between `_variables`.start_date AND `_variables`.end_date
GROUP BY ly_stg_1.acp_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly_stg_2 AS
SELECT DISTINCT dtl.acp_id,
 dtl.ly_dl_start_dt,
 dtl.ly_dl_end_dt,
 dtl.ty_dl_end_dt,
 dtl.sale_date,
 dtl.box
FROM ly_stg_1 AS dtl
 INNER JOIN ly_test_mapping AS dl ON LOWER(dl.acp_id) = LOWER(dtl.acp_id) AND dtl.sale_date >= dl.ly_dl_start_dt;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS ly_stg_3 AS
SELECT acp_id,
 sale_date,
 box,
  CASE
  WHEN box_count2 > 0 OR box_count1 > 0
  THEN 1
  ELSE 0
  END AS ret_fl,
  CASE
  WHEN box_count1 - box_count2 <> 0
  THEN 1
  ELSE 0
  END AS engaged_fl
FROM (SELECT acp_id,
   sale_date,
   box,
     (COUNT(box) OVER (PARTITION BY acp_id, box ORDER BY sale_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      )) - 1 AS box_count1,
     (COUNT(box) OVER (PARTITION BY acp_id ORDER BY sale_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) -
    1 AS box_count2
  FROM ly_stg_2 AS a
  GROUP BY acp_id,
   sale_date,
   box) AS b
WHERE sale_date between (SELECT start_date FROM `_variables`) and (SELECT end_date FROM `_variables`);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly_stg_4 AS
SELECT DISTINCT a.acp_id,
 a.sale_date,
 a.box,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(b.acp_id) AND a.sale_date = b.day_date AND LOWER(a.box) = LOWER(b.box)
  THEN 0
  ELSE 1
  END AS reactivated_fl
FROM ly_stg_3 AS a
 LEFT JOIN (SELECT DISTINCT acp_id,
   day_date,
   box
  FROM acquired_cust) AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.sale_date = b.day_date AND LOWER(a.box) = LOWER(b
    .box)
WHERE a.ret_fl = 0
 AND a.engaged_fl = 0;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_aare AS
(((SELECT acp_id,
    day_date AS day_dt,
     CASE
     WHEN LOWER(box) IN (LOWER('FLS'), LOWER('NCOM'))
     THEN 'NORDSTROM'
     WHEN LOWER(box) IN (LOWER('RACK'), LOWER('NRHL'))
     THEN 'NORDSTROM RACK'
     ELSE NULL
     END AS banner,
    aare_status_code
   FROM acquired_cust
   UNION DISTINCT
   SELECT acp_id,
    day_date AS day_dt,
     CASE
     WHEN LOWER(box) IN (LOWER('FLS'), LOWER('NCOM'))
     THEN 'NORDSTROM'
     WHEN LOWER(box) IN (LOWER('RACK'), LOWER('NRHL'))
     THEN 'NORDSTROM RACK'
     ELSE NULL
     END AS banner,
    aare_status_code
   FROM activated_cust)
  UNION DISTINCT
  SELECT acp_id,
   sale_date AS day_dt,
    CASE
    WHEN LOWER(box) IN (LOWER('FLS'), LOWER('NCOM'))
    THEN 'NORDSTROM'
    WHEN LOWER(box) IN (LOWER('RACK'), LOWER('NRHL'))
    THEN 'NORDSTROM RACK'
    ELSE NULL
    END AS banner,
    CASE
    WHEN acp_id IS NOT NULL AND ret_fl = 1
    THEN 'R'
    ELSE NULL
    END AS aare_status_code
  FROM ly_stg_3
  WHERE ret_fl = 1)
 UNION DISTINCT
 SELECT acp_id,
  sale_date AS day_dt,
   CASE
   WHEN LOWER(box) IN (LOWER('FLS'), LOWER('NCOM'))
   THEN 'NORDSTROM'
   WHEN LOWER(box) IN (LOWER('RACK'), LOWER('NRHL'))
   THEN 'NORDSTROM RACK'
   ELSE NULL
   END AS banner,
   CASE
   WHEN ret_fl = 1 AND engaged_fl = 1 AND acp_id IS NOT NULL
   THEN 'E'
   ELSE NULL
   END AS aare_status_code
 FROM ly_stg_3
 WHERE ret_fl = 1
  AND engaged_fl = 1)
UNION DISTINCT
SELECT acp_id,
 sale_date AS day_dt,
  CASE
  WHEN LOWER(box) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'NORDSTROM'
  WHEN LOWER(box) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN acp_id IS NOT NULL AND reactivated_fl = 1
  THEN 'RA'
  ELSE NULL
  END AS aare_status_code
FROM ly_stg_4
WHERE reactivated_fl = 1
GROUP BY acp_id,
 day_dt,
 banner,
 aare_status_code;
 
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS cust_aare_weightage AS
SELECT acp_id,
 day_dt,
 banner,
 aare_status_code,
  1 / CAST(COUNT(*) OVER (PARTITION BY acp_id, day_dt, banner RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC) AS weightage
FROM cust_aare;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS mta_staging_tbl1 AS
SELECT a.order_date_pacific AS day_dt,
 a.acp_id,
 a.order_number,
 a.sku_id AS sku_num,
 UPPER(a.utm_campaign) AS utm_campaign,
 f.publisher,
 f.publisher_group,
 f.publisher_subgroup,
 b.nordy_level AS loyalty_status,
  CASE
  WHEN LOWER(a.arrived_channel) = LOWER('RACK')
  THEN 'NORDSTROM RACK'
  WHEN LOWER(a.arrived_channel) = LOWER('FULL_LINE')
  THEN 'NORDSTROM'
  ELSE NULL
  END AS banner,
 c.div_desc AS division,
 c.grp_desc AS sub_division,
 c.dept_desc AS department,
 c.brand_name,
 c.npg_ind AS npg_code,
 e.price_type,
 SUM(a.attributed_demand) AS attributed_demand,
 SUM(a.attributed_orders) AS attributed_orders,
 SUM(a.attributed_pred_net) AS attributed_net_sales
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mta_acp_scoring_fact AS a
 LEFT JOIN (SELECT acp_id,
   nordy_level
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer
  GROUP BY acp_id,
   nordy_level) AS b
 ON LOWER(a.acp_id) = LOWER(b.acp_id)
 LEFT JOIN (SELECT rms_sku_num,
   div_desc,
   grp_desc,
   dept_desc,
   brand_name,
   npg_ind
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE LOWER(channel_country) = LOWER('US')
  GROUP BY rms_sku_num,
   dept_desc,
   grp_desc,
   div_desc,
   brand_name,
   npg_ind) AS c ON LOWER(a.sku_id) = LOWER(c.rms_sku_num)
 LEFT JOIN (SELECT order_line_detail_fact.order_num AS order_number,
   order_line_detail_fact.sku_num,
   SUM(order_line_detail_fact.order_line_current_amount_usd) AS amount
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
   INNER JOIN `_variables` ON TRUE
  WHERE order_line_detail_fact.order_date_pacific between (SELECT start_date FROM `_variables`) and (SELECT end_date FROM `_variables`)
  GROUP BY order_number,
   order_line_detail_fact.sku_num) AS d ON LOWER(a.order_number) = LOWER(d.order_number) AND LOWER(a.sku_id) = LOWER(d.sku_num )
 LEFT JOIN (SELECT DISTINCT order_num AS order_number,
   sku_num,
    CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN 'REGULAR'
    WHEN LOWER(price_type) = LOWER('C')
    THEN 'CLEARANCE'
    WHEN LOWER(price_type) = LOWER('P')
    THEN 'PROMOTIONAL'
    ELSE 'UNKNOWN'
    END AS price_type,
   transaction_price_amt AS amount
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact
  WHERE global_tran_id IS NOT NULL) AS e ON LOWER(d.order_number) = LOWER(e.order_number) AND LOWER(d.sku_num) = LOWER(e.sku_num) AND d.amount = e.amount
 LEFT JOIN (SELECT DISTINCT UPPER(encrypted_id) AS encrypted_id,
   UPPER(publisher_name) AS publisher,
   UPPER(publisher_group) AS publisher_group,
   UPPER(publisher_subgroup) AS publisher_subgroup
  FROM `{{params.gcp_project_id}}`.t2dl_das_funnel_io.affiliate_publisher_mapping) AS f ON LOWER(UPPER(a.utm_campaign)) = LOWER(UPPER(f.encrypted_id)) AND f.encrypted_id IS NOT NULL
 INNER JOIN `_variables` AS `_variables0` ON CAST(a.order_date_pacific AS DATE) BETWEEN `_variables0`.start_date AND `_variables0`.end_date
WHERE LOWER(a.arrived_channel) IN (LOWER('RACK'), LOWER('FULL_LINE'))
 AND LOWER(a.utm_source) LIKE LOWER('%rakuten%')
 AND LOWER(a.finance_detail) LIKE LOWER('%affiliates%')
 AND LOWER(a.order_channelcountry) = LOWER('US')
GROUP BY day_dt,
 a.acp_id,
 a.order_number,
 sku_num,
 utm_campaign,
 f.publisher,
 f.publisher_group,
 f.publisher_subgroup,
 loyalty_status,
 banner,
 division,
 sub_division,
 department,
 c.brand_name,
 npg_code,
 e.price_type;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS mta_staging_tbl2 AS
SELECT a.day_dt,
 a.sku_num,
 a.utm_campaign,
 a.publisher,
 a.publisher_group,
 a.publisher_subgroup,
 b.aare_status_code,
 a.loyalty_status,
 a.banner,
 a.division,
 a.sub_division,
 a.department,
 a.brand_name,
 a.npg_code,
 a.price_type,
 SUM(a.attributed_demand * COALESCE(b.weightage, 1)) AS attributed_demand,
 SUM(a.attributed_orders * COALESCE(b.weightage, 1)) AS attributed_orders,
 SUM(a.attributed_net_sales * COALESCE(b.weightage, 1)) AS attributed_net_sales,
 SUM(CASE
   WHEN LOWER(b.aare_status_code) = LOWER('R')
   THEN a.attributed_orders * COALESCE(b.weightage, 1)
   ELSE 0
   END) AS retained_attributed_orders,
 SUM(CASE
   WHEN LOWER(b.aare_status_code) = LOWER('NTN')
   THEN a.attributed_orders * COALESCE(b.weightage, 1)
   ELSE 0
   END) AS acquired_attributed_orders,
 SUM(CASE
   WHEN LOWER(b.aare_status_code) = LOWER('NTN')
   THEN a.attributed_demand * COALESCE(b.weightage, 1)
   ELSE 0
   END) AS acquired_attributed_demand
FROM mta_staging_tbl1 AS a
 LEFT JOIN cust_aare_weightage AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.day_dt = b.day_dt AND LOWER(a.banner) = LOWER(b.banner)
GROUP BY a.day_dt,
 a.sku_num,
 a.utm_campaign,
 a.publisher,
 a.publisher_group,
 a.publisher_subgroup,
 b.aare_status_code,
 a.loyalty_status,
 a.banner,
 a.division,
 a.sub_division,
 a.department,
 a.brand_name,
 a.npg_code,
 a.price_type;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS apd_4_box_mta AS
SELECT day_dt,
 banner,
 utm_campaign,
 publisher,
 publisher_group,
 publisher_subgroup,
 aare_status_code,
 loyalty_status,
 division,
 sub_division,
 department,
 brand_name,
 npg_code,
 price_type,
 SUM(attributed_demand) AS attributed_demand,
 SUM(attributed_orders) AS attributed_orders,
 SUM(attributed_net_sales) AS attributed_net_sales,
 SUM(retained_attributed_orders) AS retained_attributed_orders,
 SUM(acquired_attributed_orders) AS acquired_attributed_orders,
 SUM(acquired_attributed_demand) AS acquired_attributed_demand
FROM mta_staging_tbl2 AS a
GROUP BY day_dt,
 banner,
 utm_campaign,
 publisher,
 publisher_group,
 publisher_subgroup,
 aare_status_code,
 loyalty_status,
 division,
 sub_division,
 department,
 brand_name,
 npg_code,
 price_type;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.apd_t2_schema}}.mta_publisher_daily
WHERE day_dt BETWEEN (SELECT start_date  FROM `_variables`) AND (SELECT end_date  FROM `_variables`);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.apd_t2_schema}}.mta_publisher_daily
(SELECT day_dt,
  banner,
  utm_campaign,
  publisher,
  publisher_group,
  publisher_subgroup,
  aare_status_code,
  loyalty_status,
  division,
  sub_division,
  department,
  brand_name,
  npg_code,
  price_type,
  CAST(attributed_demand AS FLOAT64) AS attributed_demand,
  CAST(attributed_orders AS FLOAT64) AS attributed_orders,
  CAST(attributed_net_sales AS FLOAT64) AS attributed_net_sales,
  CAST(retained_attributed_orders AS FLOAT64) AS retained_attributed_orders,
  CAST(acquired_attributed_orders AS FLOAT64) AS acquired_attributed_orders,
  CAST(acquired_attributed_demand AS FLOAT64) AS acquired_attributed_demand,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM apd_4_box_mta AS a);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS  COLUMN (PARTITION), COLUMN (DAY_DT,BANNER,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,AARE_STATUS_CODE,LOYALTY_STATUS,DIVISION,SUB_DIVISION,DEPARTMENT,BRAND_NAME,NPG_CODE,PRICE_TYPE), COLUMN (DAY_DT,BANNER) on `{{params.gcp_project_id}}`.{{params.apd_t2_schema}}.mta_publisher_daily;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
