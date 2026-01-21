BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=mmm_mth_sales_extract_11521_ACE_ENG;
---     Task_Name=mmm_mth_sales_extract;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS last_month_date_range
AS
SELECT currcal.month_idnt,
 prev_tbl.prev_month_idnt,
 prev_tbl.prev_month_start_day_date,
 prev_tbl.prev_month_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS currcal
 INNER JOIN (SELECT DISTINCT month_idnt AS curr_month_idnt,
   LAG(month_idnt) OVER (ORDER BY month_idnt) AS prev_month_idnt,
   LAG(month_start_day_date) OVER (ORDER BY month_idnt) AS prev_month_start_day_date,
   LAG(month_end_day_date) OVER (ORDER BY month_idnt) AS prev_month_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS prev_tbl ON currcal.month_idnt = prev_tbl.curr_month_idnt AND currcal.month_idnt
    > prev_tbl.prev_month_idnt
WHERE CURRENT_DATE('PST8PDT') = currcal.day_date;


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column ( prev_month_start_day_date, prev_month_end_day_date) on last_month_date_range;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS mmm_tran_extract (
business_day_date DATE,
global_tran_id BIGINT,
line_item_seq_num SMALLINT,
intent_store_num INTEGER,
upc_num STRING,
line_net_amt NUMERIC,
line_item_quantity NUMERIC,
month_idnt INTEGER
)
PARTITION BY business_day_date;


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET ERROR_CODE  =  0;

INSERT INTO mmm_tran_extract
(SELECT tdtl.business_day_date,
  tdtl.global_tran_id,
  tdtl.line_item_seq_num,
  tdtl.intent_store_num,
  tdtl.upc_num,
  tdtl.line_net_amt,
  tdtl.line_item_quantity,
  cal.prev_month_idnt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS tdtl
  INNER JOIN last_month_date_range AS cal ON tdtl.business_day_date BETWEEN cal.prev_month_start_day_date AND cal.prev_month_end_day_date
   
 WHERE LOWER(tdtl.line_item_merch_nonmerch_ind) = LOWER('merch'));
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column ( partition), column (business_day_date, upc_num) on mmm_tran_extract;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS merch_upc_dept (
upc_num STRING,
dept_num STRING
);


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET ERROR_CODE  =  0;


INSERT INTO merch_upc_dept
(SELECT LTRIM(upc.upc_num, '0') AS upc_num,
  SUBSTR(CAST(dpt.dept_num AS STRING), 1, 8) AS dept_num
 FROM (SELECT upc_num,
    rms_sku_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY LTRIM(upc_num, '0') ORDER BY prmy_upc_ind DESC, channel_country DESC)) = 1)
  AS upc
  INNER JOIN (SELECT rms_sku_num,
    dept_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num ORDER BY channel_country DESC)) = 1) AS sku ON LOWER(upc.rms_sku_num
    ) = LOWER(sku.rms_sku_num)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt ON sku.dept_num = dpt.dept_num AND LOWER(dpt.merch_dept_ind) = LOWER('y'
     ));


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column ( upc_num) on merch_upc_dept;
BEGIN
SET ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.mmm_t2_schema}}.mmm_mth_sales_extract
WHERE month_idnt = (SELECT prev_month_idnt
        FROM last_month_date_range);


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.mmm_t2_schema}}.mmm_mth_sales_extract
(SELECT tranx.month_idnt,
  tranx.business_day_date AS day_date,
  COALESCE(fpdiv.mmm_division, opdiv.mmm_division) AS division,
  tranx.intent_store_num AS store,
  str.business_unit_desc AS channel,
  SUM(CASE
    WHEN LOWER(pt.price_type) = LOWER('r')
    THEN tranx.line_net_amt
    ELSE 0
    END) AS reg_revenue,
  SUM(CASE
    WHEN LOWER(pt.price_type) = LOWER('r')
    THEN tranx.line_item_quantity
    ELSE 0
    END) AS reg_qty,
  SUM(CASE
    WHEN LOWER(pt.price_type) = LOWER('p')
    THEN tranx.line_net_amt
    ELSE 0
    END) AS promo_revenue,
  SUM(CASE
    WHEN LOWER(pt.price_type) = LOWER('p')
    THEN tranx.line_item_quantity
    ELSE 0
    END) AS promo_qty,
  SUM(CASE
    WHEN LOWER(pt.price_type) = LOWER('c')
    THEN tranx.line_net_amt
    ELSE 0
    END) AS clr_revenue,
  SUM(CASE
    WHEN LOWER(pt.price_type) = LOWER('c')
    THEN tranx.line_item_quantity
    ELSE 0
    END) AS clr_qty,

  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATE('PST8PDT')) AS DATETIME) AS STRING), 1, 19) AS DATETIME)) AS DATETIME)
  AS dw_sys_load_tmstp


 FROM mmm_tran_extract AS tranx
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON tranx.intent_store_num = str.store_num AND str.business_unit_num IN (1000
     , 2000, 3500, 9000, 9500)
  INNER JOIN merch_upc_dept AS dpt ON LOWER(LTRIM(tranx.upc_num, '0')) = LOWER(LTRIM(dpt.upc_num, '0'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mmm.jwn_dept_mmm_div_xref_fp AS fpdiv ON CAST(dpt.dept_num AS FLOAT64) = fpdiv.dept_num AND str.business_unit_num
     IN (1000, 3500, 9000)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mmm.jwn_dept_mmm_div_xref_op AS opdiv ON CAST(dpt.dept_num AS FLOAT64) = opdiv.dept_num AND str.business_unit_num
     IN (2000, 9500)
  INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_sales_returns.retail_tran_price_type_fact AS pt ON tranx.business_day_date = pt.business_day_date
    AND tranx.global_tran_id = pt.global_tran_id AND tranx.line_item_seq_num = pt.line_item_seq_num
 GROUP BY tranx.month_idnt,
  day_date,
  division,
  store,
  channel);
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column ( day_dt, division, store), column (month_idnt) on t2dl_das_mmm.mmm_mth_sales_extract;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
