
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;


BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS last_month_week_dates
AS
SELECT DISTINCT currcal.day_date,
 prevcal.month_idnt,
 prevcal.week_start_day_date,
 prevcal.week_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS currcal
 INNER JOIN (`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS prevcal 
 LEFT JOIN (SELECT prev_tbl.prev_month_idnt AS A1780337676
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS currcal0
    INNER JOIN (SELECT DISTINCT month_idnt AS curr_month_idnt,
      LAG(month_idnt) OVER (ORDER BY month_idnt) AS prev_month_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS prev_tbl 
	 ON currcal0.month_idnt = prev_tbl.curr_month_idnt 
	 AND currcal0.month_idnt > prev_tbl.prev_month_idnt
   WHERE CURRENT_DATE('PST8PDT') = currcal0.day_date) AS t2 ON TRUE) ON prevcal.month_idnt = t2.A1780337676
WHERE CURRENT_DATE('PST8PDT') = currcal.day_date;

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS sku_dept (
rms_sku_num STRING,
dept_num INTEGER
) ;

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

INSERT INTO sku_dept
(SELECT rms_sku_num,
  dept_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS skud
 WHERE LOWER(gwp_ind) <> LOWER('y')
  AND LOWER(smart_sample_ind) <> LOWER('y')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num ORDER BY channel_country DESC)) = 1);
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS soh_scan_extract
AS
SELECT snapshot_date,
 location_id,
 rms_sku_id,
 stock_on_hand_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_logical_fact AS sohf
WHERE snapshot_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 2 MONTH)
 AND stock_on_hand_qty IS NOT NULL;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS soh_scan_week
AS
SELECT lmwd.month_idnt,
 lmwd.week_start_day_date,
 lmwd.week_end_day_date,
 soh.location_id,
 soh.rms_sku_id,
 soh.stock_on_hand_qty
FROM soh_scan_extract AS soh
 INNER JOIN last_month_week_dates AS lmwd ON soh.snapshot_date = lmwd.week_end_day_date;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;
  
CREATE TEMPORARY TABLE IF NOT EXISTS product_price_dim_date as
with SRC as  (
--dis normalize logic 
SELECT store_num,rms_sku_num,current_price_amt,current_price_type,eff_begin_tmstp,eff_end_tmstp,RANGE(eff_begin_tmstp, eff_end_tmstp) AS norm_period
    FROM (
        -- Inner normalize
        SELECT 
            store_num,rms_sku_num,current_price_amt,current_price_type,
            MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp,
            MAX(eff_end_tmstp_utc) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY 
                        store_num,rms_sku_num,current_price_amt,current_price_type
                    ORDER BY eff_begin_tmstp_utc 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp_utc) OVER (
                            PARTITION BY store_num,rms_sku_num,current_price_amt,current_price_type
                            ORDER BY eff_begin_tmstp_utc
                        ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM
  (
--main query 
select 
    store_num,
    rms_sku_num,
    selling_retail_price_amt as current_price_amt, 
    substr(selling_retail_price_type_code,1,1) as current_price_type,
    eff_begin_tmstp_utc,
    eff_end_tmstp_utc,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz,
    RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc) AS eff_period
from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim
)
  ) AS ordered_data
            ) AS grouped_data
            GROUP BY 
                store_num,rms_sku_num,current_price_amt,current_price_type,
                range_group
            ORDER BY  
                store_num,rms_sku_num,current_price_amt,current_price_type,
                eff_begin_tmstp
        )
    )
 (

SELECT store_num,
   rms_sku_num,
   current_price_amt,
   current_price_type,
   CAST(RANGE_START(norm_period) AS DATE) AS eff_begin_date,
   DATE_SUB(CAST(RANGE_END(norm_period) AS DATE), INTERVAL 1 DAY) AS eff_end_date from SRC

 );
  
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.mmm_t2_schema}}.mmm_mth_inv_extract
WHERE month_idnt = (SELECT DISTINCT month_idnt
        FROM last_month_week_dates);
		
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.mmm_t2_schema}}.mmm_mth_inv_extract
(SELECT soh.month_idnt,
  soh.week_start_day_date AS wk_start_dt,
  soh.week_end_day_date AS wk_end_dt,
  COALESCE(fpdiv.mmm_division, opdiv.mmm_division) AS division,
   CASE
   WHEN str.business_unit_num = 8500
   THEN UPPER('n.com')
   WHEN str.business_unit_num = 5500
   THEN UPPER('full line canada')
   ELSE str.business_unit_desc
   END AS chain_desc,
  soh.location_id AS loc_idnt,
  SUM(CASE
    WHEN LOWER(COALESCE(ppdd.current_price_type, 'x')) IN (LOWER('p'), LOWER('c'))
    THEN 0
    WHEN LOWER(ppdd.current_price_type) = LOWER('r') OR regp.regular_price_amt IS NOT NULL
    THEN soh.stock_on_hand_qty
    ELSE 0
    END) AS reg_qty,
  SUM(CASE
    WHEN LOWER(COALESCE(ppdd.current_price_type, 'x')) IN (LOWER('p'), LOWER('c'))
    THEN 0
    WHEN LOWER(ppdd.current_price_type) = LOWER('r')
    THEN CAST(soh.stock_on_hand_qty * ppdd.current_price_amt AS FLOAT64)
    WHEN regp.regular_price_amt IS NOT NULL
    THEN soh.stock_on_hand_qty * regp.regular_price_amt
    ELSE 0
    END) AS reg_amt,
  COUNT(DISTINCT CASE
    WHEN LOWER(COALESCE(ppdd.current_price_type, 'x')) IN (LOWER('p'), LOWER('c'))
    THEN NULL
    WHEN LOWER(ppdd.current_price_type) = LOWER('r') OR regp.regular_price_amt IS NOT NULL
    THEN soh.rms_sku_id
    ELSE NULL
    END) AS reg_sku_cnt,
  SUM(CASE
    WHEN LOWER(ppdd.current_price_type) = LOWER('p')
    THEN soh.stock_on_hand_qty
    ELSE 0
    END) AS promo_qty,
  SUM(CASE
    WHEN LOWER(ppdd.current_price_type) = LOWER('p')
    THEN soh.stock_on_hand_qty * ppdd.current_price_amt
    ELSE 0
    END) AS promo_amt,
  COUNT(DISTINCT CASE
    WHEN LOWER(ppdd.current_price_type) = LOWER('p')
    THEN soh.rms_sku_id
    ELSE NULL
    END) AS promo_sku_cnt,
  SUM(CASE
    WHEN LOWER(ppdd.current_price_type) = LOWER('c')
    THEN soh.stock_on_hand_qty
    ELSE 0
    END) AS clr_qty,
  SUM(CASE
    WHEN LOWER(ppdd.current_price_type) = LOWER('c')
    THEN soh.stock_on_hand_qty * ppdd.current_price_amt
    ELSE 0
    END) AS clr_amt,
  COUNT(DISTINCT CASE
    WHEN LOWER(ppdd.current_price_type) = LOWER('c')
    THEN soh.rms_sku_id
    ELSE NULL
    END) AS clr_sku_cnt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM soh_scan_week AS soh
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON CAST(soh.location_id AS FLOAT64) = str.store_num AND str.business_unit_num
     IN (1000, 2000, 5000, 5500, 6000, 6500, 8500, 9000, 9500)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS pstr ON CAST(soh.location_id AS FLOAT64) = pstr.store_num
  INNER JOIN sku_dept AS dpt ON LOWER(soh.rms_sku_id) = LOWER(dpt.rms_sku_num)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mmm.jwn_dept_mmm_div_xref_fp AS fpdiv ON dpt.dept_num = fpdiv.dept_num AND str.business_unit_num IN
     (1000, 5500, 6000, 6500, 8500, 9000)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mmm.jwn_dept_mmm_div_xref_op AS opdiv ON dpt.dept_num = opdiv.dept_num AND str.business_unit_num IN
     (2000, 5000, 9500)
  LEFT JOIN product_price_dim_date AS ppdd 
  ON pstr.price_store_num = CAST(ppdd.store_num AS FLOAT64) 
  AND LOWER(soh.rms_sku_id) = LOWER(ppdd.rms_sku_num) 
  AND soh.week_end_day_date BETWEEN ppdd.eff_begin_date AND ppdd.eff_end_date
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_sales_returns.product_reg_price_dim AS regp ON pstr.price_store_num = CAST(regp.price_store_num AS FLOAT64)
      AND LOWER(soh.rms_sku_id) = LOWER(regp.rms_sku_num) AND soh.week_end_day_date BETWEEN regp.pricing_start_date AND
    regp.pricing_end_date
 WHERE COALESCE(fpdiv.mmm_division, opdiv.mmm_division) IS NOT NULL
 GROUP BY soh.month_idnt,
  wk_start_dt,
  wk_end_dt,
  division,
  chain_desc,
  loc_idnt);
  
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

END;
