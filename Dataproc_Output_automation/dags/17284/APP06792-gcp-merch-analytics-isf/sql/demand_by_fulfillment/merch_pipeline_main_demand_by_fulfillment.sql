CREATE TEMPORARY TABLE IF NOT EXISTS drvr_wk
AS
SELECT day_date,
 fiscal_year_num,
 week_idnt AS wk_idnt,
 fiscal_week_num AS wk_of_fyr,
 week_454_label AS fiscal_week,
 month_454_label AS fiscal_month,
 quarter_desc AS fiscal_quarter,
 SUBSTR(half_label, 0, 6) AS fiscal_half,
  CASE
  WHEN fiscal_year_num = (SELECT MAX(fiscal_year_num)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date < CURRENT_DATE(('PST8PDT')))
  THEN SUBSTR('TY', 1, 3)
  WHEN fiscal_year_num = (SELECT MAX(fiscal_year_num) - 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
  THEN SUBSTR('LY', 1, 3)
  ELSE 'uh?'
  END AS ty_ly_lly_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS dcm
WHERE LOWER(ty_ly_lly_ind) IN (LOWER('TY'), LOWER('LY'))
 AND LOWER(ytd_last_full_week_ind) = LOWER('Y');


--COLLECT STATS 	PRIMARY INDEX (day_date) 	,COLUMN (day_date) 	on drvr_wk


CREATE TEMPORARY TABLE IF NOT EXISTS demand_by_fulfillment
AS
SELECT CASE
  WHEN LOWER(base.fulfilled_from_location_type) = LOWER('Vendor (Drop Ship)')
  THEN 'DROPSHIP'
  WHEN LOWER(base.line_item_fulfillment_type) = LOWER('StorePickup')
  THEN 'STORE PICKUP'
  WHEN LOWER(base.fulfilled_from_location_type) = LOWER('Store')
  THEN 'STORE FULFILL'
  WHEN LOWER(base.fulfilled_from_location_type) IN (LOWER('Trunk Club'), LOWER('DC'), LOWER('FC'))
  THEN 'FC'
  ELSE 'UNKNOWN'
  END AS fulfill_method,
 TRIM(FORMAT('%11d', base.division_num) || ', ' || base.division_name) AS division,
 TRIM(FORMAT('%11d', base.subdivision_num) || ', ' || base.subdivision_name) AS subdivision,
 TRIM(FORMAT('%11d', base.dept_num) || ', ' || base.dept_name) AS department,
 TRIM(FORMAT('%11d', base.class_num) || ', ' || base.class_name) AS class,
 'N/A' AS subclass,
 base.supplier_name AS supplier,
 COALESCE(base.price_type, 'Unknown') AS price_type,
 base.npg_ind,
 base.rp_ind,
 base.business_unit_desc AS channel,
 base.fulfilled_from_location AS location_index,
 d.wk_of_fyr AS fiscal_week_number,
 'NAP' AS data_source,
 d.ty_ly_lly_ind,
 SUM(base.jwn_gross_demand_amt) AS demand,
 SUM(base.demand_units) AS demand_units,
 SUM(CASE
   WHEN LOWER(base.jwn_fulfilled_demand_ind) = LOWER('Y')
   THEN base.jwn_gross_demand_amt
   ELSE 0
   END) AS demand_shipped,
 SUM(CASE
   WHEN LOWER(base.jwn_fulfilled_demand_ind) = LOWER('Y')
   THEN base.demand_units
   ELSE 0
   END) AS demand_shipped_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_demand_metric_vw AS base
 INNER JOIN drvr_wk AS d ON base.demand_date = d.day_date
WHERE LOWER(base.gift_with_purchase_ind) = LOWER('N')
 AND LOWER(base.smart_sample_ind) = LOWER('N')
 AND LOWER(base.same_day_fraud_cancel_ind) = LOWER('N')
 AND LOWER(base.same_day_cust_cancel_ind) = LOWER('N')
GROUP BY fulfill_method,
 division,
 subdivision,
 department,
 class,
 subclass,
 supplier,
 price_type,
 base.npg_ind,
 base.rp_ind,
 channel,
 location_index,
 fiscal_week_number,
 data_source,
 d.ty_ly_lly_ind;


--COLLECT STATS 	PRIMARY INDEX (fulfill_method, department, "class", supplier, location_index, fiscal_week_number, ty_ly_lly_ind) 	,COLUMN (fulfill_method, department, "class", supplier, location_index, fiscal_week_number, ty_ly_lly_ind) 		ON demand_by_fulfillment


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.demand_by_fulfillment;

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.demand_by_fulfillment
(SELECT sub.fulfill_method,
  sub.division,
  sub.subdivision,
  sub.department,
  sub.class,
  sub.subclass,
  sub.supplier,
  sub.price_type,
  sub.npg_ind,
   CASE
   WHEN LOWER(sub.channel) = LOWER('FULL LINE')
   THEN '110, FULL LINE STORES'
   WHEN LOWER(sub.channel) = LOWER('N.COM')
   THEN '120, N.COM'
   WHEN LOWER(sub.channel) = LOWER('RACK')
   THEN '210, RACK STORES'
   WHEN LOWER(sub.channel) = LOWER('OFFPRICE ONLINE')
   THEN '250, OFFPRICE ONLINE'
   ELSE sub.channel
   END AS channel,
  TRIM(FORMAT('%11d', sub.location_index) || ', ' || sd.store_short_name) AS location,
  wk.fiscal_week_number,
  wk.fiscal_week,
  wk.fiscal_month,
  wk.fiscal_quarter,
  wk.fiscal_half,
  sub.data_source,
   (SELECT TRIM(FORMAT('%11d', fiscal_year_num) || ' Week ' || TRIM(FORMAT('%6d', wk_of_fyr))) AS
    wk_of_fyr_fiscal_year_num
   FROM drvr_wk
   WHERE day_date = (SELECT MAX(day_date)
      FROM drvr_wk)) AS data_through_week,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('TY')
   THEN sub.demand
   ELSE 0
   END AS demand_ty,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('TY')
   THEN sub.demand_units
   ELSE 0
   END AS demand_units_ty,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('TY')
   THEN sub.demand_shipped
   ELSE 0
   END AS demand_shipped_ty,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('TY')
   THEN sub.demand_shipped_units
   ELSE 0
   END AS demand_shipped_units_ty,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('LY')
   THEN sub.demand
   ELSE 0
   END AS demand_ly,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('LY')
   THEN sub.demand_units
   ELSE 0
   END AS demand_units_ly,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('LY')
   THEN sub.demand_shipped
   ELSE 0
   END AS demand_shipped_ly,
   CASE
   WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('LY')
   THEN sub.demand_shipped_units
   ELSE 0
   END AS demand_shipped_units_ly,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  process_tmstp,
   `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_tmstp_tz,
  sub.rp_ind
 FROM demand_by_fulfillment AS sub
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON sub.location_index = sd.store_num
  INNER JOIN (SELECT DISTINCT fiscal_week_num AS fiscal_week_number,
    week_454_label AS fiscal_week,
    month_454_label AS fiscal_month,
    quarter_desc AS fiscal_quarter,
    SUBSTR(half_label, 0, 6) AS fiscal_half
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
   WHERE LOWER(ty_ly_lly_ind) = LOWER('TY')
    AND LOWER(ytd_last_full_week_ind) = LOWER('Y')) AS wk ON sub.fiscal_week_number = wk.fiscal_week_number
 WHERE CASE
    WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('TY')
    THEN sub.demand_units
    ELSE 0
    END <> 0
  OR CASE
    WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('LY')
    THEN sub.demand_units
    ELSE 0
    END <> 0
  OR CASE
    WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('TY')
    THEN sub.demand_shipped_units
    ELSE 0
    END <> 0
  OR CASE
    WHEN LOWER(sub.ty_ly_lly_ind) = LOWER('LY')
    THEN sub.demand_shipped_units
    ELSE 0
    END <> 0);


--COLLECT STATISTICS     PRIMARY INDEX (fulfill_method, department, "class", supplier, location, fiscal_week_number)    on t2dl_das_ace_mfp.demand_by_fulfillment