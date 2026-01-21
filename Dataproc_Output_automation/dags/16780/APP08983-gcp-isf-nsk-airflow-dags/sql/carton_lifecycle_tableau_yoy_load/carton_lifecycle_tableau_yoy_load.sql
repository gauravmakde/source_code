
/**************************************************************************************************************************************
-- This sql script will load the CARTON_LIFECYCLE_TABLEAU_YOY_FACT table using the existing CARTON_LIFECYCLE_FACT_VW and other NAP Views
**********************************************************************************************/
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : CARTON_LIFECYCLE_TABLEAU_YOY_FACT_LOAD.sql
-- Author                  : Jen Latta
-- Description             : This sql script will load the CARTON_LIFECYCLE_TABLEAU_YOY_FACT table using the existing CARTON_LIFECYCLE_FACT_VW and other NAP Views
-- Data Source             : Design view in progress
-- ETL Run Frequency       : Every Sunday
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- N/A
--*************************************************************************************************************************************




CREATE TEMPORARY TABLE IF NOT EXISTS day_cal_454_lkup
AS
SELECT DISTINCT day_date,
 fiscal_week_num,
 week_num_of_fiscal_month,
 fiscal_month_num,
 month_abrv,
 month_idnt,
 quarter_abrv,
 fiscal_halfyear_num AS fiscal_half_year_num,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim;


--COLLECT STATS PRIMARY INDEX (DAY_DATE) ON DAY_CAL_454_LKUP


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.carton_lifecycle_tableau_yoy_stg;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.carton_lifecycle_tableau_yoy_stg (purchase_order_num, original_carton_num, to_carton_num,
 rms_sku_num, inbound_warehouse_num, final_warehouse_num, store_num, vendor_ship_notice_date, unit_qty,
 inbound_warehouse_received_tmstp, final_warehouse_shipped_tmstp, store_received_tmstp, warehouse_putaway_tmstp,
 warehouse_putaway_sku_qty, warehouse_putaway_location_num, vendor_shipped_to_store_received_days,
 vendor_shipped_to_warehouse_received_days, warehouse_received_to_warehouse_shipped_days,
 warehouse_shipped_to_store_received_days, ship_window_start_to_vendor_shipped_days,
 ship_window_start_to_store_received_days, purchase_order_original_approval_date, purchase_order_start_ship_date,
 purchase_order_end_ship_date, purchase_order_premark_ind, supplier_num, shipping_method, purchase_order_type,
 order_type, direct_to_store_ind, edi_ind, import_order_ind, nordstrom_productgroup_ind, purchase_type,
 purchase_order_status, quality_check_ind, internal_po_ind, freight_terms, freight_payment_method, merchandise_source,
 fy_whrec, fiscal_half_year_num_whrec, quarter_abrv_whrec, month_idnt_whrec, fiscal_month_num_whrec, month_abrv_whrec,
 week_num_of_fiscal_month_whrec, fiscal_year_num_whshp, fiscal_half_year_num_whshp, quarter_abrv_whshp, month_idnt_whshp
 , fiscal_month_num_whshp, month_abrv_whshp, week_num_of_fiscal_month_whshp, fiscal_year_num_str_rcvd,
 fiscal_half_year_num_str_rcvd, quarter_abrv_str_rcvd, month_idnt_str_rcvd, fiscal_month_num_str_rcvd,
 month_abrv_str_rcvd, week_num_of_fiscal_month_str_rcvd, fiscal_year_num_vndr_shpd, fiscal_half_year_num_vndr_shpd,
 quarter_abrv_vndr_shpd, month_idnt_vndr_shpd, fiscal_month_num_vndr_shpd, month_abrv_vndr_shpd,
 week_num_of_fiscal_month_vndr_shpd, fiscal_year_num_putaway, fiscal_half_year_num_putaway, quarter_abrv_putaway,
 month_idnt_putaway, fiscal_month_num_putaway, month_abrv_putaway, week_num_of_fiscal_month_putaway,
 fiscal_year_num_po_start_ship, fiscal_half_year_num_po_start_ship, quarter_abrv_po_start_ship, month_idnt_po_start_ship
 , fiscal_month_num_po_start_ship, month_abrv_po_start_ship, week_num_of_fiscal_month_po_start_ship, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT isf.purchase_order_num,
  isf.original_carton_num,
  isf.to_carton_num,
  isf.rms_sku_num,
  isf.inbound_warehouse_num,
  isf.final_warehouse_num,
  isf.store_num,
  CAST(FORMAT_DATE('%m/%d/%y', isf.vendor_ship_notice_date) AS DATE) AS vendor_ship_notice_date,
  isf.unit_qty,
  CAST(isf.inbound_warehouse_received_tmstp AS DATE) AS inbound_warehouse_received_tmstp,
  CAST(isf.final_warehouse_shipped_tmstp AS DATE) AS final_warehouse_shipped_tmstp,
  CAST(isf.store_received_tmstp AS DATE) AS store_received_tmstp,
  CAST(isf.warehouse_putaway_tmstp AS DATE) AS warehouse_putaway_tmstp,
  isf.warehouse_putaway_sku_qty,
  isf.warehouse_putaway_location_num,
  isf.vendor_shipped_to_store_received_days,
  isf.vendor_shipped_to_warehouse_received_days,
  isf.warehouse_received_to_warehouse_shipped_days,
  isf.warehouse_shipped_to_store_received_days,
  isf.ship_window_start_to_vendor_shipped_days,
  isf.ship_window_start_to_store_received_days,
  CAST(FORMAT_DATE('%m/%d/%y', isf.purchase_order_original_approval_date) AS DATE) AS
  purchase_order_original_approval_date,
  CAST(FORMAT_DATE('%m/%d/%y', isf.purchase_order_start_ship_date) AS DATE) AS purchase_order_start_ship_date,
  isf.purchase_order_end_ship_date,
  isf.purchase_order_premark_ind,
  isf.supplier_num,
  isf.shipping_method,
  isf.purchase_order_type,
  isf.order_type,
  isf.direct_to_store_ind,
  isf.edi_ind,
  isf.import_order_ind,
  isf.nordstrom_productgroup_ind,
  isf.purchase_type,
  isf.purchase_order_status,
  isf.quality_check_ind,
  isf.internal_po_ind,
  isf.freight_terms,
  isf.freight_payment_method,
  isf.merchandise_source,
  tdl.fiscal_year_num AS fy_whrec,
  tdl.fiscal_half_year_num AS fiscal_half_year_num_whrec,
  tdl.quarter_abrv AS quarter_abrv_whrec,
  tdl.month_idnt AS month_idnt_whrec,
  tdl.fiscal_month_num AS fiscal_month_num_whrec,
  tdl.month_abrv AS month_abrv_whrec,
  tdl.week_num_of_fiscal_month AS week_num_of_fiscal_month_whrec,
  tdl_finalwh.fiscal_year_num AS fiscal_year_num_whshp,
  tdl_finalwh.fiscal_half_year_num AS fiscal_half_year_num_whshp,
  tdl_finalwh.quarter_abrv AS quarter_abrv_whshp,
  tdl_finalwh.month_idnt AS month_idnt_whshp,
  tdl_finalwh.fiscal_month_num AS fiscal_month_num_whshp,
  tdl_finalwh.month_abrv AS month_abrv_whshp,
  tdl_finalwh.week_num_of_fiscal_month AS week_num_of_fiscal_month_whshp,
  tdl_str_rcvd.fiscal_year_num AS fiscal_year_num_str_rcvd,
  tdl_str_rcvd.fiscal_half_year_num AS fiscal_half_year_num_str_rcvd,
  tdl_str_rcvd.quarter_abrv AS quarter_abrv_str_rcvd,
  tdl_str_rcvd.month_idnt AS month_idnt_str_rcvd,
  tdl_str_rcvd.fiscal_month_num AS fiscal_month_num_str_rcvd,
  tdl_str_rcvd.month_abrv AS month_abrv_str_rcvd,
  tdl_str_rcvd.week_num_of_fiscal_month AS week_num_of_fiscal_month_str_rcvd,
  tdl_vndr_shpd.fiscal_year_num AS fiscal_year_num_vndr_shpd,
  tdl_vndr_shpd.fiscal_half_year_num AS fiscal_half_year_num_vndr_shpd,
  tdl_vndr_shpd.quarter_abrv AS quarter_abrv_vndr_shpd,
  tdl_vndr_shpd.month_idnt AS month_idnt_vndr_shpd,
  tdl_vndr_shpd.fiscal_month_num AS fiscal_month_num_vndr_shpd,
  tdl_vndr_shpd.month_abrv AS month_abrv_vndr_shpd,
  tdl_vndr_shpd.week_num_of_fiscal_month AS week_num_of_fiscal_month_vndr_shpd,
  tdl_putaway.fiscal_year_num AS fiscal_year_num_putaway,
  tdl_putaway.fiscal_half_year_num AS fiscal_half_year_num_putaway,
  tdl_putaway.quarter_abrv AS quarter_abrv_putaway,
  tdl_putaway.month_idnt AS month_idnt_putaway,
  tdl_putaway.fiscal_month_num AS fiscal_month_num_putaway,
  tdl_putaway.month_abrv AS month_abrv_putaway,
  tdl_putaway.week_num_of_fiscal_month AS week_num_of_fiscal_month_putaway,
  tdl_po_start_ship.fiscal_year_num AS fiscal_year_num_po_start_ship,
  tdl_po_start_ship.fiscal_half_year_num AS fiscal_half_year_num_po_start_ship,
  tdl_po_start_ship.quarter_abrv AS quarter_abrv_po_start_ship,
  tdl_po_start_ship.month_idnt AS month_idnt_po_start_ship,
  tdl_po_start_ship.fiscal_month_num AS fiscal_month_num_po_start_ship,
  tdl_po_start_ship.month_abrv AS month_abrv_po_start_ship,
  tdl_po_start_ship.week_num_of_fiscal_month AS week_num_of_fiscal_month_po_start_ship,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.carton_lifecycle_fact_vw AS isf
  LEFT JOIN day_cal_454_lkup AS tdl ON tdl.day_date = COALESCE(CAST(isf.inbound_warehouse_received_tmstp AS DATE),
    PARSE_DATE('%F', '1901-01-01'))
  LEFT JOIN day_cal_454_lkup AS tdl_finalwh ON tdl_finalwh.day_date = COALESCE(CAST(isf.final_warehouse_shipped_tmstp AS DATE)
    , PARSE_DATE('%F', '1901-01-01'))
  LEFT JOIN day_cal_454_lkup AS tdl_str_rcvd ON tdl_str_rcvd.day_date = COALESCE(CAST(isf.store_received_tmstp AS DATE)
    , PARSE_DATE('%F', '1901-01-01'))
  LEFT JOIN day_cal_454_lkup AS tdl_vndr_shpd ON tdl_vndr_shpd.day_date = COALESCE(isf.vendor_ship_notice_date,
    PARSE_DATE('%F', '1901-01-01'))
  LEFT JOIN day_cal_454_lkup AS tdl_putaway ON tdl_putaway.day_date = COALESCE(CAST(isf.warehouse_putaway_tmstp AS DATE)
    , PARSE_DATE('%F', '1901-01-01'))
  LEFT JOIN day_cal_454_lkup AS tdl_po_start_ship ON tdl_po_start_ship.day_date = COALESCE(isf.purchase_order_start_ship_date
    , PARSE_DATE('%F', '1901-01-01'))
 WHERE isf.unit_qty >= 0
  AND (tdl.fiscal_year_num BETWEEN (SELECT fiscal_year_num - 2 AS fy
          FROM day_cal_454_lkup
          WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND (SELECT fiscal_year_num AS fy
          FROM day_cal_454_lkup
          WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR tdl_finalwh.fiscal_year_num BETWEEN (SELECT
            fiscal_year_num - 2 AS fy
          FROM day_cal_454_lkup
          WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND (SELECT fiscal_year_num AS fy
          FROM day_cal_454_lkup
          WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR tdl_str_rcvd.fiscal_year_num BETWEEN (SELECT
           fiscal_year_num - 2 AS fy
         FROM day_cal_454_lkup
         WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND (SELECT fiscal_year_num AS fy
         FROM day_cal_454_lkup
         WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR tdl_vndr_shpd.fiscal_year_num BETWEEN (SELECT
          fiscal_year_num - 2 AS fy
        FROM day_cal_454_lkup
        WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND (SELECT fiscal_year_num AS fy
        FROM day_cal_454_lkup
        WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR tdl_po_start_ship.fiscal_year_num BETWEEN (SELECT
         fiscal_year_num - 2 AS fy
       FROM day_cal_454_lkup
       WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND (SELECT fiscal_year_num AS fy
       FROM day_cal_454_lkup
       WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR tdl_putaway.fiscal_year_num BETWEEN (SELECT
        fiscal_year_num - 2 AS fy
      FROM day_cal_454_lkup
      WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND (SELECT fiscal_year_num AS fy
      FROM day_cal_454_lkup
      WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))));


CREATE TEMPORARY TABLE IF NOT EXISTS carton_lifecycle_fact_lkup
AS
SELECT purchase_order_num,
 original_carton_num,
 to_carton_num,
 rms_sku_num,
 inbound_warehouse_num,
 final_warehouse_num,
 store_num,
 vendor_ship_notice_date,
 unit_qty,
 inbound_warehouse_received_tmstp,
 final_warehouse_shipped_tmstp,
 store_received_tmstp,
 warehouse_putaway_tmstp,
 warehouse_putaway_sku_qty,
 warehouse_putaway_location_num,
 vendor_shipped_to_store_received_days,
 vendor_shipped_to_warehouse_received_days,
 warehouse_received_to_warehouse_shipped_days,
 warehouse_shipped_to_store_received_days,
 ship_window_start_to_vendor_shipped_days,
 ship_window_start_to_store_received_days,
 purchase_order_original_approval_date,
 purchase_order_start_ship_date,
 purchase_order_end_ship_date,
 purchase_order_premark_ind,
 supplier_num,
 shipping_method,
 purchase_order_type,
 order_type,
 direct_to_store_ind,
 edi_ind,
 import_order_ind,
 nordstrom_productgroup_ind,
 purchase_type,
 purchase_order_status,
 quality_check_ind,
 internal_po_ind,
 freight_terms,
 freight_payment_method,
 merchandise_source,
  CASE
  WHEN LOWER(nordstrom_productgroup_ind) = LOWER('t')
  THEN 'Y'
  ELSE 'N'
  END AS npg_flag,
  CASE
  WHEN LOWER(purchase_order_premark_ind) = LOWER('t')
  THEN 'Store Pack'
  WHEN LOWER(purchase_order_premark_ind) = LOWER('f')
  THEN 'Bulk Sort'
  ELSE 'Null'
  END AS premark_ind,
 DATE_DIFF(warehouse_putaway_tmstp, inbound_warehouse_received_tmstp, DAY) AS wh_receive_to_distribute_to_putaway_days,
 DATE_DIFF(store_received_tmstp, inbound_warehouse_received_tmstp, DAY) AS wh_receive_to_distribute_to_store_days,
 DATE_DIFF(warehouse_putaway_tmstp, vendor_ship_notice_date, DAY) AS vasn_to_distribute_to_putaway_days,
 DATE_DIFF(purchase_order_start_ship_date, purchase_order_original_approval_date, DAY) AS
 po_approved_ship_window_start_days,
 DATE_DIFF(store_received_tmstp, purchase_order_original_approval_date, DAY) AS
 po_approved_distribute_to_store_receipt_days,
 DATE_DIFF(final_warehouse_shipped_tmstp, purchase_order_original_approval_date, DAY) AS po_approved_to_wh_ship_days,
 DATE_DIFF(final_warehouse_shipped_tmstp, purchase_order_start_ship_date, DAY) AS ship_window_start_to_wh_ship_days,
 DATE_DIFF(final_warehouse_shipped_tmstp, vendor_ship_notice_date, DAY) AS vasn_to_wh_ship_days,
 DATE_DIFF(inbound_warehouse_received_tmstp, purchase_order_original_approval_date, DAY) AS po_approval_to_wh_rec_days,
 DATE_DIFF(inbound_warehouse_received_tmstp, purchase_order_start_ship_date, DAY) AS ship_window_start_to_wh_rec_days,
 DATE_DIFF(warehouse_putaway_tmstp, purchase_order_start_ship_date, DAY) AS
 ship_window_start_to_distribute_to_putaway_days,
 DATE_DIFF(warehouse_putaway_tmstp, purchase_order_original_approval_date, DAY) AS
 po_approved_to_distribute_to_putaway_days,
  CASE
  WHEN inbound_warehouse_received_tmstp IS NULL
  THEN NULL
  ELSE fy_whrec
  END AS fy_whrec,
 CONCAT('H', SUBSTR(TRIM(FORMAT('%11d', fiscal_half_year_num_whrec)), 5)) AS half_desc,
 TRIM(quarter_abrv_whrec) AS quarter_desc,
  CASE
  WHEN inbound_warehouse_received_tmstp IS NULL
  THEN NULL
  ELSE month_idnt_whrec
  END AS mth_idnt,
  CASE
  WHEN inbound_warehouse_received_tmstp IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(fy_whrec AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d',
        fiscal_month_num_whrec), 2, ' ') || ' ' || month_abrv_whrec, 1, 20)
  END AS mth_desc,
   'WK ' || ' ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month_whrec)) AS week_desc,
  CASE
  WHEN final_warehouse_shipped_tmstp IS NULL
  THEN NULL
  ELSE fiscal_year_num_whshp
  END AS fy_final_wh,
 CONCAT('H', SUBSTR(TRIM(FORMAT('%11d', fiscal_half_year_num_whshp)), 5)) AS half_desc_finalwh,
 TRIM(quarter_abrv_whshp) AS quarter_desc_finalwh,
  CASE
  WHEN final_warehouse_shipped_tmstp IS NULL
  THEN NULL
  ELSE month_idnt_whshp
  END AS mth_idnt_finalwh,
  CASE
  WHEN final_warehouse_shipped_tmstp IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(fiscal_year_num_whshp AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d',
        fiscal_month_num_whshp), 2, ' ') || ' ' || month_abrv_whshp, 1, 20)
  END AS mth_desc_finalwh,
   'WK ' || ' ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month_whshp)) AS week_desc_finalwh,
  CASE
  WHEN store_received_tmstp IS NULL
  THEN NULL
  ELSE fiscal_year_num_str_rcvd
  END AS fy_strrcvd,
 CONCAT('H', SUBSTR(TRIM(FORMAT('%11d', fiscal_half_year_num_str_rcvd)), 5)) AS half_desc_strrcvd,
 TRIM(quarter_abrv_str_rcvd) AS quarter_desc_strrcvd,
  CASE
  WHEN store_received_tmstp IS NULL
  THEN NULL
  ELSE month_idnt_str_rcvd
  END AS mth_idnt_strrcvd,
  CASE
  WHEN store_received_tmstp IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(fiscal_year_num_str_rcvd AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d'
        , fiscal_month_num_str_rcvd), 2, ' ') || ' ' || month_abrv_str_rcvd, 1, 20)
  END AS mth_desc_strrcvd,
   'WK ' || ' ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month_str_rcvd)) AS week_desc_strrcvd,
  CASE
  WHEN vendor_ship_notice_date IS NULL
  THEN NULL
  ELSE fiscal_year_num_vndr_shpd
  END AS fy_vndrshpd,
 CONCAT('H', SUBSTR(TRIM(FORMAT('%11d', fiscal_half_year_num_vndr_shpd)), 5)) AS half_desc_vndrshpd,
 TRIM(quarter_abrv_vndr_shpd) AS quarter_desc_vndrshpd,
  CASE
  WHEN vendor_ship_notice_date IS NULL
  THEN NULL
  ELSE month_idnt_vndr_shpd
  END AS mth_idnt_vndrshpd,
  CASE
  WHEN vendor_ship_notice_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(fiscal_year_num_vndr_shpd AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d'
        , fiscal_month_num_vndr_shpd), 2, ' ') || ' ' || month_abrv_vndr_shpd, 1, 20)
  END AS mth_desc_vndrshpd,
   'WK ' || ' ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month_vndr_shpd)) AS week_desc_vndrshpd,
  CASE
  WHEN warehouse_putaway_tmstp IS NULL
  THEN NULL
  ELSE fiscal_year_num_putaway
  END AS fy_putaway,
 CONCAT('H', SUBSTR(TRIM(FORMAT('%11d', fiscal_half_year_num_putaway)), 5)) AS half_desc_putaway,
 TRIM(quarter_abrv_putaway) AS quarter_desc_putaway,
  CASE
  WHEN warehouse_putaway_tmstp IS NULL
  THEN NULL
  ELSE month_idnt_putaway
  END AS mth_idnt_putaway,
  CASE
  WHEN warehouse_putaway_tmstp IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(fiscal_year_num_putaway AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d'
        , fiscal_month_num_putaway), 2, ' ') || ' ' || month_abrv_putaway, 1, 20)
  END AS mth_desc_putaway,
   'WK ' || ' ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month_putaway)) AS week_desc_putaway,
  CASE
  WHEN purchase_order_start_ship_date IS NULL
  THEN NULL
  ELSE fiscal_year_num_po_start_ship
  END AS fy_postartship,
 CONCAT('H', SUBSTR(TRIM(FORMAT('%11d', fiscal_half_year_num_po_start_ship)), 5)) AS half_desc_postartship,
 TRIM(quarter_abrv_po_start_ship) AS quarter_desc_postartship,
  CASE
  WHEN purchase_order_start_ship_date IS NULL
  THEN NULL
  ELSE month_idnt_po_start_ship
  END AS mth_idnt_postartship,
  CASE
  WHEN purchase_order_start_ship_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(fiscal_year_num_po_start_ship AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d'
        , fiscal_month_num_po_start_ship), 2, ' ') || ' ' || month_abrv_po_start_ship, 1, 20)
  END AS mth_desc_postartship,
   'WK ' || ' ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month_po_start_ship)) AS week_desc_postartship
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.carton_lifecycle_tableau_yoy_stg AS isf;


--COLLECT STATS PRIMARY INDEX (PURCHASE_ORDER_NUM,ORIGINAL_CARTON_NUM,TO_CARTON_NUM,RMS_SKU_NUM ) ON CARTON_LIFECYCLE_FACT_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS ban_cntry_store_dim_lkup
AS
SELECT store_num,
 store_type_code,
 channel_desc,
 channel_num,
 distribution_center_num,
 region_desc,
 store_country_code,
 store_short_name,
  CASE
  WHEN LOWER(store_country_code) = LOWER('US') AND channel_num IN (110, 120, 140, 310, 920, 940, 990)
  THEN 'FP US'
  WHEN LOWER(store_country_code) = LOWER('US') AND channel_num IN (210, 220, 250, 260)
  THEN 'OP US'
  WHEN LOWER(store_country_code) = LOWER('CA') AND channel_num IN (111, 121, 311, 921)
  THEN 'FP CA'
  WHEN LOWER(store_country_code) = LOWER('CA') AND channel_num IN (211, 221, 261, 922)
  THEN 'OP CA'
  ELSE NULL
  END AS banner_country
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS org;


--COLLECT STATS PRIMARY INDEX (STORE_NUM) ON BAN_CNTRY_STORE_DIM_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS product_sku_dim_lkup
AS
SELECT rms_sku_num,
 channel_country,
 prmy_supp_num,
 dept_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim;


--COLLECT STATS PRIMARY INDEX (RMS_SKU_NUM, CHANNEL_COUNTRY) ON PRODUCT_SKU_DIM_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS vendor_dim_lkup
AS
SELECT vendor_num,
 vendor_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim;


--COLLECT STATS PRIMARY INDEX (VENDOR_NUM) ON VENDOR_DIM_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS vendor_orderfrom_market_dim_lkup
AS
SELECT vendor_num,
 market_code,
 import_status
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_orderfrom_market_dim
WHERE LOWER(market_code) = LOWER('USA');


--COLLECT STATS PRIMARY INDEX (VENDOR_NUM,MARKET_CODE) ON VENDOR_ORDERFROM_MARKET_DIM_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS department_dim_lkup
AS
SELECT dept_num,
 division_num,
 division_short_name,
 dept_short_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim;


--COLLECT STATS PRIMARY INDEX (DEPT_NUM) ON DEPARTMENT_DIM_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS carton_lifecycle_dim_lkup
AS
SELECT isf.purchase_order_num,
 isf.original_carton_num,
 isf.to_carton_num,
 isf.rms_sku_num,
 COALESCE(dist_org.banner_country, org.banner_country) AS banner_country,
 SUBSTR(LTRIM(SUBSTR(CAST(COALESCE(dist_org.channel_num, org.channel_num) AS STRING), 1, 4)) || ', ' || COALESCE(dist_org
    .channel_desc, org.channel_desc), 1, 40) AS channel_desc,
  CASE
  WHEN LOWER(isf.nordstrom_productgroup_ind) = LOWER('t')
  THEN npg_supp.vendor_name
  ELSE supp.vendor_name
  END AS supplier_name,
  CASE
  WHEN LOWER(isf.nordstrom_productgroup_ind) = LOWER('t')
  THEN npg_supp.vendor_num
  ELSE COALESCE(sku.prmy_supp_num, isf.supplier_num)
  END AS supplier_num,
 SUBSTR(LTRIM(SUBSTR(CAST(dept.division_num AS STRING), 1, 4)) || ', ' || dept.division_short_name, 1, 40) AS div_desc,
 org.region_desc,
 SUBSTR(LTRIM(SUBSTR(CAST(sku.dept_num AS STRING), 1, 4)) || ', ' || dept.dept_short_name, 1, 40) AS dept_desc,
 SUBSTR(LTRIM(SUBSTR(CAST(isf.inbound_warehouse_num AS STRING), 1, 4)) || ', ' || org.store_short_name, 1, 40) AS
 store_desc,
 org.distribution_center_num,
 SUBSTR(LTRIM(SUBSTR(CAST(isf.inbound_warehouse_num AS STRING), 1, 4)) || ', ' || COALESCE(org.store_short_name, ''), 1
  , 40) AS inbound_warehouse_location,
 isf.inbound_warehouse_num,
 SUBSTR(LTRIM(SUBSTR(CAST(isf.final_warehouse_num AS STRING), 1, 4)) || ', ' || COALESCE(org_latest.store_short_name, ''
    ), 1, 40) AS final_warehouse_location,
 isf.final_warehouse_num,
 SUBSTR(LTRIM(SUBSTR(CAST(isf.store_num AS STRING), 1, 4)) || ', ' || COALESCE(org_store.store_short_name, ''), 1, 40)
 AS store_location,
 SUBSTR(LTRIM(SUBSTR(CAST(isf.warehouse_putaway_location_num AS STRING), 1, 4)) || ', ' || COALESCE(org_putaway.store_short_name
    , ''), 1, 40) AS putaway_location,
  CASE
  WHEN LOWER(isf.import_order_ind) = LOWER('T') AND LOWER(vomd.import_status) = LOWER('NDIM')
  THEN 'International'
  ELSE 'Domestic'
  END AS import_order_type,
  CASE
  WHEN LOWER(isf.freight_payment_method) IN (LOWER('CC')) AND LOWER(isf.freight_terms) IN (LOWER('NO_FREIGHT'))
  THEN 'Collect - Nordstrom Pays'
  WHEN LOWER(isf.freight_payment_method) IN (LOWER('CC')) AND LOWER(isf.freight_terms) IN (LOWER('TOTAL_FREIGHT'))
  THEN 'Collect - Vendor Reimburses All'
  WHEN LOWER(isf.freight_payment_method) IN (LOWER('CC')) AND LOWER(isf.freight_terms) IN (LOWER('HALF_FREIGHT'))
  THEN 'Collect - Vendor Reimburses Half'
  WHEN LOWER(isf.freight_payment_method) IN (LOWER('PP')) AND LOWER(isf.freight_terms) IN (LOWER('TOTAL_FREIGHT'))
  THEN 'Prepaid'
  ELSE 'Other'
  END AS freight_inbound_terms,
 isf.merchandise_source
FROM carton_lifecycle_fact_lkup AS isf
 LEFT JOIN (SELECT DISTINCT po.purchase_order_num,
    CASE
    WHEN po.ship_location_id IN (869, 896, 891, 859, 889, 856)
    THEN 868
    WHEN po.ship_location_id = 562
    THEN 569
    WHEN po.ship_location_id = 563
    THEN 599
    WHEN LOWER(bc.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('WH'))
    THEN bc.distribution_center_num
    ELSE CAST(po.ship_location_id AS INTEGER)
    END AS ship_location_id_drvd,
   MIN(po.min_dist_loc) OVER (PARTITION BY po.purchase_order_num, CASE
       WHEN po.ship_location_id IN (869, 896, 891, 859, 889, 856)
       THEN 868
       WHEN po.ship_location_id = 562
       THEN 569
       WHEN po.ship_location_id = 563
       THEN 599
       WHEN LOWER(bc.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('WH'))
       THEN bc.distribution_center_num
       ELSE CAST(po.ship_location_id AS INTEGER)
       END ORDER BY NULL RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_dist_loc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.po_distribute_store_lkup_vw AS po
   INNER JOIN ban_cntry_store_dim_lkup AS bc 
   ON po.ship_location_id = bc.store_num) AS DIST 
   ON isf.inbound_warehouse_num = DIST.ship_location_id_drvd 
   AND LOWER(isf.purchase_order_num) = LOWER(DIST.purchase_order_num)
 LEFT JOIN ban_cntry_store_dim_lkup AS org ON COALESCE(CAST(isf.inbound_warehouse_num AS NUMERIC), isf.store_num) = CAST(org.store_num AS NUMERIC)
  
 LEFT JOIN ban_cntry_store_dim_lkup AS org_store ON CAST(isf.store_num AS NUMERIC) = CAST(org_store.store_num AS NUMERIC)
  
 LEFT JOIN ban_cntry_store_dim_lkup AS org_putaway ON CAST(isf.warehouse_putaway_location_num AS NUMERIC) = CAST(org_putaway.store_num AS NUMERIC)
  
 LEFT JOIN ban_cntry_store_dim_lkup AS org_latest ON CAST(isf.final_warehouse_num AS NUMERIC) = CAST(org_latest.store_num AS NUMERIC)
  
 LEFT JOIN ban_cntry_store_dim_lkup AS dist_org ON DIST.min_dist_loc = dist_org.store_num
 LEFT JOIN product_sku_dim_lkup AS sku ON LOWER(isf.rms_sku_num) = LOWER(sku.rms_sku_num) AND LOWER(org.store_country_code
    ) = LOWER(sku.channel_country)
 LEFT JOIN vendor_dim_lkup AS supp ON LOWER(COALESCE(sku.prmy_supp_num, isf.supplier_num)) = LOWER(supp.vendor_num)
 LEFT JOIN vendor_dim_lkup AS npg_supp ON LOWER(isf.supplier_num) = LOWER(npg_supp.vendor_num)
 LEFT JOIN vendor_orderfrom_market_dim_lkup AS vomd ON LOWER(isf.supplier_num) = LOWER(vomd.vendor_num)
 LEFT JOIN department_dim_lkup AS dept ON sku.dept_num = dept.dept_num;


--COLLECT STATS PRIMARY INDEX (PURCHASE_ORDER_NUM,ORIGINAL_CARTON_NUM,TO_CARTON_NUM,RMS_SKU_NUM ) ON CARTON_LIFECYCLE_DIM_LKUP


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.carton_lifecycle_tableau_yoy_fact;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.carton_lifecycle_tableau_yoy_fact (purchase_order_num, original_carton_num, to_carton_num,
 rms_sku_num, banner_country, channel_desc, supplier_name, supplier_num, div_desc, shipping_method, dir_to_store_ind,
 region_desc, po_type, order_type, dept_desc, store_desc, npg_flag, premark_ind, import_order_type, internal_po_ind,
 freight_inbound_terms, merchandise_source, distribution_center_num, inbound_warehouse_num, final_warehouse_num,
 store_num, putaway_location_num, inbound_warehouse_location, final_warehouse_location, store_location, putaway_location
 , fy_wh_rec, half_desc_wh_rec, quarter_desc_wh_rec, mth_idnt_wh_rec, mth_desc_wh_rec, week_desc_wh_rec, fy_final_wh,
 half_desc_final_wh, quarter_desc_final_wh, mth_idnt_final_wh, mth_desc_final_wh, week_desc_final_wh, fy_str_rcvd,
 half_desc_str_rcvd, quarter_desc_str_rcvd, mth_idnt_str_rcvd, mth_desc_str_rcvd, week_desc_str_rcvd, fy_vndr_shpd,
 half_desc_vndr_shpd, quarter_desc_vndr_shpd, mth_idnt_vndr_shpd, mth_desc_vndr_shpd, week_desc_vndr_shpd, fy_putaway,
 half_desc_putaway, quarter_desc_putaway, mth_idnt_putaway, mth_desc_putaway, week_desc_putaway, fy_po_start_ship,
 half_desc_po_start_ship, quarter_desc_po_start_ship, mth_idnt_po_start_ship, mth_desc_po_start_ship,
 week_desc_po_start_ship, wh_receive_to_distribute_to_putaway_days, wh_receive_to_distribute_to_store_days,
 vasn_to_distribute_to_putaway_days, po_approved_ship_window_start_days, po_approved_distribute_to_store_receipt_days,
 po_approved_to_wh_ship_days, ship_window_start_to_wh_ship_days, vasn_to_wh_ship_days, po_approval_to_wh_rec_days,
 ship_window_start_to_distribute_to_putaway_days, po_approved_to_distribute_to_putaway_days,
 vendor_shipped_to_store_received_days, vendor_shipped_to_warehouse_received_days,
 warehouse_received_to_warehouse_shipped_days, warehouse_shipped_to_store_received_days,
 ship_window_to_vendor_shipped_days, ship_window_start_to_wh_rec_days, ship_window_start_to_store_received_days,
 weighted_cycle_units_po_ship_window_start, weighted_cycle_units_po_distribute_to_store,
 weighted_cycle_units_wh_distribute_putaway, weighted_cycle_units_wh_distribute_store,
 weighted_cycle_units_vendor_putaway, weighted_cycle_units_vendor_store, weighted_cycle_units_vendor_wh,
 weighted_cycle_units_wh_received_shipped, weighted_cycle_units_wh_store, weighted_cycle_units_ship_vendor,
 weighted_cycle_units_ship_store, weighted_cycle_units_po_wh_shipped, weighted_cycle_units_ship_wh,
 weighted_cycle_units_vendor_wh_ship, weighted_cycle_units_po_wh_receive, weighted_cycle_units_ship_wh_receive,
 weighted_cycle_units_ship_putaway, weighted_cycle_units_po_putaway, unit_qty, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp, dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz)
(SELECT isf.purchase_order_num,
  isf.original_carton_num,
  isf.to_carton_num,
  isf.rms_sku_num,
  isd.banner_country,
  isd.channel_desc,
  isd.supplier_name,
  isd.supplier_num,
  isd.div_desc,
  isf.shipping_method,
  isf.direct_to_store_ind AS dir_to_store_ind,
  isd.region_desc,
  isf.purchase_order_type AS po_type,
  isf.order_type,
  isd.dept_desc,
  isd.store_desc,
  isf.npg_flag,
  isf.premark_ind,
  isd.import_order_type,
  isf.internal_po_ind,
  isd.freight_inbound_terms,
  isd.merchandise_source,
  isd.distribution_center_num,
  isf.inbound_warehouse_num,
  isf.final_warehouse_num,
  isf.store_num,
  isf.warehouse_putaway_location_num AS putaway_location_num,
  isd.inbound_warehouse_location,
  isd.final_warehouse_location,
  isd.store_location,
  isd.putaway_location,
  isf.fy_whrec,
  isf.half_desc,
  isf.quarter_desc,
  isf.mth_idnt,
  isf.mth_desc,
  isf.week_desc,
  isf.fy_final_wh AS fy_whshp,
  isf.half_desc_finalwh,
  isf.quarter_desc_finalwh,
  isf.mth_idnt_finalwh,
  isf.mth_desc_finalwh,
  isf.week_desc_finalwh,
  isf.fy_strrcvd,
  isf.half_desc_strrcvd,
  isf.quarter_desc_strrcvd,
  isf.mth_idnt_strrcvd,
  isf.mth_desc_strrcvd,
  isf.week_desc_strrcvd,
  isf.fy_vndrshpd,
  isf.half_desc_vndrshpd,
  isf.quarter_desc_vndrshpd,
  isf.mth_idnt_vndrshpd,
  isf.mth_desc_vndrshpd,
  isf.week_desc_vndrshpd,
  isf.fy_putaway,
  isf.half_desc_putaway,
  isf.quarter_desc_putaway,
  isf.mth_idnt_putaway,
  isf.mth_desc_putaway,
  isf.week_desc_putaway,
  isf.fy_postartship,
  isf.half_desc_postartship,
  isf.quarter_desc_postartship,
  isf.mth_idnt_postartship,
  isf.mth_desc_postartship,
  isf.week_desc_postartship,
  isf.wh_receive_to_distribute_to_putaway_days,
  isf.wh_receive_to_distribute_to_store_days,
  isf.vasn_to_distribute_to_putaway_days,
  isf.po_approved_ship_window_start_days,
  isf.po_approved_distribute_to_store_receipt_days,
  isf.po_approved_to_wh_ship_days,
  isf.ship_window_start_to_wh_ship_days,
  isf.vasn_to_wh_ship_days,
  isf.po_approval_to_wh_rec_days,
  isf.ship_window_start_to_distribute_to_putaway_days,
  isf.po_approved_to_distribute_to_putaway_days,
  isf.vendor_shipped_to_store_received_days,
  isf.vendor_shipped_to_warehouse_received_days,
  isf.warehouse_received_to_warehouse_shipped_days,
  isf.warehouse_shipped_to_store_received_days,
  isf.ship_window_start_to_vendor_shipped_days AS ship_window_to_vendor_shipped_days,
  isf.ship_window_start_to_wh_rec_days,
  isf.ship_window_start_to_store_received_days,
  SUM(isf.po_approved_ship_window_start_days * isf.unit_qty) AS weighted_cycle_units_poshipwindowstart,
  SUM(isf.po_approved_distribute_to_store_receipt_days * isf.unit_qty) AS weighted_cycle_units_podistributetostore,
  SUM(isf.wh_receive_to_distribute_to_putaway_days * isf.unit_qty) AS weighted_cycle_units_whdistributeputaway,
  SUM(isf.wh_receive_to_distribute_to_store_days * isf.unit_qty) AS weighted_cycle_units_whdistributestore,
  SUM(isf.vasn_to_distribute_to_putaway_days * isf.unit_qty) AS weighted_cycle_units_vendorputaway,
  SUM(isf.vendor_shipped_to_store_received_days * isf.unit_qty) AS weighted_cycle_units_vendorstore,
  SUM(isf.vendor_shipped_to_warehouse_received_days * isf.unit_qty) AS weighted_cycle_units_vendorwh,
  SUM(isf.warehouse_received_to_warehouse_shipped_days * isf.unit_qty) AS weighted_cycle_units_whreceivedshipped,
  SUM(isf.warehouse_shipped_to_store_received_days * isf.unit_qty) AS weighted_cycle_units_whstore,
  SUM(isf.ship_window_start_to_vendor_shipped_days * isf.unit_qty) AS weighted_cycle_units_shipvendor,
  SUM(isf.ship_window_start_to_store_received_days * isf.unit_qty) AS weighted_cycle_units_shipstore,
  SUM(isf.po_approved_to_wh_ship_days * isf.unit_qty) AS weighted_cycle_units_powhshipped,
  SUM(isf.ship_window_start_to_wh_ship_days * isf.unit_qty) AS weighted_cycle_units_shipwh,
  SUM(isf.vasn_to_wh_ship_days * isf.unit_qty) AS weighted_cycle_units_vendorwhship,
  SUM(isf.po_approval_to_wh_rec_days * isf.unit_qty) AS weighted_cycle_units_powhreceive,
  SUM(isf.ship_window_start_to_wh_rec_days * isf.unit_qty) AS weighted_cycle_units_shipwhreceive,
  SUM(isf.ship_window_start_to_distribute_to_putaway_days * isf.unit_qty) AS weighted_cycle_units_shipputaway,
  SUM(isf.po_approved_to_distribute_to_putaway_days * isf.unit_qty) AS weighted_cycle_units_poputaway,
  SUM(isf.unit_qty) AS unit_qty,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('CARTON_LIFECYCLE_TABLEAU_YOY_FACT_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('CARTON_LIFECYCLE_TABLEAU_YOY_FACT_LOAD')) AS dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM carton_lifecycle_fact_lkup AS isf
  INNER JOIN carton_lifecycle_dim_lkup AS isd ON LOWER(isf.purchase_order_num) = LOWER(isd.purchase_order_num) AND LOWER(isf
       .original_carton_num) = LOWER(isd.original_carton_num) AND LOWER(isf.to_carton_num) = LOWER(isd.to_carton_num)
   AND LOWER(isf.rms_sku_num) = LOWER(isd.rms_sku_num)
 GROUP BY isf.purchase_order_num,
  isf.original_carton_num,
  isf.to_carton_num,
  isf.rms_sku_num,
  isd.banner_country,
  isd.channel_desc,
  isd.supplier_name,
  isd.supplier_num,
  isd.div_desc,
  isf.shipping_method,
  dir_to_store_ind,
  isd.region_desc,
  po_type,
  isf.order_type,
  isd.dept_desc,
  isd.store_desc,
  isf.npg_flag,
  isf.premark_ind,
  isd.import_order_type,
  isf.internal_po_ind,
  isd.freight_inbound_terms,
  isd.merchandise_source,
  isd.distribution_center_num,
  isf.inbound_warehouse_num,
  isf.final_warehouse_num,
  isf.store_num,
  putaway_location_num,
  isd.inbound_warehouse_location,
  isd.final_warehouse_location,
  isd.store_location,
  isd.putaway_location,
  isf.fy_whrec,
  isf.half_desc,
  isf.quarter_desc,
  isf.mth_idnt,
  isf.mth_desc,
  isf.week_desc,
  fy_whshp,
  isf.half_desc_finalwh,
  isf.quarter_desc_finalwh,
  isf.mth_idnt_finalwh,
  isf.mth_desc_finalwh,
  isf.week_desc_finalwh,
  isf.fy_strrcvd,
  isf.half_desc_strrcvd,
  isf.quarter_desc_strrcvd,
  isf.mth_idnt_strrcvd,
  isf.mth_desc_strrcvd,
  isf.week_desc_strrcvd,
  isf.fy_vndrshpd,
  isf.half_desc_vndrshpd,
  isf.quarter_desc_vndrshpd,
  isf.mth_idnt_vndrshpd,
  isf.mth_desc_vndrshpd,
  isf.week_desc_vndrshpd,
  isf.fy_putaway,
  isf.half_desc_putaway,
  isf.quarter_desc_putaway,
  isf.mth_idnt_putaway,
  isf.mth_desc_putaway,
  isf.week_desc_putaway,
  isf.fy_postartship,
  isf.half_desc_postartship,
  isf.quarter_desc_postartship,
  isf.mth_idnt_postartship,
  isf.mth_desc_postartship,
  isf.week_desc_postartship,
  isf.wh_receive_to_distribute_to_putaway_days,
  isf.wh_receive_to_distribute_to_store_days,
  isf.vasn_to_distribute_to_putaway_days,
  isf.po_approved_ship_window_start_days,
  isf.po_approved_distribute_to_store_receipt_days,
  isf.po_approved_to_wh_ship_days,
  isf.ship_window_start_to_wh_ship_days,
  isf.vasn_to_wh_ship_days,
  isf.po_approval_to_wh_rec_days,
  isf.ship_window_start_to_wh_rec_days,
  isf.ship_window_start_to_distribute_to_putaway_days,
  isf.po_approved_to_distribute_to_putaway_days,
  isf.vendor_shipped_to_store_received_days,
  isf.vendor_shipped_to_warehouse_received_days,
  isf.warehouse_received_to_warehouse_shipped_days,
  isf.warehouse_shipped_to_store_received_days,
  ship_window_to_vendor_shipped_days,
  isf.ship_window_start_to_store_received_days,
  dw_batch_id,
  dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));