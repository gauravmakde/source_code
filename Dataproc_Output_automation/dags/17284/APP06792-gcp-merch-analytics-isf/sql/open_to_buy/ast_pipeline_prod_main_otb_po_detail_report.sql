--This is the data that feeds the PO DETAIL REPORT
--It uses the data from T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL and brings in the CM data from AB
--Developed by: Isaac Wallick
--Updated last: 1/19/2024



CREATE TEMPORARY TABLE IF NOT EXISTS po_detail_base
AS
SELECT a.purchase_order_num,
 a.status,
 a.edi_ind,
 a.start_ship_date,
 a.end_ship_date,
 CAST(b.latest_approval_event_tmstp_pacific AS DATE) AS latest_approval_date,
 a.open_to_buy_endofweek_date,
 a.order_type,
 b.internal_po_ind,
 b.npg_ind,
 b.po_type,
 b.purchase_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS b 
 ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_number
   )
WHERE 
(LOWER(a.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')) 
OR LOWER(a.status) = LOWER('CLOSED') AND 
a.open_to_buy_endofweek_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY))
 AND LOWER(a.dropship_ind) <> LOWER('t')
 AND a.original_approval_date IS NOT NULL;


--COLLECT STATS      PRIMARY INDEX (purchase_order_num)      ,COLUMN(purchase_order_num)      ON po_detail_base


CREATE TEMPORARY TABLE IF NOT EXISTS cm_otb_date
AS
SELECT CAST(FORMAT('%11d', yr_454) || '-' || month_num || '-' || day_idnt AS DATE) AS day_date,
 otb_eow_date
FROM (
  SELECT DISTINCT 
   SUBSTR(otb_eow_date, 1, 2) AS day_idnt,
   SUBSTR(otb_eow_date, 4, 3) AS month_abrv,
   CAST(FORMAT('%4d', 20) || SUBSTR(otb_eow_date, 8, 2) AS INTEGER) AS yr_454,
   otb_eow_date,
    CASE
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('JAN')
    THEN '01'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('FEB')
    THEN '02'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('MAR')
    THEN '03'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('APR')
    THEN '04'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('MAY')
    THEN '05'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('JUN')
    THEN '06'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('JUL')
    THEN '07'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('AUG')
    THEN '08'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('SEP')
    THEN '09'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('OCT')
    THEN '10'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('NOV')
    THEN '11'
    WHEN LOWER(SUBSTR(otb_eow_date, 4, 3)) = LOWER('DEC')
    THEN '12'
    ELSE NULL
    END AS month_num
  FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.ab_cm_orders_current) AS a
GROUP BY day_date,
 otb_eow_date;


--COLLECT STATS      PRIMARY INDEX (day_date)      ON cm_otb_date


CREATE TEMPORARY TABLE IF NOT EXISTS po_dtd_date
AS
SELECT FORMAT('%11d', fiscal_year_num) || FORMAT('%11d', fiscal_month_num) || ' ' || month_abrv AS month,
 month_idnt,
 month_label,
 day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 180 DAY) AND (CURRENT_DATE('PST8PDT'))
GROUP BY month,
 month_idnt,
 month_label,
 day_date;


--COLLECT STATS      PRIMARY INDEX (day_date)      ON po_dtd_date


CREATE TEMPORARY TABLE IF NOT EXISTS po_mtd_date
AS
SELECT b.month_idnt
FROM (SELECT MAX(month_idnt) AS month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE week_start_day_date <= CURRENT_DATE('PST8PDT')) AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
 ON a.month_idnt = b.month_idnt
WHERE b.week_start_day_date <= CURRENT_DATE('PST8PDT')
GROUP BY b.month_idnt;


--COLLECT STATS      PRIMARY INDEX (month_idnt)      ,COLUMN(month_idnt)      ON po_mtd_date


CREATE TEMPORARY TABLE IF NOT EXISTS po_previous_date
AS
SELECT month_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 180 DAY) AND (CURRENT_DATE('PST8PDT'))
 AND month_idnt < (SELECT month_idnt FROM po_mtd_date
   GROUP BY month_idnt)
GROUP BY month_idnt;


--COLLECT STATS      PRIMARY INDEX (month_idnt)      ,COLUMN(month_idnt)      ON po_previous_date


--drop table po_current_future_date;


CREATE TEMPORARY TABLE IF NOT EXISTS po_current_future_date
AS
SELECT FORMAT('%11d', fiscal_year_num) || FORMAT('%11d', fiscal_month_num) || ' ' || month_abrv AS month,
 month_idnt,
  CASE
  WHEN 
  CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date
  THEN 1
  ELSE 0
  END AS curr_month_flag,
 quarter_label,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE CASE
   WHEN CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date
   THEN 1
   ELSE 0
   END = 1
 OR day_date >= CURRENT_DATE('PST8PDT')
GROUP BY 
 month,
 month_idnt,
 curr_month_flag,
 quarter_label,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (MONTH_IDNT)      ,COLUMN(MONTH_IDNT)      ON po_current_future_date


--DROP TABLE po_supp_gp


--where a.dept_num = '883'


CREATE TEMPORARY TABLE IF NOT EXISTS po_supp_gp
AS
SELECT dept_num,
 supplier_num,
 supplier_group,
  CASE
  WHEN LOWER(banner) = LOWER('FP')
  THEN 'NORDSTROM'
  ELSE 'NORDSTROM_RACK'
  END AS selling_brand
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.supp_dept_map_dim AS a
GROUP BY dept_num,
 supplier_num,
 supplier_group,
 selling_brand;


--COLLECT STATS      PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)      ,COLUMN(DEPT_NUM)      ,COLUMN(SUPPLIER_GROUP) 	ON po_supp_gp


--where a.dept_num = '883'


CREATE TEMPORARY TABLE IF NOT EXISTS po_cattr
AS
SELECT dept_num,
 class_num,
 sbclass_num,
 category
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS a
GROUP BY dept_num,
 class_num,
 sbclass_num,
 category;


--COLLECT STATS      PRIMARY INDEX(dept_num,CATEGORY)      ,COLUMN(dept_num)      ,COLUMN(CATEGORY)      ON po_cattr


--drop table po_detail_loc


-- can be found as CLSD or CLOSED


CREATE TEMPORARY TABLE IF NOT EXISTS po_detail_loc
AS
SELECT channel_num,
 channel_brand,
 channel_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 140, 210, 220, 240, 250, 260, 310, 930, 940, 990, 111, 121, 211, 221, 261, 311)
 AND LOWER(channel_country) = LOWER('US')
 AND LOWER(store_name) NOT LIKE LOWER('CLSD%')
 AND LOWER(store_name) NOT LIKE LOWER('CLOSED%')
GROUP BY channel_num,
 channel_brand,
 channel_desc;


--drop table po_detail_chnl_loc


-- can be found as CLSD or CLOSED


CREATE TEMPORARY TABLE IF NOT EXISTS po_detail_chnl_loc
AS
SELECT channel_num,
 channel_desc,
 channel_brand,
 store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 140, 210, 220, 240, 250, 260, 310, 930, 940, 990, 111, 121, 211, 221, 261, 311)
 AND LOWER(channel_country) = LOWER('US')
 AND LOWER(store_name) NOT LIKE LOWER('CLSD%')
 AND LOWER(store_name) NOT LIKE LOWER('CLOSED%')
GROUP BY channel_num,
 channel_desc,
 channel_brand,
 store_num;


--COLLECT STATS      PRIMARY INDEX (store_num)      ,COLUMN(CHANNEL_NUM)      ON po_detail_chnl_loc


--where purchase_order_num = '25272105'


CREATE TEMPORARY TABLE IF NOT EXISTS po_detail_rcpts
AS
SELECT g.purchase_order_num,
 g.channel_num,
 g.channel_desc,
 g.channel_brand,
 g.division_num,
 g.division_name,
 g.subdivision_num,
 g.subdivision_name,
 g.dept_num,
 g.dept_name,
 g.class_num,
 g.class_desc,
 g.sbclass_num,
 g.sbclass_desc,
 g.customer_choice,
 g.supp_num,
 g.supp_name,
 g.manufacturer_name,
 g.vpn,
 g.style_desc,
 g.color_num,
 g.supp_color,
 g.status,
  CASE
  WHEN LOWER(g.edi_ind) = LOWER('T')
  THEN 'Y'
  ELSE 'N'
  END AS edi_ind,
 g.start_ship_date,
 g.end_ship_date,
 g.latest_approval_date,
 g.otb_eow_date,
 g.otb_month,
 g.otb_month_idnt,
 g.order_type,
  CASE
  WHEN LOWER(g.internal_po_ind) = LOWER('t')
  THEN 'Y'
  ELSE 'N'
  END AS internal_po_ind,
  CASE
  WHEN LOWER(g.npg_ind) = LOWER('T')
  THEN 'Y'
  ELSE 'N'
  END AS npg_ind,
 g.po_type,
 g.purchase_type,
 g.month,
 g.month_idnt,
 g.fiscal_year_num,
 g.quarter_label,
 g.ttl_approved_qty,
 g.ttl_approved_c,
 g.ttl_approved_r,
 g.dc_received_qty,
 g.dc_received_c,
 MAX(g.unit_cost_amt) AS unit_cost_amt,
 MAX(g.last_carrier_asn_date) AS last_carrier_asn_date,
 MAX(g.last_supplier_asn_date) AS last_supplier_asn_date,
 MAX(g.last_ship_activity_date) AS last_ship_activity_date,
 MAX(g.last_outbound_activity_date) AS last_outbound_activity_date,
 MAX(g.receipt_date) AS receipt_date,
 SUM(CASE
   WHEN a.month_idnt IS NOT NULL
   THEN g.rcpt_cost
   ELSE 0
   END) AS rcpt_mtd_cost,
 SUM(CASE
   WHEN b.month_idnt IS NOT NULL
   THEN g.rcpt_cost
   ELSE 0
   END) AS rcpt_previous_mth_cost,
 SUM(CASE
   WHEN a.month_idnt IS NOT NULL
   THEN g.rcpt_units
   ELSE 0
   END) AS rcpt_mtd_units,
 SUM(CASE
   WHEN b.month_idnt IS NOT NULL
   THEN g.rcpt_units
   ELSE 0
   END) AS rcpt_previous_mth_units,
 SUM(g.rcpt_cost) AS rcpt_cost,
 SUM(g.rcpt_retail) AS rcpt_retail,
 SUM(g.rcpt_units) AS rcpt_units,
 SUM(g.quantity_ordered) AS quantity_ordered,
 SUM(g.quantity_received) AS quantity_received,
 SUM(g.quantity_canceled) AS quantity_canceled,
 SUM(g.quantity_open) AS quantity_open,
 SUM(g.total_estimated_landing_cost) AS oo_cost,
 SUM(g.total_anticipated_retail_amt) AS oo_retail
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail AS g
 LEFT JOIN po_mtd_date AS a 
 ON g.month_idnt = a.month_idnt
 LEFT JOIN po_previous_date AS b 
 ON g.month_idnt = b.month_idnt
GROUP BY g.purchase_order_num,
 g.channel_num,
 g.channel_desc,
 g.channel_brand,
 g.division_num,
 g.division_name,
 g.subdivision_num,
 g.subdivision_name,
 g.dept_num,
 g.dept_name,
 g.class_num,
 g.class_desc,
 g.sbclass_num,
 g.sbclass_desc,
 g.customer_choice,
 g.supp_num,
 g.supp_name,
 g.manufacturer_name,
 g.vpn,
 g.style_desc,
 g.color_num,
 g.supp_color,
 g.status,
 edi_ind,
 g.start_ship_date,
 g.end_ship_date,
 g.latest_approval_date,
 g.otb_eow_date,
 g.otb_month,
 g.otb_month_idnt,
 g.order_type,
 internal_po_ind,
 npg_ind,
 g.po_type,
 g.purchase_type,
 g.month,
 g.month_idnt,
 g.fiscal_year_num,
 g.quarter_label,
 g.ttl_approved_qty,
 g.ttl_approved_c,
 g.ttl_approved_r,
 g.dc_received_qty,
 g.dc_received_c;


--COLLECT STATS      PRIMARY INDEX(purchase_order_num)      ,COLUMN(purchase_order_num)      ON po_detail_rcpts


--where a.dept_num = '883'


--where c.dept_num = '883'


CREATE TEMPORARY TABLE IF NOT EXISTS po_supp_cat
AS
SELECT c.purchase_order_num,
 c.channel_num,
 c.channel_desc,
 c.channel_brand,
 c.division_num,
 c.division_name,
 c.subdivision_num,
 c.subdivision_name,
 c.dept_num,
 c.dept_name,
 c.class_num,
 c.class_desc,
 c.sbclass_num,
 c.sbclass_desc,
 c.customer_choice,
 c.supp_num,
 c.supp_name,
 c.manufacturer_name,
 c.vpn,
 c.style_desc,
 c.color_num,
 c.supp_color,
 c.status,
 c.edi_ind,
 c.start_ship_date,
 c.end_ship_date,
 c.latest_approval_date,
 c.otb_eow_date,
 c.otb_month,
 c.otb_month_idnt,
 c.order_type,
 c.internal_po_ind,
 c.npg_ind,
 c.po_type,
 c.purchase_type,
 c.month,
 c.month_idnt,
 c.fiscal_year_num,
 c.quarter_label,
 c.ttl_approved_qty,
 c.ttl_approved_c,
 c.ttl_approved_r,
 c.dc_received_qty,
 c.dc_received_c,
 c.unit_cost_amt,
 c.last_carrier_asn_date,
 c.last_supplier_asn_date,
 c.last_ship_activity_date,
 c.last_outbound_activity_date,
 c.receipt_date,
 c.rcpt_mtd_cost,
 c.rcpt_previous_mth_cost,
 c.rcpt_mtd_units,
 c.rcpt_previous_mth_units,
 c.rcpt_cost,
 c.rcpt_retail,
 c.rcpt_units,
 c.quantity_ordered,
 c.quantity_received,
 c.quantity_canceled,
 c.quantity_open,
 c.oo_cost,
 c.oo_retail,
 c.supplier_group,
 COALESCE(h.category, i.category, 'OTHER') AS category
FROM (SELECT a.purchase_order_num,
   a.channel_num,
   a.channel_desc,
   a.channel_brand,
   a.division_num,
   a.division_name,
   a.subdivision_num,
   a.subdivision_name,
   a.dept_num,
   a.dept_name,
   a.class_num,
   a.class_desc,
   a.sbclass_num,
   a.sbclass_desc,
   a.customer_choice,
   a.supp_num,
   a.supp_name,
   a.manufacturer_name,
   a.vpn,
   a.style_desc,
   a.color_num,
   a.supp_color,
   a.status,
   a.edi_ind,
   a.start_ship_date,
   a.end_ship_date,
   a.latest_approval_date,
   a.otb_eow_date,
   a.otb_month,
   a.otb_month_idnt,
   a.order_type,
   a.internal_po_ind,
   a.npg_ind,
   a.po_type,
   a.purchase_type,
   a.month,
   a.month_idnt,
   a.fiscal_year_num,
   a.quarter_label,
   a.ttl_approved_qty,
   a.ttl_approved_c,
   a.ttl_approved_r,
   a.dc_received_qty,
   a.dc_received_c,
   a.unit_cost_amt,
   a.last_carrier_asn_date,
   a.last_supplier_asn_date,
   a.last_ship_activity_date,
   a.last_outbound_activity_date,
   a.receipt_date,
   a.rcpt_mtd_cost,
   a.rcpt_previous_mth_cost,
   a.rcpt_mtd_units,
   a.rcpt_previous_mth_units,
   a.rcpt_cost,
   a.rcpt_retail,
   a.rcpt_units,
   a.quantity_ordered,
   a.quantity_received,
   a.quantity_canceled,
   a.quantity_open,
   a.oo_cost,
   a.oo_retail,
   COALESCE(b.supplier_group, 'OTHER') AS supplier_group
  FROM po_detail_rcpts AS a
   LEFT JOIN po_supp_gp AS b 
   ON a.dept_num = CAST(b.dept_num AS FLOAT64) 
   AND LOWER(a.supp_num) = LOWER(b.supplier_num)
    AND LOWER(a.channel_brand) = LOWER(b.selling_brand)) AS c
 LEFT JOIN po_cattr AS h 
 ON CAST(c.dept_num AS INTEGER) = h.dept_num 
 AND LOWER(SUBSTR(CAST(c.class_num AS STRING), 1, 50
      )) = LOWER(h.class_num) 
      AND LOWER(SUBSTR(CAST(c.sbclass_num AS STRING), 1, 50)) = LOWER(h.sbclass_num)
 LEFT JOIN po_cattr AS i 
 ON CAST(c.dept_num AS INTEGER) = i.dept_num 
 AND LOWER(SUBSTR(CAST(c.class_num AS STRING), 1, 50)) = LOWER(i.class_num) 
AND CAST(SUBSTR(i.sbclass_num, 1, 50) AS FLOAT64) = - 1;


--COLLECT STATS      PRIMARY INDEX(purchase_order_num)      ,COLUMN(purchase_order_num)      ON po_supp_cat


--drop table po_cm;
--lets get the cm data


CREATE TEMPORARY TABLE IF NOT EXISTS po_cm
AS
SELECT l.po_number,
 i.status,
 i.edi_ind,
 i.start_ship_date,
 i.end_ship_date,
 i.latest_approval_date,
 i.order_type,
 i.internal_po_ind,
 l.npg_ind,
 i.po_type,
 i.purchase_type,
 l.channel_num,
 l.channel_desc,
 l.channel_brand,
 l.division_num,
 l.division_name,
 l.subdivision_num,
 l.subdivision_name,
 l.dept_id,
 l.dept_name,
 l.class_id,
 l.subclass_id,
 l.supp_id,
 l.supp_name,
 l.fiscal_month,
 l.otb_month,
 k.day_date AS otb_eow_date,
 l.quarter_label,
 l.fiscal_year_num,
 l.fiscal_month_id,
 l.vpn,
 l.vpn_desc,
 l.supp_color,
 l.supplier_group,
 COALESCE(g.category, h.category, 'OTHER') AS category,
 l.nrf_color_code,
 l.plan_season_desc,
 l.plan_type,
 l.cm_c,
 l.cm_r,
 l.cm_u
FROM (SELECT e.po_number,
   e.channel_num,
   e.channel_desc,
   e.channel_brand,
   e.division_num,
   e.division_name,
   e.subdivision_num,
   e.subdivision_name,
   e.dept_id,
   e.dept_name,
   e.class_id,
   e.subclass_id,
   e.supp_id,
   e.supp_name,
   e.month AS fiscal_month,
   e.month AS otb_month,
   e.otb_eow_date,
   e.quarter_label,
   e.fiscal_year_num,
   e.fiscal_month_id,
   e.vpn,
   e.vpn_desc,
   e.supp_color,
   COALESCE(f.supplier_group, 'OTHER') AS supplier_group,
   e.nrf_color_code,
   e.plan_season_desc,
   e.plan_type,
   e.npg_ind,
   e.cm_c,
   e.cm_r,
   e.cm_u
  FROM (SELECT 
    a.po_number,
     a.org_id,
     b.channel_num,
     b.channel_desc,
     b.channel_brand,
     d.division_num,
     d.division_name,
     d.subdivision_num,
     d.subdivision_name,
     a.dept_id,
     d.dept_name,
     a.class_id,
     a.subclass_id,
     a.supp_id,
     a.supp_name,
     c.month,
     a.fiscal_month_id,
     c.quarter_label,
     c.fiscal_year_num,
     a.otb_eow_date,
     a.vpn,
     a.vpn_desc,
     a.supp_color,
     a.nrf_color_code,
     a.plan_season_desc,
     a.plan_type,
     a.npg_ind,
     SUM(a.ttl_cost_us) AS cm_c,
     SUM(a.rcpt_units * a.unit_rtl_us) AS cm_r,
     SUM(a.rcpt_units) AS cm_u
    FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.ab_cm_orders_current AS a
     INNER JOIN po_detail_loc AS b 
     ON a.org_id = b.channel_num
     INNER JOIN po_current_future_date AS c 
     ON CAST(a.fiscal_month_id AS FLOAT64) = c.month_idnt
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS d 
     ON a.dept_id = d.dept_num
    GROUP BY a.po_number,
     a.org_id,
     b.channel_num,
     b.channel_desc,
     b.channel_brand,
     d.division_num,
     d.division_name,
     d.subdivision_num,
     d.subdivision_name,
     a.dept_id,
     d.dept_name,
     a.class_id,
     a.subclass_id,
     a.supp_id,
     a.supp_name,
     c.month,
     a.fiscal_month_id,
     c.quarter_label,
     c.fiscal_year_num,
     a.otb_eow_date,
     a.vpn,
     a.vpn_desc,
     a.supp_color,
     a.nrf_color_code,
     a.plan_season_desc,
     a.plan_type,
     a.npg_ind) AS e
   LEFT JOIN po_supp_gp AS f 
   ON e.dept_id = CAST(f.dept_num AS FLOAT64) 
   AND LOWER(e.supp_id) = LOWER(f.supplier_num) 
   AND LOWER(e.channel_brand) = LOWER(f.selling_brand)) AS l
 LEFT JOIN po_cattr AS g 
 ON l.dept_id = g.dept_num 
 AND l.class_id = CAST(g.class_num AS FLOAT64) 
 AND l.subclass_id = CAST(g.sbclass_num AS FLOAT64)
   
 LEFT JOIN po_cattr AS h 
 ON l.dept_id = h.dept_num 
 AND l.class_id = CAST(h.class_num AS FLOAT64) 
 AND CAST(h.sbclass_num AS FLOAT64)
   = - 1
 LEFT JOIN po_detail_base AS i 
 ON LOWER(l.po_number) = LOWER(i.purchase_order_num)
 INNER JOIN cm_otb_date AS k 
 ON LOWER(l.otb_eow_date) = LOWER(k.otb_eow_date);


--COLLECT STATS      PRIMARY INDEX(po_number)      ,COLUMN(po_number)      ON po_cm


CREATE TEMPORARY TABLE IF NOT EXISTS po_supp
AS
SELECT purchase_order_num,
 channel_num,
 channel_desc,
 channel_brand,
 division_num,
 division_name,
 subdivision_num,
 subdivision_name,
 dept_num,
 dept_name,
 class_num,
 sbclass_num,
 customer_choice,
 supp_num,
 supp_name,
 manufacturer_name,
 vpn,
 vpn_desc,
 color_num,
 supp_color,
 supplier_group,
 category,
 status,
  CASE
  WHEN LOWER(edi_ind) = LOWER('T') OR LOWER(edi_ind) = LOWER('Y')
  THEN 'Y'
  ELSE 'N'
  END AS edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 otb_month,
 otb_month_idnt,
 order_type,
  CASE
  WHEN LOWER(internal_po_ind) = LOWER('T') 
  OR LOWER(internal_po_ind) = LOWER('Y')
  THEN 'Y'
  ELSE 'N'
  END AS internal_po_ind,
  CASE
  WHEN LOWER(npg_ind) = LOWER('T') 
  OR LOWER(npg_ind) = LOWER('Y')
  THEN 'Y'
  ELSE 'N'
  END AS npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 fiscal_year_num,
 quarter_label,
 plan_season_desc,
 plan_type,
 ttl_approved_c,
 ttl_approved_r,
 ttl_approved_qty,
 dc_received_qty,
 dc_received_c,
 MAX(unit_cost_amt) AS unit_cost_amt,
 MAX(last_carrier_asn_date) AS last_carrier_asn_date,
 MAX(last_supplier_asn_date) AS last_supplier_asn_date,
 MAX(last_ship_activity_date) AS last_ship_activity_date,
 MAX(last_outbound_activity_date) AS last_outbound_activity_date,
 MAX(receipt_date) AS receipt_date,
 SUM(rcpt_mtd_cost) AS rcpt_mtd_cost,
 SUM(rcpt_previous_mth_cost) AS rcpt_previous_mth_cost,
 SUM(rcpt_cost) AS rcpt_cost,
 SUM(rcpt_retail) AS rcpt_retail,
 SUM(rcpt_mtd_units) AS rcpt_mtd_units,
 SUM(rcpt_previous_mth_units) AS rcpt_previous_mth_units,
 SUM(rcpt_units) AS rcpt_units,
 SUM(quantity_ordered) AS quantity_ordered,
 SUM(quantity_received) AS quantity_received,
 SUM(quantity_canceled) AS quantity_canceled,
 SUM(quantity_open) AS quantity_open,
 SUM(oo_cost) AS oo_cost,
 SUM(oo_retail) AS oo_retail,
 SUM(cm_c) AS cm_c,
 SUM(cm_r) AS cm_r,
 SUM(cm_u) AS cm_u
FROM (
  SELECT 
    SUBSTR(purchase_order_num, 1, 50) AS purchase_order_num,
    SUBSTR(CAST(channel_num AS STRING), 1, 50) AS channel_num,
    SUBSTR(channel_desc, 1, 50) AS channel_desc,
    SUBSTR(channel_brand, 1, 50) AS channel_brand,
    SUBSTR(CAST(division_num AS STRING), 1, 50) AS division_num,
    SUBSTR(division_name, 1, 50) AS division_name,
    SUBSTR(CAST(subdivision_num AS STRING), 1, 50) AS subdivision_num,
    SUBSTR(subdivision_name, 1, 50) AS subdivision_name,
    SUBSTR(CAST(dept_num AS STRING), 1, 50) AS dept_num,
    SUBSTR(dept_name, 1, 50) AS dept_name,
    SUBSTR(CAST(class_num AS STRING), 1, 50) AS class_num,
    SUBSTR(CAST(sbclass_num AS STRING), 1, 50) AS sbclass_num,
    SUBSTR(customer_choice, 1, 50) AS customer_choice,
    SUBSTR(supp_num, 1, 50) AS supp_num,
    SUBSTR(supp_name, 1, 50) AS supp_name,
    SUBSTR(manufacturer_name, 1, 50) AS manufacturer_name,
    SUBSTR(vpn, 1, 50) AS vpn,
    SUBSTR(style_desc, 1, 50) AS vpn_desc,
    SUBSTR(color_num, 1, 50) AS color_num,
    SUBSTR(supp_color, 1, 50) AS supp_color,
    SUBSTR(supplier_group, 1, 50) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    SUBSTR(status, 1, 50) AS status,
    SUBSTR(edi_ind, 1, 50) AS edi_ind,
    CAST(start_ship_date AS DATE) AS start_ship_date,
    CAST(end_ship_date AS DATE) AS end_ship_date,
    CAST(latest_approval_date AS DATE) AS latest_approval_date,
    CAST(otb_eow_date AS DATE) AS otb_eow_date,
    SUBSTR(otb_month, 1, 50) AS otb_month,
    SUBSTR(CAST(otb_month_idnt AS STRING), 1, 50) AS otb_month_idnt,
    SUBSTR(order_type, 1, 50) AS order_type,
    SUBSTR(internal_po_ind, 1, 50) AS internal_po_ind,
    SUBSTR(npg_ind, 1, 50) AS npg_ind,
    SUBSTR(po_type, 1, 50) AS po_type,
    SUBSTR(purchase_type, 1, 50) AS purchase_type,
    SUBSTR(month, 1, 50) AS month,
    SUBSTR(CAST(month_idnt AS STRING), 1, 50) AS month_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 50) AS fiscal_year_num,
    SUBSTR(quarter_label, 1, 50) AS quarter_label,
    CAST(last_carrier_asn_date AS DATE) AS last_carrier_asn_date,
    CAST(last_supplier_asn_date AS DATE) AS last_supplier_asn_date,
    CAST(last_ship_activity_date AS DATE) AS last_ship_activity_date,
    CAST(last_outbound_activity_date AS DATE) AS last_outbound_activity_date,
    CAST(receipt_date AS DATE) AS receipt_date,
    CAST(rcpt_mtd_cost AS NUMERIC) AS rcpt_mtd_cost,
    CAST(rcpt_previous_mth_cost AS NUMERIC) AS rcpt_previous_mth_cost,
    CAST(rcpt_cost AS NUMERIC) AS rcpt_cost,
    CAST(rcpt_retail AS NUMERIC) AS rcpt_retail,
    CAST(rcpt_mtd_units AS NUMERIC) AS rcpt_mtd_units,
    CAST(rcpt_previous_mth_units AS NUMERIC) AS rcpt_previous_mth_units,
    CAST(rcpt_units AS NUMERIC) AS rcpt_units,
    CAST(quantity_ordered AS NUMERIC) AS quantity_ordered,
    CAST(quantity_received AS NUMERIC) AS quantity_received,
    CAST(quantity_canceled AS NUMERIC) AS quantity_canceled,
    CAST(quantity_open AS NUMERIC) AS quantity_open,
    CAST(unit_cost_amt AS NUMERIC) AS unit_cost_amt,
    CAST(oo_cost AS NUMERIC) AS oo_cost,
    CAST(oo_retail AS NUMERIC) AS oo_retail,
    CAST(ttl_approved_qty AS NUMERIC) AS ttl_approved_qty,
    CAST(ttl_approved_c AS NUMERIC) AS ttl_approved_c,
    CAST(ttl_approved_r AS NUMERIC) AS ttl_approved_r,
    CAST(dc_received_qty AS NUMERIC) AS dc_received_qty,
    CAST(dc_received_c AS NUMERIC) AS dc_received_c,
    SUBSTR('0', 1, 50) AS plan_season_desc,
    SUBSTR('0', 1, 50) AS plan_type,
    0 AS cm_c,
    0 AS cm_r,
    0 AS cm_u
   FROM po_supp_cat AS g
   UNION ALL
   SELECT 
    SUBSTR(po_number, 1, 50) AS purchase_order_num,
    SUBSTR(CAST(channel_num AS STRING), 1, 50) AS channel_num,
    SUBSTR(channel_desc, 1, 50) AS channel_desc,
    SUBSTR(channel_brand, 1, 50) AS channel_brand,
    SUBSTR(CAST(division_num AS STRING), 1, 50) AS division_num,
    SUBSTR(division_name, 1, 50) AS division_name,
    SUBSTR(CAST(subdivision_num AS STRING), 1, 50) AS subdivision_num,
    SUBSTR(subdivision_name, 1, 50) AS subdivision_name,
    SUBSTR(CAST(dept_id AS STRING), 1, 50) AS dept_num,
    SUBSTR(dept_name, 1, 50) AS dept_name,
    SUBSTR(CAST(class_id AS STRING), 1, 50) AS class_num,
    SUBSTR(CAST(subclass_id AS STRING), 1, 50) AS sbclass_num,
    SUBSTR('0', 1, 50) AS customer_choice,
    SUBSTR(supp_id, 1, 50) AS supp_num,
    SUBSTR(supp_name, 1, 50) AS supp_name,
    SUBSTR('0', 1, 50) AS manufacturer_name,
    SUBSTR(vpn, 1, 50) AS vpn,
    SUBSTR(vpn_desc, 1, 50) AS vpn_desc,
    SUBSTR(CAST(nrf_color_code AS STRING), 1, 50) AS color_num,
    SUBSTR(supp_color, 1, 50) AS supp_color,
    SUBSTR(supplier_group, 1, 50) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    SUBSTR(status, 1, 50) AS status,
    SUBSTR(edi_ind, 1, 50) AS edi_ind,
    CAST(start_ship_date AS DATE) AS start_ship_date,
    CAST(end_ship_date AS DATE) AS end_ship_date,
    CAST(latest_approval_date AS DATE) AS latest_approval_date,
    otb_eow_date,
    SUBSTR(otb_month, 1, 50) AS otb_month,
    SUBSTR(fiscal_month_id, 1, 50) AS otb_month_idnt,
    SUBSTR(order_type, 1, 50) AS order_type,
    SUBSTR(internal_po_ind, 1, 50) AS internal_po_ind,
    SUBSTR(npg_ind, 1, 50) AS npg_ind,
    SUBSTR(po_type, 1, 50) AS po_type,
    SUBSTR(purchase_type, 1, 50) AS purchase_type,
    SUBSTR(fiscal_month, 1, 50) AS month,
    SUBSTR(fiscal_month_id, 1, 50) AS month_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 50) AS fiscal_year_num,
    SUBSTR(quarter_label, 1, 50) AS quarter_label,
    CAST(NULL AS DATE) AS last_carrier_asn_date,
    CAST(NULL AS DATE) AS last_supplier_asn_date,
    CAST(NULL AS DATE) AS last_ship_activity_date,
    CAST(NULL AS DATE) AS last_outbound_activity_date,
    CAST(NULL AS DATE) AS receipt_date,
    0 AS rcpt_mtd_cost,
    0 AS rcpt_previous_mth_cost,
    0 AS rcpt_retail,
    0 AS rcpt_cost,
    0 AS rcpt_mtd_units,
    0 AS rcpt_previous_mth_units,
    0 AS rcpt_units,
    0 AS quantity_ordered,
    0 AS quantity_received,
    0 AS quantity_canceled,
    0 AS quantity_open,
    0 AS unit_cost_amt,
    0 AS oo_cost,
    0 AS oo_retail,
    0 AS ttl_approved_qty,
    0 AS ttl_approved_c,
    0 AS ttl_approved_r,
    0 AS dc_received_qty,
    0 AS dc_received_c,
    SUBSTR(plan_season_desc, 1, 50) AS plan_season_desc,
    SUBSTR(plan_type, 1, 50) AS plan_type,
    CAST(cm_c AS NUMERIC) AS cm_c,
    CAST(cm_r AS NUMERIC) AS cm_r,
    CAST(cm_u AS NUMERIC) AS cm_u
   FROM po_cm AS j) AS b
GROUP BY purchase_order_num,
 channel_num,
 channel_desc,
 channel_brand,
 division_num,
 division_name,
 subdivision_num,
 subdivision_name,
 dept_num,
 dept_name,
 class_num,
 sbclass_num,
 customer_choice,
 supp_num,
 supp_name,
 manufacturer_name,
 vpn,
 vpn_desc,
 color_num,
 supp_color,
 supplier_group,
 category,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 otb_month,
 otb_month_idnt,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 fiscal_year_num,
 quarter_label,
 plan_season_desc,
 plan_type,
 ttl_approved_c,
 ttl_approved_r,
 ttl_approved_qty,
 dc_received_qty,
 dc_received_c;


--COLLECT STATS      PRIMARY INDEX(purchase_order_num)      ,COLUMN(purchase_order_num)      ON po_supp


--drop table po_supp_final


CREATE TEMPORARY TABLE IF NOT EXISTS po_supp_final
AS
SELECT purchase_order_num,
 channel_num,
 channel_desc,
 channel_brand,
 division_num,
 division_name,
 subdivision_num,
 subdivision_name,
 dept_num,
 dept_name,
 supp_num,
 supp_name,
 customer_choice,
 manufacturer_name,
 supplier_group,
  CASE
  WHEN status IS NULL
  THEN 'CM'
  ELSE status
  END AS status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 otb_month,
 otb_month_idnt,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 fiscal_year_num,
 quarter_label,
 plan_season_desc,
 plan_type,
 ttl_approved_qty,
 ttl_approved_c,
 ttl_approved_r,
 dc_received_qty,
 dc_received_c,
 MAX(unit_cost_amt) AS unit_cost_amt,
 MAX(last_carrier_asn_date) AS last_carrier_asn_date,
 MAX(last_supplier_asn_date) AS last_supplier_asn_date,
 MAX(last_ship_activity_date) AS last_ship_activity_date,
 MAX(last_outbound_activity_date) AS last_outbound_activity_date,
 MAX(receipt_date) AS receipt_date,
 SUM(rcpt_mtd_cost) AS rcpt_mtd_cost,
 SUM(rcpt_previous_mth_cost) AS rcpt_previous_mth_cost,
 SUM(rcpt_cost) AS rcpt_cost,
 SUM(rcpt_retail) AS rcpt_retail,
 SUM(rcpt_mtd_units) AS rcpt_mtd_units,
 SUM(rcpt_previous_mth_units) AS rcpt_previous_mth_units,
 SUM(rcpt_units) AS rcpt_units,
 SUM(quantity_ordered) AS quantity_ordered,
 SUM(quantity_received) AS quantity_received,
 SUM(quantity_canceled) AS quantity_canceled,
 SUM(quantity_open) AS quantity_open,
 SUM(oo_cost) AS oo_cost,
 SUM(oo_retail) AS oo_retail,
 SUM(cm_c) AS cm_c,
 SUM(cm_r) AS cm_r,
 SUM(cm_u) AS cm_u,
 CURRENT_DATE('PST8PDT') AS process_dt
FROM po_supp AS g
GROUP BY purchase_order_num,
 channel_num,
 channel_desc,
 channel_brand,
 division_num,
 division_name,
 subdivision_num,
 subdivision_name,
 dept_num,
 dept_name,
 supp_num,
 supp_name,
 customer_choice,
 manufacturer_name,
 supplier_group,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 otb_month,
 otb_month_idnt,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 fiscal_year_num,
 quarter_label,
 plan_season_desc,
 plan_type,
 ttl_approved_qty,
 ttl_approved_c,
 ttl_approved_r,
 dc_received_qty,
 dc_received_c;


--COLLECT STATS      PRIMARY INDEX(purchase_order_num)      ,COLUMN(purchase_order_num)      ON po_supp_final


--drop table po_vasn


--where a.PURCHASE_ORDER_NUM = '40073120'


CREATE TEMPORARY TABLE IF NOT EXISTS po_vasn
AS
SELECT 
 g.purchase_order_num,
 MAX(g.vendor_ship_dt) AS last_vasn_signal,
 SUM(g.vasn_sku_qty) AS vasn_sku_qty,
 SUM(h.unit_cost * g.vasn_sku_qty + h.total_expenses_per_unit * g.vasn_sku_qty + h.total_duty_per_unit * g.vasn_sku_qty
  ) AS vasn_cost
FROM (
  SELECT 
   c.purchase_order_num,
   c.ship_location_id,
   c.upc_num,
   d.rms_sku_num,
   c.vasn_sku_qty,
   c.vendor_ship_dt
  FROM (SELECT 
     a.purchase_order_num,
     a.ship_location_id,
     SUBSTR(a.upc_num, 3, 
     LENGTH(a.upc_num)) AS upc_num,
     a.units_shipped AS vasn_sku_qty,
     a.vendor_ship_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_asn_fact_vw AS a
     INNER JOIN po_detail_base AS b 
     ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)) AS c
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS d 
   ON LOWER(c.upc_num) = LOWER(d.upc_num)
  WHERE LOWER(d.prmy_upc_ind) = LOWER('Y')
   AND LOWER(d.channel_country) = LOWER('US')) AS g
 INNER JOIN 
 `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact AS h 
 ON LOWER(g.purchase_order_num) = LOWER(h.purchase_order_number)
 AND LOWER(g.rms_sku_num) = LOWER(h.rms_sku_num) 
 AND LOWER(g.ship_location_id) = LOWER(h.ship_location_id)
GROUP BY g.purchase_order_num;


--COLLECT STATS PRIMARY INDEX (purchase_order_num) 	,COLUMN (purchase_order_num)  ON po_vasn


CREATE TEMPORARY TABLE IF NOT EXISTS po_xref
AS
SELECT 
 a.purchase_order_num,
 a.crossreference_external_id AS xref_po
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact AS a
INNER JOIN po_detail_base AS b 
ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)
GROUP BY a.purchase_order_num,
 xref_po;


--COLLECT STATS PRIMARY INDEX (purchase_order_num) 	,COLUMN (purchase_order_num)  ON po_xref


--drop table po_supp_stg


--where a.PURCHASE_ORDER_NUM = '40040399'


CREATE TEMPORARY TABLE IF NOT EXISTS po_supp_stg
AS
SELECT a.purchase_order_num,
 h.xref_po,
 a.channel_num,
 a.channel_desc,
 a.channel_brand,
 a.division_num,
 a.division_name,
 a.subdivision_num,
 a.subdivision_name,
 a.dept_num,
 a.dept_name,
 a.supp_num,
 a.supp_name,
 a.manufacturer_name,
 a.supplier_group,
 a.status,
 a.edi_ind,
 a.start_ship_date,
 a.end_ship_date,
 a.latest_approval_date,
 a.otb_eow_date,
 a.otb_month,
 a.otb_month_idnt,
 a.order_type,
 a.internal_po_ind,
 a.npg_ind,
 a.po_type,
 a.purchase_type,
 a.month,
 a.month_idnt,
 a.fiscal_year_num,
 a.quarter_label,
 a.plan_season_desc,
 a.plan_type,
 a.ttl_approved_qty,
 a.ttl_approved_c,
 a.ttl_approved_r,
 g.vasn_sku_qty,
 g.vasn_cost,
 a.dc_received_qty,
 a.dc_received_c,
 MAX(a.unit_cost_amt) AS unit_cost_amt,
 MAX(a.last_carrier_asn_date) AS last_carrier_asn_date,
 MAX(a.last_supplier_asn_date) AS last_supplier_asn_date,
 MAX(a.last_ship_activity_date) AS last_ship_activity_date,
 MAX(a.last_outbound_activity_date) AS last_outbound_activity_date,
 MAX(a.receipt_date) AS receipt_date,
 SUM(a.rcpt_mtd_cost) AS rcpt_mtd_cost,
 SUM(a.rcpt_previous_mth_cost) AS rcpt_previous_mth_cost,
 SUM(a.rcpt_cost) AS rcpt_cost,
 SUM(a.rcpt_retail) AS rcpt_retail,
 SUM(a.rcpt_mtd_units) AS rcpt_mtd_units,
 SUM(a.rcpt_previous_mth_units) AS rcpt_previous_mth_units,
 SUM(a.rcpt_units) AS rcpt_units,
 SUM(a.quantity_ordered) AS quantity_ordered,
 SUM(a.quantity_received) AS quantity_received,
 SUM(a.quantity_canceled) AS quantity_canceled,
 SUM(a.quantity_open) AS quantity_open,
 SUM(a.oo_cost) AS oo_cost,
 SUM(a.oo_retail) AS oo_retail,
 SUM(a.cm_c) AS cm_c,
 SUM(a.cm_r) AS cm_r,
 SUM(a.cm_u) AS cm_u,
 CURRENT_DATE('PST8PDT') AS process_dt
FROM po_supp_final AS a
 LEFT JOIN po_vasn AS g 
 ON LOWER(a.purchase_order_num) = LOWER(g.purchase_order_num)
 LEFT JOIN po_xref AS h 
 ON LOWER(a.purchase_order_num) = LOWER(h.purchase_order_num)
GROUP BY a.purchase_order_num,
 a.channel_num,
 a.channel_desc,
 a.channel_brand,
 a.division_num,
 a.division_name,
 a.subdivision_num,
 a.subdivision_name,
 a.dept_num,
 a.dept_name,
 a.supp_num,
 a.supp_name,
 a.manufacturer_name,
 a.supplier_group,
 a.status,
 a.edi_ind,
 a.start_ship_date,
 a.end_ship_date,
 a.latest_approval_date,
 a.otb_eow_date,
 a.otb_month,
 a.otb_month_idnt,
 a.order_type,
 a.internal_po_ind,
 a.npg_ind,
 a.po_type,
 a.purchase_type,
 a.month,
 a.month_idnt,
 a.fiscal_year_num,
 a.quarter_label,
 a.plan_season_desc,
 a.plan_type,
 a.ttl_approved_qty,
 a.ttl_approved_c,
 a.ttl_approved_r,
 a.dc_received_qty,
 a.dc_received_c,
 g.vasn_sku_qty,
 g.vasn_cost,
 h.xref_po;


--COLLECT STATS      PRIMARY INDEX(purchase_order_num)      ,COLUMN(purchase_order_num)      ON po_supp_stg


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail_report;


--where a.purchase_order_num = 40190040


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail_report
(SELECT 
  purchase_order_num,
  xref_po,
  channel_num,
  channel_desc,
  channel_brand,
  division_num,
  division_name,
  subdivision_num,
  subdivision_name,
  dept_num,
  dept_name,
  supp_num,
  supp_name,
  manufacturer_name,
  supplier_group,
  status,
  otb_impact,
  edi_ind,
  start_ship_date,
  end_ship_date,
  latest_approval_date,
  otb_eow_date,
  otb_month,
  otb_month_idnt,
  order_type,
  internal_po_ind,
  npg_ind,
  po_type,
  purchase_type,
  month,
  month_idnt,
  fiscal_year_num,
  quarter_label,
  plan_season_desc,
  plan_type,
  ttl_approved_qty,
  ttl_approved_c,
  ttl_approved_r,
  unit_cost_amt,
  last_carrier_asn_date,
  last_supplier_asn_date,
  last_ship_activity_date,
  last_outbound_activity_date,
  receipt_date,
  rcpt_mtd_cost,
  rcpt_previous_mth_cost,
  rcpt_cost,
  rcpt_retail,
  rcpt_mtd_units,
  rcpt_previous_mth_units,
  rcpt_units,
  quantity_ordered,
  quantity_received,
  quantity_canceled,
  quantity_open,
  oo_cost,
  oo_retail,
  SUM(`A121867`) AS vasn_sku_qty,
  SUM(`A121868`) AS vasn_cost,
  dc_received_qty,
  dc_received_c,
  cm_c,
  cm_r,
  cm_u,
  process_dt
 FROM (SELECT purchase_order_num,
    xref_po,
    channel_num,
    channel_desc,
    channel_brand,
    division_num,
    division_name,
    subdivision_num,
    subdivision_name,
    dept_num,
    dept_name,
    supp_num,
    supp_name,
    manufacturer_name,
    supplier_group,
    status,
    otb_impact,
    edi_ind,
    start_ship_date,
    end_ship_date,
    latest_approval_date,
    otb_eow_date,
    otb_month,
    otb_month_idnt,
    order_type,
    internal_po_ind,
    npg_ind,
    po_type,
    purchase_type,
    month,
    month_idnt,
    fiscal_year_num,
    quarter_label,
    plan_season_desc,
    plan_type,
    ttl_approved_qty,
    ttl_approved_c,
    ttl_approved_r,
    unit_cost_amt,
    last_carrier_asn_date,
    last_supplier_asn_date,
    last_ship_activity_date,
    last_outbound_activity_date,
    receipt_date,
    rcpt_mtd_cost,
    rcpt_previous_mth_cost,
    rcpt_cost,
    rcpt_retail,
    rcpt_mtd_units,
    rcpt_previous_mth_units,
    rcpt_units,
    quantity_ordered,
    quantity_received,
    quantity_canceled,
    quantity_open,
    oo_cost,
    oo_retail,
    dc_received_qty,
    dc_received_c,
    cm_c,
    cm_r,
    cm_u,
    process_dt,
     CASE
     WHEN vasn_sku_qty - SUM(`A121867`) < 0
     THEN 0
     ELSE vasn_sku_qty - SUM(`A121867`)
     END AS `A121867`,
     CASE
     WHEN vasn_cost - SUM(`A121868`) < 0
     THEN 0
     ELSE vasn_cost - SUM(`A121868`)
     END AS `A121868`
   FROM (SELECT purchase_order_num,
      xref_po,
      channel_num,
      channel_desc,
      channel_brand,
      division_num,
      division_name,
      subdivision_num,
      subdivision_name,
      dept_num,
      dept_name,
      supp_num,
      supp_name,
      manufacturer_name,
      supplier_group,
      status,
       CASE
       WHEN (rcpt_previous_mth_cost <> 0 
       OR rcpt_mtd_cost <> 0) 
       AND LOWER(month_idnt) < LOWER(otb_month_idnt) 
       AND LOWER(internal_po_ind
          ) = LOWER('N')
       THEN 'Shift In Future Mth'
       WHEN (rcpt_previous_mth_cost <> 0 OR rcpt_mtd_cost <> 0) AND LOWER(month_idnt) = LOWER(otb_month_idnt) AND LOWER(internal_po_ind
          ) = LOWER('N')
       THEN 'In Mth Rcvd'
       WHEN (rcpt_previous_mth_cost <> 0 OR rcpt_mtd_cost <> 0) AND LOWER(month_idnt) > LOWER(otb_month_idnt) AND LOWER(internal_po_ind
          ) = LOWER('N')
       THEN 'Shift In Prior Mth'
       WHEN (rcpt_previous_mth_cost <> 0 OR rcpt_mtd_cost <> 0) AND LOWER(internal_po_ind) = LOWER('Y')
       THEN 'Internal PO Rcvd'
       ELSE ''
       END AS otb_impact,
      edi_ind,
      start_ship_date,
      end_ship_date,
      latest_approval_date,
      otb_eow_date,
      otb_month,
      otb_month_idnt,
      order_type,
      internal_po_ind,
      npg_ind,
      po_type,
      purchase_type,
      month,
      month_idnt,
      fiscal_year_num,
      quarter_label,
      plan_season_desc,
      plan_type,
      ttl_approved_qty,
      ttl_approved_c,
      ttl_approved_r,
      unit_cost_amt,
      last_carrier_asn_date,
      last_supplier_asn_date,
      last_ship_activity_date,
      last_outbound_activity_date,
      receipt_date,
      rcpt_mtd_cost,
      rcpt_previous_mth_cost,
      rcpt_cost,
      rcpt_retail,
      rcpt_mtd_units,
      rcpt_previous_mth_units,
      rcpt_units,
      quantity_ordered,
      quantity_received,
      quantity_canceled,
      quantity_open,
      oo_cost,
      oo_retail,
      vasn_sku_qty,
      vasn_cost,
      dc_received_qty,
      dc_received_c,
      cm_c,
      cm_r,
      cm_u,
      process_dt,
        (SUM(rcpt_units) OVER (PARTITION BY purchase_order_num ORDER BY month_idnt ROWS BETWEEN UNBOUNDED PRECEDING AND
         CURRENT ROW)) - rcpt_units AS `A121867`,
        (SUM(rcpt_cost) OVER (PARTITION BY purchase_order_num ORDER BY month_idnt ROWS BETWEEN UNBOUNDED PRECEDING AND
         CURRENT ROW)) - rcpt_cost AS `A121868`
     FROM po_supp_stg AS a) AS t
   GROUP BY purchase_order_num,
    xref_po,
    channel_num,
    channel_desc,
    channel_brand,
    division_num,
    division_name,
    subdivision_num,
    subdivision_name,
    dept_num,
    dept_name,
    supp_num,
    supp_name,
    manufacturer_name,
    supplier_group,
    status,
    otb_impact,
    edi_ind,
    start_ship_date,
    end_ship_date,
    latest_approval_date,
    otb_eow_date,
    otb_month,
    otb_month_idnt,
    order_type,
    internal_po_ind,
    npg_ind,
    po_type,
    purchase_type,
    month,
    month_idnt,
    fiscal_year_num,
    quarter_label,
    plan_season_desc,
    plan_type,
    ttl_approved_qty,
    ttl_approved_c,
    ttl_approved_r,
    unit_cost_amt,
    last_carrier_asn_date,
    last_supplier_asn_date,
    last_ship_activity_date,
    last_outbound_activity_date,
    receipt_date,
    rcpt_mtd_cost,
    rcpt_previous_mth_cost,
    rcpt_cost,
    rcpt_retail,
    rcpt_mtd_units,
    rcpt_previous_mth_units,
    rcpt_units,
    quantity_ordered,
    quantity_received,
    quantity_canceled,
    quantity_open,
    oo_cost,
    oo_retail,
    vasn_sku_qty,
    vasn_cost,
    dc_received_qty,
    dc_received_c,
    cm_c,
    cm_r,
    cm_u,
    process_dt) AS t1
 GROUP BY purchase_order_num,
  xref_po,
  channel_num,
  channel_desc,
  channel_brand,
  division_num,
  division_name,
  subdivision_num,
  subdivision_name,
  dept_num,
  dept_name,
  supp_num,
  supp_name,
  manufacturer_name,
  supplier_group,
  status,
  otb_impact,
  edi_ind,
  start_ship_date,
  end_ship_date,
  latest_approval_date,
  otb_eow_date,
  otb_month,
  otb_month_idnt,
  order_type,
  internal_po_ind,
  npg_ind,
  po_type,
  purchase_type,
  month,
  month_idnt,
  fiscal_year_num,
  quarter_label,
  plan_season_desc,
  plan_type,
  ttl_approved_qty,
  ttl_approved_c,
  ttl_approved_r,
  unit_cost_amt,
  last_carrier_asn_date,
  last_supplier_asn_date,
  last_ship_activity_date,
  last_outbound_activity_date,
  receipt_date,
  rcpt_mtd_cost,
  rcpt_previous_mth_cost,
  rcpt_cost,
  rcpt_retail,
  rcpt_mtd_units,
  rcpt_previous_mth_units,
  rcpt_units,
  quantity_ordered,
  quantity_received,
  quantity_canceled,
  quantity_open,
  oo_cost,
  oo_retail,
  dc_received_qty,
  dc_received_c,
  cm_c,
  cm_r,
  cm_u,
  process_dt);


--COLLECT STATS 		PRIMARY INDEX(purchase_order_num,month_idnt)      	,COLUMN(purchase_order_num)      	,COLUMN (OTB_MONTH_IDNT)      	,COLUMN (MONTH_IDNT)      on T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL_REPORT


--drop table unkown_pos;


CREATE TEMPORARY TABLE IF NOT EXISTS unkown_pos
AS
SELECT 
 a.poreceipt_order_number,
 a.sku_num,
 b.channel_num,
 b.channel_desc,
 c.month_idnt,
 c.month,
 c.month_label,
 SUM(a.receipts_cost + a.receipts_crossdock_cost) AS rcpt_cost,
 SUM(a.receipts_retail + a.receipts_crossdock_retail) AS rcpt_retail,
 SUM(a.receipts_units + a.receipts_crossdock_units) AS rcpt_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a
 INNER JOIN po_detail_chnl_loc AS b 
 ON a.store_num = b.store_num
 INNER JOIN po_dtd_date AS c 
 ON a.tran_date = c.day_date
WHERE a.tran_code = 30
 AND LOWER(a.dropship_ind) = LOWER('N')
GROUP BY a.poreceipt_order_number,
 a.sku_num,
 b.channel_num,
 b.channel_desc,
 c.month_idnt,
 c.month,
 c.month_label;


--COLLECT STATS      PRIMARY INDEX(PORECEIPT_ORDER_NUMBER,SKU_NUM)      ,COLUMN(PORECEIPT_ORDER_NUMBER)      ,COLUMN(SKU_NUM)      ON unkown_pos


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.uknown_po_detail;


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.uknown_po_detail
(SELECT e.transfer_id,
  e.channel_num,
  e.channel_desc,
  e.month_idnt,
  e.month,
  e.month_label,
  e.div_num,
  e.div_desc,
  e.sdiv_num,
  e.sdiv_desc,
  e.dept_num,
  e.dept_desc,
  e.class_num,
  e.class_desc,
  e.sbclass_num,
  e.sbclass_desc,
  e.rms_style_num,
  e.supp_part_num AS vpn,
  e.style_desc,
  e.prmy_supp_num AS supp_num,
  f.vendor_name AS supp_name,
  e.supp_color,
  e.color_num,
  e.color_desc,
  e.rcpt_cost,
  e.rcpt_retail,
  e.rcpt_units,
  CURRENT_DATE('PST8PDT') AS process_dt
 FROM (SELECT c.poreceipt_order_number AS transfer_id,
    c.channel_num,
    c.channel_desc,
    c.month_idnt,
    c.month,
    c.month_label,
    d.div_num,
    d.div_desc,
    d.grp_num AS sdiv_num,
    d.grp_desc AS sdiv_desc,
    d.dept_num,
    d.dept_desc,
    d.class_num,
    d.class_desc,
    d.sbclass_num,
    d.sbclass_desc,
    d.rms_style_num,
    d.supp_part_num,
    d.style_desc,
    d.prmy_supp_num,
    d.supp_color,
    d.color_num,
    d.color_desc,
    SUM(c.rcpt_cost) AS rcpt_cost,
    SUM(c.rcpt_retail) AS rcpt_retail,
    SUM(c.rcpt_units) AS rcpt_units
   FROM (SELECT a.poreceipt_order_number,
      a.sku_num,
      a.channel_num,
      a.channel_desc,
      a.month_idnt,
      a.month,
      a.month_label,
      a.rcpt_cost,
      a.rcpt_retail,
      a.rcpt_units
     FROM unkown_pos AS a
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS b 
      ON LOWER(a.poreceipt_order_number) =
       LOWER(SUBSTR(CAST(b.external_distribution_id AS STRING), 1, 100))
     WHERE b.external_distribution_id IS NULL
     GROUP BY a.poreceipt_order_number,
      a.sku_num,
      a.channel_num,
      a.channel_desc,
      a.month_idnt,
      a.month,
      a.month_label,
      a.rcpt_cost,
      a.rcpt_retail,
      a.rcpt_units) AS c
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d 
    ON LOWER(c.sku_num) = LOWER(d.rms_sku_num)
   WHERE LOWER(d.channel_country) = LOWER('US')
    AND LOWER(d.dept_desc) NOT LIKE LOWER('%INACT%')
   GROUP BY transfer_id,
    c.channel_num,
    c.channel_desc,
    c.month_idnt,
    c.month,
    c.month_label,
    d.div_num,
    d.div_desc,
    sdiv_num,
    sdiv_desc,
    d.dept_num,
    d.dept_desc,
    d.class_num,
    d.class_desc,
    d.sbclass_num,
    d.sbclass_desc,
    d.rms_style_num,
    d.supp_part_num,
    d.style_desc,
    d.prmy_supp_num,
    d.supp_color,
    d.color_num,
    d.color_desc) AS e
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS f 
  ON LOWER(e.prmy_supp_num) = LOWER(f.vendor_num));


--COLLECT STATS 		PRIMARY INDEX(transfer_id,MONTH_IDNT)      	,COLUMN(transfer_id)      	,COLUMN (MONTH_IDNT)      on T2DL_DAS_OPEN_TO_BUY.UKNOWN_PO_DETAIL