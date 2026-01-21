CREATE OR REPLACE TEMPORARY TABLE ban_cntry_store_dim_vol
AS
SELECT store_short_name,
 channel_num,
 channel_desc,
 store_num,
 location_type_code,
 store_type_code,
 distribution_center_num,
 receivinglocation_location_number,
 store_country_code,
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
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim;


CREATE  OR REPLACE TEMPORARY TABLE vendor_asn_scrub_vol
AS
SELECT rms_sku_num,
 purchase_order_num,
 receiving_location_id,
 expected_ship_date,
 vasn_sku_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_asn_fact
WHERE expected_ship_date BETWEEN DATE '2020-01-01' AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 3800 DAY));


CREATE  OR REPLACE TEMPORARY TABLE inbound_shipment_distinctpo_vol
AS
SELECT DISTINCT purchase_order_number
FROM (SELECT DISTINCT purchase_order_number
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.nls_inbound_shipment_fact AS nls
   UNION ALL
   SELECT DISTINCT purchase_order_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_inbound_carton_fact_vw AS sim
   UNION ALL
   SELECT DISTINCT purchase_order_number
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw AS ulf
   WHERE LOWER(location_id) NOT IN (LOWER('889'), LOWER('859'), LOWER('896'), LOWER('869'), LOWER('891'), LOWER('868'))
  ) AS a;


CREATE  OR REPLACE TEMPORARY TABLE purchase_order_fact_vol
AS
SELECT purchase_order_num,
 order_from_vendor_id,
 shipping_method,
 edi_ind,
 dropship_ind,
 import_order_ind,
 nordstrom_productgroup_ind,
 purchaseorder_type,
 order_type,
 purchase_type,
 status,
 start_ship_date,
 end_ship_date,
 latest_ship_date,
 earliest_ship_date,
 open_to_buy_endofweek_date,
 original_approval_date,
 written_date,
 currency
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact AS pof
WHERE (LOWER(status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')) OR LOWER(status) = LOWER('CLOSED') AND
      open_to_buy_endofweek_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND purchase_order_num IN (SELECT
        purchase_order_number
      FROM inbound_shipment_distinctpo_vol))
 AND LOWER(include_on_order_ind) = LOWER('t')
 AND written_date >= DATE '2020-01-01'
 AND purchase_order_num NOT IN (SELECT purchase_order_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.removed_po_lkup)
 AND original_approval_date IS NOT NULL;


CREATE  OR REPLACE TEMPORARY TABLE inbound_shipment_fact_vol
AS
SELECT nls.rms_sku_num,
 nls.purchase_order_number,
 CAST(trunc(cast(nls.tological_id as FLOAT64)) AS INTEGER) AS store_num,
 COUNT(DISTINCT nls.received_date) AS receipt_cnt,
 AVG(DATE_DIFF(nls.received_date, po1.written_date, DAY)) AS days_received_written,
 AVG(DATE_DIFF(nls.received_date, po1.original_approval_date, DAY)) AS days_received_firstapprov,
 AVG(DATE_DIFF(nls.received_date, po1.start_ship_date, DAY)) AS days_received_notbefore,
 AVG(DATE_DIFF(nls.received_date, po1.end_ship_date, DAY)) AS days_received_notafter,
 AVG(DATE_DIFF(nls.received_date, po1.open_to_buy_endofweek_date, DAY)) AS days_received_otbeow,
 MIN(nls.received_date) AS first_received_date,
 MAX(nls.received_date) AS last_received_date,
 SUM(nls.shipment_qty) AS received_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.nls_inbound_shipment_fact AS nls
 INNER JOIN purchase_order_fact_vol AS po1 ON LOWER(po1.purchase_order_num) = LOWER(nls.purchase_order_number)
GROUP BY nls.rms_sku_num,
 nls.purchase_order_number,
 store_num
UNION ALL
SELECT sim.sku_num AS rms_sku_num,
 sim.purchase_order_num AS purchase_order_number,
 CAST(trunc(cast(sim.location_num as FLOAT64)) AS INTEGER) AS store_num,
 COUNT(DISTINCT sim.received_date) AS receipt_cnt,
 AVG(DATE_DIFF(sim.received_date, po2.written_date, DAY)) AS days_received_written,
 AVG(DATE_DIFF(sim.received_date, po2.original_approval_date, DAY)) AS days_received_firstapprov,
 AVG(DATE_DIFF(sim.received_date, po2.start_ship_date, DAY)) AS days_received_notbefore,
 AVG(DATE_DIFF(sim.received_date, po2.end_ship_date, DAY)) AS days_received_notafter,
 AVG(DATE_DIFF(sim.received_date, po2.open_to_buy_endofweek_date, DAY)) AS days_received_otbeow,
 MIN(sim.received_date) AS first_received_date,
 MAX(sim.received_date) AS last_received_date,
 SUM(sim.sku_qty) AS received_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_inbound_carton_fact_vw AS sim
 INNER JOIN purchase_order_fact_vol AS po2 ON LOWER(po2.purchase_order_num) = LOWER(sim.purchase_order_num)
GROUP BY rms_sku_num,
 purchase_order_number,
 store_num
UNION ALL
SELECT ulf.rms_sku_num,
 ulf.purchase_order_number,
 CAST(trunc(cast(ulf.location_id AS FLOAT64)) AS INTEGER) AS store_num,
 COUNT(DISTINCT ulf.received_date) AS receipt_cnt,
 AVG(DATE_DIFF(ulf.received_date, po3.written_date, DAY)) AS days_received_written,
 AVG(DATE_DIFF(ulf.received_date, po3.original_approval_date, DAY)) AS days_received_firstapprov,
 AVG(DATE_DIFF(ulf.received_date, po3.start_ship_date, DAY)) AS days_received_notbefore,
 AVG(DATE_DIFF(ulf.received_date, po3.end_ship_date, DAY)) AS days_received_notafter,
 AVG(DATE_DIFF(ulf.received_date, po3.open_to_buy_endofweek_date, DAY)) AS days_received_otbeow,
 MIN(ulf.received_date) AS first_received_date,
 MAX(ulf.received_date) AS last_received_date,
 SUM(ulf.shipment_qty) AS received_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw AS ulf
 INNER JOIN purchase_order_fact_vol AS po3 ON LOWER(po3.purchase_order_num) = LOWER(ulf.purchase_order_number)
WHERE LOWER(ulf.location_id) NOT IN (LOWER('889'), LOWER('859'), LOWER('896'), LOWER('869'), LOWER('891'), LOWER('868')
   )
GROUP BY ulf.rms_sku_num,
 ulf.purchase_order_number,
 store_num;


CREATE  OR REPLACE TEMPORARY TABLE po_main
AS
SELECT pof.purchase_order_num AS purchase_order_number,
 po_shp.rms_sku_num,
 po_shp.ship_location_id AS store_num,
 SUBSTR(LTRIM(SUBSTR(CAST(po_shp.ship_location_id AS STRING), 1, 4)) || ', ' || org.store_short_name, 1, 40) AS
 store_desc,
 COALESCE(dist_org.store_num, po_shp.ship_location_id) AS dist_store_num,
 COALESCE(dist_org.channel_num, org.channel_num) AS channel_num,
 SUBSTR(LTRIM(SUBSTR(CAST(COALESCE(dist_org.channel_num, org.channel_num) AS STRING), 1, 4)) || ', ' || COALESCE(dist_org
    .channel_desc, org.channel_desc), 1, 40) AS channel_desc,
 COALESCE(dist_org.banner_country, org.banner_country) AS banner_country,
 org.location_type_code,
 org.store_type_code,
 org.distribution_center_num,
 org.receivinglocation_location_number AS receiving_location_num,
  CASE
  WHEN pof.open_to_buy_endofweek_date IS NULL
  THEN NULL
  ELSE tdl.week_idnt
  END AS wk_idnt,
  CASE
  WHEN pof.open_to_buy_endofweek_date IS NULL
  THEN NULL
  ELSE SUBSTR(RPAD(CAST(tdl.fiscal_year_num AS STRING), 4, ' ') || ', ' || RPAD(FORMAT('%02d', 
  CAST(trunc(cast(tdl.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ', Wk ' || RPAD(CAST(tdl.week_num_of_fiscal_month AS STRING), 1, ' '), 1, 20)
  END AS wk_desc,
  CASE
  WHEN pof.open_to_buy_endofweek_date IS NULL
  THEN NULL
  ELSE tdl.month_idnt
  END AS mth_idnt,
  CASE
  WHEN pof.open_to_buy_endofweek_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(tdl.fiscal_year_num AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdl.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ' ' || tdl.month_abrv, 1, 20)
  END AS mth_desc,
  CASE
  WHEN pof.start_ship_date IS NULL
  THEN NULL
  ELSE tdlb.week_idnt
  END AS wk_idnt_nb,
  CASE
  WHEN pof.start_ship_date IS NULL
  THEN NULL
  ELSE SUBSTR(RPAD(CAST(tdlb.fiscal_year_num AS STRING), 4, ' ') || ', ' || RPAD(FORMAT('%02d',
   CAST(trunc(cast(tdlb.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ', Wk ' || RPAD(CAST(tdlb.week_num_of_fiscal_month AS STRING), 1, ' '), 1, 20)
  END AS wk_desc_nb,
  CASE
  WHEN pof.start_ship_date IS NULL
  THEN NULL
  ELSE tdlb.month_idnt
  END AS mth_idnt_nb,
  CASE
  WHEN pof.start_ship_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(tdlb.fiscal_year_num AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdlb.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ' ' || tdlb.month_abrv, 1, 20)
  END AS mth_desc_nb,
  CASE
  WHEN pof.end_ship_date IS NULL
  THEN NULL
  ELSE tdla.week_idnt
  END AS wk_idnt_na,
  CASE
  WHEN pof.end_ship_date IS NULL
  THEN NULL
  ELSE SUBSTR(RPAD(CAST(tdla.fiscal_year_num AS STRING), 4, ' ') || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdla.fiscal_month_num AS FLOAT64))AS INTEGER)
        ), 2, ' ') || ', Wk ' || RPAD(CAST(tdla.week_num_of_fiscal_month AS STRING), 1, ' '), 1, 20)
  END AS wk_desc_na,
  CASE
  WHEN pof.end_ship_date IS NULL
  THEN NULL
  ELSE tdla.month_idnt
  END AS mth_idnt_na,
  CASE
  WHEN pof.end_ship_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(tdla.fiscal_year_num AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdla.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ' ' || tdla.month_abrv, 1, 20)
  END AS mth_desc_na,
 dept.division_num AS div_num,
 SUBSTR(LTRIM(SUBSTR(CAST(dept.division_num AS STRING), 1, 4)) || ', ' || dept.division_short_name, 1, 40) AS div_desc,
 dept.subdivision_num AS grp_num,
 SUBSTR(LTRIM(SUBSTR(CAST(dept.subdivision_num AS STRING), 1, 4)) || ', ' || dept.subdivision_short_name, 1, 40) AS
 grp_desc,
 sku.dept_num,
 SUBSTR(LTRIM(SUBSTR(CAST(sku.dept_num AS STRING), 1, 4)) || ', ' || dept.dept_short_name, 1, 40) AS dept_desc,
 sku.class_num,
 SUBSTR(LTRIM(SUBSTR(CAST(sku.class_num AS STRING), 1, 4)) || ', ' || sku.class_desc, 1, 40) AS class_desc,
 sku.rms_style_num,
 sku.supp_part_num AS vpn,
   sku.supp_part_num || ', ' || sku.style_desc AS vpn_desc,
 sku.color_num AS nrf_color_code,
   sku.color_num || ', ' || sku.supp_color AS supplier_color,
   sku.size_1_num || ' | ' || sku.size_2_num AS size_desc,
  CASE
  WHEN LOWER(sku.smart_sample_ind) = LOWER('Y') OR LOWER(sku.gwp_ind) = LOWER('Y')
  THEN 'Y'
  ELSE 'N'
  END AS sample_ind,
 COALESCE(sku.prmy_supp_num, pof.order_from_vendor_id) AS supplier_num,
 supp.vendor_name AS supplier_name,
 pof.order_from_vendor_id AS manufacturer_num,
 manu.vendor_name AS manufacturer_name,
 styl.product_source_code,
 styl.product_source_desc,
 sku.selling_channel_eligibility_list AS selling_channel_eligibility,
 pof.shipping_method,
  CASE
  WHEN LOWER(COALESCE(inb_po.purchase_order_number, 'N')) = LOWER('N')
  THEN 'N'
  ELSE 'Y'
  END AS po_received_status,
  CASE
  WHEN dir_2_store.distribute_location_id = dir_2_store.ship_location_id AND LOWER(org.store_type_code) IN (LOWER('FL')
      , LOWER('RK')) AND dir_2_store.distribute_location_id <> 828
  THEN 'Y'
  ELSE 'N'
  END AS dir_to_store_ind,
 pof.edi_ind,
 pof.dropship_ind,
 pof.import_order_ind AS import_flag,
 pof.nordstrom_productgroup_ind AS npg_flag,
 pof.purchaseorder_type AS po_type,
 pof.order_type,
 pof.purchase_type,
 pof.status AS order_status,
 pof.start_ship_date AS not_before_dt,
 pof.end_ship_date AS not_after_dt,
 pof.latest_ship_date AS orig_not_after_dt,
 pof.earliest_ship_date AS orig_not_before_dt,
 pof.open_to_buy_endofweek_date AS otb_eow_dt,
 pof.original_approval_date AS orig_approval_dt,
 pof.written_date,
 pof.currency AS cost_currency_cd,
 COALESCE(po_shp.ordered_qty, 0) AS ordered_qty,
 COALESCE(po_shp.canceled_qty, 0) AS canceled_qty,
 po_shp.unit_cost_amt,
 po_shp.unit_retail_amt
FROM purchase_order_fact_vol AS pof
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_shiplocation_fact AS po_shp ON LOWER(pof.purchase_order_num) = LOWER(po_shp
   .purchase_order_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tdl ON pof.open_to_buy_endofweek_date = tdl.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tdlb ON pof.start_ship_date = tdlb.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tdla ON pof.end_ship_date = tdla.day_date
 INNER JOIN ban_cntry_store_dim_vol AS org ON po_shp.ship_location_id = org.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.po_distribute_store_lkup_vw AS dist ON po_shp.ship_location_id = dist.ship_location_id AND LOWER(po_shp
    .purchase_order_num) = LOWER(dist.purchase_order_num)
 LEFT JOIN ban_cntry_store_dim_vol AS dist_org ON dist.min_dist_loc = dist_org.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS dir_2_store ON LOWER(dir_2_store.purchase_order_num
      ) = LOWER(po_shp.purchase_order_num) AND po_shp.ship_location_id = dir_2_store.ship_location_id AND po_shp.ship_location_id
     = dir_2_store.distribute_location_id AND LOWER(dir_2_store.rms_sku_num) = LOWER(po_shp.rms_sku_num)
 LEFT JOIN inbound_shipment_distinctpo_vol AS inb_po ON LOWER(po_shp.purchase_order_num) = LOWER(inb_po.purchase_order_number
   )
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS sku ON LOWER(po_shp.rms_sku_num) = LOWER(sku.rms_sku_num) AND LOWER(org.store_country_code
    ) = LOWER(sku.channel_country)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(COALESCE(sku.prmy_supp_num, pof.order_from_vendor_id)) = LOWER(supp
   .vendor_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS manu ON LOWER(pof.order_from_vendor_id) = LOWER(manu.vendor_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim AS styl ON sku.epm_style_num = styl.epm_style_num AND LOWER(styl.channel_country
    ) = LOWER(sku.channel_country)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dept ON sku.dept_num = dept.dept_num;


CREATE  OR REPLACE TEMPORARY TABLE pvasn
AS
SELECT vasn.rms_sku_num,
 vasn.purchase_order_num,
 vasn.receiving_location_id,
 COUNT(DISTINCT vasn.expected_ship_date) AS vasn_cnt,
 AVG(DATE_DIFF(vasn.expected_ship_date, po.written_date, DAY)) AS days_vasn_written,
 AVG(DATE_DIFF(vasn.expected_ship_date, po.original_approval_date, DAY)) AS days_vasn_firstapprov,
 AVG(DATE_DIFF(vasn.expected_ship_date, po.start_ship_date, DAY)) AS days_vasn_notbefore,
 AVG(DATE_DIFF(vasn.expected_ship_date, po.end_ship_date, DAY)) AS days_vasn_notafter,
 AVG(DATE_DIFF(vasn.expected_ship_date, po.open_to_buy_endofweek_date, DAY)) AS days_vasn_otbeow,
 MIN(vasn.expected_ship_date) AS expected_ship_date,
 MAX(vasn.expected_ship_date) AS last_ship_date,
 SUM(vasn.vasn_sku_qty) AS vasn_sku_qty
FROM vendor_asn_scrub_vol AS vasn
 INNER JOIN purchase_order_fact_vol AS po ON LOWER(po.purchase_order_num) = LOWER(vasn.purchase_order_num)
GROUP BY vasn.rms_sku_num,
 vasn.purchase_order_num,
 vasn.receiving_location_id;


CREATE  OR REPLACE TEMPORARY TABLE ulf_x
AS
SELECT inb.rms_sku_num,
 inb.purchase_order_number,
 inb.store_num,
 inb.receipt_cnt,
 inb.days_received_written,
 inb.days_received_firstapprov,
 inb.days_received_notbefore,
 inb.days_received_notafter,
 inb.days_received_otbeow,
 inb.first_received_date,
  CASE
  WHEN inb.first_received_date IS NULL
  THEN NULL
  ELSE tdlfr.week_idnt
  END AS wk_idnt_fr,
  CASE
  WHEN inb.first_received_date IS NULL
  THEN NULL
  ELSE SUBSTR(RPAD(CAST(tdlfr.fiscal_year_num AS STRING), 4, ' ') || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdlfr.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ', Wk ' || RPAD(CAST(tdlfr.week_num_of_fiscal_month AS STRING), 1, ' '), 1, 20)
  END AS wk_desc_fr,
  CASE
  WHEN inb.first_received_date IS NULL
  THEN NULL
  ELSE tdlfr.month_idnt
  END AS mth_idnt_fr,
  CASE
  WHEN inb.first_received_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(tdlfr.fiscal_year_num AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdlfr.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ' ' || tdlfr.month_abrv, 1, 20)
  END AS mth_desc_fr,
 inb.last_received_date,
  CASE
  WHEN inb.last_received_date IS NULL
  THEN NULL
  ELSE tdllr.week_idnt
  END AS wk_idnt_lr,
  CASE
  WHEN inb.last_received_date IS NULL
  THEN NULL
  ELSE SUBSTR(RPAD(CAST(tdllr.fiscal_year_num AS STRING), 4, ' ') || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdllr.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ', Wk ' || RPAD(CAST(tdllr.week_num_of_fiscal_month AS STRING), 1, ' '), 1, 20)
  END AS wk_desc_lr,
  CASE
  WHEN inb.last_received_date IS NULL
  THEN NULL
  ELSE tdllr.month_idnt
  END AS mth_idnt_lr,
  CASE
  WHEN inb.last_received_date IS NULL
  THEN NULL
  ELSE SUBSTR('FY' || SUBSTR(RPAD(CAST(tdllr.fiscal_year_num AS STRING), 4, ' '), 3, 2) || ', ' || RPAD(FORMAT('%02d', CAST(trunc(cast(tdllr.fiscal_month_num AS FLOAT64)) AS INTEGER)
        ), 2, ' ') || ' ' || tdllr.month_abrv, 1, 20)
  END AS mth_desc_lr,
 inb.received_qty
FROM inbound_shipment_fact_vol AS inb
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tdlfr ON inb.first_received_date = tdlfr.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tdllr ON inb.last_received_date = tdllr.day_date;


TRUNCATE TABLE {{params.dbenv}}_nap_stg.po_inbound_tableau_extract_stg;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.po_inbound_tableau_extract_stg
(SELECT po_main.purchase_order_number AS purchase_order_num,
  po_main.rms_sku_num,
  CAST(trunc(cast(po_main.store_num AS FLOAT64)) AS INTEGER) AS store_num,
  po_main.store_desc,
  CAST(trunc(cast(po_main.dist_store_num AS FLOAT64)) AS INTEGER) AS dist_store_num,
  CAST(trunc(cast(po_main.channel_num AS FLOAT64)) AS INTEGER) AS channel_num,
  po_main.channel_desc,
  po_main.banner_country,
  po_main.location_type_code,
  po_main.store_type_code,
  CAST(trunc(cast(po_main.distribution_center_num AS FLOAT64)) AS INTEGER) AS distribution_center_num,
  ROUND(CAST(po_main.receiving_location_num AS NUMERIC), 0) AS receiving_location_num,
  CAST(trunc(cast(po_main.wk_idnt AS FLOAT64)) AS INTEGER) AS wk_idnt,
  po_main.wk_desc,
  CAST(trunc(cast(po_main.mth_idnt AS FLOAT64)) AS INTEGER) AS mth_idnt,
  po_main.mth_desc,
  CAST(trunc(cast(po_main.wk_idnt_nb AS FLOAT64)) AS INTEGER) AS wk_idnt_nb,
  po_main.wk_desc_nb,
  CAST(trunc(cast(po_main.mth_idnt_nb AS FLOAT64)) AS INTEGER) AS mth_idnt_nb,
  po_main.mth_desc_nb,
  CAST(trunc(cast(po_main.wk_idnt_na AS FLOAT64)) AS INTEGER) AS wk_idnt_na,
  po_main.wk_desc_na,
  CAST(trunc(cast(po_main.mth_idnt_na AS FLOAT64)) AS INTEGER) AS mth_idnt_na,
  po_main.mth_desc_na,
  CAST(trunc(cast(ulf_x.wk_idnt_fr AS FLOAT64)) AS INTEGER) AS wk_idnt_fr,
  ulf_x.wk_desc_fr,
  CAST(trunc(cast(ulf_x.mth_idnt_fr AS FLOAT64)) AS INTEGER) AS mth_idnt_fr,
  ulf_x.mth_desc_fr,
  CAST(trunc(cast(ulf_x.wk_idnt_lr AS FLOAT64)) AS INTEGER) AS wk_idnt_lr,
  ulf_x.wk_desc_lr,
  CAST(trunc(cast(ulf_x.mth_idnt_lr AS FLOAT64)) AS INTEGER) AS mth_idnt_lr,
  ulf_x.mth_desc_lr,
  CAST(trunc(cast(po_main.div_num AS FLOAT64)) AS INTEGER) AS div_num,
  po_main.div_desc,
  CAST(trunc(cast(po_main.grp_num AS FLOAT64)) AS INTEGER) AS grp_num,
  po_main.grp_desc,
  CAST(trunc(cast(po_main.dept_num AS FLOAT64)) AS INTEGER) AS dept_num,
  po_main.dept_desc,
  CAST(trunc(cast(po_main.class_num AS FLOAT64)) AS INTEGER) AS class_num,
  po_main.class_desc,
  po_main.rms_style_num,
  po_main.vpn,
  po_main.vpn_desc,
  po_main.nrf_color_code,
  po_main.supplier_color,
  po_main.size_desc,
  po_main.supplier_num,
  po_main.supplier_name,
  po_main.manufacturer_num,
  po_main.manufacturer_name,
  po_main.product_source_code,
  po_main.product_source_desc,
  po_main.selling_channel_eligibility,
  CAST(po_main.shipping_method AS STRING) AS shipping_method,
  po_main.po_received_status AS po_received_staus,
  po_main.dir_to_store_ind,
   CASE
   WHEN LOWER(po_main.edi_ind) = LOWER('T')
   THEN 'Y'
   ELSE 'N'
   END AS edi_ind,
   CASE
   WHEN LOWER(po_main.dropship_ind) = LOWER('t')
   THEN 'Y'
   ELSE 'N'
   END AS dropship_ind,
   CASE
   WHEN LOWER(po_main.import_flag) = LOWER('T')
   THEN 'Y'
   ELSE 'N'
   END AS import_flag,
   CASE
   WHEN LOWER(po_main.order_type) = LOWER('ARB') OR LOWER(po_main.order_type) = LOWER('BRB')
   THEN 'Y'
   ELSE 'N'
   END AS rp_flag,
   CASE
   WHEN LOWER(po_main.npg_flag) = LOWER('T')
   THEN 'Y'
   ELSE 'N'
   END AS npg_flag,
  po_main.sample_ind,
  po_main.po_type,
  po_main.order_type,
  po_main.purchase_type,
  po_main.order_status,
  po_main.not_before_dt,
  po_main.not_after_dt,
  po_main.orig_not_before_dt,
  po_main.orig_not_after_dt,
  po_main.otb_eow_dt,
  po_main.orig_approval_dt,
  ulf_x.first_received_date,
  ulf_x.last_received_date,
  po_main.written_date,
  po_main.cost_currency_cd,
  pvasn.expected_ship_date,
  pvasn.last_ship_date,
  CAST(trunc(cast(pvasn.days_vasn_written AS FLOAT64)) AS INTEGER) AS f_days_vasn_written,
  CAST(trunc(cast(pvasn.days_vasn_firstapprov AS FLOAT64)) AS INTEGER) AS f_days_vasn_firstapprov,
  CAST(trunc(cast(pvasn.days_vasn_notbefore AS FLOAT64)) AS INTEGER) AS f_days_vasn_notbefore,
  CAST(trunc(cast(pvasn.days_vasn_notafter AS FLOAT64)) AS INTEGER) AS f_days_vasn_notafter,
  CAST(trunc(cast(pvasn.days_vasn_otbeow AS FLOAT64)) AS INTEGER) AS f_days_vasn_otbeow,
  CAST(trunc(cast(ulf_x.days_received_written AS FLOAT64)) AS INTEGER) AS f_days_received_written,
  CAST(trunc(cast(ulf_x.days_received_firstapprov AS FLOAT64)) AS INTEGER) AS f_days_received_firstapprov,
  CAST(trunc(cast(ulf_x.days_received_notbefore AS FLOAT64)) AS INTEGER) AS f_days_received_notbefore,
  CAST(trunc(cast(ulf_x.days_received_notafter AS FLOAT64)) AS INTEGER) AS f_days_received_notafter,
  CAST(trunc(cast(ulf_x.days_received_otbeow AS FLOAT64)) AS INTEGER) AS f_days_received_otbeow,
  CAST(trunc(cast(COALESCE(pvasn.vasn_sku_qty, 0) AS FLOAT64)) AS INTEGER) AS f_vasn_sku_qty,
  CAST(trunc(cast(po_main.ordered_qty AS FLOAT64))AS INTEGER)  AS f_ordered_qty,
  CAST(trunc(cast(COALESCE(ulf_x.received_qty, 0) AS FLOAT64)) AS INTEGER) AS f_received_qty,
  0 AS f_arrived_qty,
  CAST(trunc(cast(po_main.canceled_qty AS FLOAT64)) AS INTEGER) AS f_canceled_qty,
  po_main.unit_cost_amt AS f_po_unit_cost,
  po_main.unit_retail_amt AS f_po_unit_retail,
   CASE
   WHEN po_main.not_before_dt < CURRENT_DATE('PST8PDT')
   THEN CURRENT_DATE('PST8PDT')
   ELSE po_main.not_before_dt
   END AS pricing_date,
  CURRENT_DATE('PST8PDT') AS rcd_load_date
 FROM po_main
  LEFT JOIN pvasn ON LOWER(po_main.rms_sku_num) = LOWER(pvasn.rms_sku_num) AND LOWER(po_main.purchase_order_number) =
     LOWER(pvasn.purchase_order_num) AND pvasn.receiving_location_id = CASE
     WHEN po_main.store_num IN (859, 869, 889, 891, 896)
     THEN 868
     WHEN LOWER(po_main.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('WH')) AND po_main.ordered_qty <> 0
     THEN po_main.distribution_center_num
     ELSE CAST(trunc(cast(po_main.store_num  AS FLOAT64)) AS INTEGER)
     END
  LEFT JOIN ulf_x ON LOWER(LTRIM(po_main.rms_sku_num, '0')) = LOWER(LTRIM(ulf_x.rms_sku_num, '0')) AND LOWER(po_main.purchase_order_number
      ) = LOWER(ulf_x.purchase_order_number) AND ulf_x.store_num = CAST(CASE
      WHEN po_main.store_num = 563
      THEN po_main.receiving_location_num
      WHEN po_main.store_num = 879
      THEN FORMAT('%6d', 873)
      WHEN po_main.store_num = 562
      THEN po_main.receiving_location_num
      WHEN po_main.store_num = 891
      THEN FORMAT('%6d', 891)
      WHEN po_main.store_num = 859
      THEN FORMAT('%6d', 859)
      WHEN po_main.store_num = 889
      THEN FORMAT('%6d', 889)
      WHEN po_main.store_num = 5629
      THEN FORMAT('%6d', 584)
      WHEN LOWER(po_main.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('WH')) AND po_main.ordered_qty <> 0
      THEN FORMAT('%11d', po_main.distribution_center_num)
      ELSE FORMAT('%20d', po_main.store_num)
      END AS FLOAT64)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY po_main.purchase_order_number, po_main.rms_sku_num, CASE
       WHEN LOWER(po_main.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('WH'))
       THEN po_main.distribution_center_num
       ELSE CAST(trunc(cast(po_main.store_num AS FLOAT64)) AS INTEGER)
       END ORDER BY po_main.ordered_qty DESC, ulf_x.received_qty DESC)) = 1);