
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_nap_sales_fact_load;
---Task_Name=t2_week_fact_load;'*/
---FOR SESSION VOLATILE;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_dlta_week_fact;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_dlta_week_fact (rms_sku_num, store_num, day_num, week_num, month_num
 , quarter_num, halfyear_num, year_num, fulfill_type_code, rp_ind, flash_event_ind, wac_avlbl_ind,
 merch_ownership_dept_ind, price_type, sales_retl_curr_code, sales_cost_curr_code, net_sales_retl, net_sales_cost,
 net_sales_units, returns_retl, returns_cost, returns_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT rms_sku_num,
  store_num,
  day_num,
  week_num,
  month_num,
  quarter_num,
  halfyear_num,
  year_num,
  fulfill_type_code,
  rp_ind,
  flash_event_ind,
  wac_avlbl_ind,
  merch_ownership_dept_ind,
  price_type,
  sales_retl_curr_code,
  sales_cost_curr_code,
  net_sales_retl,
  net_sales_cost,
  net_sales_units,
  returns_retl,
  returns_cost,
  returns_units,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_return_sku_store_day_fact
 WHERE week_num >= (SELECT start_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY'))
  AND week_num <= (SELECT end_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY'))
  AND LOWER(merch_ownership_dept_ind) = LOWER('Y'));


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_week_fact
WHERE week_num >= (SELECT start_rebuild_week_num
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
            WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY')) AND week_num <= (SELECT end_rebuild_week_num
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
            WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_week_fact (rms_sku_num, store_num, week_num, month_num, quarter_num
 , halfyear_num, year_num, fulfill_type_code, dropship_ind, rp_ind, wac_avlbl_ind, trunk_club_store_ind,
 sales_retl_curr_code, sales_cost_curr_code, gross_sales_tot_retl, gross_sales_tot_regular_retl,
 gross_sales_tot_promo_retl, gross_sales_tot_clearance_retl, gross_sales_tot_cost, gross_sales_tot_regular_cost,
 gross_sales_tot_promo_cost, gross_sales_tot_clearance_cost, gross_sales_tot_units, gross_sales_tot_regular_units,
 gross_sales_tot_promo_units, gross_sales_tot_clearance_units, gross_sales_flash_retl, gross_sales_flash_regular_retl,
 gross_sales_flash_promo_retl, gross_sales_flash_clearance_retl, gross_sales_flash_cost, gross_sales_flash_regular_cost
 , gross_sales_flash_promo_cost, gross_sales_flash_clearance_cost, gross_sales_flash_units,
 gross_sales_flash_regular_units, gross_sales_flash_promo_units, gross_sales_flash_clearance_units,
 gross_sales_persistent_retl, gross_sales_persistent_regular_retl, gross_sales_persistent_promo_retl,
 gross_sales_persistent_clearance_retl, gross_sales_persistent_cost, gross_sales_persistent_regular_cost,
 gross_sales_persistent_promo_cost, gross_sales_persistent_clearance_cost, gross_sales_persistent_units,
 gross_sales_persistent_regular_units, gross_sales_persistent_promo_units, gross_sales_persistent_clearance_units,
 net_sales_tot_retl, net_sales_tot_regular_retl, net_sales_tot_promo_retl, net_sales_tot_clearance_retl,
 net_sales_tot_cost, net_sales_tot_regular_cost, net_sales_tot_promo_cost, net_sales_tot_clearance_cost,
 net_sales_tot_units, net_sales_tot_regular_units, net_sales_tot_promo_units, net_sales_tot_clearance_units,
 net_sales_flash_retl, net_sales_flash_regular_retl, net_sales_flash_promo_retl, net_sales_flash_clearance_retl,
 net_sales_flash_cost, net_sales_flash_regular_cost, net_sales_flash_promo_cost, net_sales_flash_clearance_cost,
 net_sales_flash_units, net_sales_flash_regular_units, net_sales_flash_promo_units, net_sales_flash_clearance_units,
 net_sales_persistent_retl, net_sales_persistent_regular_retl, net_sales_persistent_promo_retl,
 net_sales_persistent_clearance_retl, net_sales_persistent_cost, net_sales_persistent_regular_cost,
 net_sales_persistent_promo_cost, net_sales_persistent_clearance_cost, net_sales_persistent_units,
 net_sales_persistent_regular_units, net_sales_persistent_promo_units, net_sales_persistent_clearance_units,
 returns_tot_retl, returns_tot_regular_retl, returns_tot_promo_retl, returns_tot_clearance_retl, returns_tot_cost,
 returns_tot_regular_cost, returns_tot_promo_cost, returns_tot_clearance_cost, returns_tot_units,
 returns_tot_regular_units, returns_tot_promo_units, returns_tot_clearance_units, returns_flash_retl,
 returns_flash_regular_retl, returns_flash_promo_retl, returns_flash_clearance_retl, returns_flash_cost,
 returns_flash_regular_cost, returns_flash_promo_cost, returns_flash_clearance_cost, returns_flash_units,
 returns_flash_regular_units, returns_flash_promo_units, returns_flash_clearance_units, returns_persistent_retl,
 returns_persistent_regular_retl, returns_persistent_promo_retl, returns_persistent_clearance_retl,
 returns_persistent_cost, returns_persistent_regular_cost, returns_persistent_promo_cost,
 returns_persistent_clearance_cost, returns_persistent_units, returns_persistent_regular_units,
 returns_persistent_promo_units, returns_persistent_clearance_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT a01.rms_sku_num,
  a01.store_num,
  a01.week_num,
  a01.month_num,
  a01.quarter_num,
  a01.halfyear_num,
  a01.year_num,
  a01.fulfill_type_code,
   CASE
   WHEN LOWER(a01.fulfill_type_code) = LOWER('VendorDropShip')
   THEN 'Y'
   ELSE 'N'
   END AS dropship_ind,
  a01.rp_ind,
  a01.wac_avlbl_ind,
   CASE
   WHEN a02.trunk_club_store IS NOT NULL
   THEN 'Y'
   ELSE 'N'
   END AS trunk_club_store_ind,
  a01.sales_retl_curr_code,
  a01.sales_cost_curr_code,
  CAST(SUM(a01.net_sales_retl + a01.returns_retl) AS NUMERIC) AS gross_sales_tot_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_tot_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_tot_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_tot_clearance_retl,
  CAST(SUM(a01.net_sales_cost + a01.returns_cost) AS NUMERIC) AS gross_sales_tot_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_tot_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_tot_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_tot_clearance_cost,
  SUM(a01.net_sales_units + a01.returns_units) AS gross_sales_tot_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('R')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_tot_regular_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('P')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_tot_promo_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('C')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_tot_clearance_units,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_clearance_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_flash_clearance_cost,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_flash_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_flash_regular_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_flash_promo_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_flash_clearance_units,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_retl + a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_clearance_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_cost + a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS gross_sales_persistent_clearance_cost,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_persistent_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_persistent_regular_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_persistent_promo_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
    THEN a01.net_sales_units + a01.returns_units
    ELSE 0
    END) AS gross_sales_persistent_clearance_units,
  SUM(a01.net_sales_retl) AS net_sales_tot_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_tot_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_tot_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_tot_clearance_retl,
  SUM(a01.net_sales_cost) AS net_sales_tot_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_tot_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_tot_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_tot_clearance_cost,
  SUM(a01.net_sales_units) AS net_sales_tot_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('R')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_tot_regular_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('P')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_tot_promo_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('C')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_tot_clearance_units,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_clearance_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_flash_clearance_cost,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_flash_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_flash_regular_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_flash_promo_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_flash_clearance_units,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_retl
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_clearance_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.net_sales_cost
     ELSE 0
     END) AS NUMERIC) AS net_sales_persistent_clearance_cost,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_persistent_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_persistent_regular_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_persistent_promo_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
    THEN a01.net_sales_units
    ELSE 0
    END) AS net_sales_persistent_clearance_units,
  SUM(a01.returns_retl) AS returns_tot_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('R')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_tot_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('P')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_tot_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('C')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_tot_clearance_retl,
  SUM(a01.returns_cost) AS returns_tot_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('R')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_tot_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('P')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_tot_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.price_type) = LOWER('C')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_tot_clearance_cost,
  SUM(a01.returns_units) AS returns_tot_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('R')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_tot_regular_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('P')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_tot_promo_units,
  SUM(CASE
    WHEN LOWER(a01.price_type) = LOWER('C')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_tot_clearance_units,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_flash_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_flash_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_flash_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_flash_clearance_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_flash_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_flash_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_flash_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_flash_clearance_cost,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_flash_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_flash_regular_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_flash_promo_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) = LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_flash_clearance_units,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_regular_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_promo_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.returns_retl
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_clearance_retl,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_regular_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_promo_cost,
  CAST(SUM(CASE
     WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
     THEN a01.returns_cost
     ELSE 0
     END) AS NUMERIC) AS returns_persistent_clearance_cost,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_persistent_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('R')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_persistent_regular_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('P')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_persistent_promo_units,
  SUM(CASE
    WHEN LOWER(a01.flash_event_ind) <> LOWER('Y') AND LOWER(a01.price_type) = LOWER('C')
    THEN a01.returns_units
    ELSE 0
    END) AS returns_persistent_clearance_units,
   (SELECT dw_batch_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
   WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_return_sku_store_dlta_week_fact AS a01
  LEFT JOIN (SELECT CAST(TRUNC(cast(CASE
      WHEN config_value = ''
      THEN '0'
      ELSE config_value
      END as float64)) AS INTEGER) AS trunk_club_store
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
   WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY')
    AND LOWER(config_key) = LOWER('TRUNK_CLUB_STORES')) AS a02 ON a01.store_num = a02.trunk_club_store
 GROUP BY a01.rms_sku_num,
  a01.store_num,
  a01.week_num,
  a01.month_num,
  a01.quarter_num,
  a01.halfyear_num,
  a01.year_num,
  a01.fulfill_type_code,
  dropship_ind,
  a01.rp_ind,
  a01.wac_avlbl_ind,
  trunk_club_store_ind,
  a01.sales_retl_curr_code,
  a01.sales_cost_curr_code);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_dlta_week_fact;

/*SET QUERY_BAND = NONE FOR SESSION;*/
