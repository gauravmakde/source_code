BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_sale_return_dept_channel_week_agg_fact_load;
---Task_Name=load_weeks_agg_fact_load_1_209;'*/
---FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_dept_channel_week_agg_fact
WHERE week_num IN (SELECT rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_nap_aggregate_rebuild_lkup
        WHERE LOWER(process_status_ind) = LOWER('N') 
          AND LOWER(config_key) = LOWER('MERCH_SALE_RETURN_DEPT_CHANNEL_WEEK_REBUILD_WEEKS')
        QUALIFY (ROW_NUMBER() OVER (ORDER BY rebuild_week_num)) <= 40);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_dept_channel_week_agg_fact (channel_num, banner_num, channel_country,
 department_num, department_desc, subdivision_num, subdivision_desc, division_num, division_desc, fulfill_type_code,
 wac_avlbl_ind, week_num, month_label, quarter_num, halfyear_num, year_num, gift_with_purchase_ind, smart_sample_ind,
 net_sales_total_units, net_sales_total_cost_amt, net_sales_total_retail_amt, net_sales_regular_units,
 net_sales_regular_cost_amt, net_sales_regular_retail_amt, net_sales_promo_units, net_sales_promo_cost_amt,
 net_sales_promo_retail_amt, net_sales_clearance_units, net_sales_clearance_cost_amt, net_sales_clearance_retail_amt,
 gross_sales_total_units, gross_sales_total_cost_amt, gross_sales_total_retail_amt, gross_sales_regular_units,
 gross_sales_regular_cost_amt, gross_sales_regular_retail_amt, gross_sales_promo_units, gross_sales_promo_cost_amt,
 gross_sales_promo_retail_amt, gross_sales_clearance_units, gross_sales_clearance_cost_amt,
 gross_sales_clearance_retail_amt, returns_total_units, returns_total_cost_amt, returns_total_retail_amt,
 returns_regular_units, returns_regular_cost_amt, returns_regular_retail_amt, returns_promo_units,
 returns_promo_cost_amt, returns_promo_retail_amt, returns_clearance_units, returns_clearance_cost_amt,
 returns_clearance_retail_amt, dw_sys_load_tmstp, dw_batch_date)
(SELECT msrsswf.channel_num,
  COALESCE(occbd.banner_id, - 1) AS banner_num,
  COALESCE(occbd.channel_country, 'UNKNOWN') AS channel_country,
  msrsswf.department_num,
  msrsswf.department_desc,
  msrsswf.subdivision_num,
  msrsswf.subdivision_desc,
  msrsswf.division_num,
  msrsswf.division_desc,
  msrsswf.fulfill_type_code,
  msrsswf.wac_avlbl_ind,
  msrsswf.week_num,
  msrsswf.month_label,
  msrsswf.quarter_num,
  msrsswf.halfyear_num,
  msrsswf.year_num,
  msrsswf.gift_with_purchase_ind,
  msrsswf.smart_sample_ind,
  SUM(msrsswf.net_sales_tot_units) AS net_sales_total_units,
  SUM(msrsswf.net_sales_tot_cost) AS net_sales_total_cost_amt,
  SUM(msrsswf.net_sales_tot_retl) AS net_sales_total_retl,
  SUM(msrsswf.net_sales_tot_regular_units) AS net_sales_regular_units,
  SUM(msrsswf.net_sales_tot_regular_cost) AS net_sales_regular_cost_amt,
  SUM(msrsswf.net_sales_tot_regular_retl) AS net_sales_regular_retail_amt,
  SUM(msrsswf.net_sales_tot_promo_units) AS net_sales_promo_units,
  SUM(msrsswf.net_sales_tot_promo_cost) AS net_sales_promo_cost_amt,
  SUM(msrsswf.net_sales_tot_promo_retl) AS net_sales_promo_retail_amt,
  SUM(msrsswf.net_sales_tot_clearance_units) AS net_sales_clearance_units,
  SUM(msrsswf.net_sales_tot_clearance_cost) AS net_sales_clearance_cost_amt,
  SUM(msrsswf.net_sales_tot_clearance_retl) AS net_sales_clearance_retail_amt,
  SUM(msrsswf.gross_sales_tot_units) AS gross_sales_total_units,
  SUM(msrsswf.gross_sales_tot_cost) AS gross_sales_total_cost_amt,
  SUM(msrsswf.gross_sales_tot_retl) AS gross_sales_total_retail_amt,
  SUM(msrsswf.gross_sales_tot_regular_units) AS gross_sales_regular_units,
  SUM(msrsswf.gross_sales_tot_regular_cost) AS gross_sales_regular_cost_amt,
  SUM(msrsswf.gross_sales_tot_regular_retl) AS gross_sales_regular_retail_amt,
  SUM(msrsswf.gross_sales_tot_promo_units) AS gross_sales_promo_units,
  SUM(msrsswf.gross_sales_tot_promo_cost) AS gross_sales_promo_cost_amt,
  SUM(msrsswf.gross_sales_tot_promo_retl) AS gross_sales_promo_retail_amt,
  SUM(msrsswf.gross_sales_tot_clearance_units) AS gross_sales_clearance_units,
  SUM(msrsswf.gross_sales_tot_clearance_cost) AS gross_sales_clearance_cost_amt,
  SUM(msrsswf.gross_sales_tot_clearance_retl) AS gross_sales_clearance_retail_amt,
  SUM(msrsswf.returns_tot_units) AS returns_total_units,
  SUM(msrsswf.returns_tot_cost) AS returns_total_cost_amt,
  SUM(msrsswf.returns_tot_retl) AS returns_total_retail_amt,
  SUM(msrsswf.returns_tot_regular_units) AS returns_regular_units,
  SUM(msrsswf.returns_tot_regular_cost) AS returns_regular_cost_amt,
  SUM(msrsswf.returns_tot_regular_retl) AS returns_regular_retail_amt,
  SUM(msrsswf.returns_tot_promo_units) AS returns_promo_units,
  SUM(msrsswf.returns_tot_promo_cost) AS returns_promo_cost_amt,
  SUM(msrsswf.returns_tot_promo_retl) AS returns_promo_retail_amt,
  SUM(msrsswf.returns_tot_clearance_units) AS returns_clearance_units,
  SUM(msrsswf.returns_tot_clearance_cost) AS returns_clearance_cost_amt,
  SUM(msrsswf.returns_tot_clearance_retl) AS returns_clearance_retail_amt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_return_sku_store_week_fact_vw AS msrsswf
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim AS occbd ON msrsswf.channel_num = occbd.channel_num
 WHERE msrsswf.week_num IN (SELECT rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_nap_aggregate_rebuild_lkup
    WHERE LOWER(process_status_ind) = LOWER('N')
     AND LOWER(config_key) = LOWER('MERCH_SALE_RETURN_DEPT_CHANNEL_WEEK_REBUILD_WEEKS')
    QUALIFY (ROW_NUMBER() OVER (ORDER BY rebuild_week_num)) <= 40)
 GROUP BY msrsswf.channel_num,
  banner_num,
  occbd.channel_country,
  msrsswf.department_num,
  msrsswf.department_desc,
  msrsswf.subdivision_num,
  msrsswf.subdivision_desc,
  msrsswf.division_num,
  msrsswf.division_desc,
  msrsswf.fulfill_type_code,
  msrsswf.week_num,
  msrsswf.month_label,
  msrsswf.quarter_num,
  msrsswf.halfyear_num,
  msrsswf.year_num,
  msrsswf.gift_with_purchase_ind,
  msrsswf.smart_sample_ind,
  msrsswf.wac_avlbl_ind);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup
SET
    process_status_ind = 'Y',
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE rebuild_week_num IN (SELECT rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_nap_aggregate_rebuild_lkup
    WHERE LOWER(process_status_ind) = LOWER('N') 
    AND LOWER(config_key) = LOWER('MERCH_SALE_RETURN_DEPT_CHANNEL_WEEK_REBUILD_WEEKS')
    QUALIFY (ROW_NUMBER() OVER (ORDER BY rebuild_week_num)) <= 40) AND LOWER(config_key) = LOWER('MERCH_SALE_RETURN_DEPT_CHANNEL_WEEK_REBUILD_WEEKS');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
