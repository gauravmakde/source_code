-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=spe_ap_supp_grp_fct;
-- Task_Name=t1_spe_ap_supgrp_bnr_fact_load;'
-- FOR SESSION VOLATILE;
-- ET;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_spe_ap_suppgrp_dept_banner_year_fact;



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_spe_ap_suppgrp_dept_banner_year_fact
(
supplier_group,
dept_num,
banner_num,
channel_country,
fiscal_year_num,
ty_actual_net_sales_total_retail_amount,
ty_actual_net_sales_regular_retail_amount,
ty_actual_net_sales_promo_retail_amount,
ty_actual_net_sales_clearance_retail_amount,
ty_actual_net_sales_regular_cost_amount,
ty_actual_net_sales_promo_cost_amount,
ty_actual_net_sales_clearance_cost_amount,
ty_actual_net_sales_total_cost_amount,
ty_actual_receipts_total_retail_amount,
ty_actual_receipts_total_cost_amount,
ty_actual_vendor_funds_total_cost_amount,
ty_actual_disc_terms_total_cost_amount,
ty_actual_mos_total_cost_amount,
ty_net_sales_total_retail_amount,
ty_net_sales_total_cost_amount,
ty_initial_markup_percentage,
ty_profitability_expectation_percentage,
ty_initial_markup_expectation_percentage,
ty_regular_price_sales_expectation_percentage,
ty_sales_retail_to_total_percentage,
ty_sales_cost_to_total_percentage,
ty_actual_profitability_percentage,
ly_actual_net_sales_total_retail_amount,
ly_actual_net_sales_regular_retail_amount,
ly_actual_net_sales_promo_retail_amount,
ly_actual_net_sales_clearance_retail_amount,
ly_actual_net_sales_regular_cost_amount,
ly_actual_net_sales_promo_cost_amount,
ly_actual_net_sales_clearance_cost_amount,
ly_actual_net_sales_total_cost_amount,
ly_actual_receipts_total_retail_amount,
ly_actual_receipts_total_cost_amount,
ly_actual_vendor_funds_total_cost_amount,
ly_actual_disc_terms_total_cost_amount,
ly_actual_mos_total_cost_amount,
ly_net_sales_total_retail_amount,
ly_net_sales_total_cost_amount,
ly_initial_markup_percentage,
ly_profitability_expectation_percentage,
ly_initial_markup_expectation_percentage,
ly_regular_price_sales_expectation_percentage,
ly_sales_retail_to_total_percentage,
ly_sales_cost_to_total_percentage,
ly_actual_profitability_percentage,
lly_actual_net_sales_total_retail_amount,
lly_actual_net_sales_regular_retail_amount,
lly_actual_net_sales_promo_retail_amount,
lly_actual_net_sales_clearance_retail_amount,
lly_actual_net_sales_regular_cost_amount,
lly_actual_net_sales_promo_cost_amount,
lly_actual_net_sales_clearance_cost_amount,
lly_actual_net_sales_total_cost_amount,
lly_actual_receipts_total_retail_amount,
lly_actual_receipts_total_cost_amount,
lly_actual_vendor_funds_total_cost_amount,
lly_actual_disc_terms_total_cost_amount,
lly_actual_mos_total_cost_amount,
lly_net_sales_total_retail_amount,
lly_net_sales_total_cost_amount,
lly_initial_markup_percentage,
lly_profitability_expectation_percentage,
lly_initial_markup_expectation_percentage,
lly_regular_price_sales_expectation_percentage,
lly_sales_retail_to_total_percentage,
lly_sales_cost_to_total_percentage,
lly_actual_profitability_percentage,
dw_batch_date,
dw_sys_load_tmstp
	)
SELECT supplier_group,
 dept_num,
 banner_num,
 channel_country,
 fiscal_year_num,
 SUM(ty_actual_net_sales_total_retail_amount) AS ty_actual_net_sales_total_retail_amount,
 SUM(ty_actual_net_sales_regular_retail_amount) AS ty_actual_net_sales_regular_retail_amount,
 SUM(ty_actual_net_sales_promo_retail_amount) AS ty_actual_net_sales_promo_retail_amount,
 SUM(ty_actual_net_sales_clearance_retail_amount) AS ty_actual_net_sales_clearance_retail_amount,
 SUM(ty_actual_net_sales_regular_cost_amount) AS ty_actual_net_sales_regular_cost_amount,
 SUM(ty_actual_net_sales_promo_cost_amount) AS ty_actual_net_sales_promo_cost_amount,
 SUM(ty_actual_net_sales_clearance_cost_amount) AS ty_actual_net_sales_clearance_cost_amount,
 SUM(ty_actual_net_sales_total_cost_amount) AS ty_actual_net_sales_total_cost_amount,
 SUM(ty_actual_receipts_total_retail_amount) AS ty_actual_receipts_total_retail_amount,
 SUM(ty_actual_receipts_total_cost_amount) AS ty_actual_receipts_total_cost_amount,
 SUM(ty_actual_vendor_funds_total_cost_amount) AS ty_actual_vendor_funds_total_cost_amount,
 SUM(ty_actual_disc_terms_total_cost_amount) AS ty_actual_disc_terms_total_cost_amount,
 SUM(ty_actual_mos_total_cost_amount) AS ty_actual_mos_total_cost_amount,
 SUM(ty_net_sales_total_retail_amount) AS ty_net_sales_total_retail_amount,
 SUM(ty_net_sales_total_cost_amount) AS ty_net_sales_total_cost_amount,
   CASE
   WHEN SUM(ty_actual_receipts_total_retail_amount) <> 0
   THEN (SUM(ty_actual_receipts_total_retail_amount) - SUM(ty_actual_receipts_total_cost_amount)) / SUM(ty_actual_receipts_total_retail_amount
     )
   ELSE 0
   END * 100 AS ty_initial_markup_percentage,
 MAX(ty_profitability_expectation_percentage) AS ty_profitability_expectation_percentage,
 MAX(ty_initial_markup_expectation_percent) AS ty_initial_markup_expectation_percent,
 MAX(ty_regular_price_sales_expectation_percent) AS ty_regular_price_sales_expectation_percent,
   CASE
   WHEN MAX(ty_total_dept_sales_retail_amount) <> 0
   THEN SUM(ty_net_sales_total_retail_amount) / MAX(ty_total_dept_sales_retail_amount)
   ELSE 0
   END * 100 AS ty_sales_retail_to_total_percentage,
   CASE
   WHEN MAX(ty_total_dept_sales_cost_amount) <> 0
   THEN SUM(ty_net_sales_total_cost_amount) / MAX(ty_total_dept_sales_cost_amount)
   ELSE 0
   END * 100 AS ty_sales_cost_to_total_percentage,
  CASE
  WHEN MAX(ty_profitability_expectation_percentage) <> 0
  THEN MAX(ty_profitability_expectation_percentage)
  ELSE CASE
    WHEN SUM(ty_act_sales_retail_amount) <> 0
    THEN (SUM(ty_act_sales_retail_amount) - SUM(ty_act_sales_cost_amount) - SUM(ty_actual_vendor_funds_total_cost_amount
          ) - SUM(ty_actual_disc_terms_total_cost_amount) + SUM(ty_actual_mos_total_cost_amount)) / SUM(ty_act_sales_retail_amount
      )
    ELSE 0
    END * 100
  END AS ty_actual_profitability_percentage,
 SUM(ly_actual_net_sales_total_retail_amount) AS ly_actual_net_sales_total_retail_amount,
 SUM(ly_actual_net_sales_regular_retail_amount) AS ly_actual_net_sales_regular_retail_amount,
 SUM(ly_actual_net_sales_promo_retail_amount) AS ly_actual_net_sales_promo_retail_amount,
 SUM(ly_actual_net_sales_clearance_retail_amount) AS ly_actual_net_sales_clearance_retail_amount,
 SUM(ly_actual_net_sales_regular_cost_amount) AS ly_actual_net_sales_regular_cost_amount,
 SUM(ly_actual_net_sales_promo_cost_amount) AS ly_actual_net_sales_promo_cost_amount,
 SUM(ly_actual_net_sales_clearance_cost_amount) AS ly_actual_net_sales_clearance_cost_amount,
 SUM(ly_actual_net_sales_total_cost_amount) AS ly_actual_net_sales_total_cost_amount,
 SUM(ly_actual_receipts_total_retail_amount) AS ly_actual_receipts_total_retail_amount,
 SUM(ly_actual_receipts_total_cost_amount) AS ly_actual_receipts_total_cost_amount,
 SUM(ly_actual_vendor_funds_total_cost_amount) AS ly_actual_vendor_funds_total_cost_amount,
 SUM(ly_actual_disc_terms_total_cost_amount) AS ly_actual_disc_terms_total_cost_amount,
 SUM(ly_actual_mos_total_cost_amount) AS ly_actual_mos_total_cost_amount,
 SUM(ly_net_sales_total_retail_amount) AS ly_net_sales_total_retail_amount,
 SUM(ly_net_sales_total_cost_amount) AS ly_net_sales_total_cost_amount,
   CASE
   WHEN SUM(ly_actual_receipts_total_retail_amount) <> 0
   THEN (SUM(ly_actual_receipts_total_retail_amount) - SUM(ly_actual_receipts_total_cost_amount)) / SUM(ly_actual_receipts_total_retail_amount
     )
   ELSE 0
   END * 100 AS ly_initial_markup_percentage,
 MAX(ly_profitability_expectation_percentage) AS ly_profitability_expectation_percentage,
 MAX(ly_initial_markup_expectation_percent) AS ly_initial_markup_expectation_percent,
 MAX(ly_regular_price_sales_expectation_percent) AS ly_regular_price_sales_expectation_percent,
   CASE
   WHEN MAX(ly_total_dept_sales_retail_amount) <> 0
   THEN SUM(ly_net_sales_total_retail_amount) / MAX(ly_total_dept_sales_retail_amount)
   ELSE 0
   END * 100 AS ly_sales_retail_to_total_percentage,
   CASE
   WHEN MAX(ly_total_dept_sales_cost_amount) <> 0
   THEN SUM(ly_net_sales_total_cost_amount) / MAX(ly_total_dept_sales_cost_amount)
   ELSE 0
   END * 100 AS ly_sales_cost_to_total_percentage,
  CASE
  WHEN MAX(ly_profitability_expectation_percentage) <> 0
  THEN MAX(ly_profitability_expectation_percentage)
  ELSE CASE
    WHEN SUM(ly_act_sales_retail_amount) <> 0
    THEN (SUM(ly_act_sales_retail_amount) - SUM(ly_act_sales_cost_amount) - SUM(ly_actual_vendor_funds_total_cost_amount
          ) - SUM(ly_actual_disc_terms_total_cost_amount) + SUM(ly_actual_mos_total_cost_amount)) / SUM(ly_act_sales_retail_amount
      )
    ELSE 0
    END * 100
  END AS ly_actual_profitability_percentage,
 SUM(lly_actual_net_sales_total_retail_amount) AS lly_actual_net_sales_total_retail_amount,
 SUM(lly_actual_net_sales_regular_retail_amount) AS lly_actual_net_sales_regular_retail_amount,
 SUM(lly_actual_net_sales_promo_retail_amount) AS lly_actual_net_sales_promo_retail_amount,
 SUM(lly_actual_net_sales_clearance_retail_amount) AS lly_actual_net_sales_clearance_retail_amount,
 SUM(lly_actual_net_sales_regular_cost_amount) AS lly_actual_net_sales_regular_cost_amount,
 SUM(lly_actual_net_sales_promo_cost_amount) AS lly_actual_net_sales_promo_cost_amount,
 SUM(lly_actual_net_sales_clearance_cost_amount) AS lly_actual_net_sales_clearance_cost_amount,
 SUM(lly_actual_net_sales_total_cost_amount) AS lly_actual_net_sales_total_cost_amount,
 SUM(lly_actual_receipts_total_retail_amount) AS lly_actual_receipts_total_retail_amount,
 SUM(lly_actual_receipts_total_cost_amount) AS lly_actual_receipts_total_cost_amount,
 SUM(lly_actual_vendor_funds_total_cost_amount) AS lly_actual_vendor_funds_total_cost_amount,
 SUM(lly_actual_disc_terms_total_cost_amount) AS lly_actual_disc_terms_total_cost_amount,
 SUM(lly_actual_mos_total_cost_amount) AS lly_actual_mos_total_cost_amount,
 SUM(lly_net_sales_total_retail_amount) AS lly_net_sales_total_retail_amount,
 SUM(lly_net_sales_total_cost_amount) AS lly_net_sales_total_cost_amount,
   CASE
   WHEN SUM(lly_actual_receipts_total_retail_amount) <> 0
   THEN (SUM(lly_actual_receipts_total_retail_amount) - SUM(lly_actual_receipts_total_cost_amount)) / SUM(lly_actual_receipts_total_retail_amount
     )
   ELSE 0
   END * 100 AS lly_initial_markup_percentage,
 MAX(lly_profitability_expectation_percentage) AS lly_profitability_expectation_percentage,
 MAX(lly_initial_markup_expectation_percent) AS lly_initial_markup_expectation_percent,
 MAX(lly_regular_price_sales_expectation_percent) AS lly_regular_price_sales_expectation_percent,
   CASE
   WHEN MAX(lly_total_dept_sales_retail_amount) <> 0
   THEN SUM(lly_net_sales_total_retail_amount) / MAX(lly_total_dept_sales_retail_amount)
   ELSE 0
   END * 100 AS lly_sales_retail_to_total_percentage,
   CASE
   WHEN MAX(lly_total_dept_sales_cost_amount) <> 0
   THEN SUM(lly_net_sales_total_cost_amount) / MAX(lly_total_dept_sales_cost_amount)
   ELSE 0
   END * 100 AS lly_sales_cost_to_total_percentage,
  CASE
  WHEN MAX(lly_profitability_expectation_percentage) <> 0
  THEN MAX(lly_profitability_expectation_percentage)
  ELSE CASE
    WHEN SUM(lly_act_sales_retail_amount) <> 0
    THEN (SUM(lly_act_sales_retail_amount) - SUM(lly_act_sales_cost_amount) - SUM(lly_actual_vendor_funds_total_cost_amount
          ) - SUM(lly_actual_disc_terms_total_cost_amount) + SUM(lly_actual_mos_total_cost_amount)) / SUM(lly_act_sales_retail_amount
      )
    ELSE 0
    END * 100
  END AS lly_actual_profitability_percentage,
 MAX(dw_batch_date),
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM (SELECT a01.supplier_group,
    a01.dept_num,
    a01.banner_num,
    a01.channel_country,
    a01.fiscal_year_num,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_total_retail_amount
     ELSE 0
     END AS ty_actual_net_sales_total_retail_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_regular_retail_amount
     ELSE 0
     END AS ty_actual_net_sales_regular_retail_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_promo_retail_amount
     ELSE 0
     END AS ty_actual_net_sales_promo_retail_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_clearance_retail_amount
     ELSE 0
     END AS ty_actual_net_sales_clearance_retail_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_promo_cost_amount
     ELSE 0
     END AS ty_actual_net_sales_promo_cost_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_regular_cost_amount
     ELSE 0
     END AS ty_actual_net_sales_regular_cost_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_clearance_cost_amount
     ELSE 0
     END AS ty_actual_net_sales_clearance_cost_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_total_cost_amount
     ELSE 0
     END AS ty_actual_net_sales_total_cost_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_receipts_total_retail_amount
     ELSE 0
     END AS ty_actual_receipts_total_retail_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_receipts_total_cost_amount
     ELSE 0
     END AS ty_actual_receipts_total_cost_amount,
     CASE
     WHEN a01.fiscal_year_num <= l01.last_completed_fiscal_year_num
     THEN a01.actual_vendor_funds_total_cost_amount
     ELSE 0
     END AS ty_actual_vendor_funds_total_cost_amount,
     CASE
     WHEN a01.fiscal_year_num <= l01.last_completed_fiscal_year_num
     THEN a01.actual_disc_terms_total_cost_amount
     ELSE 0
     END AS ty_actual_disc_terms_total_cost_amount,
     CASE
     WHEN a01.fiscal_year_num <= l01.last_completed_fiscal_year_num
     THEN a01.actual_mos_total_cost_amount
     ELSE 0
     END AS ty_actual_mos_total_cost_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_total_retail_amount
     ELSE a01.planned_net_sales_total_retail_amount
     END AS ty_net_sales_total_retail_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN a01.actual_net_sales_total_cost_amount
     ELSE a01.planned_net_sales_total_cost_amount
     END AS ty_net_sales_total_cost_amount,
     CASE
     WHEN a01.fiscal_year_num <= l01.last_completed_fiscal_year_num
     THEN a01.actual_net_sales_total_retail_amount
     ELSE 0
     END AS ty_act_sales_retail_amount,
     CASE
     WHEN a01.fiscal_year_num <= l01.last_completed_fiscal_year_num
     THEN a01.actual_net_sales_total_cost_amount
     ELSE 0
     END AS ty_act_sales_cost_amount,
     CASE
     WHEN a01.month_num <= l01.last_completed_fiscal_month_num
     THEN COALESCE(mspe.planned_profitability_expectation_percent, 0)
     ELSE 0
     END AS ty_profitability_expectation_percentage,
    COALESCE(mspe.initial_markup_expectation_percent, 0) AS ty_initial_markup_expectation_percent,
    COALESCE(mspe.regular_price_sales_expectation_percent, 0) AS ty_regular_price_sales_expectation_percent,
    SUM(CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_total_retail_amount
      ELSE a01.planned_net_sales_total_retail_amount
      END) OVER (PARTITION BY a01.dept_num, a01.banner_num, a01.channel_country, a01.fiscal_year_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ty_total_dept_sales_retail_amount,
    SUM(CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_total_cost_amount
      ELSE a01.planned_net_sales_total_cost_amount
      END) OVER (PARTITION BY a01.dept_num, a01.banner_num, a01.channel_country, a01.fiscal_year_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ty_total_dept_sales_cost_amount,
    0 AS ly_actual_net_sales_total_retail_amount,
    0 AS ly_actual_net_sales_regular_retail_amount,
    0 AS ly_actual_net_sales_promo_retail_amount,
    0 AS ly_actual_net_sales_clearance_retail_amount,
    0 AS ly_actual_net_sales_regular_cost_amount,
    0 AS ly_actual_net_sales_promo_cost_amount,
    0 AS ly_actual_net_sales_clearance_cost_amount,
    0 AS ly_actual_net_sales_total_cost_amount,
    0 AS ly_actual_receipts_total_retail_amount,
    0 AS ly_actual_receipts_total_cost_amount,
    0 AS ly_actual_vendor_funds_total_cost_amount,
    0 AS ly_actual_disc_terms_total_cost_amount,
    0 AS ly_actual_mos_total_cost_amount,
    0 AS ly_net_sales_total_retail_amount,
    0 AS ly_net_sales_total_cost_amount,
    0 AS ly_act_sales_retail_amount,
    0 AS ly_act_sales_cost_amount,
    0 AS ly_profitability_expectation_percentage,
    0 AS ly_initial_markup_expectation_percent,
    0 AS ly_regular_price_sales_expectation_percent,
    0 AS ly_total_dept_sales_retail_amount,
    0 AS ly_total_dept_sales_cost_amount,
    0 AS lly_actual_net_sales_total_retail_amount,
    0 AS lly_actual_net_sales_regular_retail_amount,
    0 AS lly_actual_net_sales_promo_retail_amount,
    0 AS lly_actual_net_sales_clearance_retail_amount,
    0 AS lly_actual_net_sales_regular_cost_amount,
    0 AS lly_actual_net_sales_promo_cost_amount,
    0 AS lly_actual_net_sales_clearance_cost_amount,
    0 AS lly_actual_net_sales_total_cost_amount,
    0 AS lly_actual_receipts_total_retail_amount,
    0 AS lly_actual_receipts_total_cost_amount,
    0 AS lly_actual_vendor_funds_total_cost_amount,
    0 AS lly_actual_disc_terms_total_cost_amount,
    0 AS lly_actual_mos_total_cost_amount,
    0 AS lly_net_sales_total_retail_amount,
    0 AS lly_net_sales_total_cost_amount,
    0 AS lly_act_sales_retail_amount,
    0 AS lly_act_sales_cost_amount,
    0 AS lly_profitability_expectation_percentage,
    0 AS lly_initial_markup_expectation_percent,
    0 AS lly_regular_price_sales_expectation_percent,
    0 AS lly_total_dept_sales_retail_amount,
    0 AS lly_total_dept_sales_cost_amount,
    l01.dw_batch_dt AS dw_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_ap_plan_actual_suppgrp_banner_fact AS a01
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_spe_profitability_expectation_vw AS mspe ON LOWER(a01.supplier_group) = LOWER(mspe.supplier_group
          ) AND a01.dept_num = mspe.department_number AND a01.banner_num = mspe.banner_num AND a01.fiscal_year_num =
       mspe.year_num AND LOWER(a01.channel_country) = LOWER(mspe.selling_country)
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS c01 ON a01.month_num = c01.month_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l01 ON LOWER(l01.interface_code) = LOWER('MERCH_NAP_SPE_DLY'
      )
   WHERE a01.fiscal_year_num >= (SELECT current_fiscal_year_num - 1
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
    AND a01.fiscal_year_num <= (SELECT current_fiscal_year_num + 2
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
   UNION ALL
   SELECT a010.supplier_group,
    a010.dept_num,
    a010.banner_num,
    a010.channel_country,
    c010.year_num AS fiscal_year_num,
    0 AS ty_actual_net_sales_total_retail_amount,
    0 AS ty_actual_net_sales_regular_retail_amount,
    0 AS ty_actual_net_sales_promo_retail_amount,
    0 AS ty_actual_net_sales_clearance_retail_amount,
    0 AS ty_actual_net_sales_regular_cost_amount,
    0 AS ty_actual_net_sales_promo_cost_amount,
    0 AS ty_actual_net_sales_clearance_cost_amount,
    0 AS ty_actual_net_sales_total_cost_amount,
    0 AS ty_actual_receipts_total_retail_amount,
    0 AS ty_actual_receipts_total_cost_amount,
    0 AS ty_actual_vendor_funds_total_cost_amount,
    0 AS ty_actual_disc_terms_total_cost_amount,
    0 AS ty_actual_mos_total_cost_amount,
    0 AS ty_net_sales_total_retail_amount,
    0 AS ty_net_sales_total_cost_amount,
    0 AS ty_act_sales_retail_amount,
    0 AS ty_act_sales_cost_amount,
    0 AS ty_profitability_expectation_percentage,
    0 AS ty_initial_markup_expectation_percent,
    0 AS ty_regular_price_sales_expectation_percent,
    0 AS ty_total_dept_sales_retail_amount,
    0 AS ty_total_dept_sales_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_total_retail_amount
     ELSE 0
     END AS ly_actual_net_sales_total_retail_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_regular_retail_amount
     ELSE 0
     END AS ly_actual_net_sales_regular_retail_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_promo_retail_amount
     ELSE 0
     END AS ly_actual_net_sales_promo_retail_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_clearance_retail_amount
     ELSE 0
     END AS ly_actual_net_sales_clearance_retail_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_regular_cost_amount
     ELSE 0
     END AS ly_actual_net_sales_regular_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_promo_cost_amount
     ELSE 0
     END AS ly_actual_net_sales_promo_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_clearance_cost_amount
     ELSE 0
     END AS ly_actual_net_sales_clearance_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_total_cost_amount
     ELSE 0
     END AS ly_actual_net_sales_total_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_receipts_total_retail_amount
     ELSE 0
     END AS ly_actual_receipts_total_retail_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_receipts_total_cost_amount
     ELSE 0
     END AS ly_actual_receipts_total_cost_amount,
     CASE
     WHEN a010.fiscal_year_num <= l010.last_completed_fiscal_year_num
     THEN a010.actual_vendor_funds_total_cost_amount
     ELSE 0
     END AS ly_actual_vendor_funds_total_cost_amount,
     CASE
     WHEN a010.fiscal_year_num <= l010.last_completed_fiscal_year_num
     THEN a010.actual_disc_terms_total_cost_amount
     ELSE 0
     END AS ly_actual_disc_terms_total_cost_amount,
     CASE
     WHEN a010.fiscal_year_num <= l010.last_completed_fiscal_year_num
     THEN a010.actual_mos_total_cost_amount
     ELSE 0
     END AS ly_actual_mos_total_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_total_retail_amount
     ELSE a010.planned_net_sales_total_retail_amount
     END AS ly_net_sales_total_retail_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN a010.actual_net_sales_total_cost_amount
     ELSE a010.planned_net_sales_total_cost_amount
     END AS ly_net_sales_total_cost_amount,
     CASE
     WHEN a010.fiscal_year_num <= l010.last_completed_fiscal_year_num
     THEN a010.actual_net_sales_total_retail_amount
     ELSE 0
     END AS ly_act_sales_retail_amount,
     CASE
     WHEN a010.fiscal_year_num <= l010.last_completed_fiscal_year_num
     THEN a010.actual_net_sales_total_cost_amount
     ELSE 0
     END AS ly_act_sales_cost_amount,
     CASE
     WHEN a010.month_num <= l010.last_completed_fiscal_month_num
     THEN COALESCE(mspe0.planned_profitability_expectation_percent, 0)
     ELSE 0
     END AS ly_profitability_expectation_percentage,
    COALESCE(mspe0.initial_markup_expectation_percent, 0) AS ly_initial_markup_expectation_percent,
    COALESCE(mspe0.regular_price_sales_expectation_percent, 0) AS ly_regular_price_sales_expectation_percent,
    SUM(CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_total_retail_amount
      ELSE a010.planned_net_sales_total_retail_amount
      END) OVER (PARTITION BY a010.dept_num, a010.banner_num, a010.channel_country, c010.year_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ly_total_dept_sales_retail_amount,
    SUM(CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_total_cost_amount
      ELSE a010.planned_net_sales_total_cost_amount
      END) OVER (PARTITION BY a010.dept_num, a010.banner_num, a010.channel_country, c010.year_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ly_total_dept_sales_cost_amount,
    0 AS lly_actual_net_sales_total_retail_amount,
    0 AS lly_actual_net_sales_regular_retail_amount,
    0 AS lly_actual_net_sales_promo_retail_amount,
    0 AS lly_actual_net_sales_clearance_retail_amount,
    0 AS lly_actual_net_sales_regular_cost_amount,
    0 AS lly_actual_net_sales_promo_cost_amount,
    0 AS lly_actual_net_sales_clearance_cost_amount,
    0 AS lly_actual_net_sales_total_cost_amount,
    0 AS lly_actual_receipts_total_retail_amount,
    0 AS lly_actual_receipts_total_cost_amount,
    0 AS lly_actual_vendor_funds_total_cost_amount,
    0 AS lly_actual_disc_terms_total_cost_amount,
    0 AS lly_actual_mos_total_cost_amount,
    0 AS lly_net_sales_retail_amount,
    0 AS lly_net_sales_cost_amount,
    0 AS lly_act_sales_retail_amount,
    0 AS lly_act_sales_cost_amount,
    0 AS lly_profitability_expectation_percentage,
    0 AS lly_initial_markup_expectation_percent,
    0 AS ly_regular_price_sales_expectation_percent0,
    0 AS lly_total_dept_sales_retail_amount,
    0 AS lly_total_dept_sales_cost_amount,
    l010.dw_batch_dt AS dw_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_ap_plan_actual_suppgrp_banner_fact AS a010
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_spe_profitability_expectation_vw AS mspe0 ON LOWER(a010.supplier_group) = LOWER(mspe0.supplier_group
          ) AND a010.dept_num = mspe0.department_number AND a010.banner_num = mspe0.banner_num AND a010.fiscal_year_num
       = mspe0.year_num AND LOWER(a010.channel_country) = LOWER(mspe0.selling_country)
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS c010 ON a010.month_num = c010.last_year_month_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l010 ON LOWER(l010.interface_code) = LOWER('MERCH_NAP_SPE_DLY'
      )
   WHERE a010.fiscal_year_num >= (SELECT current_fiscal_year_num - 2
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
    AND a010.fiscal_year_num <= (SELECT current_fiscal_year_num + 1
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
   UNION ALL
   SELECT a011.supplier_group,
    a011.dept_num,
    a011.banner_num,
    a011.channel_country,
    c011.year_num AS fiscal_year_num,
    0 AS ty_actual_net_sales_total_retail_amount,
    0 AS ty_actual_net_sales_regular_retail_amount,
    0 AS ty_actual_net_sales_promo_retail_amount,
    0 AS ty_actual_net_sales_clearance_retail_amount,
    0 AS ty_actual_net_sales_regular_cost_amount,
    0 AS ty_actual_net_sales_promo_cost_amount,
    0 AS ty_actual_net_sales_clearance_cost_amount,
    0 AS ty_actual_net_sales_total_cost_amount,
    0 AS ty_actual_receipts_total_retail_amount,
    0 AS ty_actual_receipts_total_cost_amount,
    0 AS ty_actual_vendor_funds_total_cost_amount,
    0 AS ty_actual_disc_terms_total_cost_amount,
    0 AS ty_actual_mos_total_cost_amount,
    0 AS ty_net_sales_total_retail_amount,
    0 AS ty_net_sales_total_cost_amount,
    0 AS ty_act_sales_retail_amount,
    0 AS ty_act_sales_cost_amount,
    0 AS ty_profitability_expectation_percentage,
    0 AS ty_initial_markup_expectation_percent,
    0 AS ty_regular_price_sales_expectation_percent,
    0 AS ty_total_dept_sales_retail_amount,
    0 AS ty_total_dept_sales_cost_amount,
    0 AS ly_actual_net_sales_total_retail_amount,
    0 AS ly_actual_net_sales_regular_retail_amount,
    0 AS ly_actual_net_sales_promo_retail_amount,
    0 AS ly_actual_net_sales_clearance_retail_amount,
    0 AS ly_actual_net_sales_regular_cost_amount,
    0 AS ly_actual_net_sales_promo_cost_amount,
    0 AS ly_actual_net_sales_clearance_cost_amount,
    0 AS ly_actual_net_sales_total_cost_amount,
    0 AS ly_actual_receipts_total_retail_amount,
    0 AS ly_actual_receipts_total_cost_amount,
    0 AS ly_actual_vendor_funds_total_cost_amount,
    0 AS ly_actual_disc_terms_total_cost_amount,
    0 AS ly_actual_mos_total_cost_amount,
    0 AS ly_net_sales_total_retail_amount,
    0 AS ly_net_sales_total_cost_amount,
    0 AS ly_act_sales_retail_amount,
    0 AS ly_act_sales_cost_amount,
    0 AS ly_profitability_expectation_percentage,
    0 AS ly_initial_markup_expectation_percent,
    0 AS ly_regular_price_sales_expectation_percent,
    0 AS ly_total_dept_sales_retail_amount,
    0 AS ly_total_dept_sales_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_total_retail_amount
     ELSE 0
     END AS lly_actual_net_sales_total_retail_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_regular_retail_amount
     ELSE 0
     END AS lly_actual_net_sales_regular_retail_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_promo_retail_amount
     ELSE 0
     END AS lly_actual_net_sales_promo_retail_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_clearance_retail_amount
     ELSE 0
     END AS lly_actual_net_sales_clearance_retail_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_regular_cost_amount
     ELSE 0
     END AS lly_actual_net_sales_regular_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_promo_cost_amount
     ELSE 0
     END AS lly_actual_net_sales_promo_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_clearance_cost_amount
     ELSE 0
     END AS lly_actual_net_sales_clearance_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_total_cost_amount
     ELSE 0
     END AS lly_actual_net_sales_total_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_receipts_total_retail_amount
     ELSE 0
     END AS lly_actual_receipts_total_retail_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_receipts_total_cost_amount
     ELSE 0
     END AS lly_actual_receipts_total_cost_amount,
     CASE
     WHEN a011.fiscal_year_num <= l011.last_completed_fiscal_year_num
     THEN a011.actual_vendor_funds_total_cost_amount
     ELSE 0
     END AS lly_actual_vendor_funds_total_cost_amount,
     CASE
     WHEN a011.fiscal_year_num <= l011.last_completed_fiscal_year_num
     THEN a011.actual_disc_terms_total_cost_amount
     ELSE 0
     END AS lly_actual_disc_terms_total_cost_amount,
     CASE
     WHEN a011.fiscal_year_num <= l011.last_completed_fiscal_year_num
     THEN a011.actual_mos_total_cost_amount
     ELSE 0
     END AS lly_actual_mos_total_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_total_retail_amount
     ELSE a011.planned_net_sales_total_retail_amount
     END AS lly_net_sales_total_retail_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN a011.actual_net_sales_total_cost_amount
     ELSE a011.planned_net_sales_total_cost_amount
     END AS lly_net_sales_total_cost_amount,
     CASE
     WHEN a011.fiscal_year_num <= l011.last_completed_fiscal_year_num
     THEN a011.actual_net_sales_total_retail_amount
     ELSE 0
     END AS lly_act_sales_retail_amount,
     CASE
     WHEN a011.fiscal_year_num <= l011.last_completed_fiscal_year_num
     THEN a011.actual_net_sales_total_cost_amount
     ELSE 0
     END AS lly_act_sales_cost_amount,
     CASE
     WHEN a011.month_num <= l011.last_completed_fiscal_month_num
     THEN COALESCE(mspe1.planned_profitability_expectation_percent, 0)
     ELSE 0
     END AS lly_profitability_expectation_percentage,
    COALESCE(mspe1.initial_markup_expectation_percent, 0) AS lly_initial_markup_expectation_percent,
    COALESCE(mspe1.regular_price_sales_expectation_percent, 0) AS lly_regular_price_sales_expectation_percent,
    SUM(CASE
      WHEN a011.month_num <= l011.last_completed_fiscal_month_num
      THEN a011.actual_net_sales_total_retail_amount
      ELSE a011.planned_net_sales_total_retail_amount
      END) OVER (PARTITION BY a011.dept_num, a011.banner_num, a011.channel_country, c011.year_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lly_total_dept_sales_retail_amount,
    SUM(CASE
      WHEN a011.month_num <= l011.last_completed_fiscal_month_num
      THEN a011.actual_net_sales_total_cost_amount
      ELSE a011.planned_net_sales_total_cost_amount
      END) OVER (PARTITION BY a011.dept_num, a011.banner_num, a011.channel_country, c011.year_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lly_total_dept_sales_cost_amount,
    l011.dw_batch_dt AS dw_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_ap_plan_actual_suppgrp_banner_fact AS a011
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_spe_profitability_expectation_vw AS mspe1 ON LOWER(a011.supplier_group) = LOWER(mspe1.supplier_group
          ) AND a011.dept_num = mspe1.department_number AND a011.banner_num = mspe1.banner_num AND a011.fiscal_year_num
       = mspe1.year_num AND LOWER(a011.channel_country) = LOWER(mspe1.selling_country)
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS c011 ON a011.month_num = c011.lly_month_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l011 ON LOWER(l011.interface_code) = LOWER('MERCH_NAP_SPE_DLY'
      )
   WHERE a011.fiscal_year_num >= (SELECT current_fiscal_year_num - 3
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
    AND a011.fiscal_year_num <= (SELECT current_fiscal_year_num
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))) AS I01
GROUP BY supplier_group,
 dept_num,
 banner_num,
 channel_country,
 fiscal_year_num;



-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;
