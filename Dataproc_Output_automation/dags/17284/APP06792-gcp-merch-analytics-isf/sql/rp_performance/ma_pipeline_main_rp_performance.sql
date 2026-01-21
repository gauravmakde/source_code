/*
Name:RP Performance
Project:RP Performance Dashboard
Purpose: Serve as the datasource to the Tableau RP Performance Published Data Source and to the RP Performance/RP Dept Store Callout Dashboard

DAG: merch_main_rp_contribution_9850_MERCH_ANALYTICS_insights 
schedule: 0 16 * * 1-5
Author(s):Jen Latta
Date Created:1/31/24 

Date Updated:2/15/24
Updates: 

	* Add Return R,C
	* Add Receipt U,R,C
	* Add PO Receipt U,R,C
	* Add EOH In Transit R
	* Add Total OO R,C
	* Add OO 0WK U,R,C
	* Add OO 1WK U,R,C
	* Add OO 2WK U,R,C
	* Add OO 3WK U,R,C
	* Add BOH U,R,C
	* Add BOH In Transit U,R,C
	* Add RS Transfer In U,R,C
	* Add RS Transfer Out U,R,C
	* Add Order Pack Size
	* Add NPG Ind
	* Update RP CURRENTLY_ACTIVE_RP_SKUS_DIM to include RS locations
	
Date Updated:3/1/24
Updates: 
	
	* Update RP CURRENTLY_ACTIVE_RP_SKUS_DIM to pull from `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST
	* Correct Trailing spaces on concatenated fields
	
	
Date Updated:3/7/24
Updates: 
	
	* Update RP_PERFORMANCE_FACT to cast locations in channel 120 to 808 and cast locations in channel 250 to 828
	
Date Updated:3/26/24
Updates:

	* Add Last Week Sales R,C,U
	* Add 4Wk Sales R,C,U
	* Add WTD Sales R,C,U
	* Add EOH Reg/Clr R,C,U

Date Updated:6/20/24
Updates:

	* Add Quantrix Category

*/

/* Previous day  */




CREATE TEMPORARY TABLE IF NOT EXISTS previous_day_dim
AS
SELECT day_date,
 week_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
WHERE day_date = (SELECT day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
GROUP BY day_date,
 week_idnt;


--COLLECT STATS PRIMARY INDEX(DAY_DATE,WEEK_IDNT) ON PREVIOUS_DAY_DIM


/* Current week as of the previous day + 9 weeks in the future */


CREATE TEMPORARY TABLE IF NOT EXISTS future_wks_dim
AS
(SELECT week_idnt,
 week_end_day_date,
 ROW_NUMBER() OVER (ORDER BY week_idnt) AS wk_rank
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
WHERE week_idnt >= (SELECT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
GROUP BY week_idnt,
 week_end_day_date
ORDER BY week_idnt
LIMIT 10);


--COLLECT STATS PRIMARY INDEX(WEEK_IDNT) ON FUTURE_WKS_DIM


/* Past 4 weeks + Current week WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS past_wks_dim
AS
SELECT b.week_idnt,
 d.day_date,
 d.day_idnt,
 b.week_rank,
 d.day_num_of_fiscal_week
FROM (SELECT week_idnt,
   RANK() OVER (ORDER BY week_idnt) AS week_rank
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
  WHERE week_idnt <= (SELECT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
  GROUP BY week_idnt
  ORDER BY week_idnt DESC
  LIMIT 5) AS b
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS d ON b.week_idnt = d.week_idnt AND d.day_date < CURRENT_DATE('PST8PDT')
GROUP BY b.week_idnt,
 d.day_date,
 d.day_idnt,
 b.week_rank,
 d.day_num_of_fiscal_week;


--COLLECT STATS PRIMARY INDEX(WEEK_IDNT,DAY_DATE) ON PAST_WKS_DIM


/* Skus active and on RP as of yesterday */


-- AND rms_sku_num = '8228715' AND location_num = 763


CREATE TEMPORARY TABLE IF NOT EXISTS currently_active_rp_skus_dim
AS
SELECT DISTINCT rp.rms_sku_num,
 rp.location_num,
 d.week_idnt,
 rp.rp_ind_flag AS rp_ind,
  CASE
  WHEN LOWER(rp.rp_active_flag) = LOWER('Y')
  THEN 'ACTIVE'
  ELSE 'INACTIVE'
  END AS rp_status_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rp_sku_loc_status_vw AS rp
 INNER JOIN previous_day_dim AS d ON d.day_date BETWEEN CAST(rp.eff_begin_tmstp AS DATE) AND (CAST(rp.eff_end_tmstp AS DATE)
   )
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s ON rp.location_num = s.store_num
WHERE LOWER(s.store_country_code) = LOWER('US')
 AND LOWER(rp.rp_ind_flag) = LOWER('Y');


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,LOCATION_NUM,WEEK_IDNT) ON CURRENTLY_ACTIVE_RP_SKUS_DIM


-- AND rms_sku_num = '8228715' AND location_num = 763


CREATE TEMPORARY TABLE IF NOT EXISTS active_rp_skus_status_dim
AS
SELECT DISTINCT rp.rms_sku_num,
 rp.location_num,
 d.week_idnt,
 rp.rp_ind_flag AS rp_ind,
  CASE
  WHEN LOWER(rp.rp_active_flag) = LOWER('Y')
  THEN 'ACTIVE'
  ELSE 'INACTIVE'
  END AS rp_status_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rp_sku_loc_status_vw AS rp
 INNER JOIN past_wks_dim AS d ON d.day_date > CAST(rp.eff_begin_tmstp AS DATE) AND d.day_date < CAST(rp.eff_end_tmstp AS DATE)
   
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s ON rp.location_num = s.store_num
WHERE LOWER(s.store_country_code) = LOWER('US')
 AND LOWER(rp.rp_ind_flag) = LOWER('Y');


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,LOCATION_NUM,WEEK_IDNT) ON ACTIVE_RP_SKUS_STATUS_DIM


CREATE TEMPORARY TABLE IF NOT EXISTS climate_cluster_dim
AS
SELECT a.store_num,
 COALESCE(UPPER(B.peer_group_desc), 'NA') AS peer_group_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS a
 LEFT JOIN (SELECT store_num,
   peer_group_num,
   UPPER(peer_group_desc) AS peer_group_desc,
   peer_group_type_code
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_peer_group_dim
  WHERE LOWER(peer_group_type_code) IN (LOWER('OPC'), LOWER('FPC'))) AS B ON a.store_num = B.store_num;


--COLLECT STATS PRIMARY INDEX(STORE_NUM) ON CLIMATE_CLUSTER_DIM


/* Forecast Demannd Units- Current week as of the previous day + 9 weeks in the future */


CREATE TEMPORARY TABLE IF NOT EXISTS forecast_fact
AS
SELECT p.rms_sku_num,
 a.location_id,
 rp.week_idnt AS wk_idnt,
 rp.rp_ind,
 rp.rp_status_flag,
 SUM(CASE
   WHEN d.wk_rank = 1
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_0wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 2
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_1wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 3
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_2wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 4
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_3wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 5
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_4wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 6
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_5wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 7
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_6wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 8
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_7wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 9
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_8wk_dmnd_units,
 SUM(CASE
   WHEN d.wk_rank = 10
   THEN a.inventory_forecast_qty
   ELSE 0
   END) AS fcst_9wk_dmnd_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_approved_weekly_deployment_forecast_fact AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p ON LOWER(a.sku_id) = LOWER(SUBSTR(CAST(p.epm_sku_num AS STRING), 1,
     20)) AND LOWER(p.channel_country) = LOWER('US')
 INNER JOIN future_wks_dim AS d ON LOWER(a.week_id) = LOWER(RPAD(CAST(d.week_idnt AS STRING), 6, ' '))
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(rp.rms_sku_num) = LOWER(p.rms_sku_num) AND LOWER(SUBSTR(CAST(rp.location_num AS STRING)
     , 1, 16)) = LOWER(a.location_id)
GROUP BY p.rms_sku_num,
 a.location_id,
 wk_idnt,
 rp.rp_ind,
 rp.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM ,LOCATION_ID,WK_IDNT) ON FORECAST_FACT


/* Sales and Returns - skus currently on RP for the past 4 weeks + current WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS sales_fact
AS
SELECT a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag,
 SUM(a.jwn_returns_total_units_ty) AS jwn_returns_total_units_ty,
 SUM(a.jwn_returns_total_retail_amt_ty) AS jwn_returns_total_retail_amt_ty,
 SUM(a.jwn_returns_total_cost_amt_ty) AS jwn_returns_total_cost_amt_ty,
 SUM(a.jwn_operational_gmv_total_retail_amt_ty) AS jwn_operational_gmv_total_retail_amt_ty,
 SUM(CASE
   WHEN w.wk_rank < 5
   THEN a.jwn_operational_gmv_total_retail_amt_ty
   ELSE 0
   END) AS sales_4wk_retl_dollars,
 SUM(CASE
   WHEN w.wk_rank = 4
   THEN a.jwn_operational_gmv_total_retail_amt_ty
   ELSE 0
   END) AS sales_lwk_retl_dollars,
 SUM(CASE
   WHEN w.wk_rank = 5
   THEN a.jwn_operational_gmv_total_retail_amt_ty
   ELSE 0
   END) AS sales_wtd_retl_dollars,
 SUM(a.jwn_operational_gmv_total_units_ty) AS jwn_operational_gmv_total_units_ty,
 SUM(CASE
   WHEN w.wk_rank < 5
   THEN a.jwn_operational_gmv_total_units_ty
   ELSE 0
   END) AS sales_4wk_units,
 SUM(CASE
   WHEN w.wk_rank = 4
   THEN a.jwn_operational_gmv_total_units_ty
   ELSE 0
   END) AS sales_lwk_units,
 SUM(CASE
   WHEN w.wk_rank = 5
   THEN a.jwn_operational_gmv_total_units_ty
   ELSE 0
   END) AS sales_wtd_units,
 SUM(a.jwn_operational_gmv_total_cost_amt_ty) AS jwn_operational_gmv_total_cost_amt_ty,
 SUM(CASE
   WHEN w.wk_rank < 5
   THEN a.jwn_operational_gmv_total_cost_amt_ty
   ELSE 0
   END) AS sales_4wk_cost_amt,
 SUM(CASE
   WHEN w.wk_rank = 4
   THEN a.jwn_operational_gmv_total_cost_amt_ty
   ELSE 0
   END) AS sales_lwk_cost_amt,
 SUM(CASE
   WHEN w.wk_rank = 5
   THEN a.jwn_operational_gmv_total_cost_amt_ty
   ELSE 0
   END) AS sales_wtd_cost_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_sale_return_sku_store_week_agg_fact AS a
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(a.rms_sku_num) = LOWER(rp.rms_sku_num) AND a.store_num = rp.location_num
   
 INNER JOIN active_rp_skus_status_dim AS status ON LOWER(rp.rms_sku_num) = LOWER(status.rms_sku_num) AND rp.location_num
     = status.location_num AND a.week_num = status.week_idnt
 INNER JOIN (SELECT DISTINCT week_idnt,
   week_rank AS wk_rank
  FROM past_wks_dim) AS w ON a.week_num = w.week_idnt
GROUP BY a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON SALES_FACT


/* Receipts - skus currently on RP for the past 4 weeks + current WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_fact
AS
SELECT a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag,
 SUM(a.receipts_total_units_ty) AS receipts_total_units_ty,
 SUM(a.receipts_total_retail_amt_ty) AS receipts_total_retail_amt_ty,
 SUM(a.receipts_total_cost_amt_ty) AS receipts_total_cost_amt_ty,
 SUM(a.receipts_po_units_ty) AS receipts_po_units_ty,
 SUM(a.receipts_po_retail_amt_ty) AS receipts_po_retail_amt_ty,
 SUM(a.receipts_po_cost_amt_ty) AS receipts_po_cost_amt_ty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_agg_fact AS a
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(a.rms_sku_num) = LOWER(rp.rms_sku_num) AND a.store_num = rp.location_num
   
 INNER JOIN active_rp_skus_status_dim AS status ON LOWER(rp.rms_sku_num) = LOWER(status.rms_sku_num) AND rp.location_num
     = status.location_num AND a.week_num = status.week_idnt
 INNER JOIN (SELECT DISTINCT week_idnt
  FROM past_wks_dim) AS w ON a.week_num = w.week_idnt
GROUP BY a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON RECEIPTS_FACT


/* Inventory - skus currently on RP for the past 4 weeks + current WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_fact
AS
SELECT a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag,
 SUM(a.inventory_eoh_total_retail_amt_ty) AS inventory_eoh_total_retail_amt_ty,
 SUM(a.inventory_eoh_total_units_ty) AS inventory_eoh_total_units_ty,
 SUM(a.inventory_eoh_total_cost_amt_ty) AS inventory_eoh_total_cost_amt_ty,
 SUM(a.inventory_eoh_regular_units_ty) AS inventory_eoh_total_reg_units_ty,
 SUM(a.inventory_eoh_regular_retail_amt_ty) AS inventory_eoh_total_reg_retail_amt_ty,
 SUM(a.inventory_eoh_regular_cost_amt_ty) AS inventory_eoh_total_reg_cost_amt_ty,
 SUM(a.inventory_eoh_clearance_units_ty) AS inventory_eoh_total_clr_units_ty,
 SUM(a.inventory_eoh_clearance_retail_amt_ty) AS inventory_eoh_total_clr_retail_amt_ty,
 SUM(a.inventory_eoh_clearance_cost_amt_ty) AS inventory_eoh_total_clr_cost_amt_ty,
 SUM(a.inventory_eoh_in_transit_total_units_ty) AS inventory_eoh_in_transit_total_units_ty,
 SUM(a.inventory_eoh_in_transit_total_cost_amt_ty) AS inventory_eoh_in_transit_total_cost_amt_ty,
 SUM(a.inventory_eoh_in_transit_total_retail_amt_ty) AS inventory_eoh_in_transit_total_retail_amt_ty,
 SUM(a.inventory_boh_total_units_ty) AS inventory_boh_total_units_ty,
 SUM(a.inventory_boh_total_retail_amt_ty) AS inventory_boh_total_retail_amt_ty,
 SUM(a.inventory_boh_total_cost_amt_ty) AS inventory_boh_total_cost_amt_ty,
 SUM(a.inventory_boh_in_transit_total_units_ty) AS inventory_boh_in_transit_total_units_ty,
 SUM(a.inventory_boh_in_transit_total_cost_amt_ty) AS inventory_boh_in_transit_total_cost_amt_ty,
 SUM(a.inventory_boh_in_transit_total_retail_amt_ty) AS inventory_boh_in_transit_total_retail_amt_ty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_agg_fact AS a
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(a.rms_sku_num) = LOWER(rp.rms_sku_num) AND a.store_num = rp.location_num
   
 INNER JOIN active_rp_skus_status_dim AS status ON LOWER(rp.rms_sku_num) = LOWER(status.rms_sku_num) AND rp.location_num
     = status.location_num AND a.week_num = status.week_idnt
 INNER JOIN (SELECT DISTINCT week_idnt
  FROM past_wks_dim) AS w ON a.week_num = w.week_idnt
GROUP BY a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON INVENTORY_FACT


/* Demand- skus currently on RP for the past 4 weeks + current WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS demand_fact
AS
SELECT a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag,
 SUM(a.jwn_demand_total_retail_amt_ty) AS jwn_demand_total_retail_amt_ty,
 SUM(a.jwn_demand_total_units_ty) AS jwn_demand_total_units_ty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS a
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(a.rms_sku_num) = LOWER(rp.rms_sku_num) AND a.store_num = rp.location_num
   
 INNER JOIN active_rp_skus_status_dim AS status ON LOWER(rp.rms_sku_num) = LOWER(status.rms_sku_num) AND rp.location_num
     = status.location_num AND a.week_num = status.week_idnt
 INNER JOIN (SELECT DISTINCT week_idnt
  FROM past_wks_dim) AS w ON a.week_num = w.week_idnt
GROUP BY a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON DEMAND_FACT


/* RS- skus currently on RP for the past 4 weeks + current WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS rs_fact
AS
SELECT a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag,
 SUM(a.reservestock_transfer_in_retail) AS reservestock_transfer_in_retail,
 SUM(a.reservestock_transfer_in_units) AS reservestock_transfer_in_units,
 SUM(a.reservestock_transfer_in_cost) AS reservestock_transfer_in_cost,
 SUM(a.reservestock_transfer_out_retail) AS reservestock_transfer_out_retail,
 SUM(a.reservestock_transfer_out_units) AS reservestock_transfer_out_units,
 SUM(a.reservestock_transfer_out_cost) AS reservestock_transfer_out_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS a
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(a.rms_sku_num) = LOWER(rp.rms_sku_num) AND a.store_num = rp.location_num
   
 INNER JOIN active_rp_skus_status_dim AS status ON LOWER(rp.rms_sku_num) = LOWER(status.rms_sku_num) AND rp.location_num
     = status.location_num AND a.week_num = status.week_idnt
 INNER JOIN (SELECT DISTINCT week_idnt
  FROM past_wks_dim) AS w ON a.week_num = w.week_idnt
GROUP BY a.rms_sku_num,
 a.store_num,
 a.week_num,
 rp.rp_ind,
 status.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON RS_FACT


/* On Order Units- skus currently on RP as of previous day */


-- include closed POs with ship dates in the recent past


-- include POs that aren't closed yet


CREATE TEMPORARY TABLE IF NOT EXISTS oo_fact
AS
SELECT a.rms_sku_num,
 a.store_num,
 rp.week_idnt AS week_num,
 rp.rp_ind,
 rp.rp_status_flag,
 SUM(a.quantity_open) AS on_order_total_units,
 SUM(a.quantity_open * a.anticipated_retail_amt) AS on_order_total_retail_dollars,
 SUM(a.quantity_open * a.unit_cost_amt) AS on_order_total_cost_dollars,
 SUM(CASE
   WHEN d.wk_rank = 1 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open
   ELSE 0
   END) AS on_order_0wk_units,
 SUM(CASE
   WHEN d.wk_rank = 2 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open
   ELSE 0
   END) AS on_order_1wk_units,
 SUM(CASE
   WHEN d.wk_rank = 3 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open
   ELSE 0
   END) AS on_order_2wk_units,
 SUM(CASE
   WHEN d.wk_rank = 4 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open
   ELSE 0
   END) AS on_order_3wk_units,
 SUM(CASE
   WHEN d.wk_rank = 5 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open
   ELSE 0
   END) AS on_order_4wk_units,
 SUM(CASE
   WHEN d.wk_rank = 1 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.anticipated_retail_amt
   ELSE 0
   END) AS on_order_0wk_retail_dollars,
 SUM(CASE
   WHEN d.wk_rank = 2 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.anticipated_retail_amt
   ELSE 0
   END) AS on_order_1wk_retail_dollars,
 SUM(CASE
   WHEN d.wk_rank = 3 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.anticipated_retail_amt
   ELSE 0
   END) AS on_order_2wk_retail_dollars,
 SUM(CASE
   WHEN d.wk_rank = 4 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.anticipated_retail_amt
   ELSE 0
   END) AS on_order_3wk_retail_dollars,
 SUM(CASE
   WHEN d.wk_rank = 5 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.anticipated_retail_amt
   ELSE 0
   END) AS on_order_4wk_retail_dollars,
 SUM(CASE
   WHEN d.wk_rank = 1 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.unit_cost_amt
   ELSE 0
   END) AS on_order_0wk_cost_dollars,
 SUM(CASE
   WHEN d.wk_rank = 2 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.unit_cost_amt
   ELSE 0
   END) AS on_order_1wk_cost_dollars,
 SUM(CASE
   WHEN d.wk_rank = 3 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.unit_cost_amt
   ELSE 0
   END) AS on_order_2wk_cost_dollars,
 SUM(CASE
   WHEN d.wk_rank = 4 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.unit_cost_amt
   ELSE 0
   END) AS on_order_3wk_cost_dollars,
 SUM(CASE
   WHEN d.wk_rank = 5 AND a.otb_eow_date <= d.week_end_day_date
   THEN a.quantity_open * a.unit_cost_amt
   ELSE 0
   END) AS on_order_4wk_cost_dollars
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS a
 LEFT JOIN future_wks_dim AS d ON a.week_num = d.week_idnt
 INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(rp.rms_sku_num) = LOWER(a.rms_sku_num) AND a.store_num = rp.location_num
   
WHERE LOWER(a.order_type) IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER'))
 AND a.quantity_open > 0
 AND (LOWER(a.status) = LOWER('CLOSED') AND a.end_ship_date >= DATE_SUB((SELECT MAX(week_end_day_date)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
       WHERE week_end_day_date < CURRENT_DATE('PST8PDT')), INTERVAL 45 DAY) OR LOWER(a.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'
      )))
 AND a.first_approval_date IS NOT NULL
GROUP BY a.rms_sku_num,
 a.store_num,
 week_num,
 rp.rp_ind,
 rp.rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON OO_FACT


/* In Stock Indicator and Days in Stock: Past 4 weeks + WTD */


CREATE TEMPORARY TABLE IF NOT EXISTS stock_status_fact
AS
SELECT rms_sku_num,
 store_num,
 week_num,
 rp_ind,
 rp_status_flag,
  CASE
  WHEN SUM(`A121812`) >= 1
  THEN 1
  ELSE 0
  END AS f_i_eoh_in_stock_ind,
 SUM(`A121812`) AS days_in_stock
FROM (SELECT t1.rms_sku_num,
   t1.store_num,
   t1.week_idnt AS week_num,
   t1.rp_ind,
   t1.rp_status_flag,
          CASE
          WHEN SUM(t1.`A12187`) >= 1
          THEN 1
          ELSE 0
          END + CASE
          WHEN SUM(t1.`A12188`) >= 1
          THEN 1
          ELSE 0
          END + CASE
         WHEN SUM(t1.`A12189`) >= 1
         THEN 1
         ELSE 0
         END + CASE
        WHEN SUM(t1.`A121810`) >= 1
        THEN 1
        ELSE 0
        END + CASE
       WHEN SUM(t1.`A121811`) >= 1
       THEN 1
       ELSE 0
       END + CASE
      WHEN SUM(t1.`A121812`) >= 1
      THEN 1
      ELSE 0
      END + CASE
     WHEN SUM(t1.`A121813`) >= 1
     THEN 1
     ELSE 0
     END AS `A121812`
  FROM (SELECT s.rms_sku_num,
     s.store_num,
     w.week_idnt,
     s.rp_ind,
     status.rp_status_flag,
      CASE
      WHEN w.day_num_of_fiscal_week = 1
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A12187`,
      CASE
      WHEN w.day_num_of_fiscal_week = 2
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A12188`,
      CASE
      WHEN w.day_num_of_fiscal_week = 3
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A12189`,
      CASE
      WHEN w.day_num_of_fiscal_week = 4
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A121810`,
      CASE
      WHEN w.day_num_of_fiscal_week = 5
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A121811`,
      CASE
      WHEN w.day_num_of_fiscal_week = 6
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A121812`,
      CASE
      WHEN w.day_num_of_fiscal_week = 7
      THEN SUM(s.inventory_eoh_total_units_ty)
      ELSE NULL
      END AS `A121813`
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transaction_sku_store_day_agg_fact AS s
     INNER JOIN past_wks_dim AS w ON s.day_num = w.day_idnt
     INNER JOIN currently_active_rp_skus_dim AS rp ON LOWER(s.rms_sku_num) = LOWER(rp.rms_sku_num) AND s.store_num = rp
        .location_num AND LOWER(s.rp_ind) = LOWER('Y')
     INNER JOIN active_rp_skus_status_dim AS status ON LOWER(rp.rms_sku_num) = LOWER(status.rms_sku_num) AND rp.location_num
         = status.location_num AND w.week_idnt = status.week_idnt
    GROUP BY s.rms_sku_num,
     s.store_num,
     s.rp_ind,
     w.week_idnt,
     w.day_num_of_fiscal_week,
     status.rp_status_flag) AS t1
  GROUP BY t1.rms_sku_num,
   t1.store_num,
   t1.week_idnt,
   t1.rp_ind,
   t1.rp_status_flag) AS t3
GROUP BY rms_sku_num,
 store_num,
 week_num,
 rp_ind,
 rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON STOCK_STATUS_FACT


--,CASE WHEN week_num = max(week_num) THEN 1 END AS max_week


CREATE TEMPORARY TABLE IF NOT EXISTS rp_performance_fact
AS
SELECT rms_sku_num,
 store_num,
 week_num,
 rp_ind,
 rp_status_flag,
 SUM(jwn_returns_total_units_ty) AS jwn_returns_total_units_ty,
 SUM(jwn_returns_total_retail_amt_ty) AS jwn_returns_total_retail_amt_ty,
 SUM(jwn_returns_total_cost_amt_ty) AS jwn_returns_total_cost_amt_ty,
 SUM(jwn_operational_gmv_total_retail_amt_ty) AS jwn_operational_gmv_total_retail_amt_ty,
 SUM(sales_4wk_retl_dollars) AS sales_4wk_retl_dollars,
 SUM(sales_lwk_retl_dollars) AS sales_lwk_retl_dollars,
 SUM(sales_wtd_retl_dollars) AS sales_wtd_retl_dollars,
 SUM(jwn_operational_gmv_total_units_ty) AS jwn_operational_gmv_total_units_ty,
 SUM(sales_4wk_units) AS sales_4wk_units,
 SUM(sales_lwk_units) AS sales_lwk_units,
 SUM(sales_wtd_units) AS sales_wtd_units,
 SUM(jwn_operational_gmv_total_cost_amt_ty) AS jwn_operational_gmv_total_cost_amt_ty,
 SUM(sales_4wk_cost_amt) AS sales_4wk_cost_amt,
 SUM(sales_lwk_cost_amt) AS sales_lwk_cost_amt,
 SUM(sales_wtd_cost_amt) AS sales_wtd_cost_amt,
 SUM(receipts_total_units_ty) AS receipts_total_units_ty,
 SUM(receipts_total_retail_amt_ty) AS receipts_total_retail_amt_ty,
 SUM(receipts_total_cost_amt_ty) AS receipts_total_cost_amt_ty,
 SUM(receipts_po_units_ty) AS receipts_po_units_ty,
 SUM(receipts_po_retail_amt_ty) AS receipts_po_retail_amt_ty,
 SUM(receipts_po_cost_amt_ty) AS receipts_po_cost_amt_ty,
 SUM(inventory_eoh_total_retail_amt_ty) AS inventory_eoh_total_retail_amt_ty,
 SUM(inventory_eoh_total_units_ty) AS inventory_eoh_total_units_ty,
 SUM(inventory_eoh_total_cost_amt_ty) AS inventory_eoh_total_cost_amt_ty,
 SUM(inventory_eoh_total_reg_retail_amt_ty) AS inventory_eoh_total_reg_retail_amt_ty,
 SUM(inventory_eoh_total_reg_units_ty) AS inventory_eoh_total_reg_units_ty,
 SUM(inventory_eoh_total_reg_cost_amt_ty) AS inventory_eoh_total_reg_cost_amt_ty,
 SUM(inventory_eoh_total_clr_retail_amt_ty) AS inventory_eoh_total_clr_retail_amt_ty,
 SUM(inventory_eoh_total_clr_units_ty) AS inventory_eoh_total_clr_units_ty,
 SUM(inventory_eoh_total_clr_cost_amt_ty) AS inventory_eoh_total_clr_cost_amt_ty,
 SUM(inventory_eoh_in_transit_total_units_ty) AS inventory_eoh_in_transit_total_units_ty,
 SUM(inventory_eoh_in_transit_total_cost_amt_ty) AS inventory_eoh_in_transit_total_cost_amt_ty,
 SUM(inventory_eoh_in_transit_total_retail_amt_ty) AS inventory_eoh_in_transit_total_retail_amt_ty,
 SUM(inventory_boh_total_units_ty) AS inventory_boh_total_units_ty,
 SUM(inventory_boh_total_retail_amt_ty) AS inventory_boh_total_retail_amt_ty,
 SUM(inventory_boh_total_cost_amt_ty) AS inventory_boh_total_cost_amt_ty,
 SUM(inventory_boh_in_transit_total_units_ty) AS inventory_boh_in_transit_total_units_ty,
 SUM(inventory_boh_in_transit_total_cost_amt_ty) AS inventory_boh_in_transit_total_cost_amt_ty,
 SUM(inventory_boh_in_transit_total_retail_amt_ty) AS inventory_boh_in_transit_total_retail_amt_ty,
 SUM(reservestock_transfer_in_retail) AS reservestock_transfer_in_retail,
 SUM(reservestock_transfer_in_units) AS reservestock_transfer_in_units,
 SUM(reservestock_transfer_in_cost) AS reservestock_transfer_in_cost,
 SUM(reservestock_transfer_out_retail) AS reservestock_transfer_out_retail,
 SUM(reservestock_transfer_out_units) AS reservestock_transfer_out_units,
 SUM(reservestock_transfer_out_cost) AS reservestock_transfer_out_cost,
 SUM(jwn_demand_total_retail_amt_ty) AS jwn_demand_total_retail_amt_ty,
 SUM(jwn_demand_total_units_ty) AS jwn_demand_total_units_ty,
 SUM(on_order_total_units) AS on_order_total_units,
 SUM(on_order_total_retail_dollars) AS on_order_total_retail_dollars,
 SUM(on_order_total_cost_dollars) AS on_order_total_cost_dollars,
 SUM(on_order_0wk_units) AS on_order_0wk_units,
 SUM(on_order_1wk_units) AS on_order_1wk_units,
 SUM(on_order_2wk_units) AS on_order_2wk_units,
 SUM(on_order_3wk_units) AS on_order_3wk_units,
 SUM(on_order_4wk_units) AS on_order_4wk_units,
 SUM(on_order_0wk_retail_dollars) AS on_order_0wk_retail_dollars,
 SUM(on_order_1wk_retail_dollars) AS on_order_1wk_retail_dollars,
 SUM(on_order_2wk_retail_dollars) AS on_order_2wk_retail_dollars,
 SUM(on_order_3wk_retail_dollars) AS on_order_3wk_retail_dollars,
 SUM(on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars,
 SUM(on_order_0wk_cost_dollars) AS on_order_0wk_cost_dollars,
 SUM(on_order_1wk_cost_dollars) AS on_order_1wk_cost_dollars,
 SUM(on_order_2wk_cost_dollars) AS on_order_2wk_cost_dollars,
 SUM(on_order_3wk_cost_dollars) AS on_order_3wk_cost_dollars,
 SUM(on_order_4wk_cost_dollars) AS on_order_4wk_cost_dollars,
 SUM(fcst_0wk_dmnd_units) AS fcst_0wk_dmnd_units,
 SUM(fcst_1wk_dmnd_units) AS fcst_1wk_dmnd_units,
 SUM(fcst_2wk_dmnd_units) AS fcst_2wk_dmnd_units,
 SUM(fcst_3wk_dmnd_units) AS fcst_3wk_dmnd_units,
 SUM(fcst_4wk_dmnd_units) AS fcst_4wk_dmnd_units,
 SUM(fcst_5wk_dmnd_units) AS fcst_5wk_dmnd_units,
 SUM(fcst_6wk_dmnd_units) AS fcst_6wk_dmnd_units,
 SUM(fcst_7wk_dmnd_units) AS fcst_7wk_dmnd_units,
 SUM(fcst_8wk_dmnd_units) AS fcst_8wk_dmnd_units,
 SUM(fcst_9wk_dmnd_units) AS fcst_9wk_dmnd_units,
 MAX(f_i_eoh_in_stock_ind) AS f_i_eoh_in_stock_ind,
 MAX(days_in_stock) AS days_in_stock
FROM (SELECT SUBSTR(sf.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(sf.store_num AS STRING), 1, 16)
     END AS store_num,
    sf.week_num,
    sf.rp_ind,
    sf.rp_status_flag,
    COALESCE(sf.jwn_returns_total_units_ty, 0) AS jwn_returns_total_units_ty,
    COALESCE(sf.jwn_returns_total_retail_amt_ty, 0) AS jwn_returns_total_retail_amt_ty,
    COALESCE(sf.jwn_returns_total_cost_amt_ty, 0) AS jwn_returns_total_cost_amt_ty,
    COALESCE(sf.jwn_operational_gmv_total_retail_amt_ty, 0) AS jwn_operational_gmv_total_retail_amt_ty,
    COALESCE(sf.sales_4wk_retl_dollars, 0) AS sales_4wk_retl_dollars,
    COALESCE(sf.sales_lwk_retl_dollars, 0) AS sales_lwk_retl_dollars,
    COALESCE(sf.sales_wtd_retl_dollars, 0) AS sales_wtd_retl_dollars,
    COALESCE(sf.jwn_operational_gmv_total_units_ty, 0) AS jwn_operational_gmv_total_units_ty,
    COALESCE(sf.sales_4wk_units, 0) AS sales_4wk_units,
    COALESCE(sf.sales_lwk_units, 0) AS sales_lwk_units,
    COALESCE(sf.sales_wtd_units, 0) AS sales_wtd_units,
    COALESCE(sf.jwn_operational_gmv_total_cost_amt_ty, 0) AS jwn_operational_gmv_total_cost_amt_ty,
    COALESCE(sf.sales_4wk_cost_amt, 0) AS sales_4wk_cost_amt,
    COALESCE(sf.sales_lwk_cost_amt, 0) AS sales_lwk_cost_amt,
    COALESCE(sf.sales_wtd_cost_amt, 0) AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM sales_fact AS sf
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s ON sf.store_num = CAST(SUBSTR(CAST(s.store_num AS STRING), 1, 16) AS
      FLOAT64)
   UNION ALL
   SELECT SUBSTR(rf.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s0.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s0.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(rf.store_num AS STRING), 1, 16)
     END AS store_num,
    rf.week_num,
    rf.rp_ind,
    rf.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    COALESCE(rf.receipts_total_units_ty, 0) AS receipts_total_units_ty,
    COALESCE(rf.receipts_total_retail_amt_ty, 0) AS receipts_total_retail_amt_ty,
    COALESCE(rf.receipts_total_cost_amt_ty, 0) AS receipts_total_cost_amt_ty,
    COALESCE(rf.receipts_po_units_ty, 0) AS receipts_po_units_ty,
    COALESCE(rf.receipts_po_retail_amt_ty, 0) AS receipts_po_retail_amt_ty,
    COALESCE(rf.receipts_po_cost_amt_ty, 0) AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM receipts_fact AS rf
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s0 ON rf.store_num = CAST(SUBSTR(CAST(s0.store_num AS STRING), 1, 16) AS
      FLOAT64)
   UNION ALL
   SELECT SUBSTR(invf.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s1.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s1.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(invf.store_num AS STRING), 1, 16)
     END AS store_num,
    invf.week_num,
    invf.rp_ind,
    invf.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    COALESCE(invf.inventory_eoh_total_retail_amt_ty, 0) AS inventory_eoh_total_retail_amt_ty,
    COALESCE(invf.inventory_eoh_total_units_ty, 0) AS inventory_eoh_total_units_ty,
    COALESCE(invf.inventory_eoh_total_cost_amt_ty, 0) AS inventory_eoh_total_cost_amt_ty,
    COALESCE(invf.inventory_eoh_total_reg_retail_amt_ty, 0) AS inventory_eoh_total_reg_retail_amt_ty,
    COALESCE(invf.inventory_eoh_total_reg_units_ty, 0) AS inventory_eoh_total_reg_units_ty,
    COALESCE(invf.inventory_eoh_total_reg_cost_amt_ty, 0) AS inventory_eoh_total_reg_cost_amt_ty,
    COALESCE(invf.inventory_eoh_total_clr_retail_amt_ty, 0) AS inventory_eoh_total_clr_retail_amt_ty,
    COALESCE(invf.inventory_eoh_total_clr_units_ty, 0) AS inventory_eoh_total_clr_units_ty,
    COALESCE(invf.inventory_eoh_total_clr_cost_amt_ty, 0) AS inventory_eoh_total_clr_cost_amt_ty,
    COALESCE(invf.inventory_eoh_in_transit_total_units_ty, 0) AS inventory_eoh_in_transit_total_units_ty,
    COALESCE(invf.inventory_eoh_in_transit_total_cost_amt_ty, 0) AS inventory_eoh_in_transit_total_cost_amt_ty,
    COALESCE(invf.inventory_eoh_in_transit_total_retail_amt_ty, 0) AS inventory_eoh_in_transit_total_retail_amt_ty,
    COALESCE(invf.inventory_boh_total_units_ty, 0) AS inventory_boh_total_units_ty,
    COALESCE(invf.inventory_boh_total_retail_amt_ty, 0) AS inventory_boh_total_retail_amt_ty,
    COALESCE(invf.inventory_boh_total_cost_amt_ty, 0) AS inventory_boh_total_cost_amt_ty,
    COALESCE(invf.inventory_boh_in_transit_total_units_ty, 0) AS inventory_boh_in_transit_total_units_ty,
    COALESCE(invf.inventory_boh_in_transit_total_cost_amt_ty, 0) AS inventory_boh_in_transit_total_cost_amt_ty,
    COALESCE(invf.inventory_boh_in_transit_total_retail_amt_ty, 0) AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM inventory_fact AS invf
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s1 ON invf.store_num = CAST(SUBSTR(CAST(s1.store_num AS STRING), 1, 16) AS
      FLOAT64)
   UNION ALL
   SELECT SUBSTR(rs.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s2.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s2.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(rs.store_num AS STRING), 1, 16)
     END AS store_num,
    rs.week_num,
    rs.rp_ind,
    rs.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    COALESCE(rs.reservestock_transfer_in_retail, 0) AS reservestock_transfer_in_retail,
    COALESCE(rs.reservestock_transfer_in_units, 0) AS reservestock_transfer_in_units,
    COALESCE(rs.reservestock_transfer_in_cost, 0) AS reservestock_transfer_in_cost,
    COALESCE(rs.reservestock_transfer_out_retail, 0) AS reservestock_transfer_out_retail,
    COALESCE(rs.reservestock_transfer_out_units, 0) AS reservestock_transfer_out_units,
    COALESCE(rs.reservestock_transfer_out_cost, 0) AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM rs_fact AS rs
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s2 ON rs.store_num = CAST(SUBSTR(CAST(s2.store_num AS STRING), 1, 16) AS
      FLOAT64)
   UNION ALL
   SELECT SUBSTR(df.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s3.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s3.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(df.store_num AS STRING), 1, 16)
     END AS store_num,
    df.week_num,
    df.rp_ind,
    df.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    COALESCE(df.jwn_demand_total_retail_amt_ty, 0) AS jwn_demand_total_retail_amt_ty,
    COALESCE(df.jwn_demand_total_units_ty, 0) AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM demand_fact AS df
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s3 ON df.store_num = CAST(SUBSTR(CAST(s3.store_num AS STRING), 1, 16) AS
      FLOAT64)
   UNION ALL
   SELECT ff.rms_sku_num,
     CASE
     WHEN s4.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s4.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(ff.location_id, 1, 16)
     END AS store_num,
    ff.wk_idnt AS week_num,
    RPAD(ff.rp_ind, 1, ' ') AS rp_ind,
    ff.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    COALESCE(ff.fcst_0wk_dmnd_units, 0) AS fcst_0wk_dmnd_units,
    COALESCE(ff.fcst_1wk_dmnd_units, 0) AS fcst_1wk_dmnd_units,
    COALESCE(ff.fcst_2wk_dmnd_units, 0) AS fcst_2wk_dmnd_units,
    COALESCE(ff.fcst_3wk_dmnd_units, 0) AS fcst_3wk_dmnd_units,
    COALESCE(ff.fcst_4wk_dmnd_units, 0) AS fcst_4wk_dmnd_units,
    COALESCE(ff.fcst_5wk_dmnd_units, 0) AS fcst_5wk_dmnd_units,
    COALESCE(ff.fcst_6wk_dmnd_units, 0) AS fcst_6wk_dmnd_units,
    COALESCE(ff.fcst_7wk_dmnd_units, 0) AS fcst_7wk_dmnd_units,
    COALESCE(ff.fcst_8wk_dmnd_units, 0) AS fcst_8wk_dmnd_units,
    COALESCE(ff.fcst_9wk_dmnd_units, 0) AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM forecast_fact AS ff
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s4 ON LOWER(ff.location_id) = LOWER(SUBSTR(CAST(s4.store_num AS STRING), 1,
       16))
   UNION ALL
   SELECT SUBSTR(oof.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s5.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s5.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(oof.store_num AS STRING), 1, 16)
     END AS store_num,
    oof.week_num,
    oof.rp_ind,
    oof.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    COALESCE(oof.on_order_total_units, 0) AS on_order_total_units,
    COALESCE(oof.on_order_total_retail_dollars, 0) AS on_order_total_retail_dollars,
    COALESCE(oof.on_order_total_cost_dollars, 0) AS on_order_total_cost_dollars,
    COALESCE(oof.on_order_0wk_units, 0) AS on_order_0wk_units,
    COALESCE(oof.on_order_1wk_units, 0) AS on_order_1wk_units,
    COALESCE(oof.on_order_2wk_units, 0) AS on_order_2wk_units,
    COALESCE(oof.on_order_3wk_units, 0) AS on_order_3wk_units,
    COALESCE(oof.on_order_4wk_units, 0) AS on_order_4wk_units,
    COALESCE(oof.on_order_0wk_retail_dollars, 0) AS on_order_0wk_retail_dollars,
    COALESCE(oof.on_order_1wk_retail_dollars, 0) AS on_order_1wk_retail_dollars,
    COALESCE(oof.on_order_2wk_retail_dollars, 0) AS on_order_2wk_retail_dollars,
    COALESCE(oof.on_order_3wk_retail_dollars, 0) AS on_order_3wk_retail_dollars,
    COALESCE(oof.on_order_4wk_retail_dollars, 0) AS on_order_4wk_retail_dollars,
    COALESCE(oof.on_order_0wk_cost_dollars, 0) AS on_order_0wk_cost_dollars,
    COALESCE(oof.on_order_1wk_cost_dollars, 0) AS on_order_1wk_cost_dollars,
    COALESCE(oof.on_order_2wk_cost_dollars, 0) AS on_order_2wk_cost_dollars,
    COALESCE(oof.on_order_3wk_cost_dollars, 0) AS on_order_3wk_cost_dollars,
    COALESCE(oof.on_order_4wk_cost_dollars, 0) AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    0 AS f_i_eoh_in_stock_ind,
    0 AS days_in_stock
   FROM oo_fact AS oof
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s5 ON oof.store_num = CAST(SUBSTR(CAST(s5.store_num AS STRING), 1, 16) AS
      FLOAT64)
   UNION ALL
   SELECT SUBSTR(sf0.rms_sku_num, 1, 20) AS rms_sku_num,
     CASE
     WHEN s6.channel_num = 120
     THEN TRIM(FORMAT('%6d', 808))
     WHEN s6.channel_num = 250
     THEN TRIM(FORMAT('%6d', 828))
     ELSE SUBSTR(CAST(sf0.store_num AS STRING), 1, 16)
     END AS store_num,
    sf0.week_num,
    sf0.rp_ind,
    sf0.rp_status_flag,
    0 AS jwn_returns_total_units_ty,
    0 AS jwn_returns_total_retail_amt_ty,
    0 AS jwn_returns_total_cost_amt_ty,
    0 AS jwn_operational_gmv_total_retail_amt_ty,
    0 AS sales_4wk_retl_dollars,
    0 AS sales_lwk_retl_dollars,
    0 AS sales_wtd_retl_dollars,
    0 AS jwn_operational_gmv_total_units_ty,
    0 AS sales_4wk_units,
    0 AS sales_lwk_units,
    0 AS sales_wtd_units,
    0 AS jwn_operational_gmv_total_cost_amt_ty,
    0 AS sales_4wk_cost_amt,
    0 AS sales_lwk_cost_amt,
    0 AS sales_wtd_cost_amt,
    0 AS receipts_total_units_ty,
    0 AS receipts_total_retail_amt_ty,
    0 AS receipts_total_cost_amt_ty,
    0 AS receipts_po_units_ty,
    0 AS receipts_po_retail_amt_ty,
    0 AS receipts_po_cost_amt_ty,
    0 AS inventory_eoh_total_retail_amt_ty,
    0 AS inventory_eoh_total_units_ty,
    0 AS inventory_eoh_total_cost_amt_ty,
    0 AS inventory_eoh_total_reg_retail_amt_ty,
    0 AS inventory_eoh_total_reg_units_ty,
    0 AS inventory_eoh_total_reg_cost_amt_ty,
    0 AS inventory_eoh_total_clr_retail_amt_ty,
    0 AS inventory_eoh_total_clr_units_ty,
    0 AS inventory_eoh_total_clr_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_units_ty,
    0 AS inventory_eoh_in_transit_total_cost_amt_ty,
    0 AS inventory_eoh_in_transit_total_retail_amt_ty,
    0 AS inventory_boh_total_units_ty,
    0 AS inventory_boh_total_retail_amt_ty,
    0 AS inventory_boh_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_units_ty,
    0 AS inventory_boh_in_transit_total_cost_amt_ty,
    0 AS inventory_boh_in_transit_total_retail_amt_ty,
    0 AS reservestock_transfer_in_retail,
    0 AS reservestock_transfer_in_units,
    0 AS reservestock_transfer_in_cost,
    0 AS reservestock_transfer_out_retail,
    0 AS reservestock_transfer_out_units,
    0 AS reservestock_transfer_out_cost,
    0 AS jwn_demand_total_retail_amt_ty,
    0 AS jwn_demand_total_units_ty,
    0 AS on_order_total_units,
    0 AS on_order_total_retail_dollars,
    0 AS on_order_total_cost_dollars,
    0 AS on_order_0wk_units,
    0 AS on_order_1wk_units,
    0 AS on_order_2wk_units,
    0 AS on_order_3wk_units,
    0 AS on_order_4wk_units,
    0 AS on_order_0wk_retail_dollars,
    0 AS on_order_1wk_retail_dollars,
    0 AS on_order_2wk_retail_dollars,
    0 AS on_order_3wk_retail_dollars,
    0 AS on_order_4wk_retail_dollars,
    0 AS on_order_0wk_cost_dollars,
    0 AS on_order_1wk_cost_dollars,
    0 AS on_order_2wk_cost_dollars,
    0 AS on_order_3wk_cost_dollars,
    0 AS on_order_4wk_cost_dollars,
    0 AS fcst_0wk_dmnd_units,
    0 AS fcst_1wk_dmnd_units,
    0 AS fcst_2wk_dmnd_units,
    0 AS fcst_3wk_dmnd_units,
    0 AS fcst_4wk_dmnd_units,
    0 AS fcst_5wk_dmnd_units,
    0 AS fcst_6wk_dmnd_units,
    0 AS fcst_7wk_dmnd_units,
    0 AS fcst_8wk_dmnd_units,
    0 AS fcst_9wk_dmnd_units,
    sf0.f_i_eoh_in_stock_ind,
    sf0.days_in_stock
   FROM stock_status_fact AS sf0
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s6 ON sf0.store_num = CAST(SUBSTR(CAST(s6.store_num AS STRING), 1, 16) AS
      FLOAT64)) AS factvw
GROUP BY rms_sku_num,
 store_num,
 week_num,
 rp_ind,
 rp_status_flag;


--COLLECT STATS PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM,WEEK_NUM) ON RP_PERFORMANCE_FACT


CREATE TEMPORARY TABLE IF NOT EXISTS rp_performance_fact_dim
AS
SELECT DISTINCT f.rms_sku_num AS sku_idnt,
 f.store_num,
 f.week_num AS wk_idnt,
  CASE
  WHEN LOWER(f.rp_ind) = LOWER('Y')
  THEN 1
  WHEN LOWER(f.rp_ind) = LOWER('N')
  THEN 0
  ELSE NULL
  END AS rp_product_ind,
 f.rp_status_flag,
 TRIM(concat(p.class_num, ', ', p.class_desc), ' ') AS class_label,
 TRIM(coalesce(p.color_num, p.supp_color), ' ') AS color_num,
 TRIM(concat(p.color_num, ', ', p.supp_color), ' ') AS color_label,
 TRIM(concat(p.dept_num, ', ', p.dept_desc), ' ') AS dept_label,
 p.div_desc AS div_desc,
 TRIM(concat(p.div_num, ', ', p.div_desc), ' ') AS div_label,
 TRIM(concat(p.sbclass_num, ', ', p.sbclass_desc), ' ') AS subclass_label,
 sbclass.sbclass_category AS subclass_category,
 TRIM(concat(p.grp_num, ', ', p.grp_desc), ' ') AS sdiv_label,
 p.size_1_num,
 p.size_2_desc,
 p.size_2_num,
 p.supp_color AS supplier_color,
 p.supp_part_num AS vpn,
 p.supp_size AS supplier_size,
 TRIM(CONCAT(p.supp_part_num, ', ', p.style_desc)) AS vpn_label,
 p.rms_style_num AS style_num,
 p.style_group_num,
 p.npg_ind,
 p.order_quantity_multiple AS order_pack_size,
 trim(concat(s.subgroup_num, ', ', s.subgroup_desc), ' ') AS regn_label,
 s.channel_desc AS channel_desc,
 trim(concat(s.channel_num, ', ', s.channel_desc), ' ') AS channel_label,
 trim(concat(s.region_num, ', ', s.region_desc), ' ') AS distt_label,
 trim(concat(s.store_num, ', ', s.store_short_name), ' ') AS loc_label,
 s.comp_status_desc AS store_status,
 v.vendor_name AS supplier_name,
 psd.vendor_label_name,
 c.peer_group_desc AS cluster_climate,
 f.jwn_returns_total_units_ty AS return_total_units,
 f.jwn_returns_total_retail_amt_ty AS return_total_retl_dollars,
 f.jwn_returns_total_cost_amt_ty AS return_total_cost,
 f.jwn_operational_gmv_total_retail_amt_ty AS net_sales_total_retl_dollars,
 f.sales_4wk_retl_dollars,
 f.sales_lwk_retl_dollars,
 f.sales_wtd_retl_dollars,
 f.jwn_operational_gmv_total_units_ty AS net_sales_total_units,
 f.sales_4wk_units,
 f.sales_lwk_units,
 f.sales_wtd_units,
 f.jwn_operational_gmv_total_cost_amt_ty AS net_sales_total_cost,
 f.sales_4wk_cost_amt,
 f.sales_lwk_cost_amt,
 f.sales_wtd_cost_amt,
 f.receipts_total_units_ty AS receipt_total_units,
 f.receipts_total_retail_amt_ty AS receipt_total_retl_dollars,
 f.receipts_total_cost_amt_ty AS receipt_total_cost,
 f.receipts_po_units_ty AS receipt_total_po_units,
 f.receipts_po_retail_amt_ty AS receipt_total_po_retl_dollars,
 f.receipts_po_cost_amt_ty AS receipt_total_po_cost,
 f.inventory_eoh_total_retail_amt_ty AS eoh_total_retl_dollars,
 f.inventory_eoh_total_units_ty AS eoh_total_units,
 f.inventory_eoh_total_cost_amt_ty AS eoh_total_cost,
 f.inventory_eoh_total_reg_retail_amt_ty AS eoh_total_reg_retl_dollars,
 f.inventory_eoh_total_reg_units_ty AS eoh_total_reg_units,
 f.inventory_eoh_total_reg_cost_amt_ty AS eoh_total_reg_cost,
 f.inventory_eoh_total_clr_retail_amt_ty AS eoh_total_clr_retl_dollars,
 f.inventory_eoh_total_clr_units_ty AS eoh_total_clr_units,
 f.inventory_eoh_total_clr_cost_amt_ty AS eoh_total_clr_cost,
 f.inventory_eoh_in_transit_total_units_ty AS eoh_in_trnst_total_units,
 f.inventory_eoh_in_transit_total_cost_amt_ty AS eoh_in_trnst_total_cost,
 f.inventory_eoh_in_transit_total_retail_amt_ty AS eoh_in_trnst_total_retail_dollars,
 f.inventory_boh_total_units_ty AS boh_total_units,
 f.inventory_boh_total_retail_amt_ty AS boh_total_retail_dollars,
 f.inventory_boh_total_cost_amt_ty AS boh_total_cost_dollars,
 f.inventory_boh_in_transit_total_units_ty AS boh_in_transit_total_units,
 f.inventory_boh_in_transit_total_cost_amt_ty AS boh_in_transit_total_cost_dollars,
 f.inventory_boh_in_transit_total_retail_amt_ty AS boh_in_transit_total_retail_dollars,
 f.reservestock_transfer_in_retail AS reserve_stock_transfer_in_retail,
 f.reservestock_transfer_in_units AS reserve_stock_transfer_in_units,
 f.reservestock_transfer_in_cost AS reserve_stock_transfer_in_cost,
 f.reservestock_transfer_out_retail AS reserve_stock_transfer_out_retail,
 f.reservestock_transfer_out_units AS reserve_stock_transfer_out_units,
 f.reservestock_transfer_out_cost AS reserve_stock_transfer_out_cost,
 f.jwn_demand_total_retail_amt_ty AS demand_total_retl_dollars,
 f.jwn_demand_total_units_ty AS demand_total_units,
 f.on_order_total_units,
 f.on_order_total_retail_dollars,
 f.on_order_total_cost_dollars,
 f.on_order_0wk_units,
 f.on_order_1wk_units,
 f.on_order_2wk_units,
 f.on_order_3wk_units,
 f.on_order_4wk_units,
 f.on_order_0wk_retail_dollars,
 f.on_order_1wk_retail_dollars,
 f.on_order_2wk_retail_dollars,
 f.on_order_3wk_retail_dollars,
 f.on_order_4wk_retail_dollars,
 f.on_order_0wk_cost_dollars,
 f.on_order_1wk_cost_dollars,
 f.on_order_2wk_cost_dollars,
 f.on_order_3wk_cost_dollars,
 f.on_order_4wk_cost_dollars,
 f.fcst_0wk_dmnd_units,
 f.fcst_1wk_dmnd_units,
 f.fcst_2wk_dmnd_units,
 f.fcst_3wk_dmnd_units,
 f.fcst_4wk_dmnd_units,
 f.fcst_5wk_dmnd_units,
 f.fcst_6wk_dmnd_units,
 f.fcst_7wk_dmnd_units,
 f.fcst_8wk_dmnd_units,
 f.fcst_9wk_dmnd_units,
 f.f_i_eoh_in_stock_ind AS in_stock_ind,
 f.days_in_stock
FROM rp_performance_fact AS f
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p ON LOWER(f.rms_sku_num) = LOWER(p.rms_sku_num) AND LOWER(p.channel_country
    ) = LOWER('US')
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim AS psd ON p.epm_style_num = psd.epm_style_num AND LOWER(psd.channel_country
     ) = LOWER(p.channel_country) AND LOWER(psd.prmy_supp_num) = LOWER(p.prmy_supp_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(p.prmy_supp_num) = LOWER(v.vendor_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s ON LOWER(f.store_num) = LOWER(SUBSTR(CAST(s.store_num AS STRING), 1, 16)) AND
    LOWER(s.store_country_code) = LOWER('US') AND LOWER(s.store_abbrev_name) <> LOWER('CLOSED')
 LEFT JOIN climate_cluster_dim AS c ON s.store_num = c.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_product_subclass_dim_vw AS sbclass ON p.div_num = sbclass.division_num AND p.grp_num =
      sbclass.subdivision_num AND p.dept_num = sbclass.dept_num AND p.class_num = sbclass.class_num AND p.sbclass_num =
   sbclass.sbclass_num
WHERE LOWER(UPPER(p.div_desc)) NOT LIKE LOWER('%INACT%')
 AND LOWER(UPPER(p.grp_desc)) NOT LIKE LOWER('%INACT%');


--COLLECT STATS PRIMARY INDEX(SKU_IDNT,STORE_NUM,WK_IDNT) ON RP_PERFORMANCE_FACT_DIM


--{env_suffix}


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.rp_performance_rollup{{params.env_suffix}};


--{env_suffix}
INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.rp_performance_rollup{{params.env_suffix}} (sku_idnt, store_num, wk_idnt, rp_product_ind 
,rp_status_flag
 , class_label, color_num, color_label, dept_label, div_desc, div_label, subclass_label, subclass_category, sdiv_label,
 size_1_num, size_2_desc, size_2_num, supplier_color, vpn, supplier_size, vpn_label, style_num, npg_ind, order_pack_size
 , style_group_num, regn_label, channel_desc, channel_label, distt_label, loc_label, store_status, supplier_name,
 vendor_label_name, cluster_climate, return_total_units, return_total_retl_dollars, return_total_cost,
 net_sales_total_retl_dollars, sales_4wk_retl_dollars, sales_lwk_retl_dollars, sales_wtd_retl_dollars,
 net_sales_total_units, sales_4wk_units, sales_lwk_units, sales_wtd_units, net_sales_total_cost, sales_4wk_cost_amt,
 sales_lwk_cost_amt, sales_wtd_cost_amt, receipt_total_units, receipt_total_retl_dollars, receipt_total_cost,
 receipt_total_po_units, receipt_total_po_retl_dollars, receipt_total_po_cost, eoh_total_retl_dollars, eoh_total_units,
 eoh_total_cost, eoh_total_reg_retl_dollars, eoh_total_reg_units, eoh_total_reg_cost, eoh_total_clr_retl_dollars,
 eoh_total_clr_units, eoh_total_clr_cost, eoh_in_trnst_total_units, eoh_in_trnst_total_cost,
 eoh_in_trnst_total_retail_dollars, boh_total_units, boh_total_retail_dollars, boh_total_cost_dollars,
 boh_in_transit_total_units, boh_in_transit_total_cost_dollars, boh_in_transit_total_retail_dollars,
 reserve_stock_transfer_in_retail, reserve_stock_transfer_in_units, reserve_stock_transfer_in_cost,
 reserve_stock_transfer_out_retail, reserve_stock_transfer_out_units, reserve_stock_transfer_out_cost,
 demand_total_retl_dollars, demand_total_units, on_order_total_units, on_order_total_retail_dollars,
 on_order_total_cost_dollars, on_order_0wk_units, on_order_1wk_units, on_order_2wk_units, on_order_3wk_units,
 on_order_4wk_units, on_order_0wk_retail_dollars, on_order_1wk_retail_dollars, on_order_2wk_retail_dollars,
 on_order_3wk_retail_dollars, on_order_4wk_retail_dollars, on_order_0wk_cost_dollars, on_order_1wk_cost_dollars,
 on_order_2wk_cost_dollars, on_order_3wk_cost_dollars, on_order_4wk_cost_dollars, fcst_0wk_dmnd_units,
 fcst_1wk_dmnd_units, fcst_2wk_dmnd_units, fcst_3wk_dmnd_units, fcst_4wk_dmnd_units, fcst_5wk_dmnd_units,
 fcst_6wk_dmnd_units, fcst_7wk_dmnd_units, fcst_8wk_dmnd_units, fcst_9wk_dmnd_units, in_stock_ind, days_in_stock,
 dw_sys_load_tmstp)
(SELECT sku_idnt,
  store_num,
  wk_idnt,
  rp_product_ind,
  rp_status_flag,
  class_label,
  color_num,
  color_label,
  dept_label,
  div_desc,
  div_label,
  subclass_label,
  subclass_category,
  sdiv_label,
  size_1_num,
  size_2_desc,
  size_2_num,
  supplier_color,
  vpn,
  supplier_size,
  vpn_label,
  style_num,
  npg_ind,
  order_pack_size,
  style_group_num,
  regn_label,
  channel_desc,
  channel_label,
  distt_label,
  loc_label,
  store_status,
  supplier_name,
  vendor_label_name,
  cluster_climate,
  return_total_units,
  return_total_retl_dollars,
  return_total_cost,
  net_sales_total_retl_dollars,
  sales_4wk_retl_dollars,
  sales_lwk_retl_dollars,
  sales_wtd_retl_dollars,
  net_sales_total_units,
  sales_4wk_units,
  sales_lwk_units,
  sales_wtd_units,
  net_sales_total_cost,
  sales_4wk_cost_amt,
  sales_lwk_cost_amt,
  sales_wtd_cost_amt,
  receipt_total_units,
  receipt_total_retl_dollars,
  receipt_total_cost,
  receipt_total_po_units,
  receipt_total_po_retl_dollars,
  receipt_total_po_cost,
  eoh_total_retl_dollars,
  eoh_total_units,
  eoh_total_cost,
  eoh_total_reg_retl_dollars,
  eoh_total_reg_units,
  eoh_total_reg_cost,
  eoh_total_clr_retl_dollars,
  eoh_total_clr_units,
  eoh_total_clr_cost,
  eoh_in_trnst_total_units,
  eoh_in_trnst_total_cost,
  eoh_in_trnst_total_retail_dollars,
  boh_total_units,
  cast(boh_total_retail_dollars as numeric),
  cast(boh_total_cost_dollars as numeric),
  boh_in_transit_total_units,
  cast(boh_in_transit_total_cost_dollars as numeric),
  cast(boh_in_transit_total_retail_dollars as numeric),
  cast(reserve_stock_transfer_in_retail as numeric),
  reserve_stock_transfer_in_units,
  cast(reserve_stock_transfer_in_cost as numeric),
  cast(reserve_stock_transfer_out_retail as numeric),
  cast(reserve_stock_transfer_out_units as numeric),
  cast(reserve_stock_transfer_out_cost as numeric),
  demand_total_retl_dollars,
  demand_total_units,
  on_order_total_units,
  cast(on_order_total_retail_dollars as numeric),
  cast(on_order_total_cost_dollars as numeric),
  on_order_0wk_units,
  on_order_1wk_units,
  on_order_2wk_units,
  on_order_3wk_units,
  on_order_4wk_units,
  cast(on_order_0wk_retail_dollars as numeric),
  cast(on_order_1wk_retail_dollars as numeric),
  cast(on_order_2wk_retail_dollars as numeric),
  cast(on_order_3wk_retail_dollars as numeric),
  cast(on_order_4wk_retail_dollars as numeric),
  cast(on_order_0wk_cost_dollars as numeric),
  cast(on_order_1wk_cost_dollars as numeric),
  cast(on_order_2wk_cost_dollars as numeric),
  cast(on_order_3wk_cost_dollars as numeric),
  cast(on_order_4wk_cost_dollars as numeric),
  fcst_0wk_dmnd_units,
  fcst_1wk_dmnd_units,
  fcst_2wk_dmnd_units,
  fcst_3wk_dmnd_units,
  fcst_4wk_dmnd_units,
  fcst_5wk_dmnd_units,
  fcst_6wk_dmnd_units,
  fcst_7wk_dmnd_units,
  fcst_8wk_dmnd_units,
  fcst_9wk_dmnd_units,
  in_stock_ind,
  days_in_stock,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM rp_performance_fact_dim);




--{env_suffix}


--COLLECT STATS PRIMARY INDEX(SKU_IDNT,STORE_NUM,WK_IDNT) ON T2DL_DAS_SELECTION.RP_PERFORMANCE_ROLLUP  