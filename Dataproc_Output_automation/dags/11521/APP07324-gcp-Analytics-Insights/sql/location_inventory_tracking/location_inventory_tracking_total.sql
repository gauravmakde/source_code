-- BEGIN
-- DECLARE _ERROR_CODE INT64;
-- DECLARE _ERROR_MESSAGE STRING;
-- /*SET QUERY_BAND = 'App_ID=APP09117;
-- DAG_ID="location_inventory_tracking_total_11521_ACE_ENG";
-- ---     Task_Name=location_inventory_tracking_total;'*/
-- ---     FOR SESSION VOLATILE;
-- BEGIN

-- SET _ERROR_CODE  =  0;
CREATE  TEMPORARY TABLE IF NOT EXISTS  month_range AS (
WITH distinct_month as (SELECT distinct month_idnt FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim),
month_list as (
SELECT month_idnt
,ROW_NUMBER() OVER (ORDER BY month_idnt asc) as month_order
,(SELECT distinct month_idnt FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE('PST8PDT')) as current_month
FROM distinct_month)
SELECT month_idnt FROM month_list m WHERE m.month_order BETWEEN (SELECT distinct month_order FROM month_list WHERE month_idnt = current_month) - 1 AND (SELECT distinct month_order FROM month_list WHERE month_idnt = current_month) + 5
);



CREATE  TEMPORARY TABLE IF NOT EXISTS  location_plans AS WITH mth AS (SELECT m.month_idnt,
    CASE
    WHEN m.month_idnt = (SELECT DISTINCT month_idnt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal
      WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
    THEN 1
    ELSE 0
    END AS current_month_flag,
   cal.month_desc,
   cal.month_label,
   cal.fiscal_month_num,
   cal.month_start_day_date,
   `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(month_start_day_date as string)) as month_start_day_date_tz,
   cal.month_end_day_date,
   `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(month_end_day_date as string)) as month_end_day_date_tz
  FROM month_range AS m
   LEFT JOIN (SELECT DISTINCT month_idnt,
     month_desc,
     month_label,
     fiscal_month_num,
     month_start_day_date,
     month_end_day_date
      
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS cal ON m.month_idnt = cal.month_idnt) 
    
  (SELECT REPLACE(CONCAT(plan.class_idnt
     , plan.dept_idnt, mth.month_idnt, plan.loc_idnt), ' ', '') AS pkey,
   REPLACE(CONCAT(plan.chnl_idnt, plan.class_idnt, plan.dept_idnt, mth.month_idnt), ' ', '') AS ab_key,
   mth.month_idnt,
   mth.month_desc,
   mth.month_label,
   mth.fiscal_month_num,
   mth.month_start_day_date,
   mth.month_start_day_date_tz,
   mth.month_end_day_date,
   mth.month_end_day_date_tz,
    CASE
    WHEN CAST(trunc(mth.fiscal_month_num )AS INTEGER) = 1
    THEN 4
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 2
    THEN 5
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 3
    THEN 4
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 4
    THEN 4
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 5
    THEN 5
    WHEN CAST(trunc(mth.fiscal_month_num )AS INTEGER) = 6
    THEN 4
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 7
    THEN 4
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 8
    THEN 5
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 9
    THEN 4
    WHEN CAST(trunc (mth.fiscal_month_num) AS INTEGER) = 10
    THEN 4
    WHEN CAST(trunc(mth.fiscal_month_num) AS INTEGER) = 11
    THEN 5
    WHEN CAST(trunc(mth.fiscal_month_num )AS INTEGER) = 12
    THEN 4
    ELSE NULL
    END AS wks_in_month,
   TRIM(t0.div) AS division,
   TRIM(t0.subdiv) AS subdivision,
   TRIM(t0.dept_label) AS dept_label,
   TRIM(t0.class_label) AS class_label,
   plan.chnl_idnt,
   plan.dept_idnt,
   plan.class_idnt,
   plan.loc_idnt,
   mth.current_month_flag,
    CASE
    WHEN mth.month_idnt = (SELECT MIN(month_idnt)
      FROM (SELECT m1.month_idnt
         FROM month_range AS m1
          LEFT JOIN (SELECT DISTINCT month_idnt,
            month_desc,
            month_label,
            fiscal_month_num,
            month_start_day_date,

            month_end_day_date,

       

           FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim ) AS cal ON m1.month_idnt = cal.month_idnt))
    THEN 1
    ELSE 0
    END AS last_month_flag,
   SUM(plan.bop_plan) AS bop_plan,
   SUM(plan.rcpt_plan) AS rcpt_plan,
   SUM(plan.sales_plan) AS sales_plan,
   SUM(plan.uncapped_bop) AS uncapped_bop
  FROM `{{params.gcp_project_id}}`.t2dl_das_location_planning.loc_plan_prd_vw AS plan
   LEFT JOIN (SELECT DISTINCT dept_num,
     class_num,
       FORMAT('%11d', division_num) || ', ' || division_name AS div,
       FORMAT('%11d', subdivision_num) || ', ' || subdivision_name AS subdiv,
       FORMAT('%11d', dept_num) || ', ' || dept_name AS dept_label,
       FORMAT('%11d', class_num) || ', ' || class_name AS class_label
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_class_subclass_dim AS dept) AS t0 ON plan.dept_idnt = t0.dept_num AND plan.class_idnt
      = t0.class_num
   LEFT JOIN mth AS mth ON plan.mth_idnt = mth.month_idnt
  WHERE plan.chnl_idnt IN (110, 210)
   AND plan.mth_idnt IN (SELECT month_idnt
     FROM (SELECT DISTINCT month_idnt
       FROM mth) AS t15)
   AND plan.loc_idnt NOT IN (210, 212)
  GROUP BY pkey,
   ab_key,
   mth.month_idnt,
   mth.month_desc,
   mth.month_label,
   mth.fiscal_month_num,
   mth.month_start_day_date,
   month_start_day_date_tz ,
   mth.month_end_day_date,
   mth.month_end_day_date_tz,
   wks_in_month,
   division,
   subdivision,
   dept_label,
   class_label,
   plan.chnl_idnt,
   plan.dept_idnt,
   plan.class_idnt,
   plan.loc_idnt,
   mth.current_month_flag);

--COLLECT STATS PRIMARY INDEX(pkey) ,COLUMN (ab_key)  ON location_plans;

CREATE TEMPORARY TABLE IF NOT EXISTS store_climate

AS
SELECT store_num,
 peer_group_type_desc,
 peer_group_desc AS climate
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_peer_group_dim
WHERE LOWER(peer_group_type_code) IN (LOWER('FPC'), LOWER('OPC'));



CREATE  TEMPORARY TABLE IF NOT EXISTS new_store_list AS (
WITH m as (SELECT DISTINCT Month_IDNT
FROM location_plans)
SELECT s.store_num, s.store_open_date, 
       c.month_idnt AS open_month
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim s
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim c
ON s.store_open_date = c.day_date
WHERE c.month_idnt IN (SELECT DISTINCT month_idnt FROM m)
);




CREATE TEMPORARY TABLE IF NOT EXISTS dma_data

AS
SELECT DISTINCT dma.store_num,
 dma.store_dma_code,
 mkt.dma_desc,
 TRIM(FORMAT('%11d', dma.store_num) || ', ' || dma.store_short_name) AS location,
 dma.store_type_desc,
 dma.gross_square_footage,
 dma.store_open_date,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(store_open_date as string)) as store_open_date_tz ,
 dma.store_close_date,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(store_close_date as string)) as store_close_date_tz,
dma.region_desc,
 dma.region_medium_desc,
 dma.region_short_desc,
 dma.business_unit_desc,
 dma.group_desc,
 dma.subgroup_desc,
 dma.subgroup_medium_desc,
 dma.subgroup_short_desc,
 dma.store_address_line_1,
 dma.store_address_city,
 dma.store_address_state,
 dma.store_address_state_name,
 dma.store_postal_code,
 dma.store_address_county,
 dma.store_country_code,
 dma.store_country_name,
 dma.store_location_latitude,
 dma.store_location_longitude,
 dma.distribution_center_num,
 dma.distribution_center_name,
 dma.channel_desc,
 dma.comp_status_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS dma
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_dma AS mkt ON CAST(dma.store_dma_code AS FLOAT64) = mkt.dma_code;

--COLLECT STATS PRIMARY INDEX(store_num) ,COLUMN (store_num, dma_desc) ON dma_data;
-- total location plans as primary reference table

CREATE TEMPORARY TABLE IF NOT EXISTS location_plans_total
AS
SELECT l.pkey,
 l.ab_key,
 l.month_idnt,
 l.month_desc,
 l.month_label,
 l.fiscal_month_num,
 l.month_start_day_date,
 l.month_start_day_date_tz,

 l.month_end_day_date,
 l.month_end_day_date_tz,
 l.wks_in_month,
 l.division,
 l.subdivision,
 l.dept_label,
 l.class_label,
 l.chnl_idnt,
 l.dept_idnt,
 l.class_idnt,
 l.loc_idnt,
 l.current_month_flag,
 l.last_month_flag,
 l.bop_plan,
 l.rcpt_plan,
 l.sales_plan,
 l.uncapped_bop,
 c.peer_group_type_desc,
 c.climate,
 d.store_dma_code,
 d.dma_desc,
 d.location,
 d.store_type_desc,
 d.gross_square_footage,

 d.store_open_date,
 d.store_open_date_tz,

 d.store_close_date,
 d.store_close_date_tz,


 d.region_desc,
 d.region_medium_desc,
 d.region_short_desc,
 d.business_unit_desc,
 d.group_desc,
 d.subgroup_desc,
 d.subgroup_medium_desc,
 d.subgroup_short_desc,
 d.store_address_line_1,
 d.store_address_city,
 d.store_address_state,
 d.store_address_state_name,
 d.store_postal_code,
 d.store_address_county,
 d.store_country_code,
 d.store_country_name,
 d.store_location_latitude,
 d.store_location_longitude,
 d.distribution_center_num,
 d.distribution_center_name,
 d.channel_desc,
 d.comp_status_desc,
  CASE
  WHEN nsl.store_num IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_loc_flag
FROM location_plans AS l
 LEFT JOIN store_climate AS c ON l.loc_idnt = c.store_num
 LEFT JOIN dma_data AS d ON l.loc_idnt = d.store_num
 LEFT JOIN new_store_list AS nsl ON l.loc_idnt = nsl.store_num;

--COLLECT STATS PRIMARY INDEX(pkey) ,COLUMN (ab_key)  ,COLUMN (loc_idnt,store_dma_code) ,COLUMN (loc_idnt,climate) ON location_plans_total;
--- AB OO data, receipt units -- duplicates on npg_id and plan_type (NEED TO RESOLVE BEFORE INTEGRATION)



-- AB OO data, receipt units -- duplicates on npg_id and plan_type (NEED TO RESOLVE BEFORE INTEGRATION)
CREATE TEMPORARY TABLE IF NOT EXISTS ab_receipts
AS
SELECT REPLACE(CONCAT(org_id, class_id, dept_id, fiscal_month_id), ' ', '') AS pkey,
 channel,
 fiscal_month_id,
 class_id,
 class_name,
 dept_id,
 SUM(rcpt_units) AS ab_oo_rcpt_units
FROM t2dl_das_open_to_buy.ab_cm_orders_current
WHERE org_id IN (110, 210)
GROUP BY pkey,
 channel,
 fiscal_month_id,
 class_id,
 class_name,
 dept_id;


CREATE  TEMPORARY TABLE IF NOT EXISTS  boh_u AS WITH boh_u AS (SELECT sbclass_num,
   store_num,
   week_num,
   supp_part_num,
   class_num,
   dept_num,
   supp_num,
   vndr_label_code,
   business_unit_num,
   banner_country_num,
   channel_num,
   month_num,
   quarter_num,
   half_num,
   year_num,
   rp_ind,
   npg_ind,
   comp_status_code,
   store_country_code,
   jwn_demand_total_units_ty,
   jwn_demand_total_retail_amt_ty,
   jwn_demand_regular_units_ty,
   jwn_demand_regular_retail_amt_ty,
   jwn_demand_promo_units_ty,
   jwn_demand_promo_retail_amt_ty,
   jwn_demand_clearance_units_ty,
   jwn_demand_clearance_retail_amt_ty,
   jwn_demand_store_fulfilled_units_ty,
   jwn_demand_store_fulfilled_retail_amt_ty,
   jwn_demand_store_fulfilled_regular_units_ty,
   jwn_demand_store_fulfilled_regular_retail_amt_ty,
   jwn_demand_store_fulfilled_promo_units_ty,
   jwn_demand_store_fulfilled_promo_retail_amt_ty,
   jwn_demand_store_fulfilled_clearance_units_ty,
   jwn_demand_store_fulfilled_clearance_retail_amt_ty,
   jwn_demand_persistent_units_ty,
   jwn_demand_persistent_retail_amt_ty,
   jwn_demand_persistent_regular_units_ty,
   jwn_demand_persistent_regular_retail_amt_ty,
   jwn_demand_persistent_promo_units_ty,
   jwn_demand_persistent_promo_retail_amt_ty,
   jwn_demand_persistent_clearance_units_ty,
   jwn_demand_persistent_clearance_retail_amt_ty,
   jwn_demand_other_fulfilled_units_ty,
   jwn_demand_other_fulfilled_retail_amt_ty,
   jwn_demand_other_fulfilled_regular_units_ty,
   jwn_demand_other_fulfilled_regular_retail_amt_ty,
   jwn_demand_other_fulfilled_promo_units_ty,
   jwn_demand_other_fulfilled_promo_retail_amt_ty,
   jwn_demand_other_fulfilled_clearance_units_ty,
   jwn_demand_other_fulfilled_clearance_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_units_ty,
   jwn_demand_fulfillment_center_fulfilled_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_regular_units_ty,
   jwn_demand_fulfillment_center_fulfilled_regular_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_promo_units_ty,
   jwn_demand_fulfillment_center_fulfilled_promo_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_clearance_units_ty,
   jwn_demand_fulfillment_center_fulfilled_clearance_retail_amt_ty,
   jwn_demand_flash_units_ty,
   jwn_demand_flash_retail_amt_ty,
   jwn_demand_flash_regular_units_ty,
   jwn_demand_flash_regular_retail_amt_ty,
   jwn_demand_flash_promo_units_ty,
   jwn_demand_flash_promo_retail_amt_ty,
   jwn_demand_flash_clearance_units_ty,
   jwn_demand_flash_clearance_retail_amt_ty,
   jwn_demand_dropship_fulfilled_units_ty,
   jwn_demand_dropship_fulfilled_retail_amt_ty,
   jwn_demand_dropship_fulfilled_regular_units_ty,
   jwn_demand_dropship_fulfilled_regular_retail_amt_ty,
   jwn_demand_dropship_fulfilled_promo_units_ty,
   jwn_demand_dropship_fulfilled_promo_retail_amt_ty,
   jwn_demand_dropship_fulfilled_clearance_units_ty,
   jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,
   jwn_operational_gmv_total_units_ty,
   jwn_operational_gmv_total_retail_amt_ty,
   jwn_operational_gmv_total_cost_amt_ty,
   jwn_operational_gmv_regular_units_ty,
   jwn_operational_gmv_regular_retail_amt_ty,
   jwn_operational_gmv_regular_cost_amt_ty,
   jwn_operational_gmv_promo_units_ty,
   jwn_operational_gmv_promo_retail_amt_ty,
   jwn_operational_gmv_promo_cost_amt_ty,
   jwn_operational_gmv_clearance_units_ty,
   jwn_operational_gmv_clearance_retail_amt_ty,
   jwn_operational_gmv_clearance_cost_amt_ty,
   jwn_operational_gmv_persistent_units_ty,
   jwn_operational_gmv_persistent_retail_amt_ty,
   jwn_operational_gmv_persistent_cost_amt_ty,
   jwn_operational_gmv_persistent_regular_units_ty,
   jwn_operational_gmv_persistent_regular_retail_amt_ty,
   jwn_operational_gmv_persistent_regular_cost_amt_ty,
   jwn_operational_gmv_persistent_promo_units_ty,
   jwn_operational_gmv_persistent_promo_retail_amt_ty,
   jwn_operational_gmv_persistent_promo_cost_amt_ty,
   jwn_operational_gmv_persistent_clearance_units_ty,
   jwn_operational_gmv_persistent_clearance_retail_amt_ty,
   jwn_operational_gmv_persistent_clearance_cost_amt_ty,
   jwn_operational_gmv_flash_units_ty,
   jwn_operational_gmv_flash_retail_amt_ty,
   jwn_operational_gmv_flash_cost_amt_ty,
   jwn_operational_gmv_flash_regular_units_ty,
   jwn_operational_gmv_flash_regular_retail_amt_ty,
   jwn_operational_gmv_flash_regular_cost_amt_ty,
   jwn_operational_gmv_flash_promo_units_ty,
   jwn_operational_gmv_flash_promo_retail_amt_ty,
   jwn_operational_gmv_flash_promo_cost_amt_ty,
   jwn_operational_gmv_flash_clearance_units_ty,
   jwn_operational_gmv_flash_clearance_retail_amt_ty,
   jwn_operational_gmv_flash_clearance_cost_amt_ty,
   jwn_gross_sales_total_units_ty,
   jwn_gross_sales_total_retail_amt_ty,
   jwn_gross_sales_total_cost_amt_ty,
   jwn_gross_sales_regular_units_ty,
   jwn_gross_sales_regular_retail_amt_ty,
   jwn_gross_sales_regular_cost_amt_ty,
   jwn_gross_sales_promo_units_ty,
   jwn_gross_sales_promo_retail_amt_ty,
   jwn_gross_sales_promo_cost_amt_ty,
   jwn_gross_sales_clearance_units_ty,
   jwn_gross_sales_clearance_retail_amt_ty,
   jwn_gross_sales_clearance_cost_amt_ty,
   jwn_returns_total_units_ty,
   jwn_returns_total_retail_amt_ty,
   jwn_returns_total_cost_amt_ty,
   jwn_returns_regular_units_ty,
   jwn_returns_regular_retail_amt_ty,
   jwn_returns_regular_cost_amt_ty,
   jwn_returns_promo_units_ty,
   jwn_returns_promo_retail_amt_ty,
   jwn_returns_promo_cost_amt_ty,
   jwn_returns_clearance_units_ty,
   jwn_returns_clearance_retail_amt_ty,
   jwn_returns_clearance_cost_amt_ty,
   receipts_total_units_ty,
   receipts_total_retail_amt_ty,
   receipts_total_cost_amt_ty,
   receipts_po_units_ty,
   receipts_po_retail_amt_ty,
   receipts_po_cost_amt_ty,
   receipts_dropship_units_ty,
   receipts_dropship_retail_amt_ty,
   receipts_dropship_cost_amt_ty,
   transfer_in_pack_and_hold_units_ty,
   transfer_in_pack_and_hold_retail_amt_ty,
   transfer_in_pack_and_hold_cost_amt_ty,
   transfer_in_reserve_stock_units_ty,
   transfer_in_reserve_stock_retail_amt_ty,
   transfer_in_reserve_stock_cost_amt_ty,
   transfer_in_racking_units_ty,
   transfer_in_racking_retail_amt_ty,
   transfer_in_racking_cost_amt_ty,
   transfer_out_racking_units_ty,
   transfer_out_racking_retail_amt_ty,
   transfer_out_racking_cost_amt_ty,
   transfer_in_return_to_rack_units_ty,
   transfer_in_return_to_rack_retail_amt_ty,
   transfer_in_return_to_rack_cost_amt_ty,
   inventory_boh_total_units_ty,
   inventory_boh_total_retail_amt_ty,
   inventory_boh_total_cost_amt_ty,
   inventory_boh_regular_units_ty,
   inventory_boh_regular_retail_amt_ty,
   inventory_boh_regular_cost_amt_ty,
   inventory_boh_clearance_units_ty,
   inventory_boh_clearance_retail_amt_ty,
   inventory_boh_clearance_cost_amt_ty,
   inventory_eoh_total_units_ty,
   inventory_eoh_total_retail_amt_ty,
   inventory_eoh_total_cost_amt_ty,
   inventory_eoh_regular_units_ty,
   inventory_eoh_regular_retail_amt_ty,
   inventory_eoh_regular_cost_amt_ty,
   inventory_eoh_clearance_units_ty,
   inventory_eoh_clearance_retail_amt_ty,
   inventory_eoh_clearance_cost_amt_ty,
   rtv_total_units_ty,
   rtv_total_retail_amt_ty,
   rtv_total_cost_amt_ty,
   jwn_demand_total_units_ly,
   jwn_demand_total_retail_amt_ly,
   jwn_demand_regular_units_ly,
   jwn_demand_regular_retail_amt_ly,
   jwn_demand_promo_units_ly,
   jwn_demand_promo_retail_amt_ly,
   jwn_demand_clearance_units_ly,
   jwn_demand_clearance_retail_amt_ly,
   jwn_demand_store_fulfilled_units_ly,
   jwn_demand_store_fulfilled_retail_amt_ly,
   jwn_demand_store_fulfilled_regular_units_ly,
   jwn_demand_store_fulfilled_regular_retail_amt_ly,
   jwn_demand_store_fulfilled_promo_units_ly,
   jwn_demand_store_fulfilled_promo_retail_amt_ly,
   jwn_demand_store_fulfilled_clearance_units_ly,
   jwn_demand_store_fulfilled_clearance_retail_amt_ly,
   jwn_demand_persistent_units_ly,
   jwn_demand_persistent_retail_amt_ly,
   jwn_demand_persistent_regular_units_ly,
   jwn_demand_persistent_regular_retail_amt_ly,
   jwn_demand_persistent_promo_units_ly,
   jwn_demand_persistent_promo_retail_amt_ly,
   jwn_demand_persistent_clearance_units_ly,
   jwn_demand_persistent_clearance_retail_amt_ly,
   jwn_demand_other_fulfilled_units_ly,
   jwn_demand_other_fulfilled_retail_amt_ly,
   jwn_demand_other_fulfilled_regular_units_ly,
   jwn_demand_other_fulfilled_regular_retail_amt_ly,
   jwn_demand_other_fulfilled_promo_units_ly,
   jwn_demand_other_fulfilled_promo_retail_amt_ly,
   jwn_demand_other_fulfilled_clearance_units_ly,
   jwn_demand_other_fulfilled_clearance_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_units_ly,
   jwn_demand_fulfillment_center_fulfilled_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_regular_units_ly,
   jwn_demand_fulfillment_center_fulfilled_regular_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_promo_units_ly,
   jwn_demand_fulfillment_center_fulfilled_promo_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_clearance_units_ly,
   jwn_demand_fulfillment_center_fulfilled_clearance_retail_amt_ly,
   jwn_demand_flash_units_ly,
   jwn_demand_flash_retail_amt_ly,
   jwn_demand_flash_regular_units_ly,
   jwn_demand_flash_regular_retail_amt_ly,
   jwn_demand_flash_promo_units_ly,
   jwn_demand_flash_promo_retail_amt_ly,
   jwn_demand_flash_clearance_units_ly,
   jwn_demand_flash_clearance_retail_amt_ly,
   jwn_demand_dropship_fulfilled_units_ly,
   jwn_demand_dropship_fulfilled_retail_amt_ly,
   jwn_demand_dropship_fulfilled_regular_units_ly,
   jwn_demand_dropship_fulfilled_regular_retail_amt_ly,
   jwn_demand_dropship_fulfilled_promo_units_ly,
   jwn_demand_dropship_fulfilled_promo_retail_amt_ly,
   jwn_demand_dropship_fulfilled_clearance_units_ly,
   jwn_demand_dropship_fulfilled_clearance_retail_amt_ly,
   jwn_operational_gmv_total_units_ly,
   jwn_operational_gmv_total_retail_amt_ly,
   jwn_operational_gmv_total_cost_amt_ly,
   jwn_operational_gmv_regular_units_ly,
   jwn_operational_gmv_regular_retail_amt_ly,
   jwn_operational_gmv_regular_cost_amt_ly,
   jwn_operational_gmv_promo_units_ly,
   jwn_operational_gmv_promo_retail_amt_ly,
   jwn_operational_gmv_promo_cost_amt_ly,
   jwn_operational_gmv_clearance_units_ly,
   jwn_operational_gmv_clearance_retail_amt_ly,
   jwn_operational_gmv_clearance_cost_amt_ly,
   jwn_operational_gmv_persistent_units_ly,
   jwn_operational_gmv_persistent_retail_amt_ly,
   jwn_operational_gmv_persistent_cost_amt_ly,
   jwn_operational_gmv_persistent_regular_units_ly,
   jwn_operational_gmv_persistent_regular_retail_amt_ly,
   jwn_operational_gmv_persistent_regular_cost_amt_ly,
   jwn_operational_gmv_persistent_promo_units_ly,
   jwn_operational_gmv_persistent_promo_retail_amt_ly,
   jwn_operational_gmv_persistent_promo_cost_amt_ly,
   jwn_operational_gmv_persistent_clearance_units_ly,
   jwn_operational_gmv_persistent_clearance_retail_amt_ly,
   jwn_operational_gmv_persistent_clearance_cost_amt_ly,
   jwn_operational_gmv_flash_units_ly,
   jwn_operational_gmv_flash_retail_amt_ly,
   jwn_operational_gmv_flash_cost_amt_ly,
   jwn_operational_gmv_flash_regular_units_ly,
   jwn_operational_gmv_flash_regular_retail_amt_ly,
   jwn_operational_gmv_flash_regular_cost_amt_ly,
   jwn_operational_gmv_flash_promo_units_ly,
   jwn_operational_gmv_flash_promo_retail_amt_ly,
   jwn_operational_gmv_flash_promo_cost_amt_ly,
   jwn_operational_gmv_flash_clearance_units_ly,
   jwn_operational_gmv_flash_clearance_retail_amt_ly,
   jwn_operational_gmv_flash_clearance_cost_amt_ly,
   jwn_gross_sales_total_units_ly,
   jwn_gross_sales_total_retail_amt_ly,
   jwn_gross_sales_total_cost_amt_ly,
   jwn_gross_sales_regular_units_ly,
   jwn_gross_sales_regular_retail_amt_ly,
   jwn_gross_sales_regular_cost_amt_ly,
   jwn_gross_sales_promo_units_ly,
   jwn_gross_sales_promo_retail_amt_ly,
   jwn_gross_sales_promo_cost_amt_ly,
   jwn_gross_sales_clearance_units_ly,
   jwn_gross_sales_clearance_retail_amt_ly,
   jwn_gross_sales_clearance_cost_amt_ly,
   jwn_returns_total_units_ly,
   jwn_returns_total_retail_amt_ly,
   jwn_returns_total_cost_amt_ly,
   jwn_returns_regular_units_ly,
   jwn_returns_regular_retail_amt_ly,
   jwn_returns_regular_cost_amt_ly,
   jwn_returns_promo_units_ly,
   jwn_returns_promo_retail_amt_ly,
   jwn_returns_promo_cost_amt_ly,
   jwn_returns_clearance_units_ly,
   jwn_returns_clearance_retail_amt_ly,
   jwn_returns_clearance_cost_amt_ly,
   receipts_total_units_ly,
   receipts_total_retail_amt_ly,
   receipts_total_cost_amt_ly,
   receipts_po_units_ly,
   receipts_po_retail_amt_ly,
   receipts_po_cost_amt_ly,
   receipts_dropship_units_ly,
   receipts_dropship_retail_amt_ly,
   receipts_dropship_cost_amt_ly,
   transfer_in_pack_and_hold_units_ly,
   transfer_in_pack_and_hold_retail_amt_ly,
   transfer_in_pack_and_hold_cost_amt_ly,
   transfer_in_reserve_stock_units_ly,
   transfer_in_reserve_stock_retail_amt_ly,
   transfer_in_reserve_stock_cost_amt_ly,
   transfer_in_racking_units_ly,
   transfer_in_racking_retail_amt_ly,
   transfer_in_racking_cost_amt_ly,
   transfer_out_racking_units_ly,
   transfer_out_racking_retail_amt_ly,
   transfer_out_racking_cost_amt_ly,
   transfer_in_return_to_rack_units_ly,
   transfer_in_return_to_rack_retail_amt_ly,
   transfer_in_return_to_rack_cost_amt_ly,
   inventory_boh_total_units_ly,
   inventory_boh_total_retail_amt_ly,
   inventory_boh_total_cost_amt_ly,
   inventory_boh_regular_units_ly,
   inventory_boh_regular_retail_amt_ly,
   inventory_boh_regular_cost_amt_ly,
   inventory_boh_clearance_units_ly,
   inventory_boh_clearance_retail_amt_ly,
   inventory_boh_clearance_cost_amt_ly,
   inventory_eoh_total_units_ly,
   inventory_eoh_total_retail_amt_ly,
   inventory_eoh_total_cost_amt_ly,
   inventory_eoh_regular_units_ly,
   inventory_eoh_regular_retail_amt_ly,
   inventory_eoh_regular_cost_amt_ly,
   inventory_eoh_clearance_units_ly,
   inventory_eoh_clearance_retail_amt_ly,
   inventory_eoh_clearance_cost_amt_ly,
   rtv_total_units_ly,
   rtv_total_retail_amt_ly,
   rtv_total_cost_amt_ly,
   mos_total_cost_amt_ty,
   mos_regular_cost_amt_ty,
   mos_clearance_cost_amt_ty,
   mos_total_units_ty,
   mos_regular_units_ty,
   mos_clearance_units_ty,
   mos_total_retail_amt_ty,
   mos_regular_retail_amt_ty,
   mos_clearance_retail_amt_ty,
   mos_total_cost_amt_ly,
   mos_regular_cost_amt_ly,
   mos_clearance_cost_amt_ly,
   mos_total_units_ly,
   mos_regular_units_ly,
   mos_clearance_units_ly,
   mos_total_retail_amt_ly,
   mos_regular_retail_amt_ly,
   mos_clearance_retail_amt_ly,
   receipts_regular_retail_amt_ty,
   receipts_regular_units_ty,
   receipts_regular_cost_amt_ty,
   receipts_clearance_retail_amt_ty,
   receipts_clearance_units_ty,
   receipts_clearance_cost_amt_ty,
   receipts_crossdock_regular_retail_amt_ty,
   receipts_crossdock_regular_units_ty,
   receipts_crossdock_regular_cost_amt_ty,
   receipts_crossdock_clearance_retail_amt_ty,
   receipts_crossdock_clearance_units_ty,
   receipts_crossdock_clearance_cost_amt_ty,
   receipts_regular_retail_amt_ly,
   receipts_regular_units_ly,
   receipts_regular_cost_amt_ly,
   receipts_clearance_retail_amt_ly,
   receipts_clearance_units_ly,
   receipts_clearance_cost_amt_ly,
   receipts_crossdock_regular_retail_amt_ly,
   receipts_crossdock_regular_units_ly,
   receipts_crossdock_regular_cost_amt_ly,
   receipts_crossdock_clearance_retail_amt_ly,
   receipts_crossdock_clearance_units_ly,
   receipts_crossdock_clearance_cost_amt_ly,
   transfer_in_racking_regular_cost_amt_ty,
   transfer_in_racking_regular_units_ty,
   transfer_in_racking_clearance_cost_amt_ty,
   transfer_in_racking_clearance_units_ty,
   transfer_out_racking_regular_cost_amt_ty,
   transfer_out_racking_regular_units_ty,
   transfer_out_racking_clearance_cost_amt_ty,
   transfer_out_racking_clearance_units_ty,
   transfer_in_pack_and_hold_regular_cost_amt_ty,
   transfer_in_pack_and_hold_regular_units_ty,
   transfer_in_pack_and_hold_clearance_cost_amt_ty,
   transfer_in_pack_and_hold_clearance_units_ty,
   transfer_out_pack_and_hold_cost_amt_ty,
   transfer_out_pack_and_hold_units_ty,
   transfer_out_pack_and_hold_regular_cost_amt_ty,
   transfer_out_pack_and_hold_regular_units_ty,
   transfer_out_pack_and_hold_clearance_cost_amt_ty,
   transfer_out_pack_and_hold_clearance_units_ty,
   transfer_in_reserve_stock_regular_cost_amt_ty,
   transfer_in_reserve_stock_regular_units_ty,
   transfer_in_reserve_stock_clearance_cost_amt_ty,
   transfer_in_reserve_stock_clearance_units_ty,
   transfer_out_reserve_stock_cost_amt_ty,
   transfer_out_reserve_stock_units_ty,
   transfer_out_reserve_stock_regular_cost_amt_ty,
   transfer_out_reserve_stock_regular_units_ty,
   transfer_out_reserve_stock_clearance_cost_amt_ty,
   transfer_out_reserve_stock_clearance_units_ty,
   transfer_in_racking_regular_cost_amt_ly,
   transfer_in_racking_regular_units_ly,
   transfer_in_racking_clearance_cost_amt_ly,
   transfer_in_racking_clearance_units_ly,
   transfer_out_racking_regular_cost_amt_ly,
   transfer_out_racking_regular_units_ly,
   transfer_out_racking_clearance_cost_amt_ly,
   transfer_out_racking_clearance_units_ly,
   transfer_in_pack_and_hold_regular_cost_amt_ly,
   transfer_in_pack_and_hold_regular_units_ly,
   transfer_in_pack_and_hold_clearance_cost_amt_ly,
   transfer_in_pack_and_hold_clearance_units_ly,
   transfer_out_pack_and_hold_cost_amt_ly,
   transfer_out_pack_and_hold_units_ly,
   transfer_out_pack_and_hold_regular_cost_amt_ly,
   transfer_out_pack_and_hold_regular_units_ly,
   transfer_out_pack_and_hold_clearance_cost_amt_ly,
   transfer_out_pack_and_hold_clearance_units_ly,
   transfer_in_reserve_stock_regular_cost_amt_ly,
   transfer_in_reserve_stock_regular_units_ly,
   transfer_in_reserve_stock_clearance_cost_amt_ly,
   transfer_in_reserve_stock_clearance_units_ly,
   transfer_out_reserve_stock_cost_amt_ly,
   transfer_out_reserve_stock_units_ly,
   transfer_out_reserve_stock_regular_cost_amt_ly,
   transfer_out_reserve_stock_regular_units_ly,
   transfer_out_reserve_stock_clearance_cost_amt_ly,
   transfer_out_reserve_stock_clearance_units_ly,
   inventory_eoh_in_transit_regular_units_ty,
   inventory_eoh_in_transit_regular_cost_amt_ty,
   inventory_eoh_in_transit_regular_retail_amt_ty,
   inventory_eoh_in_transit_clearance_units_ty,
   inventory_eoh_in_transit_clearance_cost_amt_ty,
   inventory_eoh_in_transit_clearance_retail_amt_ty,
   inventory_eoh_in_transit_total_units_ty,
   inventory_eoh_in_transit_total_cost_amt_ty,
   inventory_eoh_in_transit_total_retail_amt_ty,
   inventory_boh_in_transit_regular_units_ty,
   inventory_boh_in_transit_regular_cost_amt_ty,
   inventory_boh_in_transit_regular_retail_amt_ty,
   inventory_boh_in_transit_clearance_units_ty,
   inventory_boh_in_transit_clearance_cost_amt_ty,
   inventory_boh_in_transit_clearance_retail_amt_ty,
   inventory_boh_in_transit_total_units_ty,
   inventory_boh_in_transit_total_cost_amt_ty,
   inventory_boh_in_transit_total_retail_amt_ty,
   inventory_eoh_in_transit_regular_units_ly,
   inventory_eoh_in_transit_regular_cost_amt_ly,
   inventory_eoh_in_transit_regular_retail_amt_ly,
   inventory_eoh_in_transit_clearance_units_ly,
   inventory_eoh_in_transit_clearance_cost_amt_ly,
   inventory_eoh_in_transit_clearance_retail_amt_ly,
   inventory_eoh_in_transit_total_units_ly,
   inventory_eoh_in_transit_total_cost_amt_ly,
   inventory_eoh_in_transit_total_retail_amt_ly,
   inventory_boh_in_transit_regular_units_ly,
   inventory_boh_in_transit_regular_cost_amt_ly,
   inventory_boh_in_transit_regular_retail_amt_ly,
   inventory_boh_in_transit_clearance_units_ly,
   inventory_boh_in_transit_clearance_cost_amt_ly,
   inventory_boh_in_transit_clearance_retail_amt_ly,
   inventory_boh_in_transit_total_units_ly,
   inventory_boh_in_transit_total_cost_amt_ly,
   inventory_boh_in_transit_total_retail_amt_ly,
   dw_sys_load_tmstp,
   dw_sys_load_dt,
   CAST(trunc(cast(CASE
     WHEN CASE
       WHEN store_num IN (210, 212)
       THEN '209'
       ELSE FORMAT('%11d', store_num)
       END = ''
     THEN '0'
     ELSE CASE
      WHEN store_num IN (210, 212)
      THEN '209'
      ELSE FORMAT('%11d', store_num)
      END
     END as float64)) AS INTEGER) AS store_num_m
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
  WHERE week_num IN (SELECT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
     WHERE week_num_of_fiscal_month = 1)
   AND channel_num IN (110, 210)
   AND month_num IN (SELECT DISTINCT month_idnt
     FROM location_plans)) (SELECT REPLACE(CONCAT(class_num, dept_num, month_num, store_num_m), ' ', '') AS pkey,
   dept_num,
   class_num,
   month_num,
   REPLACE(CONCAT(class_num, dept_num, month_num, store_num_m), ' ', '') AS store_num,
   SUM(inventory_boh_total_units_ty + inventory_boh_in_transit_total_units_ty) AS total_bop_u_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_boh_total_units_ty + inventory_boh_in_transit_total_units_ty
     ELSE NULL
     END) AS rp_bop_u_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_boh_total_units_ty + inventory_boh_in_transit_total_units_ty
     ELSE NULL
     END) AS nrp_bop_u_ty,
   SUM(inventory_boh_clearance_units_ty + inventory_boh_in_transit_clearance_units_ty) AS clearance_bop_u_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_boh_clearance_units_ty + inventory_boh_in_transit_clearance_units_ty
     ELSE NULL
     END) AS rp_clearance_bop_u_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_boh_clearance_units_ty + inventory_boh_in_transit_clearance_units_ty
     ELSE NULL
     END) AS nrp_clearance_bop_u_ty,
   SUM(inventory_boh_total_units_ly + inventory_boh_in_transit_total_units_ly) AS total_bop_u_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_boh_total_units_ly + inventory_boh_in_transit_total_units_ly
     ELSE NULL
     END) AS rp_bop_u_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_boh_total_units_ly + inventory_boh_in_transit_total_units_ly
     ELSE NULL
     END) AS nrp_bop_u_ly,
   SUM(inventory_boh_clearance_units_ly + inventory_boh_in_transit_clearance_units_ly) AS clearance_bop_u_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_boh_clearance_units_ly + inventory_boh_in_transit_clearance_units_ly
     ELSE NULL
     END) AS rp_clearance_bop_u_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_boh_clearance_units_ly + inventory_boh_in_transit_clearance_units_ly
     ELSE NULL
     END) AS nrp_clearance_bop_u_ly
  FROM boh_u
  GROUP BY pkey,
   dept_num,
   class_num,
   month_num);

--COLLECT STATS PRIMARY INDEX(pkey) ,COLUMN (class_num,dept_num) ON boh_u;

CREATE  TEMPORARY TABLE IF NOT EXISTS  eoh_u AS WITH eoh_u AS (SELECT sbclass_num,
   store_num,
   week_num,
   supp_part_num,
   class_num,
   dept_num,
   supp_num,
   vndr_label_code,
   business_unit_num,
   banner_country_num,
   channel_num,
   month_num,
   quarter_num,
   half_num,
   year_num,
   rp_ind,
   npg_ind,
   comp_status_code,
   store_country_code,
   jwn_demand_total_units_ty,
   jwn_demand_total_retail_amt_ty,
   jwn_demand_regular_units_ty,
   jwn_demand_regular_retail_amt_ty,
   jwn_demand_promo_units_ty,
   jwn_demand_promo_retail_amt_ty,
   jwn_demand_clearance_units_ty,
   jwn_demand_clearance_retail_amt_ty,
   jwn_demand_store_fulfilled_units_ty,
   jwn_demand_store_fulfilled_retail_amt_ty,
   jwn_demand_store_fulfilled_regular_units_ty,
   jwn_demand_store_fulfilled_regular_retail_amt_ty,
   jwn_demand_store_fulfilled_promo_units_ty,
   jwn_demand_store_fulfilled_promo_retail_amt_ty,
   jwn_demand_store_fulfilled_clearance_units_ty,
   jwn_demand_store_fulfilled_clearance_retail_amt_ty,
   jwn_demand_persistent_units_ty,
   jwn_demand_persistent_retail_amt_ty,
   jwn_demand_persistent_regular_units_ty,
   jwn_demand_persistent_regular_retail_amt_ty,
   jwn_demand_persistent_promo_units_ty,
   jwn_demand_persistent_promo_retail_amt_ty,
   jwn_demand_persistent_clearance_units_ty,
   jwn_demand_persistent_clearance_retail_amt_ty,
   jwn_demand_other_fulfilled_units_ty,
   jwn_demand_other_fulfilled_retail_amt_ty,
   jwn_demand_other_fulfilled_regular_units_ty,
   jwn_demand_other_fulfilled_regular_retail_amt_ty,
   jwn_demand_other_fulfilled_promo_units_ty,
   jwn_demand_other_fulfilled_promo_retail_amt_ty,
   jwn_demand_other_fulfilled_clearance_units_ty,
   jwn_demand_other_fulfilled_clearance_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_units_ty,
   jwn_demand_fulfillment_center_fulfilled_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_regular_units_ty,
   jwn_demand_fulfillment_center_fulfilled_regular_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_promo_units_ty,
   jwn_demand_fulfillment_center_fulfilled_promo_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_clearance_units_ty,
   jwn_demand_fulfillment_center_fulfilled_clearance_retail_amt_ty,
   jwn_demand_flash_units_ty,
   jwn_demand_flash_retail_amt_ty,
   jwn_demand_flash_regular_units_ty,
   jwn_demand_flash_regular_retail_amt_ty,
   jwn_demand_flash_promo_units_ty,
   jwn_demand_flash_promo_retail_amt_ty,
   jwn_demand_flash_clearance_units_ty,
   jwn_demand_flash_clearance_retail_amt_ty,
   jwn_demand_dropship_fulfilled_units_ty,
   jwn_demand_dropship_fulfilled_retail_amt_ty,
   jwn_demand_dropship_fulfilled_regular_units_ty,
   jwn_demand_dropship_fulfilled_regular_retail_amt_ty,
   jwn_demand_dropship_fulfilled_promo_units_ty,
   jwn_demand_dropship_fulfilled_promo_retail_amt_ty,
   jwn_demand_dropship_fulfilled_clearance_units_ty,
   jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,
   jwn_operational_gmv_total_units_ty,
   jwn_operational_gmv_total_retail_amt_ty,
   jwn_operational_gmv_total_cost_amt_ty,
   jwn_operational_gmv_regular_units_ty,
   jwn_operational_gmv_regular_retail_amt_ty,
   jwn_operational_gmv_regular_cost_amt_ty,
   jwn_operational_gmv_promo_units_ty,
   jwn_operational_gmv_promo_retail_amt_ty,
   jwn_operational_gmv_promo_cost_amt_ty,
   jwn_operational_gmv_clearance_units_ty,
   jwn_operational_gmv_clearance_retail_amt_ty,
   jwn_operational_gmv_clearance_cost_amt_ty,
   jwn_operational_gmv_persistent_units_ty,
   jwn_operational_gmv_persistent_retail_amt_ty,
   jwn_operational_gmv_persistent_cost_amt_ty,
   jwn_operational_gmv_persistent_regular_units_ty,
   jwn_operational_gmv_persistent_regular_retail_amt_ty,
   jwn_operational_gmv_persistent_regular_cost_amt_ty,
   jwn_operational_gmv_persistent_promo_units_ty,
   jwn_operational_gmv_persistent_promo_retail_amt_ty,
   jwn_operational_gmv_persistent_promo_cost_amt_ty,
   jwn_operational_gmv_persistent_clearance_units_ty,
   jwn_operational_gmv_persistent_clearance_retail_amt_ty,
   jwn_operational_gmv_persistent_clearance_cost_amt_ty,
   jwn_operational_gmv_flash_units_ty,
   jwn_operational_gmv_flash_retail_amt_ty,
   jwn_operational_gmv_flash_cost_amt_ty,
   jwn_operational_gmv_flash_regular_units_ty,
   jwn_operational_gmv_flash_regular_retail_amt_ty,
   jwn_operational_gmv_flash_regular_cost_amt_ty,
   jwn_operational_gmv_flash_promo_units_ty,
   jwn_operational_gmv_flash_promo_retail_amt_ty,
   jwn_operational_gmv_flash_promo_cost_amt_ty,
   jwn_operational_gmv_flash_clearance_units_ty,
   jwn_operational_gmv_flash_clearance_retail_amt_ty,
   jwn_operational_gmv_flash_clearance_cost_amt_ty,
   jwn_gross_sales_total_units_ty,
   jwn_gross_sales_total_retail_amt_ty,
   jwn_gross_sales_total_cost_amt_ty,
   jwn_gross_sales_regular_units_ty,
   jwn_gross_sales_regular_retail_amt_ty,
   jwn_gross_sales_regular_cost_amt_ty,
   jwn_gross_sales_promo_units_ty,
   jwn_gross_sales_promo_retail_amt_ty,
   jwn_gross_sales_promo_cost_amt_ty,
   jwn_gross_sales_clearance_units_ty,
   jwn_gross_sales_clearance_retail_amt_ty,
   jwn_gross_sales_clearance_cost_amt_ty,
   jwn_returns_total_units_ty,
   jwn_returns_total_retail_amt_ty,
   jwn_returns_total_cost_amt_ty,
   jwn_returns_regular_units_ty,
   jwn_returns_regular_retail_amt_ty,
   jwn_returns_regular_cost_amt_ty,
   jwn_returns_promo_units_ty,
   jwn_returns_promo_retail_amt_ty,
   jwn_returns_promo_cost_amt_ty,
   jwn_returns_clearance_units_ty,
   jwn_returns_clearance_retail_amt_ty,
   jwn_returns_clearance_cost_amt_ty,
   receipts_total_units_ty,
   receipts_total_retail_amt_ty,
   receipts_total_cost_amt_ty,
   receipts_po_units_ty,
   receipts_po_retail_amt_ty,
   receipts_po_cost_amt_ty,
   receipts_dropship_units_ty,
   receipts_dropship_retail_amt_ty,
   receipts_dropship_cost_amt_ty,
   transfer_in_pack_and_hold_units_ty,
   transfer_in_pack_and_hold_retail_amt_ty,
   transfer_in_pack_and_hold_cost_amt_ty,
   transfer_in_reserve_stock_units_ty,
   transfer_in_reserve_stock_retail_amt_ty,
   transfer_in_reserve_stock_cost_amt_ty,
   transfer_in_racking_units_ty,
   transfer_in_racking_retail_amt_ty,
   transfer_in_racking_cost_amt_ty,
   transfer_out_racking_units_ty,
   transfer_out_racking_retail_amt_ty,
   transfer_out_racking_cost_amt_ty,
   transfer_in_return_to_rack_units_ty,
   transfer_in_return_to_rack_retail_amt_ty,
   transfer_in_return_to_rack_cost_amt_ty,
   inventory_boh_total_units_ty,
   inventory_boh_total_retail_amt_ty,
   inventory_boh_total_cost_amt_ty,
   inventory_boh_regular_units_ty,
   inventory_boh_regular_retail_amt_ty,
   inventory_boh_regular_cost_amt_ty,
   inventory_boh_clearance_units_ty,
   inventory_boh_clearance_retail_amt_ty,
   inventory_boh_clearance_cost_amt_ty,
   inventory_eoh_total_units_ty,
   inventory_eoh_total_retail_amt_ty,
   inventory_eoh_total_cost_amt_ty,
   inventory_eoh_regular_units_ty,
   inventory_eoh_regular_retail_amt_ty,
   inventory_eoh_regular_cost_amt_ty,
   inventory_eoh_clearance_units_ty,
   inventory_eoh_clearance_retail_amt_ty,
   inventory_eoh_clearance_cost_amt_ty,
   rtv_total_units_ty,
   rtv_total_retail_amt_ty,
   rtv_total_cost_amt_ty,
   jwn_demand_total_units_ly,
   jwn_demand_total_retail_amt_ly,
   jwn_demand_regular_units_ly,
   jwn_demand_regular_retail_amt_ly,
   jwn_demand_promo_units_ly,
   jwn_demand_promo_retail_amt_ly,
   jwn_demand_clearance_units_ly,
   jwn_demand_clearance_retail_amt_ly,
   jwn_demand_store_fulfilled_units_ly,
   jwn_demand_store_fulfilled_retail_amt_ly,
   jwn_demand_store_fulfilled_regular_units_ly,
   jwn_demand_store_fulfilled_regular_retail_amt_ly,
   jwn_demand_store_fulfilled_promo_units_ly,
   jwn_demand_store_fulfilled_promo_retail_amt_ly,
   jwn_demand_store_fulfilled_clearance_units_ly,
   jwn_demand_store_fulfilled_clearance_retail_amt_ly,
   jwn_demand_persistent_units_ly,
   jwn_demand_persistent_retail_amt_ly,
   jwn_demand_persistent_regular_units_ly,
   jwn_demand_persistent_regular_retail_amt_ly,
   jwn_demand_persistent_promo_units_ly,
   jwn_demand_persistent_promo_retail_amt_ly,
   jwn_demand_persistent_clearance_units_ly,
   jwn_demand_persistent_clearance_retail_amt_ly,
   jwn_demand_other_fulfilled_units_ly,
   jwn_demand_other_fulfilled_retail_amt_ly,
   jwn_demand_other_fulfilled_regular_units_ly,
   jwn_demand_other_fulfilled_regular_retail_amt_ly,
   jwn_demand_other_fulfilled_promo_units_ly,
   jwn_demand_other_fulfilled_promo_retail_amt_ly,
   jwn_demand_other_fulfilled_clearance_units_ly,
   jwn_demand_other_fulfilled_clearance_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_units_ly,
   jwn_demand_fulfillment_center_fulfilled_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_regular_units_ly,
   jwn_demand_fulfillment_center_fulfilled_regular_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_promo_units_ly,
   jwn_demand_fulfillment_center_fulfilled_promo_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_clearance_units_ly,
   jwn_demand_fulfillment_center_fulfilled_clearance_retail_amt_ly,
   jwn_demand_flash_units_ly,
   jwn_demand_flash_retail_amt_ly,
   jwn_demand_flash_regular_units_ly,
   jwn_demand_flash_regular_retail_amt_ly,
   jwn_demand_flash_promo_units_ly,
   jwn_demand_flash_promo_retail_amt_ly,
   jwn_demand_flash_clearance_units_ly,
   jwn_demand_flash_clearance_retail_amt_ly,
   jwn_demand_dropship_fulfilled_units_ly,
   jwn_demand_dropship_fulfilled_retail_amt_ly,
   jwn_demand_dropship_fulfilled_regular_units_ly,
   jwn_demand_dropship_fulfilled_regular_retail_amt_ly,
   jwn_demand_dropship_fulfilled_promo_units_ly,
   jwn_demand_dropship_fulfilled_promo_retail_amt_ly,
   jwn_demand_dropship_fulfilled_clearance_units_ly,
   jwn_demand_dropship_fulfilled_clearance_retail_amt_ly,
   jwn_operational_gmv_total_units_ly,
   jwn_operational_gmv_total_retail_amt_ly,
   jwn_operational_gmv_total_cost_amt_ly,
   jwn_operational_gmv_regular_units_ly,
   jwn_operational_gmv_regular_retail_amt_ly,
   jwn_operational_gmv_regular_cost_amt_ly,
   jwn_operational_gmv_promo_units_ly,
   jwn_operational_gmv_promo_retail_amt_ly,
   jwn_operational_gmv_promo_cost_amt_ly,
   jwn_operational_gmv_clearance_units_ly,
   jwn_operational_gmv_clearance_retail_amt_ly,
   jwn_operational_gmv_clearance_cost_amt_ly,
   jwn_operational_gmv_persistent_units_ly,
   jwn_operational_gmv_persistent_retail_amt_ly,
   jwn_operational_gmv_persistent_cost_amt_ly,
   jwn_operational_gmv_persistent_regular_units_ly,
   jwn_operational_gmv_persistent_regular_retail_amt_ly,
   jwn_operational_gmv_persistent_regular_cost_amt_ly,
   jwn_operational_gmv_persistent_promo_units_ly,
   jwn_operational_gmv_persistent_promo_retail_amt_ly,
   jwn_operational_gmv_persistent_promo_cost_amt_ly,
   jwn_operational_gmv_persistent_clearance_units_ly,
   jwn_operational_gmv_persistent_clearance_retail_amt_ly,
   jwn_operational_gmv_persistent_clearance_cost_amt_ly,
   jwn_operational_gmv_flash_units_ly,
   jwn_operational_gmv_flash_retail_amt_ly,
   jwn_operational_gmv_flash_cost_amt_ly,
   jwn_operational_gmv_flash_regular_units_ly,
   jwn_operational_gmv_flash_regular_retail_amt_ly,
   jwn_operational_gmv_flash_regular_cost_amt_ly,
   jwn_operational_gmv_flash_promo_units_ly,
   jwn_operational_gmv_flash_promo_retail_amt_ly,
   jwn_operational_gmv_flash_promo_cost_amt_ly,
   jwn_operational_gmv_flash_clearance_units_ly,
   jwn_operational_gmv_flash_clearance_retail_amt_ly,
   jwn_operational_gmv_flash_clearance_cost_amt_ly,
   jwn_gross_sales_total_units_ly,
   jwn_gross_sales_total_retail_amt_ly,
   jwn_gross_sales_total_cost_amt_ly,
   jwn_gross_sales_regular_units_ly,
   jwn_gross_sales_regular_retail_amt_ly,
   jwn_gross_sales_regular_cost_amt_ly,
   jwn_gross_sales_promo_units_ly,
   jwn_gross_sales_promo_retail_amt_ly,
   jwn_gross_sales_promo_cost_amt_ly,
   jwn_gross_sales_clearance_units_ly,
   jwn_gross_sales_clearance_retail_amt_ly,
   jwn_gross_sales_clearance_cost_amt_ly,
   jwn_returns_total_units_ly,
   jwn_returns_total_retail_amt_ly,
   jwn_returns_total_cost_amt_ly,
   jwn_returns_regular_units_ly,
   jwn_returns_regular_retail_amt_ly,
   jwn_returns_regular_cost_amt_ly,
   jwn_returns_promo_units_ly,
   jwn_returns_promo_retail_amt_ly,
   jwn_returns_promo_cost_amt_ly,
   jwn_returns_clearance_units_ly,
   jwn_returns_clearance_retail_amt_ly,
   jwn_returns_clearance_cost_amt_ly,
   receipts_total_units_ly,
   receipts_total_retail_amt_ly,
   receipts_total_cost_amt_ly,
   receipts_po_units_ly,
   receipts_po_retail_amt_ly,
   receipts_po_cost_amt_ly,
   receipts_dropship_units_ly,
   receipts_dropship_retail_amt_ly,
   receipts_dropship_cost_amt_ly,
   transfer_in_pack_and_hold_units_ly,
   transfer_in_pack_and_hold_retail_amt_ly,
   transfer_in_pack_and_hold_cost_amt_ly,
   transfer_in_reserve_stock_units_ly,
   transfer_in_reserve_stock_retail_amt_ly,
   transfer_in_reserve_stock_cost_amt_ly,
   transfer_in_racking_units_ly,
   transfer_in_racking_retail_amt_ly,
   transfer_in_racking_cost_amt_ly,
   transfer_out_racking_units_ly,
   transfer_out_racking_retail_amt_ly,
   transfer_out_racking_cost_amt_ly,
   transfer_in_return_to_rack_units_ly,
   transfer_in_return_to_rack_retail_amt_ly,
   transfer_in_return_to_rack_cost_amt_ly,
   inventory_boh_total_units_ly,
   inventory_boh_total_retail_amt_ly,
   inventory_boh_total_cost_amt_ly,
   inventory_boh_regular_units_ly,
   inventory_boh_regular_retail_amt_ly,
   inventory_boh_regular_cost_amt_ly,
   inventory_boh_clearance_units_ly,
   inventory_boh_clearance_retail_amt_ly,
   inventory_boh_clearance_cost_amt_ly,
   inventory_eoh_total_units_ly,
   inventory_eoh_total_retail_amt_ly,
   inventory_eoh_total_cost_amt_ly,
   inventory_eoh_regular_units_ly,
   inventory_eoh_regular_retail_amt_ly,
   inventory_eoh_regular_cost_amt_ly,
   inventory_eoh_clearance_units_ly,
   inventory_eoh_clearance_retail_amt_ly,
   inventory_eoh_clearance_cost_amt_ly,
   rtv_total_units_ly,
   rtv_total_retail_amt_ly,
   rtv_total_cost_amt_ly,
   mos_total_cost_amt_ty,
   mos_regular_cost_amt_ty,
   mos_clearance_cost_amt_ty,
   mos_total_units_ty,
   mos_regular_units_ty,
   mos_clearance_units_ty,
   mos_total_retail_amt_ty,
   mos_regular_retail_amt_ty,
   mos_clearance_retail_amt_ty,
   mos_total_cost_amt_ly,
   mos_regular_cost_amt_ly,
   mos_clearance_cost_amt_ly,
   mos_total_units_ly,
   mos_regular_units_ly,
   mos_clearance_units_ly,
   mos_total_retail_amt_ly,
   mos_regular_retail_amt_ly,
   mos_clearance_retail_amt_ly,
   receipts_regular_retail_amt_ty,
   receipts_regular_units_ty,
   receipts_regular_cost_amt_ty,
   receipts_clearance_retail_amt_ty,
   receipts_clearance_units_ty,
   receipts_clearance_cost_amt_ty,
   receipts_crossdock_regular_retail_amt_ty,
   receipts_crossdock_regular_units_ty,
   receipts_crossdock_regular_cost_amt_ty,
   receipts_crossdock_clearance_retail_amt_ty,
   receipts_crossdock_clearance_units_ty,
   receipts_crossdock_clearance_cost_amt_ty,
   receipts_regular_retail_amt_ly,
   receipts_regular_units_ly,
   receipts_regular_cost_amt_ly,
   receipts_clearance_retail_amt_ly,
   receipts_clearance_units_ly,
   receipts_clearance_cost_amt_ly,
   receipts_crossdock_regular_retail_amt_ly,
   receipts_crossdock_regular_units_ly,
   receipts_crossdock_regular_cost_amt_ly,
   receipts_crossdock_clearance_retail_amt_ly,
   receipts_crossdock_clearance_units_ly,
   receipts_crossdock_clearance_cost_amt_ly,
   transfer_in_racking_regular_cost_amt_ty,
   transfer_in_racking_regular_units_ty,
   transfer_in_racking_clearance_cost_amt_ty,
   transfer_in_racking_clearance_units_ty,
   transfer_out_racking_regular_cost_amt_ty,
   transfer_out_racking_regular_units_ty,
   transfer_out_racking_clearance_cost_amt_ty,
   transfer_out_racking_clearance_units_ty,
   transfer_in_pack_and_hold_regular_cost_amt_ty,
   transfer_in_pack_and_hold_regular_units_ty,
   transfer_in_pack_and_hold_clearance_cost_amt_ty,
   transfer_in_pack_and_hold_clearance_units_ty,
   transfer_out_pack_and_hold_cost_amt_ty,
   transfer_out_pack_and_hold_units_ty,
   transfer_out_pack_and_hold_regular_cost_amt_ty,
   transfer_out_pack_and_hold_regular_units_ty,
   transfer_out_pack_and_hold_clearance_cost_amt_ty,
   transfer_out_pack_and_hold_clearance_units_ty,
   transfer_in_reserve_stock_regular_cost_amt_ty,
   transfer_in_reserve_stock_regular_units_ty,
   transfer_in_reserve_stock_clearance_cost_amt_ty,
   transfer_in_reserve_stock_clearance_units_ty,
   transfer_out_reserve_stock_cost_amt_ty,
   transfer_out_reserve_stock_units_ty,
   transfer_out_reserve_stock_regular_cost_amt_ty,
   transfer_out_reserve_stock_regular_units_ty,
   transfer_out_reserve_stock_clearance_cost_amt_ty,
   transfer_out_reserve_stock_clearance_units_ty,
   transfer_in_racking_regular_cost_amt_ly,
   transfer_in_racking_regular_units_ly,
   transfer_in_racking_clearance_cost_amt_ly,
   transfer_in_racking_clearance_units_ly,
   transfer_out_racking_regular_cost_amt_ly,
   transfer_out_racking_regular_units_ly,
   transfer_out_racking_clearance_cost_amt_ly,
   transfer_out_racking_clearance_units_ly,
   transfer_in_pack_and_hold_regular_cost_amt_ly,
   transfer_in_pack_and_hold_regular_units_ly,
   transfer_in_pack_and_hold_clearance_cost_amt_ly,
   transfer_in_pack_and_hold_clearance_units_ly,
   transfer_out_pack_and_hold_cost_amt_ly,
   transfer_out_pack_and_hold_units_ly,
   transfer_out_pack_and_hold_regular_cost_amt_ly,
   transfer_out_pack_and_hold_regular_units_ly,
   transfer_out_pack_and_hold_clearance_cost_amt_ly,
   transfer_out_pack_and_hold_clearance_units_ly,
   transfer_in_reserve_stock_regular_cost_amt_ly,
   transfer_in_reserve_stock_regular_units_ly,
   transfer_in_reserve_stock_clearance_cost_amt_ly,
   transfer_in_reserve_stock_clearance_units_ly,
   transfer_out_reserve_stock_cost_amt_ly,
   transfer_out_reserve_stock_units_ly,
   transfer_out_reserve_stock_regular_cost_amt_ly,
   transfer_out_reserve_stock_regular_units_ly,
   transfer_out_reserve_stock_clearance_cost_amt_ly,
   transfer_out_reserve_stock_clearance_units_ly,
   inventory_eoh_in_transit_regular_units_ty,
   inventory_eoh_in_transit_regular_cost_amt_ty,
   inventory_eoh_in_transit_regular_retail_amt_ty,
   inventory_eoh_in_transit_clearance_units_ty,
   inventory_eoh_in_transit_clearance_cost_amt_ty,
   inventory_eoh_in_transit_clearance_retail_amt_ty,
   inventory_eoh_in_transit_total_units_ty,
   inventory_eoh_in_transit_total_cost_amt_ty,
   inventory_eoh_in_transit_total_retail_amt_ty,
   inventory_boh_in_transit_regular_units_ty,
   inventory_boh_in_transit_regular_cost_amt_ty,
   inventory_boh_in_transit_regular_retail_amt_ty,
   inventory_boh_in_transit_clearance_units_ty,
   inventory_boh_in_transit_clearance_cost_amt_ty,
   inventory_boh_in_transit_clearance_retail_amt_ty,
   inventory_boh_in_transit_total_units_ty,
   inventory_boh_in_transit_total_cost_amt_ty,
   inventory_boh_in_transit_total_retail_amt_ty,
   inventory_eoh_in_transit_regular_units_ly,
   inventory_eoh_in_transit_regular_cost_amt_ly,
   inventory_eoh_in_transit_regular_retail_amt_ly,
   inventory_eoh_in_transit_clearance_units_ly,
   inventory_eoh_in_transit_clearance_cost_amt_ly,
   inventory_eoh_in_transit_clearance_retail_amt_ly,
   inventory_eoh_in_transit_total_units_ly,
   inventory_eoh_in_transit_total_cost_amt_ly,
   inventory_eoh_in_transit_total_retail_amt_ly,
   inventory_boh_in_transit_regular_units_ly,
   inventory_boh_in_transit_regular_cost_amt_ly,
   inventory_boh_in_transit_regular_retail_amt_ly,
   inventory_boh_in_transit_clearance_units_ly,
   inventory_boh_in_transit_clearance_cost_amt_ly,
   inventory_boh_in_transit_clearance_retail_amt_ly,
   inventory_boh_in_transit_total_units_ly,
   inventory_boh_in_transit_total_cost_amt_ly,
   inventory_boh_in_transit_total_retail_amt_ly,
   dw_sys_load_tmstp,
   dw_sys_load_dt,
   CAST(trunc( cast(CASE
     WHEN CASE
       WHEN store_num IN (210, 212)
       THEN '209'
       ELSE FORMAT('%11d', store_num)
       END = ''
     THEN '0'
     ELSE CASE
      WHEN store_num IN (210, 212)
      THEN '209'
      ELSE FORMAT('%11d', store_num)
      END
     END  as float64))AS INTEGER) AS store_num_m
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
  WHERE week_num IN ((SELECT week_idnt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
      WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)), (SELECT DISTINCT week_idnt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
      WHERE day_date IN (SELECT DISTINCT month_end_day_date
         FROM location_plans
         WHERE last_month_flag = 1)))
   AND channel_num IN (110, 210)) (SELECT REPLACE(CONCAT(class_num, dept_num, month_num, store_num_m), ' ', '') AS pkey
   ,
   dept_num,
   class_num,
   REPLACE(CONCAT(class_num, dept_num, month_num, store_num_m), ' ', '') AS store_num,
   month_num,
   SUM(inventory_eoh_total_units_ty) AS total_ty_eoh_u,
   SUM(inventory_eoh_clearance_units_ty) AS total_ty_cleareoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_eoh_total_units_ty
     ELSE NULL
     END) AS rp_ty_eoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_eoh_clearance_units_ty
     ELSE NULL
     END) AS rp_ty_cleareoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_eoh_total_units_ty
     ELSE NULL
     END) AS nrp_ty_eoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_eoh_clearance_units_ty
     ELSE NULL
     END) AS nrp_ty_cleareoh_u,
   SUM(inventory_eoh_total_units_ly) AS total_ly_eoh_u,
   SUM(inventory_eoh_clearance_units_ly) AS total_ly_cleareoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_eoh_total_units_ly
     ELSE NULL
     END) AS rp_ly_eoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN inventory_eoh_clearance_units_ly
     ELSE NULL
     END) AS rp_ly_cleareoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_eoh_total_units_ly
     ELSE NULL
     END) AS nrp_ly_eoh_u,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN inventory_eoh_clearance_units_ly
     ELSE NULL
     END) AS nrp_ly_cleareoh_u
  FROM eoh_u
   GROUP BY pkey,
   dept_num,
   class_num,
   month_num);

--COLLECT STATS PRIMARY INDEX(pkey) ,COLUMN (class_num,dept_num) ON eoh_u;

CREATE  TEMPORARY TABLE IF NOT EXISTS  dsr_u AS 
WITH m as (SELECT DISTINCT Month_IDNT
            FROM location_plans),
 dsr AS (SELECT sbclass_num,
   store_num,
   week_num,
   supp_part_num,
   class_num,
   dept_num,
   supp_num,
   vndr_label_code,
   business_unit_num,
   banner_country_num,
   channel_num,
   month_num,
   quarter_num,
   half_num,
   year_num,
   rp_ind,
   npg_ind,
   comp_status_code,
   store_country_code,
   jwn_demand_total_units_ty,
   jwn_demand_total_retail_amt_ty,
   jwn_demand_regular_units_ty,
   jwn_demand_regular_retail_amt_ty,
   jwn_demand_promo_units_ty,
   jwn_demand_promo_retail_amt_ty,
   jwn_demand_clearance_units_ty,
   jwn_demand_clearance_retail_amt_ty,
   jwn_demand_store_fulfilled_units_ty,
   jwn_demand_store_fulfilled_retail_amt_ty,
   jwn_demand_store_fulfilled_regular_units_ty,
   jwn_demand_store_fulfilled_regular_retail_amt_ty,
   jwn_demand_store_fulfilled_promo_units_ty,
   jwn_demand_store_fulfilled_promo_retail_amt_ty,
   jwn_demand_store_fulfilled_clearance_units_ty,
   jwn_demand_store_fulfilled_clearance_retail_amt_ty,
   jwn_demand_persistent_units_ty,
   jwn_demand_persistent_retail_amt_ty,
   jwn_demand_persistent_regular_units_ty,
   jwn_demand_persistent_regular_retail_amt_ty,
   jwn_demand_persistent_promo_units_ty,
   jwn_demand_persistent_promo_retail_amt_ty,
   jwn_demand_persistent_clearance_units_ty,
   jwn_demand_persistent_clearance_retail_amt_ty,
   jwn_demand_other_fulfilled_units_ty,
   jwn_demand_other_fulfilled_retail_amt_ty,
   jwn_demand_other_fulfilled_regular_units_ty,
   jwn_demand_other_fulfilled_regular_retail_amt_ty,
   jwn_demand_other_fulfilled_promo_units_ty,
   jwn_demand_other_fulfilled_promo_retail_amt_ty,
   jwn_demand_other_fulfilled_clearance_units_ty,
   jwn_demand_other_fulfilled_clearance_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_units_ty,
   jwn_demand_fulfillment_center_fulfilled_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_regular_units_ty,
   jwn_demand_fulfillment_center_fulfilled_regular_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_promo_units_ty,
   jwn_demand_fulfillment_center_fulfilled_promo_retail_amt_ty,
   jwn_demand_fulfillment_center_fulfilled_clearance_units_ty,
   jwn_demand_fulfillment_center_fulfilled_clearance_retail_amt_ty,
   jwn_demand_flash_units_ty,
   jwn_demand_flash_retail_amt_ty,
   jwn_demand_flash_regular_units_ty,
   jwn_demand_flash_regular_retail_amt_ty,
   jwn_demand_flash_promo_units_ty,
   jwn_demand_flash_promo_retail_amt_ty,
   jwn_demand_flash_clearance_units_ty,
   jwn_demand_flash_clearance_retail_amt_ty,
   jwn_demand_dropship_fulfilled_units_ty,
   jwn_demand_dropship_fulfilled_retail_amt_ty,
   jwn_demand_dropship_fulfilled_regular_units_ty,
   jwn_demand_dropship_fulfilled_regular_retail_amt_ty,
   jwn_demand_dropship_fulfilled_promo_units_ty,
   jwn_demand_dropship_fulfilled_promo_retail_amt_ty,
   jwn_demand_dropship_fulfilled_clearance_units_ty,
   jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,
   jwn_operational_gmv_total_units_ty,
   jwn_operational_gmv_total_retail_amt_ty,
   jwn_operational_gmv_total_cost_amt_ty,
   jwn_operational_gmv_regular_units_ty,
   jwn_operational_gmv_regular_retail_amt_ty,
   jwn_operational_gmv_regular_cost_amt_ty,
   jwn_operational_gmv_promo_units_ty,
   jwn_operational_gmv_promo_retail_amt_ty,
   jwn_operational_gmv_promo_cost_amt_ty,
   jwn_operational_gmv_clearance_units_ty,
   jwn_operational_gmv_clearance_retail_amt_ty,
   jwn_operational_gmv_clearance_cost_amt_ty,
   jwn_operational_gmv_persistent_units_ty,
   jwn_operational_gmv_persistent_retail_amt_ty,
   jwn_operational_gmv_persistent_cost_amt_ty,
   jwn_operational_gmv_persistent_regular_units_ty,
   jwn_operational_gmv_persistent_regular_retail_amt_ty,
   jwn_operational_gmv_persistent_regular_cost_amt_ty,
   jwn_operational_gmv_persistent_promo_units_ty,
   jwn_operational_gmv_persistent_promo_retail_amt_ty,
   jwn_operational_gmv_persistent_promo_cost_amt_ty,
   jwn_operational_gmv_persistent_clearance_units_ty,
   jwn_operational_gmv_persistent_clearance_retail_amt_ty,
   jwn_operational_gmv_persistent_clearance_cost_amt_ty,
   jwn_operational_gmv_flash_units_ty,
   jwn_operational_gmv_flash_retail_amt_ty,
   jwn_operational_gmv_flash_cost_amt_ty,
   jwn_operational_gmv_flash_regular_units_ty,
   jwn_operational_gmv_flash_regular_retail_amt_ty,
   jwn_operational_gmv_flash_regular_cost_amt_ty,
   jwn_operational_gmv_flash_promo_units_ty,
   jwn_operational_gmv_flash_promo_retail_amt_ty,
   jwn_operational_gmv_flash_promo_cost_amt_ty,
   jwn_operational_gmv_flash_clearance_units_ty,
   jwn_operational_gmv_flash_clearance_retail_amt_ty,
   jwn_operational_gmv_flash_clearance_cost_amt_ty,
   jwn_gross_sales_total_units_ty,
   jwn_gross_sales_total_retail_amt_ty,
   jwn_gross_sales_total_cost_amt_ty,
   jwn_gross_sales_regular_units_ty,
   jwn_gross_sales_regular_retail_amt_ty,
   jwn_gross_sales_regular_cost_amt_ty,
   jwn_gross_sales_promo_units_ty,
   jwn_gross_sales_promo_retail_amt_ty,
   jwn_gross_sales_promo_cost_amt_ty,
   jwn_gross_sales_clearance_units_ty,
   jwn_gross_sales_clearance_retail_amt_ty,
   jwn_gross_sales_clearance_cost_amt_ty,
   jwn_returns_total_units_ty,
   jwn_returns_total_retail_amt_ty,
   jwn_returns_total_cost_amt_ty,
   jwn_returns_regular_units_ty,
   jwn_returns_regular_retail_amt_ty,
   jwn_returns_regular_cost_amt_ty,
   jwn_returns_promo_units_ty,
   jwn_returns_promo_retail_amt_ty,
   jwn_returns_promo_cost_amt_ty,
   jwn_returns_clearance_units_ty,
   jwn_returns_clearance_retail_amt_ty,
   jwn_returns_clearance_cost_amt_ty,
   receipts_total_units_ty,
   receipts_total_retail_amt_ty,
   receipts_total_cost_amt_ty,
   receipts_po_units_ty,
   receipts_po_retail_amt_ty,
   receipts_po_cost_amt_ty,
   receipts_dropship_units_ty,
   receipts_dropship_retail_amt_ty,
   receipts_dropship_cost_amt_ty,
   transfer_in_pack_and_hold_units_ty,
   transfer_in_pack_and_hold_retail_amt_ty,
   transfer_in_pack_and_hold_cost_amt_ty,
   transfer_in_reserve_stock_units_ty,
   transfer_in_reserve_stock_retail_amt_ty,
   transfer_in_reserve_stock_cost_amt_ty,
   transfer_in_racking_units_ty,
   transfer_in_racking_retail_amt_ty,
   transfer_in_racking_cost_amt_ty,
   transfer_out_racking_units_ty,
   transfer_out_racking_retail_amt_ty,
   transfer_out_racking_cost_amt_ty,
   transfer_in_return_to_rack_units_ty,
   transfer_in_return_to_rack_retail_amt_ty,
   transfer_in_return_to_rack_cost_amt_ty,
   inventory_boh_total_units_ty,
   inventory_boh_total_retail_amt_ty,
   inventory_boh_total_cost_amt_ty,
   inventory_boh_regular_units_ty,
   inventory_boh_regular_retail_amt_ty,
   inventory_boh_regular_cost_amt_ty,
   inventory_boh_clearance_units_ty,
   inventory_boh_clearance_retail_amt_ty,
   inventory_boh_clearance_cost_amt_ty,
   inventory_eoh_total_units_ty,
   inventory_eoh_total_retail_amt_ty,
   inventory_eoh_total_cost_amt_ty,
   inventory_eoh_regular_units_ty,
   inventory_eoh_regular_retail_amt_ty,
   inventory_eoh_regular_cost_amt_ty,
   inventory_eoh_clearance_units_ty,
   inventory_eoh_clearance_retail_amt_ty,
   inventory_eoh_clearance_cost_amt_ty,
   rtv_total_units_ty,
   rtv_total_retail_amt_ty,
   rtv_total_cost_amt_ty,
   jwn_demand_total_units_ly,
   jwn_demand_total_retail_amt_ly,
   jwn_demand_regular_units_ly,
   jwn_demand_regular_retail_amt_ly,
   jwn_demand_promo_units_ly,
   jwn_demand_promo_retail_amt_ly,
   jwn_demand_clearance_units_ly,
   jwn_demand_clearance_retail_amt_ly,
   jwn_demand_store_fulfilled_units_ly,
   jwn_demand_store_fulfilled_retail_amt_ly,
   jwn_demand_store_fulfilled_regular_units_ly,
   jwn_demand_store_fulfilled_regular_retail_amt_ly,
   jwn_demand_store_fulfilled_promo_units_ly,
   jwn_demand_store_fulfilled_promo_retail_amt_ly,
   jwn_demand_store_fulfilled_clearance_units_ly,
   jwn_demand_store_fulfilled_clearance_retail_amt_ly,
   jwn_demand_persistent_units_ly,
   jwn_demand_persistent_retail_amt_ly,
   jwn_demand_persistent_regular_units_ly,
   jwn_demand_persistent_regular_retail_amt_ly,
   jwn_demand_persistent_promo_units_ly,
   jwn_demand_persistent_promo_retail_amt_ly,
   jwn_demand_persistent_clearance_units_ly,
   jwn_demand_persistent_clearance_retail_amt_ly,
   jwn_demand_other_fulfilled_units_ly,
   jwn_demand_other_fulfilled_retail_amt_ly,
   jwn_demand_other_fulfilled_regular_units_ly,
   jwn_demand_other_fulfilled_regular_retail_amt_ly,
   jwn_demand_other_fulfilled_promo_units_ly,
   jwn_demand_other_fulfilled_promo_retail_amt_ly,
   jwn_demand_other_fulfilled_clearance_units_ly,
   jwn_demand_other_fulfilled_clearance_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_units_ly,
   jwn_demand_fulfillment_center_fulfilled_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_regular_units_ly,
   jwn_demand_fulfillment_center_fulfilled_regular_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_promo_units_ly,
   jwn_demand_fulfillment_center_fulfilled_promo_retail_amt_ly,
   jwn_demand_fulfillment_center_fulfilled_clearance_units_ly,
   jwn_demand_fulfillment_center_fulfilled_clearance_retail_amt_ly,
   jwn_demand_flash_units_ly,
   jwn_demand_flash_retail_amt_ly,
   jwn_demand_flash_regular_units_ly,
   jwn_demand_flash_regular_retail_amt_ly,
   jwn_demand_flash_promo_units_ly,
   jwn_demand_flash_promo_retail_amt_ly,
   jwn_demand_flash_clearance_units_ly,
   jwn_demand_flash_clearance_retail_amt_ly,
   jwn_demand_dropship_fulfilled_units_ly,
   jwn_demand_dropship_fulfilled_retail_amt_ly,
   jwn_demand_dropship_fulfilled_regular_units_ly,
   jwn_demand_dropship_fulfilled_regular_retail_amt_ly,
   jwn_demand_dropship_fulfilled_promo_units_ly,
   jwn_demand_dropship_fulfilled_promo_retail_amt_ly,
   jwn_demand_dropship_fulfilled_clearance_units_ly,
   jwn_demand_dropship_fulfilled_clearance_retail_amt_ly,
   jwn_operational_gmv_total_units_ly,
   jwn_operational_gmv_total_retail_amt_ly,
   jwn_operational_gmv_total_cost_amt_ly,
   jwn_operational_gmv_regular_units_ly,
   jwn_operational_gmv_regular_retail_amt_ly,
   jwn_operational_gmv_regular_cost_amt_ly,
   jwn_operational_gmv_promo_units_ly,
   jwn_operational_gmv_promo_retail_amt_ly,
   jwn_operational_gmv_promo_cost_amt_ly,
   jwn_operational_gmv_clearance_units_ly,
   jwn_operational_gmv_clearance_retail_amt_ly,
   jwn_operational_gmv_clearance_cost_amt_ly,
   jwn_operational_gmv_persistent_units_ly,
   jwn_operational_gmv_persistent_retail_amt_ly,
   jwn_operational_gmv_persistent_cost_amt_ly,
   jwn_operational_gmv_persistent_regular_units_ly,
   jwn_operational_gmv_persistent_regular_retail_amt_ly,
   jwn_operational_gmv_persistent_regular_cost_amt_ly,
   jwn_operational_gmv_persistent_promo_units_ly,
   jwn_operational_gmv_persistent_promo_retail_amt_ly,
   jwn_operational_gmv_persistent_promo_cost_amt_ly,
   jwn_operational_gmv_persistent_clearance_units_ly,
   jwn_operational_gmv_persistent_clearance_retail_amt_ly,
   jwn_operational_gmv_persistent_clearance_cost_amt_ly,
   jwn_operational_gmv_flash_units_ly,
   jwn_operational_gmv_flash_retail_amt_ly,
   jwn_operational_gmv_flash_cost_amt_ly,
   jwn_operational_gmv_flash_regular_units_ly,
   jwn_operational_gmv_flash_regular_retail_amt_ly,
   jwn_operational_gmv_flash_regular_cost_amt_ly,
   jwn_operational_gmv_flash_promo_units_ly,
   jwn_operational_gmv_flash_promo_retail_amt_ly,
   jwn_operational_gmv_flash_promo_cost_amt_ly,
   jwn_operational_gmv_flash_clearance_units_ly,
   jwn_operational_gmv_flash_clearance_retail_amt_ly,
   jwn_operational_gmv_flash_clearance_cost_amt_ly,
   jwn_gross_sales_total_units_ly,
   jwn_gross_sales_total_retail_amt_ly,
   jwn_gross_sales_total_cost_amt_ly,
   jwn_gross_sales_regular_units_ly,
   jwn_gross_sales_regular_retail_amt_ly,
   jwn_gross_sales_regular_cost_amt_ly,
   jwn_gross_sales_promo_units_ly,
   jwn_gross_sales_promo_retail_amt_ly,
   jwn_gross_sales_promo_cost_amt_ly,
   jwn_gross_sales_clearance_units_ly,
   jwn_gross_sales_clearance_retail_amt_ly,
   jwn_gross_sales_clearance_cost_amt_ly,
   jwn_returns_total_units_ly,
   jwn_returns_total_retail_amt_ly,
   jwn_returns_total_cost_amt_ly,
   jwn_returns_regular_units_ly,
   jwn_returns_regular_retail_amt_ly,
   jwn_returns_regular_cost_amt_ly,
   jwn_returns_promo_units_ly,
   jwn_returns_promo_retail_amt_ly,
   jwn_returns_promo_cost_amt_ly,
   jwn_returns_clearance_units_ly,
   jwn_returns_clearance_retail_amt_ly,
   jwn_returns_clearance_cost_amt_ly,
   receipts_total_units_ly,
   receipts_total_retail_amt_ly,
   receipts_total_cost_amt_ly,
   receipts_po_units_ly,
   receipts_po_retail_amt_ly,
   receipts_po_cost_amt_ly,
   receipts_dropship_units_ly,
   receipts_dropship_retail_amt_ly,
   receipts_dropship_cost_amt_ly,
   transfer_in_pack_and_hold_units_ly,
   transfer_in_pack_and_hold_retail_amt_ly,
   transfer_in_pack_and_hold_cost_amt_ly,
   transfer_in_reserve_stock_units_ly,
   transfer_in_reserve_stock_retail_amt_ly,
   transfer_in_reserve_stock_cost_amt_ly,
   transfer_in_racking_units_ly,
   transfer_in_racking_retail_amt_ly,
   transfer_in_racking_cost_amt_ly,
   transfer_out_racking_units_ly,
   transfer_out_racking_retail_amt_ly,
   transfer_out_racking_cost_amt_ly,
   transfer_in_return_to_rack_units_ly,
   transfer_in_return_to_rack_retail_amt_ly,
   transfer_in_return_to_rack_cost_amt_ly,
   inventory_boh_total_units_ly,
   inventory_boh_total_retail_amt_ly,
   inventory_boh_total_cost_amt_ly,
   inventory_boh_regular_units_ly,
   inventory_boh_regular_retail_amt_ly,
   inventory_boh_regular_cost_amt_ly,
   inventory_boh_clearance_units_ly,
   inventory_boh_clearance_retail_amt_ly,
   inventory_boh_clearance_cost_amt_ly,
   inventory_eoh_total_units_ly,
   inventory_eoh_total_retail_amt_ly,
   inventory_eoh_total_cost_amt_ly,
   inventory_eoh_regular_units_ly,
   inventory_eoh_regular_retail_amt_ly,
   inventory_eoh_regular_cost_amt_ly,
   inventory_eoh_clearance_units_ly,
   inventory_eoh_clearance_retail_amt_ly,
   inventory_eoh_clearance_cost_amt_ly,
   rtv_total_units_ly,
   rtv_total_retail_amt_ly,
   rtv_total_cost_amt_ly,
   mos_total_cost_amt_ty,
   mos_regular_cost_amt_ty,
   mos_clearance_cost_amt_ty,
   mos_total_units_ty,
   mos_regular_units_ty,
   mos_clearance_units_ty,
   mos_total_retail_amt_ty,
   mos_regular_retail_amt_ty,
   mos_clearance_retail_amt_ty,
   mos_total_cost_amt_ly,
   mos_regular_cost_amt_ly,
   mos_clearance_cost_amt_ly,
   mos_total_units_ly,
   mos_regular_units_ly,
   mos_clearance_units_ly,
   mos_total_retail_amt_ly,
   mos_regular_retail_amt_ly,
   mos_clearance_retail_amt_ly,
   receipts_regular_retail_amt_ty,
   receipts_regular_units_ty,
   receipts_regular_cost_amt_ty,
   receipts_clearance_retail_amt_ty,
   receipts_clearance_units_ty,
   receipts_clearance_cost_amt_ty,
   receipts_crossdock_regular_retail_amt_ty,
   receipts_crossdock_regular_units_ty,
   receipts_crossdock_regular_cost_amt_ty,
   receipts_crossdock_clearance_retail_amt_ty,
   receipts_crossdock_clearance_units_ty,
   receipts_crossdock_clearance_cost_amt_ty,
   receipts_regular_retail_amt_ly,
   receipts_regular_units_ly,
   receipts_regular_cost_amt_ly,
   receipts_clearance_retail_amt_ly,
   receipts_clearance_units_ly,
   receipts_clearance_cost_amt_ly,
   receipts_crossdock_regular_retail_amt_ly,
   receipts_crossdock_regular_units_ly,
   receipts_crossdock_regular_cost_amt_ly,
   receipts_crossdock_clearance_retail_amt_ly,
   receipts_crossdock_clearance_units_ly,
   receipts_crossdock_clearance_cost_amt_ly,
   transfer_in_racking_regular_cost_amt_ty,
   transfer_in_racking_regular_units_ty,
   transfer_in_racking_clearance_cost_amt_ty,
   transfer_in_racking_clearance_units_ty,
   transfer_out_racking_regular_cost_amt_ty,
   transfer_out_racking_regular_units_ty,
   transfer_out_racking_clearance_cost_amt_ty,
   transfer_out_racking_clearance_units_ty,
   transfer_in_pack_and_hold_regular_cost_amt_ty,
   transfer_in_pack_and_hold_regular_units_ty,
   transfer_in_pack_and_hold_clearance_cost_amt_ty,
   transfer_in_pack_and_hold_clearance_units_ty,
   transfer_out_pack_and_hold_cost_amt_ty,
   transfer_out_pack_and_hold_units_ty,
   transfer_out_pack_and_hold_regular_cost_amt_ty,
   transfer_out_pack_and_hold_regular_units_ty,
   transfer_out_pack_and_hold_clearance_cost_amt_ty,
   transfer_out_pack_and_hold_clearance_units_ty,
   transfer_in_reserve_stock_regular_cost_amt_ty,
   transfer_in_reserve_stock_regular_units_ty,
   transfer_in_reserve_stock_clearance_cost_amt_ty,
   transfer_in_reserve_stock_clearance_units_ty,
   transfer_out_reserve_stock_cost_amt_ty,
   transfer_out_reserve_stock_units_ty,
   transfer_out_reserve_stock_regular_cost_amt_ty,
   transfer_out_reserve_stock_regular_units_ty,
   transfer_out_reserve_stock_clearance_cost_amt_ty,
   transfer_out_reserve_stock_clearance_units_ty,
   transfer_in_racking_regular_cost_amt_ly,
   transfer_in_racking_regular_units_ly,
   transfer_in_racking_clearance_cost_amt_ly,
   transfer_in_racking_clearance_units_ly,
   transfer_out_racking_regular_cost_amt_ly,
   transfer_out_racking_regular_units_ly,
   transfer_out_racking_clearance_cost_amt_ly,
   transfer_out_racking_clearance_units_ly,
   transfer_in_pack_and_hold_regular_cost_amt_ly,
   transfer_in_pack_and_hold_regular_units_ly,
   transfer_in_pack_and_hold_clearance_cost_amt_ly,
   transfer_in_pack_and_hold_clearance_units_ly,
   transfer_out_pack_and_hold_cost_amt_ly,
   transfer_out_pack_and_hold_units_ly,
   transfer_out_pack_and_hold_regular_cost_amt_ly,
   transfer_out_pack_and_hold_regular_units_ly,
   transfer_out_pack_and_hold_clearance_cost_amt_ly,
   transfer_out_pack_and_hold_clearance_units_ly,
   transfer_in_reserve_stock_regular_cost_amt_ly,
   transfer_in_reserve_stock_regular_units_ly,
   transfer_in_reserve_stock_clearance_cost_amt_ly,
   transfer_in_reserve_stock_clearance_units_ly,
   transfer_out_reserve_stock_cost_amt_ly,
   transfer_out_reserve_stock_units_ly,
   transfer_out_reserve_stock_regular_cost_amt_ly,
   transfer_out_reserve_stock_regular_units_ly,
   transfer_out_reserve_stock_clearance_cost_amt_ly,
   transfer_out_reserve_stock_clearance_units_ly,
   inventory_eoh_in_transit_regular_units_ty,
   inventory_eoh_in_transit_regular_cost_amt_ty,
   inventory_eoh_in_transit_regular_retail_amt_ty,
   inventory_eoh_in_transit_clearance_units_ty,
   inventory_eoh_in_transit_clearance_cost_amt_ty,
   inventory_eoh_in_transit_clearance_retail_amt_ty,
   inventory_eoh_in_transit_total_units_ty,
   inventory_eoh_in_transit_total_cost_amt_ty,
   inventory_eoh_in_transit_total_retail_amt_ty,
   inventory_boh_in_transit_regular_units_ty,
   inventory_boh_in_transit_regular_cost_amt_ty,
   inventory_boh_in_transit_regular_retail_amt_ty,
   inventory_boh_in_transit_clearance_units_ty,
   inventory_boh_in_transit_clearance_cost_amt_ty,
   inventory_boh_in_transit_clearance_retail_amt_ty,
   inventory_boh_in_transit_total_units_ty,
   inventory_boh_in_transit_total_cost_amt_ty,
   inventory_boh_in_transit_total_retail_amt_ty,
   inventory_eoh_in_transit_regular_units_ly,
   inventory_eoh_in_transit_regular_cost_amt_ly,
   inventory_eoh_in_transit_regular_retail_amt_ly,
   inventory_eoh_in_transit_clearance_units_ly,
   inventory_eoh_in_transit_clearance_cost_amt_ly,
   inventory_eoh_in_transit_clearance_retail_amt_ly,
   inventory_eoh_in_transit_total_units_ly,
   inventory_eoh_in_transit_total_cost_amt_ly,
   inventory_eoh_in_transit_total_retail_amt_ly,
   inventory_boh_in_transit_regular_units_ly,
   inventory_boh_in_transit_regular_cost_amt_ly,
   inventory_boh_in_transit_regular_retail_amt_ly,
   inventory_boh_in_transit_clearance_units_ly,
   inventory_boh_in_transit_clearance_cost_amt_ly,
   inventory_boh_in_transit_clearance_retail_amt_ly,
   inventory_boh_in_transit_total_units_ly,
   inventory_boh_in_transit_total_cost_amt_ly,
   inventory_boh_in_transit_total_retail_amt_ly,
   dw_sys_load_tmstp,
   dw_sys_load_dt,
   CAST(trunc(cast(CASE
     WHEN CASE
       WHEN store_num IN (210, 212)
       THEN '209'
       ELSE FORMAT('%11d', store_num)
       END = ''
     THEN '0'
     ELSE CASE
      WHEN store_num IN (210, 212)
      THEN '209'
      ELSE FORMAT('%11d', store_num)
      END
     END as float64))AS INTEGER) AS store_num_m
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
  WHERE channel_num IN (110, 210)
   AND month_num IN (SELECT month_idnt
     FROM m) ) (SELECT REPLACE(CONCAT(class_num, dept_num, month_num, store_num_m), ' ', '') AS pkey,
   dept_num,
   class_num,
   REPLACE(CONCAT(class_num, dept_num, month_num, store_num_m), ' ', '') AS store_num,
   month_num,
   SUM(transfer_in_pack_and_hold_units_ty) AS total_transfer_in_pack_and_hold_units_ty,
   SUM(transfer_in_reserve_stock_units_ty) AS total_transfer_in_reserve_stock_units_ty,
   SUM(transfer_in_racking_units_ty) AS total_transfer_in_racking_units_ty,
   SUM(transfer_in_return_to_rack_units_ty) AS total_transfer_in_return_to_rack_units_ty,
   SUM(receipts_total_units_ty) AS total_receipts_total_units_ty,
   SUM(jwn_gross_sales_total_units_ty) AS total_jwn_gross_sales_total_units_ty,
   SUM(jwn_demand_total_units_ty) AS total_jwn_demand_total_units_ty,
   SUM(jwn_operational_gmv_total_units_ty) AS total_jwn_operational_gmv_total_units_ty,
   SUM(jwn_returns_total_units_ty) AS total_jwn_returns_total_units_ty,
   SUM(jwn_demand_regular_units_ty) AS total_jwn_demand_regular_units_ty,
   SUM(jwn_operational_gmv_regular_units_ty) AS total_jwn_operational_gmv_regular_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_pack_and_hold_units_ty
     ELSE NULL
     END) AS rp_transfer_in_pack_and_hold_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_reserve_stock_units_ty
     ELSE NULL
     END) AS rp_transfer_in_reserve_stock_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_racking_units_ty
     ELSE NULL
     END) AS rp_transfer_in_racking_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_return_to_rack_units_ty
     ELSE NULL
     END) AS rp_transfer_in_return_to_rack_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN receipts_total_units_ty
     ELSE NULL
     END) AS rp_receipts_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_gross_sales_total_units_ty
     ELSE NULL
     END) AS rp_jwn_gross_sales_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_demand_total_units_ty
     ELSE NULL
     END) AS rp_jwn_demand_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_operational_gmv_total_units_ty
     ELSE NULL
     END) AS rp_jwn_operational_gmv_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_returns_total_units_ty
     ELSE NULL
     END) AS rp_jwn_returns_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_pack_and_hold_units_ty
     ELSE NULL
     END) AS nrp_transfer_in_pack_and_hold_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_reserve_stock_units_ty
     ELSE NULL
     END) AS nrp_transfer_in_reserve_stock_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_racking_units_ty
     ELSE NULL
     END) AS nrp_transfer_in_racking_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_return_to_rack_units_ty
     ELSE NULL
     END) AS nrp_transfer_in_return_to_rack_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN receipts_total_units_ty
     ELSE NULL
     END) AS nrp_receipts_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_gross_sales_total_units_ty
     ELSE NULL
     END) AS nrp_jwn_gross_sales_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_demand_total_units_ty
     ELSE NULL
     END) AS nrp_jwn_demand_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_operational_gmv_total_units_ty
     ELSE NULL
     END) AS nrp_jwn_operational_gmv_total_units_ty,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_returns_total_units_ty
     ELSE NULL
     END) AS nrp_jwn_returns_total_units_ty,
   SUM(transfer_in_pack_and_hold_units_ly) AS total_transfer_in_pack_and_hold_units_ly,
   SUM(transfer_in_reserve_stock_units_ly) AS total_transfer_in_reserve_stock_units_ly,
   SUM(transfer_in_racking_units_ly) AS total_transfer_in_racking_units_ly,
   SUM(transfer_in_return_to_rack_units_ly) AS total_transfer_in_return_to_rack_units_ly,
   SUM(receipts_total_units_ly) AS total_receipts_total_units_ly,
   SUM(jwn_gross_sales_total_units_ly) AS total_jwn_gross_sales_total_units_ly,
   SUM(jwn_demand_total_units_ly) AS total_jwn_demand_total_units_ly,
   SUM(jwn_operational_gmv_total_units_ly) AS total_jwn_operational_gmv_total_units_ly,
   SUM(jwn_returns_total_units_ly) AS total_jwn_returns_total_units_ly,
   SUM(jwn_demand_regular_units_ly) AS total_jwn_demand_regular_units_ly,
   SUM(jwn_operational_gmv_regular_units_ly) AS total_jwn_operational_gmv_regular_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_pack_and_hold_units_ly
     ELSE NULL
     END) AS rp_transfer_in_pack_and_hold_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_reserve_stock_units_ly
     ELSE NULL
     END) AS rp_transfer_in_reserve_stock_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_racking_units_ly
     ELSE NULL
     END) AS rp_transfer_in_racking_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN transfer_in_return_to_rack_units_ly
     ELSE NULL
     END) AS rp_transfer_in_return_to_rack_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN receipts_total_units_ly
     ELSE NULL
     END) AS rp_receipts_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_gross_sales_total_units_ly
     ELSE NULL
     END) AS rp_jwn_gross_sales_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_demand_total_units_ly
     ELSE NULL
     END) AS rp_jwn_demand_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_operational_gmv_total_units_ly
     ELSE NULL
     END) AS rp_jwn_operational_gmv_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('Y')
     THEN jwn_returns_total_units_ly
     ELSE NULL
     END) AS rp_jwn_returns_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_pack_and_hold_units_ly
     ELSE NULL
     END) AS nrp_transfer_in_pack_and_hold_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_reserve_stock_units_ly
     ELSE NULL
     END) AS nrp_transfer_in_reserve_stock_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_racking_units_ly
     ELSE NULL
     END) AS nrp_transfer_in_racking_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN transfer_in_return_to_rack_units_ly
     ELSE NULL
     END) AS nrp_transfer_in_return_to_rack_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN receipts_total_units_ly
     ELSE NULL
     END) AS nrp_receipts_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_gross_sales_total_units_ly
     ELSE NULL
     END) AS nrp_jwn_gross_sales_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_demand_total_units_ly
     ELSE NULL
     END) AS nrp_jwn_demand_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_operational_gmv_total_units_ly
     ELSE NULL
     END) AS nrp_jwn_operational_gmv_total_units_ly,
   SUM(CASE
     WHEN LOWER(rp_ind) = LOWER('N')
     THEN jwn_returns_total_units_ly
     ELSE NULL
     END) AS nrp_jwn_returns_total_units_ly
  FROM dsr
  GROUP BY pkey,
   dept_num,
   class_num,
   month_num);

--COLLECT STATS PRIMARY INDEX(pkey) ,COLUMN (class_num,dept_num) ON dsr_u;

CREATE  TEMPORARY TABLE IF NOT EXISTS  po_oo_u AS WITH po_oo_u AS (SELECT rms_sku_num,
   purchase_order_number,
   department_num,
   department_desc,
   class_num,
   class_desc,
   subclass_num,
   subclass_desc,
   division_num,
   division_desc,
   subdivision_num,
   subdivision_desc,
   supplier_num,
   supplier_name,
   smart_sample_ind,
   gift_with_purchase_ind,
   npg_ind,
   week_num,
   year_num,
   month_num,
   month_label,
   store_num,
   channel_num,
   drop_ship_eligible_ind,
   rp_ind,
   banner_num,
   rp_oo_active_cost,
   rp_oo_active_units,
   rp_oo_active_retail,
   non_rp_oo_active_cost,
   non_rp_oo_active_units,
   non_rp_oo_active_retail,
   oo_inactive_cost,
   oo_inactive_units,
   oo_inactive_retail,
   CAST(trunc(cast(CASE
     WHEN CASE
       WHEN store_num IN (210, 212)
       THEN '209'
       ELSE FORMAT('%11d', store_num)
       END = ''
     THEN '0'
     ELSE CASE
      WHEN store_num IN (210, 212)
      THEN '209'
      ELSE FORMAT('%11d', store_num)
      END
     END as float64)) AS INTEGER) AS store_num_m
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_apt_on_order_insight_fact_vw AS a
  WHERE channel_num IN (110, 210)
   AND month_num IN (SELECT DISTINCT month_idnt
     FROM location_plans)) (SELECT REPLACE(CONCAT(class_num, department_num, month_num, store_num_m), ' ', '') AS pkey
   ,
   department_num,
   class_num,
   REPLACE(CONCAT(class_num, department_num, month_num, store_num_m), ' ', '') AS store_num,
   month_num,
   SUM(non_rp_oo_active_units) AS nrp_po_oo_u,
   SUM(rp_oo_active_units) AS rp_po_oo_u,
    SUM(non_rp_oo_active_units) + SUM(rp_oo_active_units) AS total_po_oo_u
  FROM po_oo_u
  GROUP BY pkey,
   department_num,
   class_num,
   month_num);

--COLLECT STATS PRIMARY INDEX(pkey) ON po_oo_u;

CREATE  TEMPORARY TABLE IF NOT EXISTS  rp_demand_u AS WITH mth AS (SELECT DISTINCT week_idnt,
   month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE month_idnt IN (SELECT DISTINCT month_idnt
     FROM location_plans AS l)) (SELECT t25.pkey,
   t25.month_idnt,
   t25.store_idnt,
   t25.dept_num,
   t25.class_num,
   t25.chnl_idnt,
   SUM(t25.inventory_forecast_qty) AS rp_sales_forecast,
   t25.total_mth_forecast,
    t25.total_mth_forecast / t25.total_mth_forecast AS prcnt_of_rp_forecast
  FROM (SELECT REPLACE(CONCAT(ps.class_num, ps.dept_num, t5.month_idnt, frcst.location_id), ' ', '') AS pkey,
     t5.month_idnt,
     frcst.location_id AS store_idnt,
     ps.dept_num,
     ps.class_num,
      CASE
      WHEN LOWER(sd.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'))
      THEN 110
      WHEN LOWER(sd.business_unit_desc) IN (LOWER('RACK'), LOWER('OFFPRICE ONLINE'))
      THEN 210
      ELSE NULL
      END AS chnl_idnt,
     frcst.inventory_forecast_qty,
     SUM(SUM(frcst.inventory_forecast_qty)) OVER (PARTITION BY t5.month_idnt, CASE
         WHEN LOWER(sd.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'))
         THEN 110
         WHEN LOWER(sd.business_unit_desc) IN (LOWER('RACK'), LOWER('OFFPRICE ONLINE'))
         THEN 210
         ELSE NULL
         END, ps.dept_num ORDER BY SUM(frcst.inventory_forecast_qty) DESC RANGE BETWEEN UNBOUNDED PRECEDING AND
      UNBOUNDED FOLLOWING) AS total_mth_forecast
    FROM (SELECT sku_id,
       sku_type,
       location_id,
       week_id,
       last_updated_time,
       approved_date,
       approved_by_user_id,
       approved_by_user_type,
       approved_by_service_account,
       service_name,
       forecast_method,
       inventory_forecast_qty,
       standard_deviation,
       dw_batch_id,
       dw_batch_date,
       dw_sys_load_tmstp,
       dw_sys_updt_tmstp
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_approved_weekly_deployment_forecast_fact
      WHERE inventory_forecast_qty > 0) AS frcst
     LEFT JOIN mth AS t5 ON t5.week_idnt = CAST(frcst.week_id AS FLOAT64)
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS ps ON LOWER(frcst.sku_id) = LOWER(SUBSTR(CAST(ps.epm_sku_num AS STRING)
        , 1, 100))
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON sd.store_num = CAST(frcst.location_id AS FLOAT64)
    WHERE CAST(trunc(cast(frcst.location_id as float64)) AS INTEGER) IN (SELECT DISTINCT store_num
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
       WHERE LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('RACK'), LOWER('OFFPRICE ONLINE'))
       )
     AND t5.month_idnt IN (SELECT DISTINCT month_idnt
       FROM mth)
     AND ps.dept_num IN (SELECT DISTINCT dept_idnt
       FROM `{{params.gcp_project_id}}`.t2dl_das_location_planning.loc_plan_prd_vw)
     AND ps.class_num IN (SELECT DISTINCT class_idnt
       FROM `{{params.gcp_project_id}}`.t2dl_das_location_planning.loc_plan_prd_vw) group by ps.class_num, ps.dept_num, t5.month_idnt,frcst.location_id 
       ,sd.business_unit_desc,frcst.inventory_forecast_qty) AS t25
  GROUP BY t25.pkey,
   t25.month_idnt,
   t25.store_idnt,
   t25.dept_num,
   t25.class_num,
   t25.chnl_idnt,
   t25.inventory_forecast_qty,
   t25.total_mth_forecast
   
    );

--COLLECT STATS PRIMARY INDEX(pkey) ON rp_demand_u;

CREATE  TEMPORARY TABLE IF NOT EXISTS  rp_ant_rcpt_u AS WITH rp_anticipated_rcpt_u AS (SELECT REPLACE(CONCAT(month_454, dept_idnt), ' ', '') AS pkey,
   month_454,
   dept_idnt,
   banner_id,
   ROUND(CAST(SUM(rp_antspnd_u) AS BIGNUMERIC), 0) AS rp_anticipated_rcpt_u
  FROM t2dl_das_open_to_buy.rp_anticipated_spend_current AS spend
  WHERE banner_id IN (1, 3)
   AND month_454 IN (SELECT DISTINCT month_idnt
     FROM location_plans AS l)
  GROUP BY pkey,
   month_454,
   dept_idnt,
   banner_id) (SELECT l.pkey,
   l.rp_sales_forecast,
   l.total_mth_forecast,
   l.prcnt_of_rp_forecast,
   r.rp_anticipated_rcpt_u,
    ROUND(CAST(l.prcnt_of_rp_forecast AS NUMERIC), 0) * r.rp_anticipated_rcpt_u AS rp_anticipated_rcpt_u_splits
  FROM rp_demand_u AS l
   LEFT JOIN rp_anticipated_rcpt_u AS r ON l.month_idnt = r.month_454 AND l.dept_num = r.dept_idnt AND CASE
      WHEN l.chnl_idnt = 110
      THEN 1
      WHEN l.chnl_idnt = 210
      THEN 3
      ELSE 0
      END = r.banner_id);



CREATE  TEMPORARY TABLE IF NOT EXISTS store_fulfill_actuals AS (
WITH m as (SELECT DISTINCT Month_IDNT
FROM location_plans)
SELECT

REPLACE(CONCAT(jdodv.class_num,jdodv.dept_num,dcd.month_idnt,CASE WHEN jdodv.fulfilled_from_location IN (210, 212, 209) THEN 209 ELSE jdodv.fulfilled_from_location END),' ','') as pkey
,dcd.month_idnt as month_num
,jdodv.dept_num
,jdodv.class_num
,CASE WHEN jdodv.fulfilled_from_location IN (210, 212, 209) THEN 209 ELSE jdodv.fulfilled_from_location END as loc_idnt
,SUM(jdodv.demand_units) AS store_fulfill_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_demand_order_detail_vw jdodv
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim dcd ON dcd.day_date = jdodv.demand_date
WHERE selling_channel IN ('ONLINE')
AND cast(jdodv.fulfilled_from_location as string) IN (SELECT  cast(sd.store_num as string)  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim sd WHERE CAST(sd.channel_num AS STRING) IN ('110','210'))
AND jdodv.canceled_tmstp_pacific IS NULL
AND jdodv.fulfilled_from_method NOT IN ('Vendor (Drop Ship)', 'BOPUS')
AND dcd.month_idnt IN (SELECT DISTINCT month_idnt FROM m)
AND jdodv.demand_store_num NOT IN (
923,
867,
835,
834,
833,
832,
831,
830)
GROUP BY 1,2,3,4,5
)
;


CREATE  TEMPORARY TABLE IF NOT EXISTS store_fulfill AS (
WITH total_sff_u as
(SELECT month_num
,SUM(store_fulfill_u) as total_jwn_demand_store_fulfilled_units_ty
FROM store_fulfill_actuals
WHERE month_num IN (SELECT DISTINCT month_idnt FROM location_plans WHERE last_month_flag = 1)
GROUP BY 1
),
sff_splits as(
SELECT REPLACE(CONCAT(w.class_num,w.dept_num,w.month_num,w.loc_idnt),' ','') as pkey
,w.class_num
,w.dept_num
,w.month_num
,w.loc_idnt as store_num

,SUM(store_fulfill_u) as jwn_demand_store_fulfilled_units_ty
FROM store_fulfill_actuals w
WHERE month_num IN (SELECT DISTINCT month_idnt FROM location_plans WHERE last_month_flag = 1)


GROUP BY 1,2,3,4,5
),
sff_percent as(
SELECT s.pkey
,s.class_num
,s.dept_num
,s.month_num
,s.store_num
,s.jwn_demand_store_fulfilled_units_ty
,u.total_jwn_demand_store_fulfilled_units_ty
,CAST(s.jwn_demand_store_fulfilled_units_ty as numeric)/u.total_jwn_demand_store_fulfilled_units_ty as percent_split
FROM sff_splits s
LEFT JOIN total_sff_u u
ON s.month_num = u.month_num
),
sff_forecast as(
SELECT
CAST(trunc(cast(CASE WHEN cast(FISCAL_MONTH  as string)IN ('1','2','3','4','5','6','7','8','9')
THEN CONCAT(FISCAL_YEAR,'0',TRIM(cast(FISCAL_MONTH as string)))
ELSE CONCAT(FISCAL_YEAR,TRIM(cast(FISCAL_MONTH as string))) END  as float64)) as int)
AS month_num
,CAST(trunc(SUM(FORECAST_VALUE) )as int) as sff_sales_forecast_u
FROM t2dl_sca_vws.metric_daily_with_current_fc_vw
WHERE BUSINESS_UNIT IN ('NCOM')
AND NODE_TYPE IN ('FLS')
AND ORDER_TYPE NOT IN ('BOPUS','DTC')
GROUP BY 1
)
SELECT
REPLACE(CONCAT(s.class_num,s.dept_num,f.month_num,s.store_num),' ','') as pkey
,f.month_num
,f.sff_sales_forecast_u
,s.class_num
,s.dept_num
,s.store_num
,s.percent_split
,CAST(s.percent_split as numeric) * f.sff_sales_forecast_u as split_sff_sales_forecast_u
FROM sff_forecast f,sff_percent s
WHERE f.month_num IN (SELECT DISTINCT month_idnt FROM location_plans WHERE last_month_flag = 0)
)
;





CREATE TEMPORARY TABLE IF NOT EXISTS in_transit_oo
AS
SELECT REPLACE(CONCAT(class_num, dept_num, (SELECT DISTINCT month_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT')), store_num), ' ', '') AS pkey,
 dept_num,
 class_num,
 store_num,
  (SELECT DISTINCT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS month_num,
 SUM(inventory_in_transit_total_units) AS in_transit_qty,
 SUM(CASE
   WHEN LOWER(rp_ind) = LOWER('Y')
   THEN inventory_in_transit_total_units
   ELSE NULL
   END) AS rp_in_transit_u,
 SUM(CASE
   WHEN LOWER(rp_ind) = LOWER('N')
   THEN inventory_in_transit_total_units
   ELSE NULL
   END) AS nrp_in_transit_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_inbound_sku_store_vw
WHERE channel_num IN (110, 210)
GROUP BY pkey,
 dept_num,
 class_num,
 store_num,
 month_num;

--COLLECT STATS PRIMARY INDEX(pkey) ON in_transit_oo;



CREATE TEMPORARY TABLE IF NOT EXISTS supplier_base_data AS
SELECT a.dept_num,
 a.class_num,
 a.store_num,
 a.month_num,
 a.supp_part_num,
 b.brand_name
FROM (SELECT dept_num,
   class_num,
   store_num,
   month_num,
   supp_part_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw
  WHERE channel_num IN (110, 210)
   AND week_num IN (SELECT DISTINCT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tdl
     WHERE month_idnt IN (SELECT DISTINCT month_idnt
        FROM location_plans))
  GROUP BY store_num,
   supp_part_num,
   class_num,
   dept_num,
   month_num) AS a
 LEFT JOIN (SELECT brand_name,
   supp_part_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  GROUP BY supp_part_num,
   brand_name) AS b ON LOWER(a.supp_part_num) = LOWER(b.supp_part_num);




CREATE TEMPORARY TABLE IF NOT EXISTS supplier_count AS WITH supplier_base AS (SELECT DISTINCT dept_num,
   class_num,
   store_num,
   month_num,
   brand_name
  FROM supplier_base_data AS a), 
  class_supplier_count AS (SELECT CAST(REPLACE(CONCAT(class_num, month_num, store_num), ' ', '') AS STRING) AS pkey,

   dept_num,
   class_num,
   store_num,
   month_num,
   COUNT(brand_name) AS class_supplier_count_brand
  FROM supplier_base
  GROUP BY pkey,
   dept_num,
   class_num,
   store_num,
   month_num), dept_base AS (SELECT DISTINCT dept_num,
   store_num,
   month_num,
   brand_name
  FROM supplier_base_data AS a), dept_supplier_count AS(SELECT CAST(REPLACE(CONCAT(dept_num, month_num, store_num), ' ', '') AS STRING) AS pkey,
   dept_num,
   store_num,
   month_num,
   COUNT(brand_name) AS dept_supplier_count_brand
  FROM dept_base
  GROUP BY pkey,
   dept_num,
   store_num,
   month_num), loc_base AS (SELECT DISTINCT store_num,
   month_num,
   brand_name
  FROM supplier_base_data AS a), location_supplier_count AS (SELECT REPLACE(CONCAT(month_num, store_num), ' ', '') AS
   pkey,
   store_num,
   month_num,
   COUNT(brand_name) AS loc_supplier_count
  FROM loc_base
  GROUP BY pkey,
   store_num,
   month_num) (SELECT c.class_pkey,
   c.dept_pkey,
   c.loc_pkey,
   c.dept_num,
   c.class_num,
   c.store_num,
   c.month_num,
   c.class_supplier_count_brand AS class_supplier_count,
   d.dept_supplier_count_brand AS dept_supplier_count,
   l.loc_supplier_count
  FROM (SELECT pkey AS class_pkey,
     
     CAST(REPLACE(CONCAT(dept_num, month_num, store_num), ' ', '') AS STRING)AS dept_pkey,

     CAST(REPLACE(CONCAT(month_num, store_num), ' ', '') AS STRING)AS loc_pkey,
     dept_num,
     class_num,
     store_num,
     month_num,
     class_supplier_count_brand
    FROM class_supplier_count AS c) AS c
   LEFT JOIN dept_supplier_count AS d ON c.dept_pkey = d.pkey
   LEFT JOIN location_supplier_count AS l ON c.loc_pkey = l.pkey);

--COLLECT STATS PRIMARY INDEX(class_pkey) ON supplier_count;



CREATE  TEMPORARY TABLE IF NOT EXISTS  store_grade AS WITH hybrid_gross_sales AS (SELECT l.pkey,
   l.division,
   l.subdivision,
   l.dept_idnt,
   l.class_idnt,
   l.loc_idnt,
   l.chnl_idnt,
   l.month_idnt,
   l.last_month_flag,
   d.total_jwn_gross_sales_total_units_ty,
   l.sales_plan,
    CASE
    WHEN l.last_month_flag = 1
    THEN d.total_jwn_gross_sales_total_units_ty
    ELSE l.sales_plan
    END AS hybrid_gross_sales_ty
  FROM location_plans AS l
   LEFT JOIN dsr_u AS d ON l.pkey = d.pkey
  WHERE l.division IS NOT NULL), month_location_aggregate AS (SELECT loc_idnt,
   month_idnt,
   chnl_idnt,
   SUM(hybrid_gross_sales_ty) AS month_location_gross_sales
  FROM hybrid_gross_sales
  GROUP BY loc_idnt,
   chnl_idnt,
   month_idnt), month_avg_std AS (SELECT month_idnt,
   chnl_idnt,
   AVG(month_location_gross_sales) AS avg_gross_sales,
   stddev_pop(month_location_gross_sales) AS std_gross_sales
  FROM (SELECT loc_idnt,
     chnl_idnt,
     month_idnt,
     SUM(hybrid_gross_sales_ty) AS month_location_gross_sales
    FROM hybrid_gross_sales
    GROUP BY loc_idnt,
     chnl_idnt,
     month_idnt) AS t9
  GROUP BY chnl_idnt,
   month_idnt) (SELECT s.loc_idnt,
   s.chnl_idnt,
   s.month_idnt,
   s.month_location_gross_sales,
   mas.avg_gross_sales,
   mas.std_gross_sales,
   CAST(trunc((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) AS INT64) AS
   location_gross_sales_z_score,
    CASE
    WHEN CAST(trunc((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) AS INT64) >= 2.5
    THEN 'A'
    WHEN CAST(trunc((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) AS INT64) >= 1
    THEN 'B'
    WHEN CAST(trunc((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) AS INT64) >= 0
    THEN 'C'
    WHEN CAST(trunc((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) AS INT64) >= - 0.5
    THEN 'D'
    WHEN CAST(trunc((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) AS INT64) >= - 1
    THEN 'E'
    ELSE 'F'
    END AS store_grade
  FROM month_location_aggregate AS s
   LEFT JOIN month_avg_std AS mas ON s.month_idnt = mas.month_idnt AND s.chnl_idnt = mas.chnl_idnt);



CREATE TEMPORARY TABLE IF NOT EXISTS total_aggregate AS (
SELECT DISTINCT l.*

, ar.AB_OO_RCPT_UNITS as ab_oo_rcpt_u
, b.total_bop_u_ty
, b.rp_bop_u_ty
, b.nrp_bop_u_ty
, b.clearance_bop_u_ty
, b.rp_clearance_bop_u_ty
, b.nrp_clearance_bop_u_ty
, b.total_bop_u_ly
, b.rp_bop_u_ly
, b.nrp_bop_u_ly
, b.clearance_bop_u_ly
, b.rp_clearance_bop_u_ly
, b.nrp_clearance_bop_u_ly
,e.total_ty_EOH_U
,e.total_ty_ClearEOH_U
,e.rp_ty_EOH_U
,e.rp_ty_ClearEOH_U
,e.nrp_ty_EOH_U
,e.nrp_ty_ClearEOH_U
,e.total_ly_EOH_U
,e.total_ly_ClearEOH_U
,e.rp_ly_EOH_U
,e.rp_ly_ClearEOH_U
,e.nrp_ly_EOH_U
,e.nrp_ly_ClearEOH_U
,d.total_transfer_in_pack_and_hold_units_ty
,d.total_transfer_in_reserve_stock_units_ty
,d.total_transfer_in_racking_units_ty
,d.total_transfer_in_return_to_rack_units_ty
,d.total_receipts_total_units_ty
,d.total_jwn_gross_sales_total_units_ty
,d.total_jwn_demand_total_units_ty
,d.total_jwn_operational_gmv_total_units_ty

,d.total_jwn_returns_total_units_ty
,d.total_jwn_demand_regular_units_ty
,d.total_jwn_operational_gmv_regular_units_ty
,d.rp_transfer_in_pack_and_hold_units_ty
,d.rp_transfer_in_reserve_stock_units_ty
,d.rp_transfer_in_racking_units_ty
,d.rp_transfer_in_return_to_rack_units_ty
,d.rp_receipts_total_units_ty
,d.rp_jwn_gross_sales_total_units_ty
,d.rp_jwn_demand_total_units_ty
,d.rp_jwn_operational_gmv_total_units_ty

,d.rp_jwn_returns_total_units_ty
,d.nrp_transfer_in_pack_and_hold_units_ty
,d.nrp_transfer_in_reserve_stock_units_ty
,d.nrp_transfer_in_racking_units_ty
,d.nrp_transfer_in_return_to_rack_units_ty
,d.nrp_receipts_total_units_ty
,d.nrp_jwn_gross_sales_total_units_ty
,d.nrp_jwn_demand_total_units_ty
,d.nrp_jwn_operational_gmv_total_units_ty

,d.nrp_jwn_returns_total_units_ty

,d.total_transfer_in_pack_and_hold_units_ly
,d.total_transfer_in_reserve_stock_units_ly
,d.total_transfer_in_racking_units_ly
,d.total_transfer_in_return_to_rack_units_ly
,d.total_receipts_total_units_ly
,d.total_jwn_gross_sales_total_units_ly
,d.total_jwn_demand_total_units_ly
,d.total_jwn_operational_gmv_total_units_ly
,d.total_jwn_returns_total_units_ly
,d.total_jwn_demand_regular_units_ly
,d.total_jwn_operational_gmv_regular_units_ly
,d.rp_transfer_in_pack_and_hold_units_ly
,d.rp_transfer_in_reserve_stock_units_ly
,d.rp_transfer_in_racking_units_ly
,d.rp_transfer_in_return_to_rack_units_ly
,d.rp_receipts_total_units_ly
,d.rp_jwn_gross_sales_total_units_ly
,d.rp_jwn_demand_total_units_ly
,d.rp_jwn_operational_gmv_total_units_ly
,d.rp_jwn_returns_total_units_ly
,d.nrp_transfer_in_pack_and_hold_units_ly
,d.nrp_transfer_in_reserve_stock_units_ly
,d.nrp_transfer_in_racking_units_ly
,d.nrp_transfer_in_return_to_rack_units_ly
,d.nrp_receipts_total_units_ly
,d.nrp_jwn_gross_sales_total_units_ly
,d.nrp_jwn_demand_total_units_ly
,d.nrp_jwn_operational_gmv_total_units_ly
,d.nrp_jwn_returns_total_units_ly
,ao.nrp_po_oo_u
,ao.rp_po_oo_u
,ao.total_po_oo_u
,ra.rp_anticipated_rcpt_u_splits
,rpd.RP_Sales_Forecast
,rpd.Total_Mth_Forecast
,rpd.Prcnt_of_RP_Forecast
,sf.split_sff_sales_forecast_u as sff_sales_forecast_u
,t.in_transit_qty
,t.rp_in_transit_u
,t.nrp_in_transit_u
,s.class_supplier_count
,s.dept_supplier_count
,s.loc_supplier_count

,z.location_gross_sales_z_score
,z.store_grade
,sfa.store_fulfill_u
,CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM location_plans_total l
LEFT JOIN ab_receipts ar
ON l.ab_key = ar.pkey
LEFT JOIN boh_u b
ON l.pkey = b.pkey
LEFT JOIN eoh_u e
ON l.pkey = e.pkey
LEFT JOIN dsr_u d
ON l.pkey = d.pkey

LEFT JOIN po_oo_u ao
ON l.pkey = ao.pkey
LEFT JOIN rp_ant_rcpt_u ra
ON l.pkey = ra.pkey
LEFT JOIN rp_demand_u rpd
ON l.pkey = rpd.pkey
LEFT JOIN store_fulfill sf
ON l.pkey = sf.pkey
LEFT JOIN in_transit_oo t
ON l.pkey = t.pkey
LEFT JOIN supplier_count s
ON l.pkey = s.class_pkey


LEFT JOIN store_grade z
ON l.month_idnt = z.month_idnt
AND l.loc_idnt = z.loc_idnt
LEFT JOIN store_fulfill_actuals sfa
ON l.pkey = sfa.pkey
);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.location_inventory_tracking_t2_schema}}.location_inventory_tracking_total;


INSERT INTO `{{params.gcp_project_id}}`.{{params.location_inventory_tracking_t2_schema}}.location_inventory_tracking_total (
pkey
,ab_key
,month_idnt
,month_desc
,month_label
,fiscal_month_num
,month_start_day_date
,month_start_day_date_tz
,month_end_day_date
,month_end_day_date_tz
,wks_in_month
,division
,subdivision
,dept_label
,class_label
,chnl_idnt
,dept_idnt
,class_idnt
,loc_idnt
,current_month_flag
,last_month_flag
,bop_plan
,rcpt_plan
,sales_plan
,uncapped_bop
,peer_group_type_desc
,climate
,store_dma_code
,dma_desc
,location
,store_type_desc
,gross_square_footage

,store_open_date
,store_open_date_tz

,store_close_date

,store_close_date_tz

,region_desc
,region_medium_desc
,region_short_desc
,business_unit_desc
,group_desc
,subgroup_desc
,subgroup_medium_desc
,subgroup_short_desc
,store_address_line_1
,store_address_city
,store_address_state
,store_address_state_name
,store_postal_code
,store_address_county
,store_country_code
,store_country_name
,store_location_latitude
,store_location_longitude
,distribution_center_num
,distribution_center_name
,channel_desc
,comp_status_desc
,new_loc_flag
,ab_oo_rcpt_u
,total_bop_u_ty
,rp_bop_u_ty
,nrp_bop_u_ty
,clearance_bop_u_ty
,rp_clearance_bop_u_ty
,nrp_clearance_bop_u_ty
,total_bop_u_ly
,rp_bop_u_ly
,nrp_bop_u_ly
,clearance_bop_u_ly
,rp_clearance_bop_u_ly
,nrp_clearance_bop_u_ly
,total_ty_EOH_U
,total_ty_ClearEOH_U
,rp_ty_EOH_U
,rp_ty_ClearEOH_U
,nrp_ty_EOH_U
,nrp_ty_ClearEOH_U
,total_ly_EOH_U
,total_ly_ClearEOH_U
,rp_ly_EOH_U
,rp_ly_ClearEOH_U
,nrp_ly_EOH_U
,nrp_ly_ClearEOH_U
,total_transfer_in_pack_and_hold_units_ty
,total_transfer_in_reserve_stock_units_ty
,total_transfer_in_racking_units_ty
,total_transfer_in_return_to_rack_units_ty
,total_receipts_total_units_ty
,total_jwn_gross_sales_total_units_ty
,total_jwn_demand_total_units_ty
,total_jwn_operational_gmv_total_units_ty

,total_jwn_returns_total_units_ty
,total_jwn_demand_regular_units_ty
,total_jwn_operational_gmv_regular_units_ty
,rp_transfer_in_pack_and_hold_units_ty
,rp_transfer_in_reserve_stock_units_ty
,rp_transfer_in_racking_units_ty
,rp_transfer_in_return_to_rack_units_ty
,rp_receipts_total_units_ty
,rp_jwn_gross_sales_total_units_ty
,rp_jwn_demand_total_units_ty
,rp_jwn_operational_gmv_total_units_ty

,rp_jwn_returns_total_units_ty
,nrp_transfer_in_pack_and_hold_units_ty
,nrp_transfer_in_reserve_stock_units_ty
,nrp_transfer_in_racking_units_ty
,nrp_transfer_in_return_to_rack_units_ty
,nrp_receipts_total_units_ty
,nrp_jwn_gross_sales_total_units_ty
,nrp_jwn_demand_total_units_ty
,nrp_jwn_operational_gmv_total_units_ty

,nrp_jwn_returns_total_units_ty
,total_transfer_in_pack_and_hold_units_ly
,total_transfer_in_reserve_stock_units_ly
,total_transfer_in_racking_units_ly
,total_transfer_in_return_to_rack_units_ly
,total_receipts_total_units_ly
,total_jwn_gross_sales_total_units_ly
,total_jwn_demand_total_units_ly
,total_jwn_operational_gmv_total_units_ly
,total_jwn_returns_total_units_ly
,total_jwn_demand_regular_units_ly
,total_jwn_operational_gmv_regular_units_ly
,rp_transfer_in_pack_and_hold_units_ly
,rp_transfer_in_reserve_stock_units_ly
,rp_transfer_in_racking_units_ly
,rp_transfer_in_return_to_rack_units_ly
,rp_receipts_total_units_ly
,rp_jwn_gross_sales_total_units_ly
,rp_jwn_demand_total_units_ly
,rp_jwn_operational_gmv_total_units_ly
,rp_jwn_returns_total_units_ly
,nrp_transfer_in_pack_and_hold_units_ly
,nrp_transfer_in_reserve_stock_units_ly
,nrp_transfer_in_racking_units_ly
,nrp_transfer_in_return_to_rack_units_ly
,nrp_receipts_total_units_ly
,nrp_jwn_gross_sales_total_units_ly
,nrp_jwn_demand_total_units_ly
,nrp_jwn_operational_gmv_total_units_ly
,nrp_jwn_returns_total_units_ly
,nrp_po_oo_u
,rp_po_oo_u
,total_po_oo_u
,rp_anticipated_rcpt_u_splits
,RP_Sales_Forecast
,Total_Mth_Forecast
,Prcnt_of_RP_Forecast
,sff_sales_forecast_u
,in_transit_qty
,rp_in_transit_u
,nrp_in_transit_u
,class_supplier_count
,dept_supplier_count
,loc_supplier_count

,location_gross_sales_z_score
,store_grade
,store_fulfill_u
,dw_sys_load_tmstp
)
select 
pkey,                                                            
ab_key,
month_idnt,
month_desc,
month_label,
fiscal_month_num,
cast(month_start_day_date as timestamp),
month_start_day_date_tz,
cast(month_end_day_date as timestamp),
month_end_day_date_tz,
wks_in_month,
division,
subdivision,
dept_label,
class_label,
chnl_idnt,
dept_idnt,
class_idnt,
loc_idnt,
current_month_flag,
last_month_flag,
bop_plan,
rcpt_plan,
sales_plan,
uncapped_bop,
peer_group_type_desc,
climate,
store_dma_code,
dma_desc,
location,
store_type_desc,
gross_square_footage,
cast(store_open_date as timestamp),
store_open_date_tz,
cast(store_close_date as timestamp),
store_close_date_tz,
region_desc,
region_medium_desc,
region_short_desc,
business_unit_desc,
group_desc,
subgroup_desc,
subgroup_medium_desc,
subgroup_short_desc,
store_address_line_1,
store_address_city,
store_address_state,
store_address_state_name,
store_postal_code,
store_address_county,
store_country_code,
store_country_name,
cast(store_location_latitude as numeric),
cast(store_location_longitude as numeric),
distribution_center_num,
distribution_center_name,
channel_desc,
comp_status_desc,
new_loc_flag,
ab_oo_rcpt_u,
total_bop_u_ty,
rp_bop_u_ty,
nrp_bop_u_ty,
clearance_bop_u_ty,
rp_clearance_bop_u_ty,
nrp_clearance_bop_u_ty,
total_bop_u_ly,
rp_bop_u_ly,
nrp_bop_u_ly,
clearance_bop_u_ly,
rp_clearance_bop_u_ly,
nrp_clearance_bop_u_ly,
total_ty_eoh_u,
total_ty_cleareoh_u,
rp_ty_eoh_u,
rp_ty_cleareoh_u,
nrp_ty_eoh_u,
nrp_ty_cleareoh_u,
total_ly_eoh_u,
total_ly_cleareoh_u,
rp_ly_eoh_u,
rp_ly_cleareoh_u,
nrp_ly_eoh_u,
nrp_ly_cleareoh_u,
total_transfer_in_pack_and_hold_units_ty,
total_transfer_in_reserve_stock_units_ty,
total_transfer_in_racking_units_ty,
total_transfer_in_return_to_rack_units_ty,
total_receipts_total_units_ty,
total_jwn_gross_sales_total_units_ty,
total_jwn_demand_total_units_ty,
total_jwn_operational_gmv_total_units_ty,
total_jwn_returns_total_units_ty,
total_jwn_demand_regular_units_ty,
total_jwn_operational_gmv_regular_units_ty,
rp_transfer_in_pack_and_hold_units_ty,
rp_transfer_in_reserve_stock_units_ty,
rp_transfer_in_racking_units_ty,
rp_transfer_in_return_to_rack_units_ty,
rp_receipts_total_units_ty,
rp_jwn_gross_sales_total_units_ty,
rp_jwn_demand_total_units_ty,
rp_jwn_operational_gmv_total_units_ty,
rp_jwn_returns_total_units_ty,
nrp_transfer_in_pack_and_hold_units_ty,
nrp_transfer_in_reserve_stock_units_ty,
nrp_transfer_in_racking_units_ty,
nrp_transfer_in_return_to_rack_units_ty,
nrp_receipts_total_units_ty,
nrp_jwn_gross_sales_total_units_ty,
nrp_jwn_demand_total_units_ty,
nrp_jwn_operational_gmv_total_units_ty,
nrp_jwn_returns_total_units_ty,
total_transfer_in_pack_and_hold_units_ly,
total_transfer_in_reserve_stock_units_ly,
total_transfer_in_racking_units_ly,
total_transfer_in_return_to_rack_units_ly,
total_receipts_total_units_ly,
total_jwn_gross_sales_total_units_ly,
total_jwn_demand_total_units_ly,
total_jwn_operational_gmv_total_units_ly,
total_jwn_returns_total_units_ly,
total_jwn_demand_regular_units_ly,
total_jwn_operational_gmv_regular_units_ly,
rp_transfer_in_pack_and_hold_units_ly,
rp_transfer_in_reserve_stock_units_ly,
rp_transfer_in_racking_units_ly,
rp_transfer_in_return_to_rack_units_ly,
rp_receipts_total_units_ly,
rp_jwn_gross_sales_total_units_ly,
rp_jwn_demand_total_units_ly,
rp_jwn_operational_gmv_total_units_ly,
rp_jwn_returns_total_units_ly,
nrp_transfer_in_pack_and_hold_units_ly,
nrp_transfer_in_reserve_stock_units_ly,
nrp_transfer_in_racking_units_ly,
nrp_transfer_in_return_to_rack_units_ly,
nrp_receipts_total_units_ly,
nrp_jwn_gross_sales_total_units_ly,
nrp_jwn_demand_total_units_ly,
nrp_jwn_operational_gmv_total_units_ly,
nrp_jwn_returns_total_units_ly,
nrp_po_oo_u,
rp_po_oo_u,
total_po_oo_u,
cast(rp_anticipated_rcpt_u_splits as numeric),
cast(rp_sales_forecast as numeric),
cast(total_mth_forecast as numeric),
cast(prcnt_of_rp_forecast as numeric),
cast(sff_sales_forecast_u as numeric),
cast(in_transit_qty as numeric),
cast(rp_in_transit_u as numeric),
cast(nrp_in_transit_u as numeric),
cast(class_supplier_count as numeric),
cast(dept_supplier_count as numeric),
cast(loc_supplier_count as numeric),
cast(location_gross_sales_z_score as numeric),
store_grade,
cast(store_fulfill_u as numeric),
CURRENT_DATETIME('PST8PDT') as dw_sys_load_tmstp
FROM total_aggregate;




--COLLECT STATISTICS COLUMN (pkey) ON t2dl_das_lit.location_inventory_tracking_total;
--COLLECT STATISTICS COLUMN (class_idnt,dept_idnt,month_idnt,loc_idnt) ON t2dl_das_lit.location_inventory_tracking_total;
--COLLECT STATISTICS COLUMN (class_idnt,month_idnt,loc_idnt) ON t2dl_das_lit.location_inventory_tracking_total;
--COLLECT STATISTICS COLUMN (pkey,current_month_flag,last_month_flag) ON t2dl_das_lit.location_inventory_tracking_total;
/*SET QUERY_BAND = NONE FOR SESSION;*/

