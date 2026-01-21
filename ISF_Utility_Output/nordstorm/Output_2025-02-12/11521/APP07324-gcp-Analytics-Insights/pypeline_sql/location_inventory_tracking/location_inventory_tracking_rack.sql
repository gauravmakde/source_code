/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09117;
     DAG_ID="location_inventory_tracking_rack_11521_ACE_ENG";
     Task_Name=location_inventory_tracking_rack;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_LIT.LOCATION_INVENTORY_TRACKING_RACK
Team/Owner: David Selover
Date Created/Modified: 10/26/2023

Note:
Location Inventory Tracking table that is intended to replace current MADM LIT Dashboard with NAP LIT Dashboard, as well as be the data source for Inventory Health Dashboard
Timeframe scope includes past month, current month, and 5 month future month range
Granularity at a dept/class/location/month level
Updated daily
*/











-- Total LIT Dashboard NAP datasource to replace current MADM sourced version




--Location plan base table with various primary and foreign keys to be used for JOINs

--DROP TABLE location_plans;
CREATE MULTISET VOLATILE TABLE location_plans AS (
WITH mth as (SELECT DISTINCT cal.month_idnt,
	CASE WHEN month_idnt = (SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM cal WHERE day_date = CURRENT_DATE) THEN 1 ELSE 0 END AS current_month_flag,
	cal.month_desc,
	cal.month_label,
	cal.fiscal_month_num,
	cal.month_start_day_date,
	cal.month_end_day_date
	FROM (SELECT distinct month_idnt,month_desc,month_label,fiscal_month_num,month_start_day_date,month_end_day_date FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM cal WHERE day_date BETWEEN CURRENT_DATE - INTERVAL '27' DAY AND CURRENT_DATE + INTERVAL '148' DAY) cal
	)
Select
OREPLACE(CONCAT(plan.class_idnt,plan.dept_idnt,mth.month_idnt,plan.loc_idnt),' ','') as pkey,
OREPLACE(CONCAT(plan.class_idnt,plan.dept_idnt,mth.month_idnt),' ','') as ab_key,
OREPLACE(CONCAT(mth.month_idnt, plan.dept_idnt),' ','') as rp_spend_key,
mth.month_idnt,
mth.month_desc,
mth.month_label,
mth.fiscal_month_num,
mth.month_start_day_date,
mth.month_end_day_date,
Case mth.fiscal_month_num
when 1 then 4
when 2 then 5
when 3 then 4
when 4 then 4
when 5 then 5
when 6 then 4
when 7 then 4
when 8 then 5
when 9 then 4
when 10 then 4
when 11 then 5
when 12 then 4
end as "wks_in_month",
TRIM(div.Div) AS division,-- "Division",
TRIM(div.Subdiv) AS subdivision,-- "Subdivision",
TRIM(div.Dept_Label) AS dept_label,--"Department Label",
TRIM(div.Class_Label) AS class_label,--"Class Label",
plan.DEPT_IDNT, --as department --"Department",
plan.CLASS_IDNT, --as  "class",--"Class",
plan.LOC_IDNT, --as "Location",
mth.current_month_flag,
CASE WHEN month_idnt = (SELECT MIN(month_idnt) FROM mth) THEN 1 ELSE 0 END as last_month_flag,
SUM(plan.BOP_PLAN) as bop_plan,--as "BOP Plan",
SUM(plan.RCPT_PLAN) as rcpt_plan, --"Rcpt Plan",
SUM(plan.SALES_PLAN) as sales_plan, --"Sales Plan",
SUM(plan.UNCAPPED_BOP) as uncapped_bop --"Uncapped BOP"
FROM T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_PRD_VW plan
LEFT JOIN
(SELECT DISTINCT dept.dept_num,
dept.class_num,
dept.div_num||', '||div_desc as Div,
dept.grp_num||', '||grp_desc as Subdiv,
dept.dept_num||', '||dept_desc AS Dept_Label,
dept.class_num||', '||class_desc AS Class_Label
FROM PRD_NAP_USR_VWS.PRODUCT_HIERARCHY_DIM_VW dept
WHERE eff_end_tmstp = (select MAX (eff_end_tmstp) FROM PRD_NAP_USR_VWS.PRODUCT_HIERARCHY_DIM_VW)) AS div
     ON plan.DEPT_IDNT = div.DEPT_NUM
     AND div.CLASS_NUM = plan.CLASS_IDNT
LEFT JOIN mth mth
  ON mth.month_idnt = plan.mth_idnt
WHERE chnl_idnt IN (210)
AND plan.MTH_IDNT IN (SELECT DISTINCT Month_IDNT FROM mth)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ,COLUMN (ab_key)
     ,COLUMN (rp_spend_key)
     ON location_plans;



--SELECT * FROM location_plans

-- store climate to be joined to location plans on store_num
--DROP TABLE store_climate;
CREATE MULTISET VOLATILE TABLE store_climate AS (
    select
        store_num
        ,peer_group_type_desc
        ,peer_group_desc as climate
        FROM prd_nap_usr_vws.STORE_PEER_GROUP_DIM
        WHERE peer_group_type_code = 'OPC'
)
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(store_num)
     ON store_climate;



-- New store flag, include if it was launched in current or last MONTH

--DROP TABLE new_store_list;
CREATE MULTISET VOLATILE TABLE new_store_list AS (
WITH m as (SELECT DISTINCT Month_IDNT
            FROM location_plans)
	SELECT s.store_num, s.store_open_date, c.month_idnt AS open_month
	FROM PRD_NAP_USR_VWS.STORE_DIM s
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c
	ON s.store_open_date = c.day_date
	WHERE c.month_idnt IN (SELECT DISTINCT month_idnt FROM m)
)
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(store_num)
     ON new_store_list;


  --  SELECT * FROM new_store_list

-- additional store info to be joined to location plans on store_num

CREATE MULTISET VOLATILE TABLE dma_data AS (
select
    distinct dma.store_num-- AS "Store Number"
    ,dma.store_dma_code-- as "DMA Code"
    ,mkt.dma_desc
    ,TRIM(dma.store_num||', '||dma.store_short_name) as location
    ,dma.store_type_desc --AS "Store Type Desc"
    ,dma.gross_square_footage-- AS "Gross Square Footage"
    ,dma.store_open_date-- AS "Store Open Date"
    ,dma.store_close_date-- AS "Store Close Date"
    ,dma.region_desc-- AS "Region Desc"
    ,dma.region_medium_desc-- AS "Region Medium Desc"
    ,dma.region_short_desc-- AS "Region Short Desc"
    ,dma.business_unit_desc --AS "Business Unit Desc"
    ,dma.group_desc-- AS "Group Desc"
    ,dma.subgroup_desc-- AS "Subgroup Desc"
    ,dma.subgroup_medium_desc-- AS "Subgrou Med Desc"
    ,dma.subgroup_short_desc-- AS "Subgroup Short Desc"
    ,dma.store_address_line_1-- AS "Store Address Line 1"
    ,dma.store_address_city-- AS "Store Address City"
    ,dma.store_address_state-- AS "Store Address State"
    ,dma.store_address_state_name-- AS "Store Address State Name"
    ,dma.store_postal_code-- AS "Store Postal Code"
    ,dma.store_address_county-- AS "Store Address County"
    ,dma.store_country_code-- AS "Store Country Code"
    ,dma.store_country_name-- AS "Store Country Name"
    ,dma.store_location_latitude-- AS "Latitude"
    ,dma.store_location_longitude-- AS "Longitude"
    ,dma.distribution_center_num-- AS "Distribution Center Num"
    ,dma.distribution_center_name-- AS "Distribution Center Name"
    ,dma.channel_desc-- AS "Channel Desc"
    ,dma.comp_status_desc-- AS "Comp Status Desc"
    FROM PRD_NAP_USR_VWS.STORE_DIM DMA
    LEFT JOIN PRD_NAP_USR_VWS.ORG_DMA MKT
    ON DMA.store_dma_code = MKT.dma_code
    )
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(store_num)
     ,COLUMN (store_num, dma_desc)
     ON dma_data;

 --   SELECT * FROM dma_data

-- total location plans as primary reference table

CREATE MULTISET VOLATILE TABLE location_plans_total AS (
    SELECT l.*
    ,c.peer_group_type_desc
    ,c.climate
    ,d.store_dma_code-- as "DMA Code"
    ,d.dma_desc
    ,d.location --AS "Location"
    ,d.store_type_desc --AS "Store Type Desc"
    ,d.gross_square_footage-- AS "Gross Square Footage"
    ,d.store_open_date-- AS "Store Open Date"
    ,d.store_close_date-- AS "Store Close Date"
    ,d.region_desc-- AS "Region Desc"
    ,d.region_medium_desc-- AS "Region Medium Desc"
    ,d.region_short_desc-- AS "Region Short Desc"
    ,d.business_unit_desc --AS "Business Unit Desc"
    ,d.group_desc-- AS "Group Desc"
    ,d.subgroup_desc-- AS "Subgroup Desc"
    ,d.subgroup_medium_desc-- AS "Subgrou Med Desc"
    ,d.subgroup_short_desc-- AS "Subgroup Short Desc"
    ,d.store_address_line_1-- AS "Store Address Line 1"
    ,d.store_address_city-- AS "Store Address City"
    ,d.store_address_state-- AS "Store Address State"
    ,d.store_address_state_name-- AS "Store Address State Name"
    ,d.store_postal_code-- AS "Store Postal Code"
    ,d.store_address_county-- AS "Store Address County"
    ,d.store_country_code-- AS "Store Country Code"
    ,d.store_country_name-- AS "Store Country Name"
    ,d.store_location_latitude-- AS "Latitude"
    ,d.store_location_longitude-- AS "Longitude"
    ,d.distribution_center_num-- AS "Distribution Center Num"
    ,d.distribution_center_name-- AS "Distribution Center Name"
    ,d.channel_desc-- AS "Channel Desc"
    ,d.comp_status_desc-- AS "Comp Status Desc"
    ,CASE WHEN nsl.store_num IS NOT NULL THEN 1 ELSE 0 END as new_loc_flag
    FROM location_plans l
    LEFT JOIN store_climate c
    ON l.loc_idnt = c.store_num
    LEFT JOIN dma_data d
    ON l.loc_idnt = d.store_num
    LEFT JOIN new_store_list nsl
    ON l.loc_idnt = nsl.store_num
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ,COLUMN (ab_key)
     ,COLUMN (rp_spend_key)
     ,COLUMN (loc_idnt,store_dma_code)
     ,COLUMN (loc_idnt,climate)
     ON location_plans_total;

--SELECT * FROM location_plans_total

-- location plans lag 1 and lag 2

CREATE MULTISET VOLATILE TABLE location_plan_lags AS (
WITH m as (
	SELECT DISTINCT Month_IDNT
    FROM location_plans
    ),
distinct_month as (
	SELECT DISTINCT month_idnt
	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
	),
lag_months as (
 	SELECT month_idnt ,
    Lag(month_idnt, 1) OVER(
    ORDER BY month_idnt ASC) AS month_idnt_lag_1,
    Lag(month_idnt, 2) OVER(
    ORDER BY month_idnt ASC) AS month_idnt_lag_2
 	FROM distinct_month),
current_month as (
	SELECT month_idnt,
	month_idnt_lag_1,
	month_idnt_lag_2
	FROM lag_months WHERE month_idnt IN (SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date IN (CURRENT_DATE()))
),
location_plan_lag_1 as (
	SELECT OREPLACE(CONCAT(class_idnt,dept_idnt,mth_idnt,loc_idnt),' ','') as pkey,
	loc_idnt,
	mth_idnt,
	dept_idnt,
	class_idnt,
	plan_cycle as plan_cycle_lag_1,
	SUM(SALES_PLAN) as sales_plan_lag_1,
	SUM(bop_plan) as bop_plan_lag_1,
	SUM(rcpt_plan) as rcpt_plan_lag_1,
	SUM(uncapped_bop) as uncapped_bop_lag_1
	FROM T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_CYC_PRD_VW
	WHERE chnl_idnt IN ('210')
	AND plan_cycle IN (SELECT month_idnt_lag_1 FROM current_month)
	GROUP BY 1,2,3,4,5,6
),
location_plan_lag_2 as (
SELECT OREPLACE(CONCAT(class_idnt,dept_idnt,mth_idnt,loc_idnt),' ','') as pkey,
	loc_idnt,
	mth_idnt,
	dept_idnt,
	class_idnt,
	plan_cycle as plan_cycle_lag_2,
	SUM(SALES_PLAN) as sales_plan_lag_2,
	SUM(bop_plan) as bop_plan_lag_2,
	SUM(rcpt_plan) as rcpt_plan_lag_2,
	SUM(uncapped_bop) as uncapped_bop_lag_2
	FROM T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_CYC_PRD_VW
	WHERE chnl_idnt IN ('210')
	AND plan_cycle IN (SELECT month_idnt_lag_2 FROM current_month)
	GROUP BY 1,2,3,4,5,6
)
SELECT
	l2.pkey,
	l2.loc_idnt,
	l2.mth_idnt,
	l2.dept_idnt,
	l2.class_idnt,
	l1.plan_cycle_lag_1,
	l1.sales_plan_lag_1,
	l1.bop_plan_lag_1,
	l1.rcpt_plan_lag_1,
	l1.uncapped_bop_lag_1,
	l2.plan_cycle_lag_2,
	l2.sales_plan_lag_2,
	l2.bop_plan_lag_2,
	l2.rcpt_plan_lag_2,
	l2.uncapped_bop_lag_2
FROM location_plan_lag_2 l2
LEFT JOIN location_plan_lag_1 l1
ON l1.pkey = l2.pkey
WHERE l2.mth_idnt IN (SELECT month_idnt FROM m)
 )
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ,COLUMN (plan_cycle_lag_1)
     ,COLUMN (plan_cycle_lag_2)
     ON location_plan_lags;

-- AB OO data, receipt units -- duplicates on npg_id and plan_type (NEED TO RESOLVE BEFORE INTEGRATION)

CREATE MULTISET VOLATILE TABLE ab_receipts AS (
SELECT
OREPLACE(CONCAT(class_id,dept_id,fiscal_month_id),' ','') as pkey
,channel
,fiscal_month_id
,class_id
,class_name
,dept_id
,SUM(rcpt_units) AS AB_OO_RCPT_UNITS
FROM T2DL_DAS_OPEN_TO_BUY.AB_CM_ORDERS_CURRENT
WHERE org_id IN (210)
GROUP BY 1,2,3,4,5,6
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ON ab_receipts;

-- SELECT * from ab_receipts

-- SELECT TOP 10 * FROM T2DL_DAS_OPEN_TO_BUY.AB_CM_ORDERS_CURRENT

-- BOP U TY actuals data -- BOH U - duplicates on rp_id - (NEED TO RESOLVE BEFORE INTEGRATION)

--DROP TABLE boh_u;
CREATE MULTISET VOLATILE TABLE boh_u AS (
SELECT
OREPLACE(CONCAT(class_num,dept_num,month_num,store_num),' ','') as pkey
,dept_num
,class_num
,store_num
,month_num
,SUM(inventory_boh_total_units_ty + inventory_boh_in_transit_total_units_ty) as total_bop_u_ty
,SUM(CASE WHEN rp_ind = 'Y' THEN inventory_boh_total_units_ty + inventory_boh_in_transit_total_units_ty END) as rp_bop_u_ty
,SUM(CASE WHEN rp_ind = 'N' THEN inventory_boh_total_units_ty + inventory_boh_in_transit_total_units_ty END) as nrp_bop_u_ty
,SUM(inventory_boh_clearance_units_ty + inventory_boh_in_transit_clearance_units_ty) as clearance_bop_u_ty
,SUM(CASE WHEN rp_ind = 'Y' THEN inventory_boh_clearance_units_ty + inventory_boh_in_transit_clearance_units_ty END) as rp_clearance_bop_u_ty
,SUM(CASE WHEN rp_ind = 'N' THEN inventory_boh_clearance_units_ty + inventory_boh_in_transit_clearance_units_ty END) as nrp_clearance_bop_u_ty
-- adding LY
,SUM(inventory_boh_total_units_ly + inventory_boh_in_transit_total_units_ly) as total_bop_u_ly
,SUM(CASE WHEN rp_ind = 'Y' THEN inventory_boh_total_units_ly + inventory_boh_in_transit_total_units_ly END) as rp_bop_u_ly
,SUM(CASE WHEN rp_ind = 'N' THEN inventory_boh_total_units_ly + inventory_boh_in_transit_total_units_ly END) as nrp_bop_u_ly
,SUM(inventory_boh_clearance_units_ly + inventory_boh_in_transit_clearance_units_ly) as clearance_bop_u_ly
,SUM(CASE WHEN rp_ind = 'Y' THEN inventory_boh_clearance_units_ly + inventory_boh_in_transit_clearance_units_ly END) as rp_clearance_bop_u_ly
,SUM(CASE WHEN rp_ind = 'N' THEN inventory_boh_clearance_units_ly + inventory_boh_in_transit_clearance_units_ly END) as nrp_clearance_bop_u_ly
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW
WHERE week_num IN (select dcd.week_idnt from  PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd where dcd.week_num_of_fiscal_month = '1')
AND channel_num = 210
AND week_num IN (SELECT DISTINCT week_IDNT
            FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM tdl
            WHERE tdl.day_date BETWEEN CURRENT_DATE - INTERVAL '52' DAY AND CURRENT_DATE + INTERVAL '90' DAY)
GROUP BY 1,2,3,4,5
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ,COLUMN (class_num,dept_num)
     ON boh_u;

 --   SELECT * FROM boh_u


    -- EOP U TY actuals data -- EOH U

--DROP TABLE eoh_u;
CREATE MULTISET VOLATILE TABLE eoh_u AS (
SELECT
OREPLACE(CONCAT(class_num,dept_num,month_num,store_num),' ','') as pkey
,dept_num
,class_num
,store_num
,month_num
-- total
,sum(inventory_eoh_total_units_ty) as total_ty_EOH_U
,sum (inventory_eoh_clearance_units_ty) as total_ty_ClearEOH_U
-- rp
,sum(CASE WHEN rp_ind = 'Y' THEN inventory_eoh_total_units_ty END) as rp_ty_EOH_U
,sum(CASE WHEN rp_ind = 'Y' THEN inventory_eoh_clearance_units_ty END) as rp_ty_ClearEOH_U
-- nrp
,sum(CASE WHEN rp_ind = 'N' THEN inventory_eoh_total_units_ty END) as nrp_ty_EOH_U
,sum(CASE WHEN rp_ind = 'N' THEN inventory_eoh_clearance_units_ty END) as nrp_ty_ClearEOH_U
--adding LY
-- total
,sum(inventory_eoh_total_units_ly) as total_ly_EOH_U
,sum (inventory_eoh_clearance_units_ly) as total_ly_ClearEOH_U
-- rp
,sum(CASE WHEN rp_ind = 'Y' THEN inventory_eoh_total_units_ly END) as rp_ly_EOH_U
,sum(CASE WHEN rp_ind = 'Y' THEN inventory_eoh_clearance_units_ly END) as rp_ly_ClearEOH_U
-- nrp
,sum(CASE WHEN rp_ind = 'N' THEN inventory_eoh_total_units_ly END) as nrp_ly_EOH_U
,sum(CASE WHEN rp_ind = 'N' THEN inventory_eoh_clearance_units_ly END) as nrp_ly_ClearEOH_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW
WHERE week_num IN (
(select dcd.week_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd where day_date = current_date()-1)
,(SELECT DISTINCT dcd.week_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd where day_date IN (SELECT DISTINCT month_end_day_date FROM location_plans WHERE last_month_flag = 1))
)
AND channel_num = 210
GROUP BY 1,2,3,4,5
    )
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ,COLUMN (class_num,dept_num)
     ON eoh_u;

   -- SELECT * FROM eoh_u


--SELECT TOP 10 * FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW

--- Demand, Sales, Receipts U
--DROP TABLE dsr_u;
CREATE MULTISET VOLATILE TABLE dsr_u AS (
WITH m as (SELECT DISTINCT Month_IDNT
            FROM location_plans)
SELECT
OREPLACE(CONCAT(class_num,dept_num,month_num,store_num),' ','') as pkey
,dept_num
,class_num
,store_num
,month_num
-- total
,SUM(transfer_in_pack_and_hold_units_ty) as total_transfer_in_pack_and_hold_units_ty
,SUM(transfer_in_reserve_stock_units_ty) as total_transfer_in_reserve_stock_units_ty
,SUM(transfer_in_racking_units_ty) AS total_transfer_in_racking_units_ty
,SUM(transfer_in_return_to_rack_units_ty) AS total_transfer_in_return_to_rack_units_ty
,sum(receipts_total_units_ty ) AS total_receipts_total_units_ty
,sum(jwn_gross_sales_total_units_ty) AS total_jwn_gross_sales_total_units_ty
,sum(jwn_demand_total_units_ty ) AS total_jwn_demand_total_units_ty
,sum(jwn_operational_gmv_total_units_ty) AS total_jwn_operational_gmv_total_units_ty
,sum(jwn_returns_total_units_ty) AS total_jwn_returns_total_units_ty
-- rp
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_pack_and_hold_units_ty END) as rp_transfer_in_pack_and_hold_units_ty
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_reserve_stock_units_ty END) as rp_transfer_in_reserve_stock_units_ty
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_racking_units_ty END) AS rp_transfer_in_racking_units_ty
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_return_to_rack_units_ty END) AS rp_transfer_in_return_to_rack_units_ty
,sum(CASE WHEN rp_ind = 'Y' THEN receipts_total_units_ty END) AS rp_receipts_total_units_ty
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_gross_sales_total_units_ty END) AS rp_jwn_gross_sales_total_units_ty
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_demand_total_units_ty END) AS rp_jwn_demand_total_units_ty
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_operational_gmv_total_units_ty END) AS rp_jwn_operational_gmv_total_units_ty
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_returns_total_units_ty END) AS rp_jwn_returns_total_units_ty
-- nrp
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_pack_and_hold_units_ty END) as nrp_transfer_in_pack_and_hold_units_ty
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_reserve_stock_units_ty END) as nrp_transfer_in_reserve_stock_units_ty
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_racking_units_ty END) AS nrp_transfer_in_racking_units_ty
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_return_to_rack_units_ty END) AS nrp_transfer_in_return_to_rack_units_ty
,sum(CASE WHEN rp_ind = 'N' THEN receipts_total_units_ty END) AS nrp_receipts_total_units_ty
,sum(CASE WHEN rp_ind = 'N' THEN jwn_gross_sales_total_units_ty END) AS nrp_jwn_gross_sales_total_units_ty
,sum(CASE WHEN rp_ind = 'N' THEN jwn_demand_total_units_ty END) AS nrp_jwn_demand_total_units_ty
,sum(CASE WHEN rp_ind = 'N' THEN jwn_operational_gmv_total_units_ty END) AS nrp_jwn_operational_gmv_total_units_ty
,sum(CASE WHEN rp_ind = 'N' THEN jwn_returns_total_units_ty END) AS nrp_jwn_returns_total_units_ty
--adding LY
-- total
,SUM(transfer_in_pack_and_hold_units_ly) as total_transfer_in_pack_and_hold_units_ly
,SUM(transfer_in_reserve_stock_units_ly) as total_transfer_in_reserve_stock_units_ly
,SUM(transfer_in_racking_units_ly) AS total_transfer_in_racking_units_ly
,SUM(transfer_in_return_to_rack_units_ly) AS total_transfer_in_return_to_rack_units_ly
,sum(receipts_total_units_ly ) AS total_receipts_total_units_ly
,sum(jwn_gross_sales_total_units_ly) AS total_jwn_gross_sales_total_units_ly
,sum(jwn_demand_total_units_ly ) AS total_jwn_demand_total_units_ly
,sum(jwn_operational_gmv_total_units_ly) AS total_jwn_operational_gmv_total_units_ly
,sum(jwn_returns_total_units_ly) AS total_jwn_returns_total_units_ly
-- rp
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_pack_and_hold_units_ly END) as rp_transfer_in_pack_and_hold_units_ly
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_reserve_stock_units_ly END) as rp_transfer_in_reserve_stock_units_ly
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_racking_units_ly END) AS rp_transfer_in_racking_units_ly
,SUM(CASE WHEN rp_ind = 'Y' THEN transfer_in_return_to_rack_units_ly END) AS rp_transfer_in_return_to_rack_units_ly
,sum(CASE WHEN rp_ind = 'Y' THEN receipts_total_units_ly END) AS rp_receipts_total_units_ly
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_gross_sales_total_units_ly END) AS rp_jwn_gross_sales_total_units_ly
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_demand_total_units_ly END) AS rp_jwn_demand_total_units_ly
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_operational_gmv_total_units_ly END) AS rp_jwn_operational_gmv_total_units_ly
,sum(CASE WHEN rp_ind = 'Y' THEN jwn_returns_total_units_ly END) AS rp_jwn_returns_total_units_ly
-- nrp
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_pack_and_hold_units_ly END) as nrp_transfer_in_pack_and_hold_units_ly
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_reserve_stock_units_ly END) as nrp_transfer_in_reserve_stock_units_ly
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_racking_units_ly END) AS nrp_transfer_in_racking_units_ly
,SUM(CASE WHEN rp_ind = 'N' THEN transfer_in_return_to_rack_units_ly END) AS nrp_transfer_in_return_to_rack_units_ly
,sum(CASE WHEN rp_ind = 'N' THEN receipts_total_units_ly END) AS nrp_receipts_total_units_ly
,sum(CASE WHEN rp_ind = 'N' THEN jwn_gross_sales_total_units_ly END) AS nrp_jwn_gross_sales_total_units_ly
,sum(CASE WHEN rp_ind = 'N' THEN jwn_demand_total_units_ly END) AS nrp_jwn_demand_total_units_ly
,sum(CASE WHEN rp_ind = 'N' THEN jwn_operational_gmv_total_units_ly END) AS nrp_jwn_operational_gmv_total_units_ly
,sum(CASE WHEN rp_ind = 'N' THEN jwn_returns_total_units_ly END) AS nrp_jwn_returns_total_units_ly
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW
WHERE channel_num = 210
AND month_num IN (SELECT month_idnt FROM m)
GROUP BY 1,2,3,4,5
    )
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ,COLUMN (class_num,dept_num)
     ON dsr_u;

   -- SELECT * FROM dsr_u





-- On Order
--DROP TABLE po_oo_u;
CREATE MULTISET VOLATILE TABLE po_oo_u AS (
SELECT
OREPLACE(CONCAT(class_num,department_num,month_num,store_num),' ','') as pkey
,DEPARTMENT_NUM
,CLASS_NUM
,STORE_NUM
,MONTH_NUM
,SUM(NON_RP_OO_ACTIVE_UNITS ) AS nrp_po_oo_u
,sum(RP_OO_ACTIVE_UNITS) AS rp_po_oo_u
,SUM(NON_RP_OO_ACTIVE_UNITS) + sum(RP_OO_ACTIVE_UNITS) as total_po_oo_u
from prd_nap_usr_vws.merch_apt_on_order_insight_fact_vw
WHERE channel_num = 210
AND month_num <= (SELECT DISTINCT Month_IDNT
            FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM tdl
            WHERE tdl.day_date = current_date + 150)
group by 1,2,3,4,5
    )
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ON po_oo_u;

  --  SELECT * FROM po_oo_u




-- RP Demand Forecast
--DROP TABLE rp_demand_u;
CREATE MULTISET VOLATILE TABLE rp_demand_u AS (
WITH mth AS (
    SELECT DISTINCT week_idnt
    ,month_idnt
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
    WHERE month_idnt IN (SELECT distinct month_idnt FROM location_plans l)
    )
SELECT
OREPLACE(CONCAT(ps.class_num,ps.dept_num,mth.month_idnt,frcst.location_id),' ','') as pkey
,mth.month_idnt
,frcst.location_id as store_idnt
,PS.dept_num
,PS.class_num
,sum(frcst.inventory_forecast_qty) AS RP_Sales_Forecast
,sum(RP_Sales_Forecast) OVER (PARTITION by mth.month_idnt, PS.dept_num ORDER BY RP_Sales_Forecast desc) AS Total_Mth_Forecast
,RP_Sales_Forecast/Total_Mth_Forecast AS Prcnt_of_RP_Forecast
FROM (SELECT* FROM PRD_NAP_USR_VWS.INVENTORY_APPROVED_WEEKLY_DEPLOYMENT_FORECAST_FACT WHERE inventory_forecast_qty > 0) frcst
JOIN mth ON mth.week_idnt = frcst.week_id
JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW PS ON frcst.sku_id = cast(PS.epm_sku_num as varchar(100))
WHERE frcst.location_id IN (SELECT DISTINCT store_num FROM PRD_NAP_USR_VWS.STORE_DIM WHERE business_unit_desc IN ('RACK', 'OFFPRICE ONLINE'))
AND month_idnt IN (SELECT DISTINCT month_idnt FROM mth)
AND dept_num IN (SELECT DISTINCT(DEPT_IDNT)FROM T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_PRD_VW)
AND class_num IN (SELECT DISTINCT(CLASS_IDNT)FROM T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_PRD_VW)
GROUP BY 1,2,3,4,5
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(pkey) ON rp_demand_u;


--- Anticipated SPEND
--DROP TABLE rp_ant_rcpt_u;
CREATE MULTISET VOLATILE TABLE rp_ant_rcpt_u AS (
WITH rp_anticipated_rcpt_u as (
SELECT
OREPLACE(CONCAT(month_454, dept_idnt),' ','') as pkey,
month_454
,dept_idnt
,CAST(SUM(rp_antspnd_u) AS decimal(38,0)) AS RP_anticipated_RCPT_U
FROM T2DL_DAS_OPEN_TO_BUY.rp_anticipated_spend_current spend
WHERE banner_id = 3
AND month_454 IN (SELECT DISTINCT Month_IDNT
            FROM location_plans l)
GROUP BY 1,2,3
)
SELECT
l.pkey
,l.RP_Sales_Forecast
,l.Total_Mth_Forecast
,l.Prcnt_of_RP_Forecast
,r.RP_anticipated_RCPT_U
,CAST(l.Prcnt_of_RP_Forecast as NUMBER) * r.RP_anticipated_RCPT_U as rp_anticipated_rcpt_u_splits
FROM rp_demand_u l
LEFT JOIN rp_anticipated_rcpt_u r
ON l.month_idnt = r.month_454
AND l.dept_num = r.dept_idnt
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ON rp_ant_rcpt_u;




-- In Transit and Stock On Hand DATA
--DROP TABLE in_transit_oo;
CREATE MULTISET VOLATILE TABLE in_transit_oo AS (
SELECT
OREPLACE(CONCAT(class_num,dept_num,(SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = CURRENT_DATE()),store_num),' ','') as pkey
,dept_num
,class_num
,store_num
,(SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = CURRENT_DATE()) as month_num
-- total
,sum(INVENTORY_IN_TRANSIT_TOTAL_UNITS) as in_transit_qty
-- rp
,sum(CASE WHEN rp_ind = 'Y' THEN INVENTORY_IN_TRANSIT_TOTAL_UNITS END) as rp_in_transit_u
-- nrp
,sum(CASE WHEN rp_ind = 'N' THEN INVENTORY_IN_TRANSIT_TOTAL_UNITS END) as nrp_in_transit_u
FROM PRD_NAP_VWS.MERCH_INBOUND_SKU_STORE_VW
WHERE channel_num = 210
GROUP BY 1,2,3,4,5
    )
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ON in_transit_oo;

--REPLACING SUPPLIER COUNT BASED ON CODE TUNING RECOMMENDATION

create multiset volatile table supplier_base_data as (
SELECT
a.*
,b.brand_name
FROM
(SELECT
dept_num,
class_num,
store_num,
month_num,
supp_part_num
from PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW WHERE channel_num = 210
and week_num IN (SELECT DISTINCT week_IDNT
            FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM tdl
            WHERE tdl.day_date BETWEEN CURRENT_DATE - INTERVAL '27' DAY AND CURRENT_DATE + INTERVAL '148' DAY)
group by 1,2,3,4,5
) a
LEFT JOIN
(
sel
brand_name,
supp_part_num
from
PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW
group by 1,2
) b
ON a.supp_part_num = b.supp_part_num
) with data
no primary index
on commit preserve rows;

--- CODE TUNING REPLACEMENT CONT.

CREATE MULTISET VOLATILE TABLE supplier_count AS (
---class
WITH supplier_base AS (
SELECT
DISTINCT a.dept_num
,a.class_num
,a.store_num
,a.month_num
--,a.supp_part_num
,a.brand_name
from supplier_base_data a
),
--dept
dept_base AS (
SELECT
DISTINCT a.dept_num
,a.store_num
,a.month_num
--,a.supp_part_num
,a.brand_name
FROM supplier_base_data a
),
--class
class_supplier_count AS (
SELECT
--OREPLACE(CONCAT(class_num,dept_num,month_num,store_num),' ','') as pkey
(TRANSLATE((OREPLACE(CONCAT(class_num,dept_num,month_num,store_num),' ','') )USING LATIN_TO_UNICODE)) as pkey
,dept_num
,class_num
,store_num
,month_num
,COUNT(brand_name) AS class_supplier_count_brand
FROM supplier_base
GROUP BY 1,2,3,4,5
),
dept_supplier_count AS (
SELECT
--OREPLACE(CONCAT(dept_num,month_num,store_num),' ','') as pkey
(TRANSLATE((OREPLACE(CONCAT(dept_num,month_num,store_num),' ','') )USING LATIN_TO_UNICODE)) as pkey
,dept_num
,store_num
,month_num
,COUNT(brand_name) AS dept_supplier_count_brand
FROM dept_base
GROUP BY 1,2,3,4
),
--location
loc_base AS (
SELECT
DISTINCT a.store_num
,a.month_num
--,a.supp_part_num
,a.brand_name
FROM supplier_base_data a
),
location_supplier_count AS (
SELECT
OREPLACE(CONCAT(month_num,store_num),' ','') as pkey
,store_num
,month_num
,COUNT(brand_name) AS loc_supplier_count
FROM loc_base
GROUP BY 1,2,3
)
-- main query
SELECT
c.class_pkey
,c.dept_pkey
,c.loc_pkey
,c.dept_num
,c.class_num
,c.store_num
,c.month_num
,c.class_supplier_count_brand as class_supplier_count
,d.dept_supplier_count_brand as dept_supplier_count
,l.loc_supplier_count
FROM
(
SELECT
c.pkey as class_pkey
--,OREPLACE(CONCAT(c.dept_num,c.month_num,c.store_num),' ','') as dept_pkey
,(TRANSLATE((OREPLACE(CONCAT(c.dept_num,c.month_num,c.store_num),' ','') )USING LATIN_TO_UNICODE)) as dept_pkey
,(TRANSLATE((OREPLACE(CONCAT(c.month_num,c.store_num),' ','') )USING LATIN_TO_UNICODE)) as loc_pkey
--,OREPLACE(CONCAT(c.month_num,c.store_num),' ','') as loc_pkey
,c.dept_num
,c.class_num
,c.store_num
,c.month_num
,c.class_supplier_count_brand
from
class_supplier_count c
) c
LEFT JOIN dept_supplier_count d
ON c.dept_pkey = d.pkey
LEFT JOIN location_supplier_count l
ON c.loc_pkey = l.pkey
)
WITH DATA
PRIMARY INDEX (class_pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(class_pkey)
     ON supplier_count;

--Total JOIN
--Table Creation and LOAD



 
--Z-score of all months actualized and planned combined, and Store Grade assigned based on these    
--DROP TABLE store_grade;
CREATE MULTISET VOLATILE TABLE store_grade AS (
WITH hybrid_gross_sales as (
SELECT
l.pkey
,l.division
,l.subdivision
,l.dept_idnt
,l.class_idnt
,l.loc_idnt
,l.month_idnt
,l.last_month_flag
,d.total_jwn_gross_sales_total_units_ty
,l.sales_plan
,CASE
	WHEN last_month_flag = 1
	THEN d.total_jwn_gross_sales_total_units_ty
	ELSE l.sales_plan
END as hybrid_gross_sales_ty
FROM location_plans l
LEFT JOIN dsr_u d 
ON l.pkey = d.pkey
WHERE division IS NOT NULL),
month_location_aggregate as (
SELECT 
loc_idnt
,month_idnt
,SUM(hybrid_gross_sales_ty) as month_location_gross_sales
FROM hybrid_gross_sales
GROUP BY 1,2),
month_avg_std as (
SELECT 
month_idnt
,AVERAGE(month_location_gross_sales) as avg_gross_sales
,STDDEV_POP(month_location_gross_sales) as std_gross_sales
FROM month_location_aggregate
GROUP BY 1)
SELECT 
s.loc_idnt
,s.month_idnt
,s.month_location_gross_sales
,mas.avg_gross_sales
,mas.std_gross_sales
,((s.month_location_gross_sales - mas.avg_gross_sales) / mas.std_gross_sales) as location_gross_sales_z_score
,CASE 
	WHEN location_gross_sales_z_score >= 2.5 THEN 'A'
	WHEN location_gross_sales_z_score >= 1 THEN 'B'
	WHEN location_gross_sales_z_score >= 0 THEN 'C'
	WHEN location_gross_sales_z_score >= -0.5 THEN 'D'
	WHEN location_gross_sales_z_score >= -1 THEN 'E'
	ELSE 'F'
END as store_grade
FROM month_location_aggregate s
LEFT JOIN month_avg_std mas
ON s.month_idnt = mas.month_idnt
)
WITH DATA
PRIMARY INDEX (loc_idnt, month_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(loc_idnt, month_idnt)
     ON store_grade;      

--DROP TABLE total_aggregate;
CREATE MULTISET VOLATILE TABLE total_aggregate AS (
SELECT l.*
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
---space to add Clarity tables
/*,cet.clarity_total_physical_sellable_qty
,cet.clarity_total_unavailable_qty
,cet.clarity_total_damaged_qty
,cet.clarity_total_problem_qty
,cet.clarity_total_damaged_return_qty
,cet.clarity_total_hold_qty
,cet.clarity_total_reserved_qty
,cet.clarity_total_in_transit_qty
,im.clarity_total_received_qty
,im.clarity_total_p_h_transfer_in_qty
,im.clarity_total_rs_transfer_in_qty
,im.clarity_total_rack_transfer_in_qty
,im.clarity_total_other_transfer_in
,im.clarity_total_rack_transfer_out_qty
,im.clarity_total_po_transfer_in_qty
,im.clarity_total_po_transfer_out_qty
,im.clarity_total_rs_transfer_out_qty
,im.clarity_total_p_h_tranfer_out_qty
,im.clarity_total_other_transfer_out
,im.clarity_total_rtv_qty
,im.clarity_aoi_days
,im.clarity_aoi_weeks
,im.clarity_exposed_aoi_days
,im.clarity_exposed_aoi_weeks*/
,ao.nrp_po_oo_u
,ao.rp_po_oo_u
,ao.total_po_oo_u
,ra.rp_anticipated_rcpt_u_splits
,rpd.RP_Sales_Forecast
,rpd.Total_Mth_Forecast
,rpd.Prcnt_of_RP_Forecast
,t.in_transit_qty
,t.rp_in_transit_u
,t.nrp_in_transit_u
,s.class_supplier_count
,s.dept_supplier_count
,s.loc_supplier_count
,lpl.plan_cycle_lag_1
,lpl.sales_plan_lag_1
,lpl.bop_plan_lag_1
,lpl.rcpt_plan_lag_1
,lpl.uncapped_bop_lag_1
,lpl.plan_cycle_lag_2
,lpl.sales_plan_lag_2
,lpl.bop_plan_lag_2
,lpl.rcpt_plan_lag_2
,lpl.uncapped_bop_lag_2
,z.location_gross_sales_z_score
,z.store_grade
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
/*LEFT JOIN clarity_inv_snap cet
ON l.pkey = cet.pkey
LEFT JOIN inventory_movement im
ON l.pkey = im.pkey*/
LEFT JOIN po_oo_u ao
ON l.pkey = ao.pkey
LEFT JOIN rp_ant_rcpt_u ra
ON l.pkey = ra.pkey
LEFT JOIN rp_demand_u rpd
ON l.pkey = rpd.pkey
LEFT JOIN in_transit_oo t
ON l.pkey = t.pkey
LEFT JOIN supplier_count s
ON l.pkey = s.class_pkey
LEFT JOIN location_plan_lags lpl
ON l.pkey = lpl.pkey
LEFT JOIN store_grade z 
ON l.month_idnt = z.month_idnt
AND l.loc_idnt = z.loc_idnt
)
WITH DATA
PRIMARY INDEX (pkey)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(pkey)
     ON total_aggregate;

   -- SELECT * FROM total_aggregate




DELETE FROM {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack;


INSERT INTO {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack (
pkey
,ab_key
,rp_spend_key
,month_idnt
,month_desc
,month_label
,fiscal_month_num
,month_start_day_date
,month_end_day_date
,wks_in_month
,division -- "Division",
,subdivision -- "Subdivision",
,dept_label --"Department Label",
,class_label --"Class Label",
,dept_idnt  --as department --"Department",
,class_idnt  --as  "class",--"Class",
,loc_idnt  --as "Location",
,current_month_flag
,last_month_flag
,bop_plan --as "BOP Plan",
,rcpt_plan  --"Rcpt Plan",
,sales_plan  --"Sales Plan",
,uncapped_bop  --"Uncapped BOP"
,peer_group_type_desc
,climate
,store_dma_code -- as "DMA Code"
,dma_desc
,location  --AS "Location"
,store_type_desc  --AS "Store Type Desc"
,gross_square_footage -- AS "Gross Square Footage"
,store_open_date -- AS "Store Open Date"
,store_close_date -- AS "Store Close Date"
,region_desc -- AS "Region Desc"
,region_medium_desc -- AS "Region Medium Desc"
,region_short_desc -- AS "Region Short Desc"
,business_unit_desc  --AS "Business Unit Desc"
,group_desc -- AS "Group Desc"
,subgroup_desc -- AS "Subgroup Desc"
,subgroup_medium_desc -- AS "Subgrou Med Desc"
,subgroup_short_desc -- AS "Subgroup Short Desc"
,store_address_line_1 -- AS "Store Address Line 1"
,store_address_city -- AS "Store Address City"
,store_address_state -- AS "Store Address State"
,store_address_state_name -- AS "Store Address State Name"
,store_postal_code -- AS "Store Postal Code"
,store_address_county -- AS "Store Address County"
,store_country_code -- AS "Store Country Code"
,store_country_name -- AS "Store Country Name"
,store_location_latitude -- AS "Latitude"
,store_location_longitude -- AS "Longitude"
,distribution_center_num -- AS "Distribution Center Num"
,distribution_center_name -- AS "Distribution Center Name"
,channel_desc -- AS "Channel Desc"
,comp_status_desc -- AS "Comp Status Desc"
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
,in_transit_qty
,rp_in_transit_u
,nrp_in_transit_u
,class_supplier_count
,dept_supplier_count
,loc_supplier_count
,plan_cycle_lag_1
,sales_plan_lag_1
,bop_plan_lag_1
,rcpt_plan_lag_1
,uncapped_bop_lag_1
,plan_cycle_lag_2
,sales_plan_lag_2
,bop_plan_lag_2
,rcpt_plan_lag_2
,uncapped_bop_lag_2
,location_gross_sales_z_score
,store_grade
,dw_sys_load_tmstp
)
SELECT * FROM total_aggregate;


COLLECT STATISTICS COLUMN (pkey) ON {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack;
COLLECT STATISTICS COLUMN (class_idnt,dept_idnt,month_idnt,loc_idnt) ON {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack;
COLLECT STATISTICS COLUMN (class_idnt,month_idnt,loc_idnt) ON {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack;
COLLECT STATISTICS COLUMN (pkey,current_month_flag,last_month_flag) ON {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack;




/*

SELECT TOP 10 * FROM T3DL_NAP_PLANNING.LIT_AGGREGATE_TABLE_RACK

SELECT * FROM T3DL_NAP_PLANNING.LIT_AGGREGATE_TABLE_RACK
WHERE loc_idnt IN ('3')
*/

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
