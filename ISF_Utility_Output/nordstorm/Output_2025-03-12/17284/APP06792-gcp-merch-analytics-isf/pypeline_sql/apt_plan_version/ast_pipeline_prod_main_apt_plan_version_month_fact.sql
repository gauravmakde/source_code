/*
Purpose:        Inserts data in table `apt_plan_version_month_fact` for APT Plan Version monthly table
Variable(s):    {{environment_schema}} T2DL_DAS_APT_COST_REPORTING (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' table suffix for prod testing 
Author(s):      Sara Scott
*/


--grab channel_info
CREATE MULTISET VOLATILE TABLE sc_cluster_map AS (
SELECT DISTINCT
  	   SELLING_COUNTRY
  	  ,SELLING_BRAND
  	  ,CLUSTER_NAME
  	  ,CASE WHEN cluster_name = 'NORDSTROM_CANADA_STORES' THEN 111
            WHEN cluster_name = 'NORDSTROM_CANADA_ONLINE' THEN 121
            WHEN cluster_name = 'NORDSTROM_STORES' THEN 110
            WHEN cluster_name = 'NORDSTROM_ONLINE' THEN 120
            WHEN cluster_name = 'RACK_ONLINE' THEN 250
            WHEN cluster_name = 'RACK_CANADA_STORES' THEN 211
            WHEN cluster_name IN ('RACK STORES', 'PRICE','HYBRID','BRAND') THEN 210
            WHEN cluster_name = 'NORD CA RSWH' THEN 311
            WHEN cluster_name = 'NORD US RSWH' THEN 310
            WHEN cluster_name = 'RACK CA RSWH' THEN 261
            WHEN cluster_name = 'RACK US RSWH' THEN 260
            END AS chnl_idnt
  	from T2DL_DAS_APT_COST_REPORTING.merch_assortment_supplier_cluster_plan_history
  	GROUP BY 1,2,3,4)

WITH DATA
PRIMARY INDEX (chnl_idnt, cluster_name,SELLING_COUNTRY,SELLING_BRAND) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, cluster_name,SELLING_COUNTRY,SELLING_BRAND)
	,COLUMN(chnl_idnt)
	,COLUMN(cluster_name)
	,COLUMN(SELLING_COUNTRY)
	,COLUMN(SELLING_BRAND)
ON sc_cluster_map ;

--lets create a week to month mapping
CREATE MULTISET VOLATILE TABLE sc_date AS(
SELECT
	month_idnt
	,month_label
	,month_454_label
	,fiscal_year_num||''||fiscal_month_num||' '||month_abrv AS month_id 
	,quarter_idnt 
	,quarter_label 
	,'FY'||SUBSTR(CAST(fiscal_year_num AS VARCHAR(5)),3,2)||' '||quarter_abrv AS quarter
	,half_label
	,fiscal_year_num 
	,COUNT(fiscal_week_num) AS week_count
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE month_idnt >= 202210
GROUP BY 1,2,3,4,5,6,7,8,9)

WITH DATA
   PRIMARY INDEX(month_idnt)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (month_idnt)
     ,COLUMN(month_idnt)
     ,COLUMN(quarter_idnt)
     ,COLUMN(fiscal_year_num)
     ON sc_date;

    
    
--lets collect category attributes
CREATE MULTISET VOLATILE TABLE sc_cattr AS( 
SELECT 
	a.dept_num
	,a.CATEGORY
	,a.category_group
from PRD_NAP_USR_VWS.CATG_SUBCLASS_MAP_DIM a
group by 1,2,3)

WITH DATA
   PRIMARY INDEX(dept_num,CATEGORY)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(dept_num,CATEGORY)
     ,COLUMN(dept_num)
     ,COLUMN(CATEGORY)
     ON sc_cattr;
    
--grabbing hierarchy info
CREATE MULTISET VOLATILE TABLE hierarchy AS (
SELECT DISTINCT
		
		 CAST(div_num AS INTEGER) AS div_idnt
	    , sku.div_desc
	    , CAST(grp_num AS INTEGER) AS subdiv_idnt -- subdivision
	    , sku.grp_desc as subdiv_desc
	    , CAST(dept_num  AS INTEGER)AS dept_idnt
	    , sku.dept_desc

FROM prd_nap_usr_vws.product_sku_dim_vw sku
)    WITH DATA
   PRIMARY INDEX(dept_idnt)
   ON COMMIT PRESERVE ROWS;
    
    
    
    
    
    
--Lets get the current and future plan data    
--drop table sc_plan
DELETE FROM {environment_schema}.apt_plan_version_month_fact{env_suffix} ALL; 
INSERT INTO {environment_schema}.apt_plan_version_month_fact{env_suffix} 
SELECT
	d.selling_country 
	,d.selling_brand
	,d.chnl_idnt
	,d.cluster_name
	,d.supplier_group
	,d.category 
	,d.category_group
	,d.dept_idnt
	,d.dept_desc
	,d.div_idnt
	,d.div_desc
	,d.subdiv_idnt
	,d.subdiv_desc
	,d.snapshot_plan_month_idnt
	,d.snapshot_month_id
	,d.snapshot_month_label
	,d.month_id
	,d.month_label
	,d.half_label
	,d.quarter
	,d.fiscal_year_num
	,d.alternate_inventory_model
	,SUM(d.net_sales_retail_amount) AS net_sales_retail_amount
	,SUM(d.net_sales_cost_amount) AS net_sales_cost_amount
	,SUM(d.net_sales_units) AS net_sales_units
	,SUM(d.average_inventory_retail_amount) AS average_inventory_retail_amount
	,SUM(d.average_inventory_cost_amount) AS average_inventory_cost_amount
	,SUM(d.average_inventory_units) AS average_inventory_units
	,SUM(d.beginning_of_period_inventory_retail_amount) AS beginning_of_period_inventory_retail_amount
	,SUM(d.beginning_of_period_inventory_cost_amount) AS beginning_of_period_inventory_cost_amount
	,SUM(d.beginning_of_period_inventory_units) AS beginning_of_period_inventory_units
	,SUM(d.replenishment_receipts_retail_amount) AS replenishment_receipts_retail_amount
	,SUM(d.replenishment_receipts_cost_amount) AS replenishment_receipts_cost_amount
	,SUM(d.replenishment_receipts_units) AS replenishment_receipts_units
	,SUM(d.nonreplenishment_receipts_retail_amount) AS nonreplenishment_receipts_retail_amount
	,SUM(d.nonreplenishment_receipts_cost_amount) AS nonreplenishment_receipts_cost_amount
	,SUM(d.nonreplenishment_receipts_units) AS nonreplenishment_receipts_units
	,SUM(d.replenishment_receipts_less_reserve_retail_amount+nonreplenishment_receipts_less_reserve_retail_amount) AS ttl_plan_rcpt_less_reserve_retail_amount
	,SUM(d.replenishment_receipts_less_reserve_cost_amount+nonreplenishment_receipts_less_reserve_cost_amount) AS ttl_plan_rcpt_less_reserve_cost_amount
	,SUM(d.replenishment_receipts_less_reserve_units+nonreplenishment_receipts_less_reserve_units) AS ttl_plan_rcpt_less_reserve_units
	 	FROM (SELECT
			a.selling_country 
			,a.selling_brand 
			,b.chnl_idnt
			,a.cluster_name 
			,a.supplier_group
			,a.category 
			,g.category_group
			,a.dept_idnt 
			,h.dept_desc
			,h.div_idnt
			,h.div_desc
			,h.subdiv_idnt
			,h.subdiv_desc
			,a.snapshot_plan_month_idnt
			,s.month_id AS snapshot_month_id
			,s.month_label AS snapshot_month_label
			,c.month_id
			,c.month_label
			,c.half_label
			,c.quarter
			,c.fiscal_year_num
			,a.alternate_inventory_model
			,a.net_sales_retail_amount
			,a.net_sales_cost_amount
			,a.net_sales_units
			,a.average_inventory_retail_amount
			,a.average_inventory_cost_amount
			,a.average_inventory_units
			,a.beginning_of_period_inventory_retail_amount
			,a.beginning_of_period_inventory_cost_amount
			,a.beginning_of_period_inventory_units
			,a.replenishment_receipts_retail_amount
			,a.replenishment_receipts_cost_amount 
			,a.replenishment_receipts_units 
			,a.replenishment_receipts_less_reserve_retail_amount
			,a.replenishment_receipts_less_reserve_cost_amount
			,a.replenishment_receipts_less_reserve_units
			,a.nonreplenishment_receipts_less_reserve_retail_amount
			,a.nonreplenishment_receipts_less_reserve_cost_amount
			,a.nonreplenishment_receipts_less_reserve_units
			,a.nonreplenishment_receipts_retail_amount
			,a.nonreplenishment_receipts_cost_amount 
			,a.nonreplenishment_receipts_units 
		FROM t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_history a
		JOIN sc_date c
		ON a.month_id = c.month_454_label
		JOIN sc_date s 
		ON a.snapshot_plan_month_idnt = s.month_idnt
		JOIN sc_cluster_map b 
		ON a.cluster_name = b.cluster_name
		JOIN hierarchy h
		ON a.dept_idnt = h.dept_idnt
		JOIN sc_cattr g 
		ON a.dept_idnt = g.dept_num
		and a.category = g.category
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43)d
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;


COLLECT STATS
     PRIMARY INDEX(dept_idnt,supplier_group,category,month_id)
     ,COLUMN(dept_idnt)
     ,COLUMN(supplier_group)
     ,COLUMN(category)
     ,COLUMN(month_id)
     ON {environment_schema}.apt_plan_version_month_fact{env_suffix};