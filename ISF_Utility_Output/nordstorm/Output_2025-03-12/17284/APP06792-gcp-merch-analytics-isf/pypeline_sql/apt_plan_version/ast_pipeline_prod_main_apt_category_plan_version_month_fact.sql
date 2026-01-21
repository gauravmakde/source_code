
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
WHERE month_idnt >= 202211
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


 CREATE MULTISET VOLATILE TABLE full_data AS (  
 SELECT
		 	a.country as selling_country 
			,a.banner as selling_brand 
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
			,CAST (0 as INTEGER) as average_inventory_retail_amount
			,CAST (0 as INTEGER) as average_inventory_cost_amount
			,CAST (0 as INTEGER) as average_inventory_units
			,CAST (0 as INTEGER) as beginning_of_period_inventory_retail_amount
			,CAST (0 as INTEGER) as beginning_of_period_inventory_cost_amount
			,CAST (0 as INTEGER) as beginning_of_period_inventory_units
			,CAST (0 as INTEGER) as receipts_retail_amount
			,CAST (0 as INTEGER) as receipts_cost_amount 
			,CAST (0 as INTEGER) as receipts_units 
		    ,CAST (0 as INTEGER) as receipts_less_reserve_retail_amount
			,CAST (0 as INTEGER) as receipts_less_reserve_cost_amount
			,CAST (0 as INTEGER) as receipts_less_reserve_units

		FROM T2DL_DAS_APT_COST_REPORTING.merch_assortment_category_cluster_plan_history a
		JOIN sc_date c
		ON a.fiscal_month_idnt = c.month_idnt
		JOIN sc_date s 
		ON a.snapshot_plan_month_idnt = s.month_idnt
		JOIN hierarchy h
		ON a.dept_idnt = h.dept_idnt
		JOIN sc_cattr g 
		ON a.dept_idnt = g.dept_num
		and a.category = g.category
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30 ,31,32,33,34 
    
   UNION ALL 

    SELECT
		 	a.selling_country 
			,a.selling_brand 
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
			,CAST (0 as INTEGER) as net_sales_retail_amount
			,CAST (0 as INTEGER) as net_sales_cost_amount
			,CAST (0 as INTEGER) as net_sales_units
			,a.average_inventory_retail_amount
			,a.average_inventory_cost_amount
			,a.average_inventory_units
			,a.beginning_of_period_inventory_retail_amount
			,a.beginning_of_period_inventory_cost_amount
			,a.beginning_of_period_inventory_units
			,a.receipts_retail_amount
			,a.receipts_cost_amount 
			,a.receipts_units 
		    ,a.receipts_less_reserve_retail_amount
			,a.receipts_less_reserve_cost_amount
			,a.receipts_less_reserve_units
		FROM T2DL_DAS_APT_COST_REPORTING.merch_assortment_category_country_plan_history a
		JOIN sc_date c
		ON a.fiscal_month_idnt = c.month_idnt
		JOIN sc_date s 
		ON a.snapshot_plan_month_idnt = s.month_idnt
		JOIN hierarchy h
		ON a.dept_idnt = h.dept_idnt
		JOIN sc_cattr g 
		ON a.dept_idnt = g.dept_num
		and a.category = g.category
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34  
	
	)    WITH DATA
   PRIMARY INDEX(category, month_id, dept_idnt)
   ON COMMIT PRESERVE ROWS;
    
--Lets get the current and future plan data    
--drop table sc_plan
DELETE FROM {environment_schema}.apt_category_plan_version_month_fact{env_suffix} ALL; 
INSERT INTO {environment_schema}.apt_category_plan_version_month_fact{env_suffix} 
SELECT
	d.selling_country 
	,d.selling_brand
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
	,SUM(d.receipts_retail_amount) AS rcpt_retail_amount
	,SUM(d.receipts_cost_amount) AS rcpt_cost_amount
	,SUM(d.receipts_units) AS rcpt_units
    ,SUM(d.receipts_less_reserve_retail_amount) AS rcpt_less_reserve_retail_amount
    ,SUM(d.receipts_less_reserve_cost_amount) AS rcpt_less_reserve_cost_amount
    ,SUM(d.receipts_less_reserve_units) AS rcpt_less_reserve_units
	FROM full_data d
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19;


COLLECT STATS
     PRIMARY INDEX(dept_idnt,category,month_id)
     ,COLUMN(dept_idnt)
     ,COLUMN(category)
     ,COLUMN(month_id)
     ON {environment_schema}.apt_category_plan_version_month_fact{env_suffix};