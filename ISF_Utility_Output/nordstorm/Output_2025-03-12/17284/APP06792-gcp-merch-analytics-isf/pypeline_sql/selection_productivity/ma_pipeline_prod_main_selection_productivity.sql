
/*
Purpose:        Creates table with deciles to use in Decile Analysis Tableau report.
				Inserts data in {{environment_schema}} tables for Selection Productivity
                    selection_productivity
Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
                 {{start_date}} as beginning of time period
                 {{end_date}} as end of time period
Author(s):       Sara Scott
*/





CREATE MULTISET VOLATILE TABLE calendar AS (
		SELECT 
			month_idnt 
			, month_start_day_date
			, month_end_day_date
			, month_abrv
			, quarter_idnt
			, fiscal_halfyear_num as half_idnt
			, fiscal_year_num as year_idnt
			, CASE WHEN current_date >= month_start_day_date and current_date <= month_end_day_date THEN 1 else 0 END as cur_month
			, MIN(month_start_day_date) OVER(ORDER BY month_idnt) as window_start_date
			, MAX(month_end_day_date) OVER (ORDER BY month_idnt) as window_end_date

		FROM prd_nap_usr_vws.day_cal_454_dim 
		WHERE day_date BETWEEN {start_date} and {end_date}
		
		GROUP BY 1,2,3,4,5,6,7,8
		                                            
)
WITH DATA
PRIMARY INDEX (month_idnt)
ON COMMIT PRESERVE ROWS;

-- Fanatics
CREATE MULTISET VOLATILE TABLE fanatics AS (
SELECT DISTINCT
    rms_sku_num AS sku_idnt
from PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psd
JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM payto
  ON psd.prmy_supp_num = payto.order_from_vendor_num
WHERE payto.payto_vendor_num = '5179609'
) WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS 
;
COLLECT STATS 
     PRIMARY INDEX (sku_idnt)
    ON fanatics;

-- Anniversary
  create volatile multiset table anniversary as (
	select
		sku_idnt
		, channel_country 
		, CASE WHEN selling_channel = 'ONLINE' THEN 'DIGITAL' ELSE selling_channel END as channel
		, 'NORDSTROM' AS channel_brand
		, cal.month_idnt
		, 1 as anniversary_flag
	from T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE a
	JOIN prd_nap_usr_vws.day_cal_454_dim c
	ON a.day_idnt = c.day_idnt
	join calendar cal
		ON c.month_idnt = cal.month_idnt
	group by 1, 2, 3,4,5
)
with data and stats
primary index (sku_idnt, channel, month_idnt)
on commit preserve rows; 


CREATE MULTISET VOLATILE TABLE los_agg_fact AS (
SELECT month_idnt, customer_choice,  channel_country, channel_brand, channel, SUM(days_live) as days_live 
FROM (
SELECT day_date
, base.month_idnt
, base.quarter_idnt
, base.half_idnt
, base.year_idnt
, base.customer_choice
, base.channel
, base.channel_brand
, base.channel_country 

, MAX(days_live) as days_live

FROM T2DL_DAS_Selection.selection_productivity_base base
JOIN calendar
ON base.month_idnt = calendar.month_idnt
GROUP BY 1,2,3,4,5,6,7,8,9
) a
GROUP BY 1,2,3,4,5)
WITH DATA 
PRIMARY INDEX (month_idnt, customer_choice, channel_country)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (month_idnt, customer_choice, channel_country)
    ON los_agg_fact;


CREATE MULTISET VOLATILE TABLE hierarchy AS (
SELECT DISTINCT 
	TRIM(COALESCE(dept_num,'UNKNOWN') || '_' || COALESCE(prmy_supp_num,'UNKNOWN') || '_' || COALESCE(supp_part_num,'UNKNOWN') || '_' || COALESCE(color_num,'UNKNOWN')) AS customer_choice -- dept || supplier || vpn || color
    ,rms_sku_num AS sku_idnt
    ,channel_country 
    ,prmy_supp_num AS supplier_idnt
    ,supp.vendor_name as supplier_name
    ,CAST(div_num AS INTEGER) AS div_idnt
    ,sku.div_desc
    ,CAST(grp_num AS INTEGER) AS subdiv_idnt
    ,sku.grp_desc AS subdiv_desc
    ,CAST(dept_num  AS INTEGER)AS dept_idnt
    ,sku.dept_desc
    ,CAST(class_num AS INTEGER) AS class_idnt
    ,sku.class_desc 
    ,CAST(sbclass_num AS INTEGER) AS sbclass_idnt
    ,sku.sbclass_desc
    ,supp_part_num 
    ,CASE WHEN npg_ind  = 'Y' THEN 1 ELSE 0 END as npg_flag
	,ccs.quantrix_category
	,ccs.ccs_category
	,ccs.ccs_subcategory
	,ccs.nord_role_desc
	,ccs.rack_role_desc
	,ccs.parent_group
FROM prd_nap_usr_vws.product_sku_dim_vw sku
LEFT JOIN T2DL_DAS_CCS_CATEGORIES.CCS_MERCH_THEMES ccs
on sku.div_num = ccs.DIV_IDNT
and sku.dept_num = ccs.DEPT_IDNT
and sku.class_num = ccs.CLASS_IDNT
and sku.sbclass_num = ccs.SBCLASS_IDNT
LEFT JOIN prd_nap_usr_vws.vendor_dim supp
ON sku.prmy_supp_num = supp.vendor_num
WHERE sku.div_desc NOT LIKE 'INACTIVE%'
QUALIFY dense_rank() OVER (PARTITION BY sku_idnt, channel_country ORDER BY sku.dw_sys_load_tmstp DESC) = 1
)
WITH DATA 
PRIMARY INDEX (sku_idnt, channel_country)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (sku_idnt, channel_country)
    ON hierarchy;

CREATE MULTISET VOLATILE TABLE receipts_base AS (
SELECT 
r.sku_idnt
, mnth_idnt
, month_abrv
, quarter_idnt
, half_idnt
, year_idnt
,CASE WHEN st.business_unit_desc IN ('RACK CANADA', 'FULL LINE CANADA', 'N.CA') THEN 'CA'
    ELSE 'US' 
    END AS channel_country
,CASE WHEN st.business_unit_desc IN ('RACK', 'OFFPRICE ONLINE', 'RACK CANADA') THEN 'NORDSTROM_RACK'
    ELSE 'NORDSTROM' 
    END AS channel_brand
,CASE WHEN st.business_unit_desc IN ('FULL LINE', 'RACK', 'RACK CANADA', 'FULL LINE CANADA') THEN 'STORE'
    ELSE 'DIGITAL' 
    END AS channel
, CASE WHEN ds_ind = 'Y' THEN 1 ELSE 0 END as ds_ind
, CASE WHEN rp_ind = 'Y' THEN 1 ELSE 0 END as rp_ind
, SUM(receipt_tot_units + receipt_rsk_units) as rcpt_tot_units 
, SUM(receipt_tot_retail + receipt_rsk_retail) as rcpt_tot_retail
FROM T2DL_DAS_ASSORTMENT_DIM.receipt_sku_loc_week_agg_fact r 
JOIN prd_nap_usr_vws.store_dim st on r.store_num = st.store_num
JOIN calendar cal on r.mnth_idnt = cal.month_idnt
and cal.cur_month = 0
WHERE week_num >= 202301
AND st.business_unit_num NOT IN ('8000', '5500')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
UNION ALL 
SELECT 
r.sku_idnt 
, mnth_idnt
, month_abrv
, quarter_idnt
, half_idnt
, year_idnt
,CASE WHEN st.business_unit_desc IN ('RACK CANADA', 'FULL LINE CANADA', 'N.CA') THEN 'CA'
    ELSE 'US' 
    END AS channel_country
,CASE WHEN st.business_unit_desc IN ('RACK', 'OFFPRICE ONLINE', 'RACK CANADA') THEN 'NORDSTROM_RACK'
    ELSE 'NORDSTROM' 
    END AS channel_brand
,CASE WHEN st.business_unit_desc IN ('FULL LINE', 'RACK', 'RACK CANADA', 'FULL LINE CANADA') THEN 'STORE'
    ELSE 'DIGITAL' 
    END AS channel
, CASE WHEN receipt_ds_units >0 THEN 1 ELSE 0 END as ds_ind
, rp_ind
, SUM(receipt_po_units + receipt_ds_units + receipt_rsk_units) as rcpt_tot_units 
, SUM(receipt_po_retail + receipt_ds_retail + receipt_rsk_retail) as rcpt_tot_retail
FROM T2DL_DAS_ASSORTMENT_DIM.receipt_sku_loc_week_agg_fact_madm r
JOIN prd_nap_usr_vws.store_dim st on r.store_num = st.store_num
JOIN calendar cal on r.mnth_idnt = cal.month_idnt
and cal.cur_month = 0
WHERE week_num < 202301
AND st.business_unit_num NOT IN ('8000', '5500')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
WITH DATA
PRIMARY INDEX (sku_idnt, channel)
ON COMMIT PRESERVE ROWS;




    


CREATE MULTISET VOLATILE TABLE cc_stage as 
(  
SELECT 
base.month_idnt
, base.month_abrv
, base.quarter_idnt
, base.half_idnt
, base.year_idnt
, base.customer_choice
, base.channel
, base.channel_country 
, base.channel_brand
, h.div_idnt
, h.div_desc
, h.subdiv_idnt
, h.subdiv_desc
, h.dept_idnt
, h.dept_desc
, h.quantrix_category
, h.ccs_category
, h.ccs_subcategory
, h.nord_role_desc
, h.rack_role_desc
, h.parent_group
, h.supplier_name
, h.supp_part_num
, MAX(los.days_live) days_live
, MIN(days_published) as days_published
, MAX(new_flag) new_flag
, MAX(cf_flag) cf_flag
, MAX(h.npg_flag) npg_flag
, MAX(rp_flag) rp_flag
, MAX(dropship_flag) dropship_flag
, MAX(anniversary_flag) anniversary_flag
, MAX(fanatics_flag) fanatics_flag
, SUM(demand_dollars) AS demand_dollars
, SUM(demand_units) as demand_units
, SUM(sales_dollars) as sales_dollars 
, SUM(sales_units) as sales_units 
, CAST(0 AS DECIMAL(10,0)) AS receipt_units
, CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
, SUM(COALESCE(product_views,0)) as product_views
, SUM(COALESCE(order_sessions,0)) as order_sessions
, SUM(COALESCE(instock_views,0)) as instock_views
, SUM(COALESCE(product_view_sessions,0)) as product_view_sessions
, SUM(COALESCE(order_demand,0)) as order_demand
, SUM(COALESCE(order_quantity,0)) as order_quantity
, SUM(CASE WHEN last_day_of_month = 1 then eoh_dollars else 0 end) as eoh_dollars
, SUM(CASE WHEN last_day_of_month = 1 then eoh_units else 0 end) as eoh_units
, SUM(CASE WHEN first_day_of_month = 1 then boh_dollars else 0 end) as boh_dollars
, SUM(CASE WHEN first_day_of_month = 1 then boh_units else 0 end) as boh_units

FROM T2DL_DAS_Selection.selection_productivity_base base
JOIN hierarchy h ON 
base.customer_choice = h.customer_choice
AND base.channel_country = h.channel_country
AND base.sku_idnt = h.sku_idnt
LEFT JOIN los_agg_fact los
ON base.customer_choice = los.customer_choice
and base.channel_country = los.channel_country
and base.channel = los.channel
and base.channel_brand = los.channel_brand
and base.month_idnt = los.month_idnt
JOIN calendar cal
ON base.month_idnt = cal.month_idnt
AND cal.cur_month = 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23

UNION ALL 

 SELECT 
 	r.mnth_idnt as month_idnt
    , r.month_abrv
    , r.quarter_idnt 
    , r.half_idnt
	, r.year_idnt
    , s.customer_choice
	, r.channel
    , r.channel_country
	, r.channel_brand
	, h.div_idnt
	, h.div_desc
	, h.subdiv_idnt
	, h.subdiv_desc
	, h.dept_idnt
	, h.dept_desc
	, h.quantrix_category
	, h.ccs_category
	, h.ccs_subcategory
	, h.nord_role_desc
	, h.rack_role_desc
	, h.parent_group
	, h.supplier_name
	, h.supp_part_num
	, MAX(days_live) as days_live
    , Null as days_published
	, CASE WHEN cc_type = 'NEW' THEN 1 ELSE 0 END as new_flag
	, COALESCE(CASE WHEN cc_type = 'CF' THEN 1 ELSE 0 END,0) as cf_flag
	, MAX(h.npg_flag) npg_flag
    , MAX(rp_ind) as rp_ind
    , MAX(ds_ind) as ds_ind
	, MAX(anniversary_flag) anniversary_flag
	, MAX(CASE WHEN f.sku_idnt IS NOT NULL then 1 ELSE 0 END) AS fanatics_flag
	, CAST(0 AS DECIMAL(12,2)) AS demand_dollars
    , CAST(0 AS DECIMAL(10,0)) AS demand_units
    , CAST(0 AS DECIMAL(12,2)) AS sales_dollars
	, CAST(0 AS DECIMAL(10,0)) AS sales_units
	, SUM(COALESCE(rcpt_tot_units, 0)) as receipt_units
    , SUM(COALESCE(rcpt_tot_retail, 0)) as receipt_dollars
    , CAST(0 AS DECIMAL(38,6)) AS product_views
	, CAST(0 AS INTEGER) AS order_sessions
	, CAST(0 AS DECIMAL(38,6)) AS instock_views
	, CAST(0 AS DECIMAL(38,6)) AS product_view_sessions
	, CAST(0 AS DECIMAL(20,2)) AS order_demand
	, CAST(0 AS INTEGER) AS order_quantity
    , CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
    , CAST(0 AS DECIMAL(10,0)) AS eoh_units
    , CAST(0 AS DECIMAL(12,2)) AS boh_dollars
    , CAST(0 AS DECIMAL(10,0)) AS boh_units
  FROM receipts_base r
  JOIN t2dl_das_assortment_dim.sku_cc_lkp  s
  ON r.sku_idnt = s.sku_idnt 
  AND r.channel_country = s.channel_country
  JOIN hierarchy h ON 
	s.customer_choice = h.customer_choice
  AND r.channel_country = h.channel_country
  AND r.sku_idnt = h.sku_idnt
  LEFT JOIN los_agg_fact los
  ON s.customer_choice = los.customer_choice
  and r.channel_country = los.channel_country
  and r.channel = los.channel
  and r.channel_brand = los.channel_brand
  and r.mnth_idnt = los.month_idnt
  LEFT JOIN anniversary a
  ON r.sku_idnt = a.sku_idnt
  and r.mnth_idnt = a.month_idnt
  and r.channel_brand = a.channel_brand 
  and r.channel = a.channel
  and r.channel_country = a.channel_country
  LEFT JOIN fanatics f 
  ON r.sku_idnt = f.sku_idnt
  LEFT JOIN t2dl_das_assortment_dim.cc_type c
  ON c.customer_choice = s.customer_choice
  and c.mnth_idnt = r.mnth_idnt
  and c.channel_country = r.channel_country
  and c.channel_brand = r.channel_brand
  and c.channel = r.channel
  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25,26,27

)
WITH DATA 
PRIMARY INDEX (month_idnt, customer_choice)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (month_idnt, customer_choice)
    ON cc_stage;



CREATE MULTISET VOLATILE TABLE cc_month_base AS 
( 
	SELECT 
month_idnt
, month_abrv
, quarter_idnt
, half_idnt
, year_idnt
, customer_choice
, channel
, channel_country 
, channel_brand
, div_idnt
, div_desc
, subdiv_idnt
, subdiv_desc
, dept_idnt
, dept_desc
, quantrix_category
, ccs_category
, ccs_subcategory
, nord_role_desc
, rack_role_desc
, parent_group
, supplier_name
, supp_part_num
, MAX(days_live) days_live
, MIN(days_published) as days_published
, MAX(new_flag) new_flag
, MAX(cf_flag) cf_flag
, MAX(npg_flag) npg_flag
, MAX(rp_flag) rp_flag
, MAX(dropship_flag) dropship_flag
, MAX(anniversary_flag) anniversary_flag
, MAX(fanatics_flag) fanatics_flag
, SUM(COALESCE(demand_dollars,0)) AS demand_dollars
, SUM(COALESCE(demand_units,0)) as demand_units
, SUM(COALESCE(sales_dollars,0)) as sales_dollars 
, SUM(COALESCE(sales_units,0)) as sales_units 
, SUM(COALESCE(receipt_units,0)) AS receipt_units
, SUM(COALESCE(receipt_dollars,0)) AS receipt_dollars
, SUM(COALESCE(product_views,0)) as product_views
, SUM(COALESCE(order_sessions,0)) as order_sessions
, SUM(COALESCE(instock_views,0)) as instock_views
, SUM(COALESCE(product_view_sessions,0)) as product_view_sessions
, SUM(COALESCE(order_demand,0)) as order_demand
, SUM(COALESCE(order_quantity,0)) as order_quantity
, SUM(COALESCE(eoh_dollars,0)) as eoh_dollars
, SUM(COALESCE(eoh_units,0)) as eoh_units
, SUM(COALESCE(boh_dollars,0)) as boh_dollars
, SUM(COALESCE(boh_units,0)) as boh_units
FROM cc_stage
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
WITH DATA 
PRIMARY INDEX (month_idnt, customer_choice)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (month_idnt, customer_choice)
    ON cc_month_base;


CREATE MULTISET VOLATILE TABLE cc_month as 
(
SELECT month_idnt
, month_abrv
, quarter_idnt
, half_idnt
, year_idnt
, customer_choice
, channel
, channel_brand
, channel_country
, div_idnt
, div_desc
, subdiv_idnt
, subdiv_desc
, dept_idnt
, dept_desc
, supplier_name
, supp_part_num
, quantrix_category
, ccs_category
, ccs_subcategory
, nord_role_desc
, rack_role_desc
, parent_group
, days_live
, days_published
, new_flag
, cf_flag
, npg_flag
, rp_flag
, dropship_flag
, anniversary_flag
, fanatics_flag
, sales_dollars
, demand_dollars
, demand_units
, sales_units 
, receipt_dollars 
, receipt_units 
, eoh_dollars
, eoh_units
, boh_dollars
, boh_units
, product_views
, order_sessions
, instock_views
, product_view_sessions
, order_demand
, order_quantity
--Running sums
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt, dept_idnt, quantrix_category order by sales_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_category_sales
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt, dept_idnt, quantrix_category order by demand_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_category_demand
, SUM(sales_dollars) over (partition by month_idnt,channel, channel_brand, channel_country, div_idnt, subdiv_idnt, dept_idnt order by sales_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_sales
, SUM(demand_dollars) over (partition by month_idnt,channel, channel_brand, channel_country, div_idnt, subdiv_idnt, dept_idnt order by demand_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_demand
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt order by sales_dollars desc ROWS UNBOUNDED PRECEDING) channel_subdiv_sales
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt order by demand_dollars desc ROWS UNBOUNDED PRECEDING) channel_subdiv_demand
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_brand, channel_country, div_idnt order by sales_dollars desc ROWS UNBOUNDED PRECEDING) channel_div_sales
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_brand, channel_country, div_idnt order by demand_dollars desc ROWS UNBOUNDED PRECEDING) channel_div_demand
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_brand, channel_country order by sales_dollars desc ROWS UNBOUNDED PRECEDING) channel_sales
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_brand, channel_country order by demand_dollars desc ROWS UNBOUNDED PRECEDING) channel_demand
--Totals
, SUM(sales_dollars) over (partition by month_idnt,channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt, quantrix_category) as sales_channel_dept_cat
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt, quantrix_category) as demand_channel_dept_cat
, SUM(sales_dollars) over( partition by month_idnt,channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt) as sales_channel_dept
, SUM(demand_dollars) over (partition by month_idnt,channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt) as demand_channel_dept
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt) as sales_channel_subdiv
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt) as demand_channel_subdiv
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_country, channel_brand, div_idnt) as sales_channel_div
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_country, channel_brand, div_idnt) as demand_channel_div
, SUM(sales_dollars) over (partition by month_idnt, channel, channel_country, channel_brand) as sales_channel
, SUM(demand_dollars) over (partition by month_idnt, channel, channel_country, channel_brand) as demand_channel
FROM cc_month_base 

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31 ,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48
)
WITH DATA 
PRIMARY INDEX (month_idnt, customer_choice)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (month_idnt, customer_choice)
    ON cc_month;


CREATE MULTISET VOLATILE TABLE cc_quarter_base as (
SELECT

 base.quarter_idnt
, base.half_idnt
, base.year_idnt
, base.customer_choice
, base.channel
, base.channel_country 
, base.channel_brand
, h.div_idnt
, h.div_desc
, h.subdiv_idnt
, h.subdiv_desc
, h.dept_idnt
, h.dept_desc
, h.quantrix_category
, h.ccs_category
, h.ccs_subcategory
, h.nord_role_desc
, h.rack_role_desc
, h.parent_group
, h.supplier_name
, h.supp_part_num
, MAX(new_flag) new_flag
, MAX(cf_flag) cf_flag
, MAX(h.npg_flag) npg_flag
, MAX(rp_flag) rp_flag
, MAX(dropship_flag) dropship_flag
, MAX(anniversary_flag) anniversary_flag
, MAX(fanatics_flag) fanatics_flag
, SUM(demand_dollars) AS demand_dollars
, SUM(sales_dollars) as sales_dollars 
, SUM(CASE WHEN last_day_of_qtr  = 1 then eoh_dollars else 0 end) as eoh_dollars
, SUM(CASE WHEN last_day_of_qtr = 1 then eoh_units else 0 end) as eoh_units
, SUM(CASE WHEN first_day_of_qtr = 1 then boh_dollars else 0 end) as boh_dollars
, SUM(CASE WHEN first_day_of_qtr = 1 then boh_units else 0 end) as boh_units

FROM T2DL_DAS_Selection.selection_productivity_base base
JOIN hierarchy h ON 
base.customer_choice = h.customer_choice
AND base.channel_country = h.channel_country
AND base.sku_idnt = h.sku_idnt
JOIN calendar cal
ON base.month_idnt = cal.month_idnt
and cur_month = 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

)

WITH DATA 
PRIMARY INDEX (quarter_idnt, customer_choice)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (quarter_idnt, customer_choice)
    ON cc_quarter_base;

CREATE MULTISET VOLATILE TABLE cc_quarter as (
SELECT 

  quarter_idnt
, half_idnt
, year_idnt
, customer_choice
, channel
, channel_brand
, channel_country
, div_idnt
, div_desc
, subdiv_idnt
, subdiv_desc
, dept_idnt
, dept_desc
, supplier_name
, supp_part_num
, quantrix_category
, ccs_category
, ccs_subcategory
, nord_role_desc
, rack_role_desc
, parent_group
, new_flag
, cf_flag
, npg_flag
, rp_flag
, dropship_flag
, anniversary_flag
, fanatics_flag
, sales_dollars
, demand_dollars
, eoh_dollars
, eoh_units
, boh_dollars
, boh_units
--Running sums
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt, dept_idnt, quantrix_category order by sales_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_category_sales
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country,div_idnt, subdiv_idnt, dept_idnt, quantrix_category order by demand_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_category_demand
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country, div_idnt,subdiv_idnt, dept_idnt order by sales_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_sales
, SUM(demand_dollars) over (partition by quarter_idnt,channel, channel_brand, channel_country, div_idnt,subdiv_idnt, dept_idnt order by demand_dollars desc ROWS UNBOUNDED PRECEDING) as channel_dept_demand
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt order by sales_dollars desc ROWS UNBOUNDED PRECEDING) channel_subdiv_sales
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt order by demand_dollars desc ROWS UNBOUNDED PRECEDING) channel_subdiv_demand
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country, div_idnt order by sales_dollars desc ROWS UNBOUNDED PRECEDING) channel_div_sales
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country, div_idnt order by demand_dollars desc ROWS UNBOUNDED PRECEDING) channel_div_demand
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country order by sales_dollars desc ROWS UNBOUNDED PRECEDING) channel_sales
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_brand, channel_country order by demand_dollars desc ROWS UNBOUNDED PRECEDING) channel_demand
--Totals
, SUM(sales_dollars) over (partition by quarter_idnt,channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt, quantrix_category) as sales_channel_dept_cat
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt, quantrix_category) as demand_channel_dept_cat
, SUM(sales_dollars) over( partition by quarter_idnt,channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt) as sales_channel_dept
, SUM(demand_dollars) over (partition by quarter_idnt,channel, channel_country, channel_brand, div_idnt, subdiv_idnt, dept_idnt) as demand_channel_dept
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt) as sales_channel_subdiv
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt) as demand_channel_subdiv
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand, div_idnt) as sales_channel_div
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand, div_idnt) as demand_channel_div
, SUM(sales_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand) as sales_channel
, SUM(demand_dollars) over (partition by quarter_idnt, channel, channel_country, channel_brand) as demand_channel
FROM cc_quarter_base 


GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34)


WITH DATA 
PRIMARY INDEX (quarter_idnt, customer_choice)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (quarter_idnt, customer_choice)
    ON cc_quarter;



DELETE FROM {environment_schema}.selection_productivity{env_suffix} WHERE month_idnt IN (SELECT DISTINCT month_idnt FROM calendar); 
INSERT INTO {environment_schema}.selection_productivity{env_suffix}   
 SELECT d.month_idnt
, d.month_abrv
, d.quarter_idnt
, d.half_idnt
, d.year_idnt
, d.customer_choice
, d.channel
, d.channel_brand
, d.channel_country
, d.div_idnt
, d.div_desc
, d.subdiv_idnt
, d.subdiv_desc
, d.dept_idnt
, d.dept_desc
, d.supplier_name
, d.supp_part_num
, d.quantrix_category
, d.ccs_category
, d.ccs_subcategory
, d.nord_role_desc
, d.rack_role_desc
, d.parent_group
, d.days_live
, d.days_published
, d.new_flag
, d.cf_flag
, CASE WHEN d.npg_flag = 1 THEN 'Y' ELSE 'N' END as npg_flag
, d.rp_flag
, d.dropship_flag
, d.anniversary_flag
, CASE WHEN d.fanatics_flag = 1 THEN 'Y' ELSE 'N' END as fanatics_flag
--month deciles
,CASE WHEN d.demand_dollars <= 0 then 'Demand <=0'
	WHEN d.channel_dept_category_demand >= .9 * d.demand_channel_dept_cat THEN '10'
	WHEN d.channel_dept_category_demand >= .8 * d.demand_channel_dept_cat THEN '9'
	WHEN d.channel_dept_category_demand >= .7 * d.demand_channel_dept_cat THEN '8'
	WHEN d.channel_dept_category_demand >= .6 * d.demand_channel_dept_cat THEN '7'
	WHEN d.channel_dept_category_demand >= .5 * d.demand_channel_dept_cat THEN '6'
	WHEN d.channel_dept_category_demand >= .4 * d.demand_channel_dept_cat THEN '5'
	WHEN d.channel_dept_category_demand >= .3 * d.demand_channel_dept_cat THEN '4'
	WHEN d.channel_dept_category_demand >= .2 * d.demand_channel_dept_cat THEN '3'
	WHEN d.channel_dept_category_demand >= .1 * d.demand_channel_dept_cat THEN '2'
	ELSE '1'
END as channel_dept_cat_demand_deciles
,CASE WHEN d.sales_dollars <= 0 then 'Sales <=0'
	WHEN d.channel_dept_category_sales >= .9 * d.sales_channel_dept_cat THEN '10' 
	WHEN d.channel_dept_category_sales >= .8 * d.sales_channel_dept_cat THEN '9'
	WHEN d.channel_dept_category_sales >= .7 * d.sales_channel_dept_cat THEN '8'
	WHEN d.channel_dept_category_sales >= .6 * d.sales_channel_dept_cat THEN '7'
	WHEN d.channel_dept_category_sales >= .5 * d.sales_channel_dept_cat THEN '6'
	WHEN d.channel_dept_category_sales >= .4 * d.sales_channel_dept_cat THEN '5'
	WHEN d.channel_dept_category_sales >= .3 * d.sales_channel_dept_cat THEN '4'
	WHEN d.channel_dept_category_sales >= .2 * d.sales_channel_dept_cat THEN '3'
	WHEN d.channel_dept_category_sales >= .1 * d.sales_channel_dept_cat THEN '2'
	ELSE '1'
END as channel_dept_cat_sales_deciles
,CASE WHEN d.demand_dollars <= 0 then 'Demand <=0'
	WHEN d.channel_dept_demand >= .9* d.demand_channel_dept THEN '10' 
	WHEN d.channel_dept_demand >= .8* d.demand_channel_dept THEN '9'
	WHEN d.channel_dept_demand >= .7* d.demand_channel_dept THEN '8'
	WHEN d.channel_dept_demand >= .6* d.demand_channel_dept THEN '7'
	WHEN d.channel_dept_demand >= .5* d.demand_channel_dept THEN '6'
	WHEN d.channel_dept_demand >= .4* d.demand_channel_dept THEN '5'
	WHEN d.channel_dept_demand >= .3* d.demand_channel_dept THEN '4'
	WHEN d.channel_dept_demand >= .2* d.demand_channel_dept THEN '3'
	WHEN d.channel_dept_demand >= .1* d.demand_channel_dept THEN '2'
	ELSE '1'
END as channel_dept_demand_deciles
,CASE WHEN d.sales_dollars <= 0 then 'Sales <=0'
	WHEN d.channel_dept_sales >= .9 * d.sales_channel_dept THEN '10' 
	WHEN d.channel_dept_sales >= .8 * d.sales_channel_dept THEN '9'
	WHEN d.channel_dept_sales >= .7 * d.sales_channel_dept THEN '8'
	WHEN d.channel_dept_sales >= .6 * d.sales_channel_dept THEN '7'
	WHEN d.channel_dept_sales >= .5 * d.sales_channel_dept THEN '6'
	WHEN d.channel_dept_sales >= .4 * d.sales_channel_dept THEN '5'
	WHEN d.channel_dept_sales >= .3 * d.sales_channel_dept THEN '4'
	WHEN d.channel_dept_sales >= .2 * d.sales_channel_dept THEN '3'
	WHEN d.channel_dept_sales >= .1 * d.sales_channel_dept THEN '2'
	ELSE '1'
END as channel_dept_sales_deciles
,CASE WHEN d.demand_dollars <= 0 then 'Demand <=0'
	WHEN d.channel_subdiv_demand >= .9 * d.demand_channel_subdiv THEN '10' 
	WHEN d.channel_subdiv_demand >= .8 * d.demand_channel_subdiv THEN '9'
	WHEN d.channel_subdiv_demand >= .7 * d.demand_channel_subdiv THEN '8'
	WHEN d.channel_subdiv_demand >= .6 * d.demand_channel_subdiv THEN '7'
	WHEN d.channel_subdiv_demand >= .5 * d.demand_channel_subdiv THEN '6'
	WHEN d.channel_subdiv_demand >= .4 * d.demand_channel_subdiv THEN '5'
	WHEN d.channel_subdiv_demand >= .3 * d.demand_channel_subdiv THEN '4'
	WHEN d.channel_subdiv_demand >= .2 * d.demand_channel_subdiv THEN '3'
	WHEN d.channel_subdiv_demand >= .1 * d.demand_channel_subdiv THEN '2'
	ELSE '1'
END as channel_subdiv_demand_deciles
,CASE WHEN d.sales_dollars <= 0 then 'Sales <=0'
	WHEN d.channel_subdiv_sales >= .9 * d.sales_channel_subdiv THEN '10' 
	WHEN d.channel_subdiv_sales >= .8 * d.sales_channel_subdiv THEN '9'
	WHEN d.channel_subdiv_sales >= .7 * d.sales_channel_subdiv THEN '8'
	WHEN d.channel_subdiv_sales >= .6 * d.sales_channel_subdiv THEN '7'
	WHEN d.channel_subdiv_sales >= .5 * d.sales_channel_subdiv THEN '6'
	WHEN d.channel_subdiv_sales >= .4 * d.sales_channel_subdiv THEN '5'
	WHEN d.channel_subdiv_sales >= .3 * d.sales_channel_subdiv THEN '4'
	WHEN d.channel_subdiv_sales >= .2 * d.sales_channel_subdiv THEN '3'
	WHEN d.channel_subdiv_sales >= .1 * d.sales_channel_subdiv THEN '2'
	ELSE '1'
END as channel_subdiv_sales_deciles
,CASE WHEN d.demand_dollars <= 0 then 'Demand <=0'
	WHEN d.channel_div_demand >= .9 * d.demand_channel_div THEN '10' 
	WHEN d.channel_div_demand >= .8 * d.demand_channel_div THEN '9'
	WHEN d.channel_div_demand >= .7 * d.demand_channel_div THEN '8'
	WHEN d.channel_div_demand >= .6 * d.demand_channel_div THEN '7'
	WHEN d.channel_div_demand >= .5 * d.demand_channel_div THEN '6'
	WHEN d.channel_div_demand >= .4 * d.demand_channel_div THEN '5'
	WHEN d.channel_div_demand >= .3 * d.demand_channel_div THEN '4'
	WHEN d.channel_div_demand >= .2 * d.demand_channel_div THEN '3'
	WHEN d.channel_div_demand >= .1 * d.demand_channel_div THEN '2'
	ELSE '1'
END as channel_div_demand_deciles
,CASE WHEN d.sales_dollars <= 0 then 'Sales <=0'
	WHEN d.channel_div_sales >= .9 * d.sales_channel_div THEN '10' 
	WHEN d.channel_div_sales >= .8 * d.sales_channel_div THEN '9'
	WHEN d.channel_div_sales >= .7 * d.sales_channel_div THEN '8'
	WHEN d.channel_div_sales >= .6 * d.sales_channel_div THEN '7'
	WHEN d.channel_div_sales >= .5 * d.sales_channel_div THEN '6'
	WHEN d.channel_div_sales >= .4 * d.sales_channel_div THEN '5'
	WHEN d.channel_div_sales >= .3 * d.sales_channel_div THEN '4'
	WHEN d.channel_div_sales >= .2 * d.sales_channel_div THEN '3'
	WHEN d.channel_div_sales >= .1 * d.sales_channel_div THEN '2'
	ELSE '1'
END as channel_div_sales_deciles
,CASE WHEN d.demand_dollars <= 0 then 'Demand <=0'
	WHEN d.channel_demand >= .9 * d.demand_channel THEN '10' 
	WHEN d.channel_demand >= .8 * d.demand_channel THEN '9'
	WHEN d.channel_demand >= .7 * d.demand_channel THEN '8'
	WHEN d.channel_demand >= .6 * d.demand_channel THEN '7'
	WHEN d.channel_demand >= .5 * d.demand_channel THEN '6'
	WHEN d.channel_demand >= .4 * d.demand_channel THEN '5'
	WHEN d.channel_demand >= .3 * d.demand_channel THEN '4'
	WHEN d.channel_demand >= .2 * d.demand_channel THEN '3'
	WHEN d.channel_demand >= .1 * d.demand_channel THEN '2'
	ELSE '1'
END as channel_demand_deciles
,CASE WHEN d.sales_dollars <= 0 then 'Sales <=0'
	WHEN d.channel_sales >= .9 * d.sales_channel THEN '10' 
	WHEN d.channel_sales >= .8 * d.sales_channel THEN '9'
	WHEN d.channel_sales >= .7 * d.sales_channel THEN '8'
	WHEN d.channel_sales >= .6 * d.sales_channel THEN '7'
	WHEN d.channel_sales >= .5 * d.sales_channel THEN '6'
	WHEN d.channel_sales >= .4 * d.sales_channel THEN '5'
	WHEN d.channel_sales >= .3 * d.sales_channel THEN '4'
	WHEN d.channel_sales >= .2 * d.sales_channel THEN '3'
	WHEN d.channel_sales >= .1 * d.sales_channel THEN '2'
	ELSE '1'
END as channel_sales_deciles

,CASE WHEN qtr.demand_dollars <= 0 then 'Demand <=0'
	WHEN qtr.channel_dept_category_demand >= .9 * qtr.demand_channel_dept_cat THEN '10'
	WHEN qtr.channel_dept_category_demand >= .8 * qtr.demand_channel_dept_cat THEN '9'
	WHEN qtr.channel_dept_category_demand >= .7 * qtr.demand_channel_dept_cat THEN '8'
	WHEN qtr.channel_dept_category_demand >= .6 * qtr.demand_channel_dept_cat THEN '7'
	WHEN qtr.channel_dept_category_demand >= .5 * qtr.demand_channel_dept_cat THEN '6'
	WHEN qtr.channel_dept_category_demand >= .4 * qtr.demand_channel_dept_cat THEN '5'
	WHEN qtr.channel_dept_category_demand >= .3 * qtr.demand_channel_dept_cat THEN '4'
	WHEN qtr.channel_dept_category_demand >= .2 * qtr.demand_channel_dept_cat THEN '3'
	WHEN qtr.channel_dept_category_demand >= .1 * qtr.demand_channel_dept_cat THEN '2'
	ELSE '1'
END as qtr_channel_dept_cat_demand_deciles
,CASE WHEN qtr.sales_dollars <= 0 then 'Sales <=0'
	WHEN qtr.channel_dept_category_sales >= .9 * qtr.sales_channel_dept_cat THEN '10' 
	WHEN qtr.channel_dept_category_sales >= .8 * qtr.sales_channel_dept_cat THEN '9'
	WHEN qtr.channel_dept_category_sales >= .7 * qtr.sales_channel_dept_cat THEN '8'
	WHEN qtr.channel_dept_category_sales >= .6 * qtr.sales_channel_dept_cat THEN '7'
	WHEN qtr.channel_dept_category_sales >= .5 * qtr.sales_channel_dept_cat THEN '6'
	WHEN qtr.channel_dept_category_sales >= .4 * qtr.sales_channel_dept_cat THEN '5'
	WHEN qtr.channel_dept_category_sales >= .3 * qtr.sales_channel_dept_cat THEN '4'
	WHEN qtr.channel_dept_category_sales >= .2 * qtr.sales_channel_dept_cat THEN '3'
	WHEN qtr.channel_dept_category_sales >= .1 * qtr.sales_channel_dept_cat THEN '2'
	ELSE '1'
END as qtr_channel_dept_cat_sales_deciles
,CASE WHEN qtr.demand_dollars <= 0 then 'Demand <=0'
	WHEN qtr.channel_dept_demand >= .9* qtr.demand_channel_dept THEN '10' 
	WHEN qtr.channel_dept_demand >= .8* qtr.demand_channel_dept THEN '9'
	WHEN qtr.channel_dept_demand >= .7* qtr.demand_channel_dept THEN '8'
	WHEN qtr.channel_dept_demand >= .6* qtr.demand_channel_dept THEN '7'
	WHEN qtr.channel_dept_demand >= .5* qtr.demand_channel_dept THEN '6'
	WHEN qtr.channel_dept_demand >= .4* qtr.demand_channel_dept THEN '5'
	WHEN qtr.channel_dept_demand >= .3* qtr.demand_channel_dept THEN '4'
	WHEN qtr.channel_dept_demand >= .2* qtr.demand_channel_dept THEN '3'
	WHEN qtr.channel_dept_demand >= .1* qtr.demand_channel_dept THEN '2'
	ELSE '1'
END as qtr_channel_dept_demand_deciles
,CASE WHEN qtr.sales_dollars <= 0 then 'Sales <=0'
	WHEN qtr.channel_dept_sales >= .9 * qtr.sales_channel_dept THEN '10' 
	WHEN qtr.channel_dept_sales >= .8 * qtr.sales_channel_dept THEN '9'
	WHEN qtr.channel_dept_sales >= .7 * qtr.sales_channel_dept THEN '8'
	WHEN qtr.channel_dept_sales >= .6 * qtr.sales_channel_dept THEN '7'
	WHEN qtr.channel_dept_sales >= .5 * qtr.sales_channel_dept THEN '6'
	WHEN qtr.channel_dept_sales >= .4 * qtr.sales_channel_dept THEN '5'
	WHEN qtr.channel_dept_sales >= .3 * qtr.sales_channel_dept THEN '4'
	WHEN qtr.channel_dept_sales >= .2 * qtr.sales_channel_dept THEN '3'
	WHEN qtr.channel_dept_sales >= .1 * qtr.sales_channel_dept THEN '2'
	ELSE '1'
END as qtr_channel_dept_sales_deciles
,CASE WHEN qtr.demand_dollars <= 0 then 'Demand <=0'
	WHEN qtr.channel_subdiv_demand >= .9 * qtr.demand_channel_subdiv THEN '10' 
	WHEN qtr.channel_subdiv_demand >= .8 * qtr.demand_channel_subdiv THEN '9'
	WHEN qtr.channel_subdiv_demand >= .7 * qtr.demand_channel_subdiv THEN '8'
	WHEN qtr.channel_subdiv_demand >= .6 * qtr.demand_channel_subdiv THEN '7'
	WHEN qtr.channel_subdiv_demand >= .5 * qtr.demand_channel_subdiv THEN '6'
	WHEN qtr.channel_subdiv_demand >= .4 * qtr.demand_channel_subdiv THEN '5'
	WHEN qtr.channel_subdiv_demand >= .3 * qtr.demand_channel_subdiv THEN '4'
	WHEN qtr.channel_subdiv_demand >= .2 * qtr.demand_channel_subdiv THEN '3'
	WHEN qtr.channel_subdiv_demand >= .1 * qtr.demand_channel_subdiv THEN '2'
	ELSE '1'
END as qtr_channel_subdiv_demand_deciles
,CASE WHEN qtr.sales_dollars <= 0 then 'Sales <=0'
	WHEN qtr.channel_subdiv_sales >= .9 * qtr.sales_channel_subdiv THEN '10' 
	WHEN qtr.channel_subdiv_sales >= .8 * qtr.sales_channel_subdiv THEN '9'
	WHEN qtr.channel_subdiv_sales >= .7 * qtr.sales_channel_subdiv THEN '8'
	WHEN qtr.channel_subdiv_sales >= .6 * qtr.sales_channel_subdiv THEN '7'
	WHEN qtr.channel_subdiv_sales >= .5 * qtr.sales_channel_subdiv THEN '6'
	WHEN qtr.channel_subdiv_sales >= .4 * qtr.sales_channel_subdiv THEN '5'
	WHEN qtr.channel_subdiv_sales >= .3 * qtr.sales_channel_subdiv THEN '4'
	WHEN qtr.channel_subdiv_sales >= .2 * qtr.sales_channel_subdiv THEN '3'
	WHEN qtr.channel_subdiv_sales >= .1 * qtr.sales_channel_subdiv THEN '2'
	ELSE '1'
END as qtr_channel_subdiv_sales_deciles
,CASE WHEN qtr.demand_dollars <= 0 then 'Demand <=0'
	WHEN qtr.channel_div_demand >= .9 * qtr.demand_channel_div THEN '10' 
	WHEN qtr.channel_div_demand >= .8 * qtr.demand_channel_div THEN '9'
	WHEN qtr.channel_div_demand >= .7 * qtr.demand_channel_div THEN '8'
	WHEN qtr.channel_div_demand >= .6 * qtr.demand_channel_div THEN '7'
	WHEN qtr.channel_div_demand >= .5 * qtr.demand_channel_div THEN '6'
	WHEN qtr.channel_div_demand >= .4 * qtr.demand_channel_div THEN '5'
	WHEN qtr.channel_div_demand >= .3 * qtr.demand_channel_div THEN '4'
	WHEN qtr.channel_div_demand >= .2 * qtr.demand_channel_div THEN '3'
	WHEN qtr.channel_div_demand >= .1 * qtr.demand_channel_div THEN '2'
	ELSE '1'
END as qtr_channel_div_demand_deciles
,CASE WHEN qtr.sales_dollars <= 0 then 'Sales <=0'
	WHEN qtr.channel_div_sales >= .9 * qtr.sales_channel_div THEN '10' 
	WHEN qtr.channel_div_sales >= .8 * qtr.sales_channel_div THEN '9'
	WHEN qtr.channel_div_sales >= .7 * qtr.sales_channel_div THEN '8'
	WHEN qtr.channel_div_sales >= .6 * qtr.sales_channel_div THEN '7'
	WHEN qtr.channel_div_sales >= .5 * qtr.sales_channel_div THEN '6'
	WHEN qtr.channel_div_sales >= .4 * qtr.sales_channel_div THEN '5'
	WHEN qtr.channel_div_sales >= .3 * qtr.sales_channel_div THEN '4'
	WHEN qtr.channel_div_sales >= .2 * qtr.sales_channel_div THEN '3'
	WHEN qtr.channel_div_sales >= .1 * qtr.sales_channel_div THEN '2'
	ELSE '1'
END as qtr_channel_div_sales_deciles
,CASE WHEN qtr.demand_dollars <= 0 then 'Demand <=0'
	WHEN qtr.channel_demand >= .9 * qtr.demand_channel THEN '10' 
	WHEN qtr.channel_demand >= .8 * qtr.demand_channel THEN '9'
	WHEN qtr.channel_demand >= .7 * qtr.demand_channel THEN '8'
	WHEN qtr.channel_demand >= .6 * qtr.demand_channel THEN '7'
	WHEN qtr.channel_demand >= .5 * qtr.demand_channel THEN '6'
	WHEN qtr.channel_demand >= .4 * qtr.demand_channel THEN '5'
	WHEN qtr.channel_demand >= .3 * qtr.demand_channel THEN '4'
	WHEN qtr.channel_demand >= .2 * qtr.demand_channel THEN '3'
	WHEN qtr.channel_demand >= .1 * qtr.demand_channel THEN '2'
	ELSE '1'
END as qtr_channel_demand_deciles
,CASE WHEN qtr.sales_dollars <= 0 then 'Sales <=0'
	WHEN qtr.channel_sales >= .9 * qtr.sales_channel THEN '10' 
	WHEN qtr.channel_sales >= .8 * qtr.sales_channel THEN '9'
	WHEN qtr.channel_sales >= .7 * qtr.sales_channel THEN '8'
	WHEN qtr.channel_sales >= .6 * qtr.sales_channel THEN '7'
	WHEN qtr.channel_sales >= .5 * qtr.sales_channel THEN '6'
	WHEN qtr.channel_sales >= .4 * qtr.sales_channel THEN '5'
	WHEN qtr.channel_sales >= .3 * qtr.sales_channel THEN '4'
	WHEN qtr.channel_sales >= .2 * qtr.sales_channel THEN '3'
	WHEN qtr.channel_sales >= .1 * qtr.sales_channel THEN '2'
	ELSE '1'
END as qtr_channel_sales_deciles
, SUM(COALESCE(d.sales_dollars,0)) as sales_dollars
, SUM(COALESCE(d.demand_dollars,0)) as demand_dollars
, SUM(COALESCE(demand_units,0)) as demand_units
, SUM(COALESCE(sales_units,0)) as sales_units
, SUM(COALESCE(receipt_dollars,0)) as receipt_dollars
, SUM(COALESCE(receipt_units,0)) as receipt_units
, SUM(COALESCE(qtr.eoh_dollars,0))as qtr_eoh_dollars
, SUM(COALESCE(qtr.eoh_units,0)) as qtr_eoh_units
, SUM(COALESCE(qtr.boh_dollars,0)) as qtr_boh_dollars
, SUM(COALESCE(qtr.boh_units,0)) as qtr_boh_units
, SUM(COALESCE(d.eoh_dollars,0)) as eoh_dollars
, SUM(COALESCE(d.eoh_units,0)) as eoh_units
, SUM(COALESCE(d.boh_dollars,0)) as boh_dollars
, SUM(COALESCE(d.boh_units,0)) as boh_units
, SUM(COALESCE(product_views,0)) as product_views
, SUM(COALESCE(order_sessions,0)) as order_sessions
, SUM(COALESCE(instock_views,0)) as instock_views
, SUM(COALESCE(product_view_sessions,0)) as product_view_sessions
, SUM(COALESCE(order_demand,0)) as order_demand
, SUM(COALESCE(order_quantity,0)) as order_quantity
FROM cc_month d    
 LEFT JOIN cc_quarter qtr
 ON d.quarter_idnt = qtr.quarter_idnt
AND d.channel = qtr.channel 
AND d.channel_country = qtr.channel_country
and d.channel_brand = qtr.channel_brand
AND d.div_idnt = qtr.div_idnt 
AND d.dept_idnt = qtr.dept_idnt 
and d.customer_choice = qtr.customer_choice 
and d.supp_part_num = qtr.supp_part_num
and d.supplier_name = qtr.supplier_name 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35 ,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52;