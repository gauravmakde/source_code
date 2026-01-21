/*
Marketplace Summary Main
Author: Asiyah Fox
Date Created: 4/25/24
Date Last Updated: 5/24/24

Datalab: t2dl_das_insights_delivery_secure
Service Account: T2DL_NAP_IDS_BATCH
Deletes and Inserts into Table: mp_summary
*/

/******************************************************************/
/***************************** DATES ******************************/
/******************************************************************/


--DROP TABLE dates;
CREATE VOLATILE TABLE dates AS 
(
    SELECT
		day_date
		,day_idnt
		,week_end_day_date
		,week_start_day_date
		,week_idnt
		,TRIM(fiscal_year_num) || ', ' || TRIM(fiscal_month_num) || ', Wk ' || TRIM(week_num_of_fiscal_month) AS week_label
		,month_idnt
		,TRIM(fiscal_year_num) || ' ' || TRIM(fiscal_month_num) || ' ' || TRIM(month_abrv) AS month_label
		,quarter_idnt
		,quarter_label
		,fiscal_year_num
	FROM prd_nap_usr_vws.day_cal_454_dim fct
--	WHERE day_date BETWEEN CAST('2024-04-15' AS DATE) AND CURRENT_DATE - 1 --PROD PRE-LAUNCH
	WHERE day_date BETWEEN CAST('2024-04-29' AS DATE) AND CURRENT_DATE - 1 --PROD POST-LAUNCH
--	WHERE week_idnt BETWEEN 202410 AND 202412 --TESTING
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
WITH DATA
PRIMARY INDEX (day_date, day_idnt)
PARTITION BY RANGE_N (day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (day_date, day_idnt)
,COLUMN (day_date)
,COLUMN (day_idnt)
,COLUMN (PARTITION)
ON dates
;


/******************************************************************/
/****************************** SKUS ******************************/
/******************************************************************/


--DROP TABLE sku;
CREATE VOLATILE TABLE sku AS 
(
	SELECT
		sku.rms_sku_num
		,CASE 
			WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'MP' 
			WHEN ven.vendor_name LIKE '%MKTPL' THEN 'MP' 
			ELSE 'NCOM' END AS mp_ind
		,CASE WHEN mp_ind = 'MP' THEN sku.style_group_num ELSE NULL END AS style_group_num
		,CASE WHEN mp_ind = 'MP' THEN sku.style_group_desc ELSE NULL END AS style_group_desc
		,CASE WHEN mp_ind = 'MP' THEN sku.style_desc ELSE NULL END AS style_desc
		,CASE WHEN mp_ind = 'MP' THEN sku.color_num ELSE NULL END AS color_num
		,CASE WHEN mp_ind = 'MP' THEN sku.color_desc ELSE NULL END AS color_desc
		,TRIM(sku.div_num) AS div_num
		,TRIM(sku.div_num) || ', ' || sku.div_desc AS division
		,TRIM(sku.grp_num) AS sdiv_num
		,TRIM(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,TRIM(sku.dept_num) AS dept_num
		,TRIM(sku.dept_num) || ', ' || sku.dept_desc AS department
		,TRIM(sku.class_num) AS class_num
		,TRIM(sku.class_num) || ', ' || sku.class_desc AS "class"
		,TRIM(sku.sbclass_num) AS sbclass_num
		,TRIM(sku.sbclass_num) || ', ' || sku.sbclass_desc AS subclass
		,sku.prmy_supp_num AS supp_num
		,ven.vendor_name AS seller
		,COALESCE(cat.category,'OTHER') AS category
		,COALESCE(cat.category_group,'OTHER') AS category_group
		,CASE WHEN fan.order_from_vendor_num IS NOT NULL THEN 'Y' ELSE 'N' END AS fanatics_ind
	FROM prd_nap_usr_vws.product_sku_dim sku
	JOIN prd_nap_usr_vws.vendor_dim ven --Inner join, as most that don't join are test data vendors or DNU items
		ON sku.prmy_supp_num = ven.vendor_num
	LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim cat
		ON sku.dept_num = cat.dept_num
		AND sku.class_num = cat.class_num
		AND COALESCE(sku.sbclass_num, -1) = cat.sbclass_num --Some Categories don't have subclasses and show -1 placeholder
	LEFT JOIN --Need distinct for duplicate order_from_vendor_num in table
		(
		SELECT order_from_vendor_num
		FROM prd_nap_usr_vws.vendor_payto_relationship_dim
		WHERE payto_vendor_num = '5179609'
		GROUP BY 1
		) fan
	  		ON sku.prmy_supp_num = fan.order_from_vendor_num
	WHERE sku.channel_country = 'US'
		AND COALESCE(sku.smart_sample_ind,'N') <> 'Y'
		AND COALESCE(sku.gwp_ind,'N') <> 'Y'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
WITH DATA
PRIMARY INDEX (rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (rms_sku_num)
ON sku
;

/******************************************************************/
/***************************** STORE ******************************/
/******************************************************************/

CREATE MULTISET VOLATILE TABLE store AS
(
	SELECT
		store_num
		,channel_num
		,TRIM(channel_num || ', ' ||channel_desc) AS channel_label
	FROM prd_nap_usr_vws.store_dim
	WHERE channel_num IN (120, 580)
	GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (store_num)
ON store;

/******************************************************************/
/************************* LIVE ON SITE ***************************/
/******************************************************************/

--DROP TABLE los;
CREATE MULTISET VOLATILE TABLE los AS 
(
	SELECT
		fct.sku_id
		,sku.mp_ind
		,fct.day_date
		,1 AS days_live
		,MIN(fct.day_date) OVER (PARTITION BY fct.sku_id) AS first_pub_date
	FROM t2dl_das_site_merch.live_on_site_daily fct
	JOIN sku
		ON fct.sku_id = sku.rms_sku_num 
		AND sku.mp_ind = 'MP'
	WHERE fct.channel_brand = 'NORDSTROM'
		AND fct.channel_country = 'US'
	GROUP BY 1,2,3,4
	QUALIFY fct.day_date BETWEEN (SELECT MIN(day_date) FROM dates) AND (SELECT MAX(day_date) FROM dates)
)
WITH DATA
PRIMARY INDEX (sku_id, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '7' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (sku_id, day_date)
,COLUMN (PARTITION)
ON los
;

/******************************************************************/
/****************************** SALES *****************************/
/******************************************************************/

--DROP TABLE sales;
CREATE VOLATILE TABLE sales AS 
(
	SELECT
		dt.day_date
		,fct.rms_sku_num
		,sku.mp_ind
		,SUM(COALESCE(fct.line_item_quantity,0))                                                              AS op_gmv_u    
		,SUM(COALESCE(fct.operational_gmv_amt,0))                                                             AS op_gmv_r 
		,SUM(ABS(COALESCE(CASE WHEN fct.product_return_ind = 'Y' THEN fct.line_item_quantity ELSE 0 END,0)))  AS returns_u
		,SUM(ABS(COALESCE(CASE WHEN fct.product_return_ind = 'Y' THEN fct.operational_gmv_amt ELSE 0 END,0))) AS returns_r
	FROM prd_nap_usr_vws.jwn_operational_gmv_metric_vw fct
	JOIN store st
		ON fct.intent_store_num = st.store_num
	JOIN dates dt
		ON fct.business_day_date = dt.day_date
	JOIN sku
		ON fct.rms_sku_num = sku.rms_sku_num
	WHERE fct.jwn_operational_gmv_ind = 'Y'
	    AND fct.non_merch_ind <> 'Y' --Not needed for online but including for knowledge if ever pulling for store channels
	    AND fct.rms_sku_num IS NOT NULL
--		AND COALESCE(fct.same_day_price_adjust_ind,'N') = 'N' --Not used in JWN Merch tables, matches Daily Sales for Op GMV, and doesn't affect Demand metrics
--		AND COALESCE(fct.same_day_store_return_ind,'N') = 'N' --Not used in JWN Merch tables, matches Daily Sales for Op GMV, and doesn't affect Demand metrics
	GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_date)
PARTITION BY RANGE_N (day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (rms_sku_num, day_date)
,COLUMN (PARTITION)
ON sales
;

/******************************************************************/
/***************************** DEMAND *****************************/
/******************************************************************/

--DROP TABLE demand;
CREATE VOLATILE TABLE demand AS 
(
	SELECT
		dt.day_date
		,fct.rms_sku_num
		,sku.mp_ind
		,SUM(COALESCE(CASE WHEN fct.jwn_fulfilled_demand_ind = 'Y' THEN fct.demand_units END,0))                  AS ful_demand_u
		,SUM(COALESCE(CASE WHEN fct.jwn_fulfilled_demand_ind = 'Y' THEN fct.jwn_reported_demand_usd_amt END,0))   AS ful_demand_r
		,SUM(COALESCE(CASE WHEN fct.jwn_reported_demand_ind = 'Y' THEN fct.demand_units END,0))                   AS rep_demand_u
		,SUM(COALESCE(CASE WHEN fct.jwn_reported_demand_ind = 'Y' THEN fct.jwn_reported_demand_usd_amt END,0))    AS rep_demand_r
	FROM prd_nap_usr_vws.jwn_demand_metric_vw fct
	LEFT JOIN store st
		ON fct.intent_store_num = st.store_num
	JOIN dates dt
		ON fct.demand_date = dt.day_date
	JOIN sku
		ON fct.rms_sku_num = sku.rms_sku_num
	WHERE fct.rms_sku_num IS NOT NULL
		AND (st.store_num IS NOT NULL OR sku.mp_ind = 'MP') --LEFT JOIN on store and this due to table location issue; once fixed, change to INNER JOIN on store and delete this
		AND fct.delivery_method <> 'BOPUS' --Comment in for Merch Demand
	GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_date)
PARTITION BY RANGE_N (day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (rms_sku_num, day_date)
,COLUMN (PARTITION)
ON demand
;

/******************************************************************/
/******************************** PFD *****************************/
/******************************************************************/

--DROP TABLE pfd;
CREATE VOLATILE TABLE pfd
AS 
(
	SELECT
		dt.day_date
		,sku.rms_sku_num
		,sku.mp_ind
		,SUM(fct.order_sessions) AS order_sessions
		,SUM(fct.order_demand) as order_demand_r
		,SUM(fct.order_quantity) as order_demand_u
		,SUM(fct.add_to_bag_quantity) AS bag_add_u
		,SUM(fct.product_views) AS product_views
		,SUM(fct.product_views * fct.pct_instock) AS instock_views
		,SUM(CASE WHEN fct.pct_instock IS NOT NULL THEN fct.product_views END) AS scored_views --Divisional uses for TWIST but Item doesn't
	FROM t2dl_das_product_funnel.product_price_funnel_daily fct
	JOIN dates dt
		ON fct.event_date_pacific = dt.day_date
	JOIN sku --Not including SKUs with 'UNKNOWN' hierarchy
		ON fct.rms_sku_num = sku.rms_sku_num
	WHERE fct.channelcountry = 'US'
		AND fct.channel = 'FULL_LINE'
	GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_date)
PARTITION BY RANGE_N (day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (rms_sku_num, day_date)
,COLUMN (PARTITION)
ON pfd
;

DROP TABLE store;

/******************************************************************/
/******************************* BASE *****************************/
/******************************************************************/

--DROP TABLE base;
CREATE VOLATILE TABLE base
AS 
(
SELECT
	COALESCE(f.rms_sku_num, s.rms_sku_num, d.rms_sku_num, l.sku_id) AS rms_sku_num
	,COALESCE(f.mp_ind, s.mp_ind, d.mp_ind, l.mp_ind) AS mp_ind
	,COALESCE(f.day_date, s.day_date, d.day_date, l.day_date) AS day_date
	,CASE WHEN l.sku_id IS NOT NULL THEN 'Y' ELSE 'N' END AS los_ind
	,SUM(COALESCE(f.order_sessions   ,0)) AS order_sessions  
	,SUM(COALESCE(f.order_demand_r   ,0)) AS order_demand_r  
	,SUM(COALESCE(f.order_demand_u   ,0)) AS order_demand_u  
	,SUM(COALESCE(f.bag_add_u        ,0)) AS bag_add_u       
	,SUM(COALESCE(f.product_views    ,0)) AS product_views   
	,SUM(COALESCE(f.instock_views    ,0)) AS instock_views   
	,SUM(COALESCE(f.scored_views     ,0)) AS scored_views    
	,SUM(COALESCE(s.op_gmv_u         ,0)) AS op_gmv_u        
	,SUM(COALESCE(s.op_gmv_r         ,0)) AS op_gmv_r        
	,SUM(COALESCE(d.ful_demand_u     ,0)) AS ful_demand_u    
	,SUM(COALESCE(d.ful_demand_r     ,0)) AS ful_demand_r    
	,SUM(COALESCE(d.rep_demand_u     ,0)) AS rep_demand_u    
	,SUM(COALESCE(d.rep_demand_r     ,0)) AS rep_demand_r    
	,SUM(COALESCE(s.returns_u        ,0)) AS returns_u       
	,SUM(COALESCE(s.returns_r        ,0)) AS returns_r       
FROM pfd f
FULL OUTER JOIN sales s
	ON f.rms_sku_num = s.rms_sku_num
	AND f.day_date = s.day_date
FULL OUTER JOIN demand d
	ON f.rms_sku_num = d.rms_sku_num
	AND f.day_date = d.day_date
FULL OUTER JOIN los l
	ON f.rms_sku_num = l.sku_id
	AND f.day_date = l.day_date
GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_date)
PARTITION BY RANGE_N (day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (rms_sku_num, day_date)
,COLUMN (PARTITION)
ON base
;

DROP TABLE pfd;
DROP TABLE sales;
DROP TABLE demand;

--DROP TABLE base_price;
CREATE VOLATILE TABLE base_price
AS 
(
SELECT
	bs.rms_sku_num
	,bs.mp_ind
	,bs.day_date
	,bs.los_ind
	,bs.order_sessions  
	,bs.order_demand_r  
	,bs.order_demand_u  
	,bs.bag_add_u       
	,bs.product_views   
	,bs.instock_views   
	,bs.scored_views    
	,bs.op_gmv_u        
	,bs.op_gmv_r        
	,bs.ful_demand_u    
	,bs.ful_demand_r    
	,bs.rep_demand_u    
	,bs.rep_demand_r    
	,bs.returns_u       
	,bs.returns_r
	,AVG(pr.selling_retail_price_amt) AS selling_retail_price_amt
FROM base bs
LEFT JOIN prd_nap_usr_vws.product_price_timeline_dim pr
	ON bs.rms_sku_num = pr.rms_sku_num
	AND CAST(bs.day_date AS TIMESTAMP) BETWEEN pr.eff_begin_tmstp AND pr.eff_end_tmstp
	AND bs.mp_ind = 'MP'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_date)
PARTITION BY RANGE_N (day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;


COLLECT STATS
PRIMARY INDEX (rms_sku_num, day_date)
,COLUMN (PARTITION)
ON base_price
;

DROP TABLE base;

/******************************************************************/
/************************* FINAL INSERT ***************************/
/******************************************************************/

--DROP TABLE final_insert;
CREATE VOLATILE TABLE final_insert
AS 
(
SELECT
    dt.week_end_day_date
    ,dt.week_start_day_date
    ,dt.week_idnt
    ,dt.week_label
    ,dt.month_idnt
    ,dt.month_label
    ,dt.quarter_idnt
    ,dt.quarter_label
	,dt.fiscal_year_num
	,sku.mp_ind             
	,sku.style_group_num    
	,sku.style_group_desc    
	,sku.style_desc         
	,sku.color_num          
	,sku.color_desc          
	,sku.div_num            
	,sku.division           
	,sku.sdiv_num           
	,sku.subdivision        
	,sku.dept_num           
	,sku.department         
	,sku.class_num          
	,sku."class"            
	,sku.sbclass_num        
	,sku.subclass           
	,sku.category           
	,sku.category_group           
	,sku.supp_num           
	,sku.seller             
	,bsp.los_ind
	,MIN(los.first_pub_date         ) AS first_pub_date
	,MAX(los.days_live              ) AS days_live
	,SUM(bsp.order_sessions         ) AS order_sessions  
	,SUM(bsp.order_demand_r         ) AS order_demand_r  
	,SUM(bsp.order_demand_u         ) AS order_demand_u  
	,SUM(bsp.bag_add_u              ) AS bag_add_u       
	,SUM(bsp.product_views          ) AS product_views   
	,SUM(bsp.instock_views          ) AS instock_views   
	,SUM(bsp.scored_views           ) AS scored_views    
	,SUM(bsp.op_gmv_u               ) AS op_gmv_u        
	,SUM(bsp.op_gmv_r               ) AS op_gmv_r        
	,SUM(bsp.ful_demand_u           ) AS ful_demand_u    
	,SUM(bsp.ful_demand_r           ) AS ful_demand_r    
	,SUM(bsp.rep_demand_u           ) AS rep_demand_u    
	,SUM(bsp.rep_demand_r           ) AS rep_demand_r    
	,SUM(bsp.returns_u              ) AS returns_u       
	,SUM(bsp.returns_r              ) AS returns_r 
	,COALESCE(
		 NULLIFZERO(SUM(COALESCE(bsp.rep_demand_r    ,0))) / NULLIFZERO(SUM(COALESCE(bsp.rep_demand_u    ,0)))
		,NULLIFZERO(SUM(COALESCE(bsp.ful_demand_r    ,0))) / NULLIFZERO(SUM(COALESCE(bsp.ful_demand_u    ,0)))
		,NULLIFZERO(SUM(COALESCE(bsp.op_gmv_r        ,0))) / NULLIFZERO(SUM(COALESCE(bsp.op_gmv_u        ,0)))
		,NULLIFZERO(SUM(COALESCE(bsp.order_demand_r  ,0))) / NULLIFZERO(SUM(COALESCE(bsp.order_demand_u  ,0)))
		,NULLIFZERO(SUM(COALESCE(bsp.returns_r       ,0))) / NULLIFZERO(SUM(COALESCE(bsp.returns_u       ,0)))
		,AVG(bsp.selling_retail_price_amt)
		,SUM(0)) AS price_band_aur
	,MAX(CURRENT_TIMESTAMP) AS update_timestamp
FROM base_price bsp
LEFT JOIN dates dt
	ON bsp.day_date = dt.day_date
LEFT JOIN los
	ON bsp.rms_sku_num = los.sku_id
	AND bsp.day_date = los.day_date
LEFT JOIN sku
	ON bsp.rms_sku_num = sku.rms_sku_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
)
WITH DATA
PRIMARY INDEX (week_start_day_date, supp_num, category, subclass, department)
PARTITION BY RANGE_N (week_start_day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
PRIMARY INDEX (week_start_day_date, supp_num, category, subclass, department)
,COLUMN (PARTITION)
ON final_insert
;

DELETE FROM {environment_schema}.mp_summary{env_suffix}
;

INSERT INTO {environment_schema}.mp_summary{env_suffix}
SELECT * FROM final_insert
;

COLLECT STATS
PRIMARY INDEX (week_start_day_date, supp_num, category, subclass, department)
,COLUMN (PARTITION)
ON {environment_schema}.mp_summary{env_suffix}
;