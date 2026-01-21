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
CREATE TEMPORARY TABLE IF NOT EXISTS dates
-- CLUSTER BY day_date, day_idnt
AS
SELECT day_date,
 day_idnt,
 week_end_day_date,
 week_start_day_date,
 week_idnt,
 TRIM(CAST(fiscal_year_num AS STRING)) || ', ' || TRIM(CAST(fiscal_month_num AS STRING)) || ', Wk ' || TRIM(CAST(week_num_of_fiscal_month AS STRING)) AS week_label,
 month_idnt,
 TRIM(CAST(fiscal_year_num AS STRING)) || ' ' || TRIM(CAST(fiscal_month_num AS STRING)) || ' ' || TRIM(month_abrv) AS month_label,
 quarter_idnt,
 quarter_label,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS fct
WHERE day_date BETWEEN DATE '2024-04-29' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
GROUP BY day_date,
 day_idnt,
 week_end_day_date,
 week_start_day_date,
 week_idnt,
 week_label,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 fiscal_year_num
 ;



/******************************************************************/
/****************************** SKUS ******************************/
/******************************************************************/


--DROP TABLE sku;
CREATE TEMPORARY TABLE IF NOT EXISTS sku
-- CLUSTER BY rms_sku_num
AS
SELECT sku.rms_sku_num,
  CASE
  WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'MP'
  WHEN LOWER(ven.vendor_name) LIKE LOWER('%MKTPL') THEN 'MP'
  ELSE 'NCOM' END AS mp_ind,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'MP'
     WHEN LOWER(ven.vendor_name) LIKE LOWER('%MKTPL') THEN 'MP'
     ELSE 'NCOM' END) = LOWER('MP')
  THEN sku.style_group_num
  ELSE NULL
  END AS style_group_num,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'MP'
     WHEN LOWER(ven.vendor_name) LIKE LOWER('%MKTPL') THEN 'MP'
     ELSE 'NCOM' END) = LOWER('MP')
  THEN sku.style_group_desc
  ELSE NULL
  END AS style_group_desc,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'MP'
     WHEN LOWER(ven.vendor_name) LIKE LOWER('%MKTPL') THEN 'MP'
     ELSE 'NCOM' END) = LOWER('MP')
  THEN sku.style_desc
  ELSE NULL
  END AS style_desc,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'MP'
     WHEN LOWER(ven.vendor_name) LIKE LOWER('%MKTPL') THEN 'MP'
     ELSE 'NCOM' END) = LOWER('MP')
  THEN sku.color_num
  ELSE NULL
  END AS color_num,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'MP'
     WHEN LOWER(ven.vendor_name) LIKE LOWER('%MKTPL') THEN 'MP'
     ELSE 'NCOM' END) = LOWER('MP')
  THEN sku.color_desc
  ELSE NULL
  END AS color_desc,
 TRIM(FORMAT('%11d', sku.div_num)) AS div_num,
   TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
 TRIM(FORMAT('%11d', sku.grp_num)) AS sdiv_num,
   TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || sku.grp_desc AS subdivision,
 TRIM(FORMAT('%11d', sku.dept_num)) AS dept_num,
   TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS department,
 TRIM(FORMAT('%11d', sku.class_num)) AS class_num,
   TRIM(FORMAT('%11d', sku.class_num)) || ', ' || sku.class_desc AS class,
 TRIM(FORMAT('%11d', sku.sbclass_num)) AS sbclass_num,
   TRIM(FORMAT('%11d', sku.sbclass_num)) || ', ' || sku.sbclass_desc AS subclass,
 sku.prmy_supp_num AS supp_num,
 ven.vendor_name AS seller,
 COALESCE(cat.category, 'OTHER') AS category,
 COALESCE(cat.category_group, 'OTHER') AS category_group,
  CASE
  WHEN fan.order_from_vendor_num IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS sku
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS ven ON LOWER(sku.prmy_supp_num) = LOWER(ven.vendor_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat ON sku.dept_num = cat.dept_num AND sku.class_num = CAST(cat.class_num AS FLOAT64)
     AND COALESCE(sku.sbclass_num, - 1) = CAST(cat.sbclass_num AS FLOAT64)
 LEFT JOIN (SELECT order_from_vendor_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim
  WHERE LOWER(payto_vendor_num) = LOWER('5179609')
  GROUP BY order_from_vendor_num) AS fan ON LOWER(sku.prmy_supp_num) = LOWER(fan.order_from_vendor_num)
WHERE LOWER(sku.channel_country) = LOWER('US')
 AND LOWER(COALESCE(sku.smart_sample_ind, 'N')) <> LOWER('Y')
 AND LOWER(COALESCE(sku.gwp_ind, 'N')) <> LOWER('Y')
GROUP BY sku.rms_sku_num,
 mp_ind,
 style_group_num,
 style_group_desc,
 style_desc,
 color_num,
 color_desc,
 div_num,
 division,
 sdiv_num,
 subdivision,
 dept_num,
 department,
 class_num,
 class,
 sbclass_num,
 subclass,
 supp_num,
 seller,
 category,
 category_group,
 fanatics_ind;  



/******************************************************************/
/***************************** STORE ******************************/
/******************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS store
-- CLUSTER BY store_num
AS
SELECT store_num,
 channel_num,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel_label
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (120, 580)
GROUP BY store_num,
 channel_num,
 channel_label;



/******************************************************************/
/************************* LIVE ON SITE ***************************/
/******************************************************************/

--DROP TABLE los;
CREATE TEMPORARY TABLE IF NOT EXISTS los 
-- CLUSTER BY sku_id, day_date
AS 
( SELECT sku_id,
  mp_ind,
  day_date,
  days_live,
  first_pub_date
	FROM (SELECT
		fct.sku_id
		,sku.mp_ind
		,fct.day_date
		,1 AS days_live
		,MIN(fct.day_date) OVER (PARTITION BY fct.sku_id) AS first_pub_date
	FROM `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_daily fct
	INNER JOIN sku
		ON LOWER(fct.sku_id) = LOWER(sku.rms_sku_num) 
		AND LOWER(sku.mp_ind) = LOWER('MP')
	WHERE LOWER(fct.channel_brand) = LOWER('NORDSTROM')
		AND LOWER(fct.channel_country) = LOWER('US')
	GROUP BY sku_id,mp_ind,day_date,days_live )
  WHERE day_date BETWEEN (SELECT MIN(day_date) FROM dates) AND (SELECT MAX(day_date) FROM dates)
);


/******************************************************************/
/****************************** SALES *****************************/
/******************************************************************/

--DROP TABLE sales;
CREATE TEMPORARY TABLE IF NOT EXISTS sales 
-- CLUSTER BY rms_sku_num, day_date
AS 
(
	SELECT
		dt.day_date
		,fct.rms_sku_num
		,sku.mp_ind
		,SUM(COALESCE(fct.line_item_quantity,0))  AS op_gmv_u    
		,SUM(COALESCE(fct.operational_gmv_amt,0)) AS op_gmv_r 
		,SUM(ABS(COALESCE(CASE WHEN LOWER(fct.product_return_ind) = LOWER('Y') THEN fct.line_item_quantity ELSE 0 END,0)))  AS returns_u
		,SUM(ABS(COALESCE(CASE WHEN LOWER(fct.product_return_ind) = LOWER('Y') THEN fct.operational_gmv_amt ELSE 0 END,0))) AS returns_r
	FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_operational_gmv_metric_vw AS fct
	INNER JOIN store AS st
		ON fct.intent_store_num = st.store_num
	INNER JOIN dates AS dt
		ON fct.business_day_date = dt.day_date
	INNER JOIN sku
		ON LOWER(fct.rms_sku_num) = LOWER(sku.rms_sku_num)
	WHERE LOWER(fct.jwn_operational_gmv_ind) = LOWER('Y')
	    AND LOWER(fct.non_merch_ind) <> LOWER('Y') 
	    AND fct.rms_sku_num IS NOT NULL
	GROUP BY day_date,rms_sku_num,mp_ind
);


/******************************************************************/
/***************************** DEMAND *****************************/
/******************************************************************/

--DROP TABLE demand;
CREATE TEMPORARY TABLE IF NOT EXISTS demand 
-- CLUSTER BY rms_sku_num, day_date
AS 
(
	SELECT
		dt.day_date
		,fct.rms_sku_num
		,sku.mp_ind
		,SUM(COALESCE(CASE WHEN LOWER(fct.jwn_fulfilled_demand_ind) = LOWER('Y') THEN fct.demand_units END,0)) AS ful_demand_u
		,SUM(COALESCE(CASE WHEN LOWER(fct.jwn_fulfilled_demand_ind) = LOWER('Y') THEN fct.jwn_reported_demand_usd_amt END,0)) AS ful_demand_r
		,SUM(COALESCE(CASE WHEN LOWER(fct.jwn_reported_demand_ind) = LOWER('Y') THEN fct.demand_units END,0)) AS rep_demand_u
		,SUM(COALESCE(CASE WHEN LOWER(fct.jwn_reported_demand_ind) = LOWER('Y') THEN fct.jwn_reported_demand_usd_amt END,0)) AS rep_demand_r
	FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_demand_metric_vw AS fct
	LEFT JOIN store AS st
		ON fct.intent_store_num = st.store_num
	INNER JOIN dates AS dt
		ON fct.demand_date = dt.day_date
	INNER JOIN sku
		ON LOWER(fct.rms_sku_num) = LOWER(sku.rms_sku_num)
	WHERE fct.rms_sku_num IS NOT NULL
		AND (st.store_num IS NOT NULL OR LOWER(sku.mp_ind) = LOWER('MP')) --LEFT JOIN on store and this due to table location issue; once fixed, change to INNER JOIN on store and delete this
		AND LOWER(fct.delivery_method) <> LOWER('BOPUS') --Comment in for Merch Demand
	GROUP BY day_date,rms_sku_num,mp_ind
);

/******************************************************************/
/******************************** PFD *****************************/
/******************************************************************/

--DROP TABLE pfd;
CREATE TEMPORARY TABLE IF NOT EXISTS pfd
-- CLUSTER BY rms_sku_num, day_date
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
	FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS fct
	INNER JOIN dates AS dt
		ON fct.event_date_pacific = dt.day_date
	INNER JOIN sku --Not including SKUs with 'UNKNOWN' hierarchy
		ON LOWER(fct.rms_sku_num) = LOWER(sku.rms_sku_num)
	WHERE LOWER(fct.channelcountry) = LOWER('US')
		AND LOWER(fct.channel) = LOWER('FULL_LINE')
	GROUP BY day_date,rms_sku_num,mp_ind
);


DROP TABLE store;

/******************************************************************/
/******************************* BASE *****************************/
/******************************************************************/

--DROP TABLE base;
CREATE TEMPORARY TABLE IF NOT EXISTS base
-- CLUSTER BY rms_sku_num, day_date
AS 
(
SELECT
	COALESCE(f.rms_sku_num, s.rms_sku_num, d.rms_sku_num, l.sku_id) AS rms_sku_num
	,COALESCE(f.mp_ind, s.mp_ind, d.mp_ind, l.mp_ind) AS mp_ind
	,COALESCE(f.day_date, s.day_date, d.day_date, l.day_date) AS day_date
	,CASE WHEN l.sku_id IS NOT NULL THEN 'Y' ELSE 'N' END AS los_ind
	,SUM(COALESCE(f.order_sessions,0)) AS order_sessions  
	,SUM(COALESCE(f.order_demand_r,0)) AS order_demand_r  
	,SUM(COALESCE(f.order_demand_u,0)) AS order_demand_u  
	,SUM(COALESCE(f.bag_add_u,0)) AS bag_add_u       
	,SUM(COALESCE(f.product_views,0)) AS product_views   
	,SUM(COALESCE(f.instock_views,0)) AS instock_views   
	,SUM(COALESCE(f.scored_views,0)) AS scored_views    
	,SUM(COALESCE(s.op_gmv_u,0)) AS op_gmv_u        
	,SUM(COALESCE(s.op_gmv_r,0)) AS op_gmv_r        
	,SUM(COALESCE(d.ful_demand_u,0)) AS ful_demand_u    
	,SUM(COALESCE(d.ful_demand_r,0)) AS ful_demand_r    
	,SUM(COALESCE(d.rep_demand_u,0)) AS rep_demand_u    
	,SUM(COALESCE(d.rep_demand_r,0)) AS rep_demand_r    
	,SUM(COALESCE(s.returns_u,0)) AS returns_u       
	,SUM(COALESCE(s.returns_r,0)) AS returns_r       
FROM pfd AS f
FULL OUTER JOIN sales AS s
	ON LOWER(f.rms_sku_num) = LOWER(s.rms_sku_num)
	AND f.day_date = s.day_date
FULL OUTER JOIN demand AS d
	ON LOWER(f.rms_sku_num) = LOWER(d.rms_sku_num)
	AND f.day_date = d.day_date
FULL OUTER JOIN los AS l
	ON LOWER(f.rms_sku_num) = LOWER(l.sku_id)
	AND f.day_date = l.day_date
GROUP BY rms_sku_num,mp_ind,day_date,los_ind
);



DROP TABLE pfd;
DROP TABLE sales;
DROP TABLE demand;

--DROP TABLE base_price;
CREATE TEMPORARY TABLE IF NOT EXISTS base_price
-- CLUSTER BY rms_sku_num, day_date
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
FROM base AS bs
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS pr
	ON LOWER(bs.rms_sku_num) = LOWER(pr.rms_sku_num)
	AND cast(bs.day_date as timestamp) BETWEEN pr.eff_begin_tmstp_utc AND pr.eff_end_tmstp_utc
	AND LOWER(bs.mp_ind) = LOWER('MP')
GROUP BY rms_sku_num,mp_ind,day_date,los_ind,order_sessions,order_demand_r,order_demand_u,bag_add_u,product_views,instock_views,scored_views,op_gmv_u,op_gmv_r,ful_demand_u,ful_demand_r,rep_demand_u,rep_demand_r,returns_u,returns_r
);


DROP TABLE base;

/******************************************************************/
/************************* FINAL INSERT ***************************/
/******************************************************************/


CREATE TEMPORARY TABLE IF NOT EXISTS final_insert
-- CLUSTER BY week_start_day_date, supp_num, category, subclass
AS
SELECT dt.week_end_day_date,                                                                                  
 dt.week_start_day_date,                                                  
 dt.week_idnt,                                                  
 dt.week_label,                                                 
 dt.month_idnt,                                                 
 dt.month_label,                                                  
 dt.quarter_idnt,                                                 
 dt.quarter_label,                                                  
 dt.fiscal_year_num,                                                  
 sku.mp_ind,                                                  
 sku.style_group_num,                                                 
 sku.style_group_desc,                                                  
 sku.style_desc,                                                  
 sku.color_num,                                                 
 sku.color_desc,                                                  
 CAST(trunc(CAST(sku.div_num AS FLOAT64)) AS INT64) AS div_num,                                                 
 sku.division,                                                  
 CAST(trunc(CAST(sku.sdiv_num AS FLOAT64)) AS INT64) AS sdiv_num,                                                  
 sku.subdivision,                                                 
 CAST(trunc(CAST(sku.dept_num AS FLOAT64)) AS INT64) AS dept_num,                                                  
 sku.department,                                                  
 CAST(TRUNC(CAST(sku.class_num AS FLOAT64)) AS INT64) AS class_num,                                                 
 sku.class,                                                 
 CAST(TRUNC(CAST(sku.sbclass_num AS FLOAT64)) AS INT64) AS sbclass_num,                                                 
 sku.subclass,                                                  
 sku.category,                                                  
 sku.category_group,                                                  
 CAST(TRUNC(CAST(sku.supp_num AS FLOAT64)) AS INT64) AS supp_num,                                                  
 sku.seller,                                                  
 bsp.los_ind,                                                 
 MIN(los.first_pub_date) AS first_pub_date,
 MAX(los.days_live) AS days_live,
 SUM(bsp.order_sessions) AS order_sessions,
 SUM(bsp.order_demand_r) AS order_demand_r,
 SUM(bsp.order_demand_u) AS order_demand_u,
 SUM(bsp.bag_add_u) AS bag_add_u,
 SUM(bsp.product_views) AS product_views,
 CAST(SUM(bsp.instock_views) AS NUMERIC) AS instock_views,
 SUM(bsp.scored_views) AS scored_views,
 SUM(bsp.op_gmv_u) AS op_gmv_u,
 SUM(bsp.op_gmv_r) AS op_gmv_r,
 SUM(bsp.ful_demand_u) AS ful_demand_u,
 SUM(bsp.ful_demand_r) AS ful_demand_r,
 SUM(bsp.rep_demand_u) AS rep_demand_u,
 SUM(bsp.rep_demand_r) AS rep_demand_r,
 SUM(bsp.returns_u) AS returns_u,
 SUM(bsp.returns_r) AS returns_r,
 COALESCE(IF(SUM(bsp.rep_demand_r) = 0, NULL, SUM(bsp.rep_demand_r)) / IF(SUM(bsp.rep_demand_u) = 0, NULL, SUM(bsp.rep_demand_u
     )), IF(SUM(bsp.ful_demand_r) = 0, NULL, SUM(bsp.ful_demand_r)) / IF(SUM(bsp.ful_demand_u) = 0, NULL, SUM(bsp.ful_demand_u
     )), IF(SUM(bsp.op_gmv_r) = 0, NULL, SUM(bsp.op_gmv_r)) / IF(SUM(bsp.op_gmv_u) = 0, NULL, SUM(bsp.op_gmv_u)), IF(SUM(bsp
      .order_demand_r) = 0, NULL, SUM(bsp.order_demand_r)) / IF(SUM(bsp.order_demand_u) = 0, NULL, SUM(bsp.order_demand_u
     )), IF(SUM(bsp.returns_r) = 0, NULL, SUM(bsp.returns_r)) / IF(SUM(bsp.returns_u) = 0, NULL, SUM(bsp.returns_u)),
  AVG(bsp.selling_retail_price_amt), SUM(0)) AS price_band_aur,
 CAST(MAX(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)) AS TIMESTAMP) AS update_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz 
FROM base_price AS bsp
 LEFT JOIN dates AS dt ON bsp.day_date = dt.day_date
 LEFT JOIN los ON LOWER(bsp.rms_sku_num) = LOWER(los.sku_id) AND bsp.day_date = los.day_date
 LEFT JOIN sku ON LOWER(bsp.rms_sku_num) = LOWER(sku.rms_sku_num)
GROUP BY 
 dt.week_end_day_date,
 dt.week_start_day_date,
 dt.week_idnt,
 dt.week_label,
 dt.month_idnt,
 dt.month_label,
 dt.quarter_idnt,
 dt.quarter_label,
 dt.fiscal_year_num,
 sku.mp_ind,
 sku.style_group_num,
 sku.style_group_desc,
 sku.style_desc,
 sku.color_num,
 sku.color_desc,
 sku.div_num,
 sku.division,
 sku.sdiv_num,
 sku.subdivision,
 sku.dept_num,
 sku.department,
 sku.class_num,
 sku.class,
 sku.sbclass_num,
 sku.subclass,
 sku.category,
 sku.category_group,
 sku.supp_num,
 sku.seller,
 bsp.los_ind;



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.mp_summary{{params.env_suffix}}

;

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.mp_summary{{params.env_suffix}}
SELECT * FROM final_insert
;

-- COLLECT STATS
-- PRIMARY INDEX (week_start_day_date, supp_num, category, subclass, department)
-- ,COLUMN (PARTITION)
-- ON T2DL_DAS_SELECTION.mp_summary --{env_suffix}

-- ;