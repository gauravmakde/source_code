CREATE MULTISET VOLATILE TABLE ia_cluster_map AS (
SELECT
	a.CHANNEL_NUM as CHANNEL_IDNT
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND as SELLING_BRAND
	,a.CHANNEL_COUNTRY AS SELLING_COUNTRY
	,TRIM(a.CHANNEL_NUM||','||a.CHANNEL_DESC) as CHANNEL
	,CHANNEL_BRAND as BANNER
	FROM (
		SELECT
			STORE_NUM
			,CHANNEL_NUM
			,CHANNEL_DESC
			,CHANNEL_BRAND
			,CHANNEL_COUNTRY
		FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
		GROUP BY 1,2,3,4,5
		--WHERE CHANNEL_BRAND in ('Nordstrom','Nordstrom_Rack')
		WHERE CHANNEL_NUM IN ('930','220','221')
		)a
GROUP BY 1,2,3,4,5)
WITH DATA
PRIMARY INDEX (CHANNEL_IDNT,SELLING_COUNTRY,SELLING_BRAND) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (CHANNEL_IDNT,SELLING_COUNTRY,SELLING_BRAND)
	,COLUMN(CHANNEL_IDNT)
	,COLUMN(SELLING_COUNTRY)
	,COLUMN(SELLING_BRAND)
ON ia_cluster_map ;


--drop table sc_date
--lets create a week to month mapping
CREATE MULTISET VOLATILE TABLE ia_date AS(
SELECT
	WEEK_IDNT
	,MONTH_IDNT
	,MONTH_LABEL
	,month_454_label
	,fiscal_year_num||''||fiscal_month_num||' '||month_abrv AS MONTH_ID 
	,QUARTER_IDNT 
	,QUARTER_LABEL 
	,'FY'||substr(cast(fiscal_year_num as varchar(5)),3,2)||' '||quarter_abrv as QUARTER
	,HALF_LABEL
	,FISCAL_YEAR_NUM 
	,CASE WHEN CURRENT_DATE BETWEEN month_start_day_date and month_end_day_date THEN 1 ELSE 0 END as curr_month_flag
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE curr_month_flag =1 OR day_date >= current_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

WITH DATA
   PRIMARY INDEX(WEEK_IDNT)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (WEEK_IDNT)
     ,COLUMN(WEEK_IDNT)
     ,COLUMN(MONTH_IDNT)
     ,COLUMN(QUARTER_IDNT)
     ,COLUMN(FISCAL_YEAR_NUM)
     ON ia_date;
       
--Lets current week so we can get MTD rcpts
CREATE MULTISET VOLATILE TABLE ia_wtd_date AS(
SELECT
	b.WEEK_IDNT
	,a.MONTH_IDNT
	,b.MONTH_LABEL
	,b.month_454_label
	,b.fiscal_year_num||''||b.fiscal_month_num||' '||b.month_abrv as MONTH_ID 
	,b.QUARTER_IDNT 
	,b.QUARTER_LABEL 
	,'FY'||substr(cast(b.fiscal_year_num as varchar(5)),3,2)||' '||b.quarter_abrv as QUARTER
	,b.HALF_LABEL
	,b.FISCAL_YEAR_NUM
FROM
	(SELECT 
	MAX(month_idnt) month_idnt
	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
	WHERE week_start_day_date <= CURRENT_DATE) a
JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM b
ON a.MONTH_IDNT = b.month_idnt
WHERE week_start_day_date <= CURRENT_DATE
GROUP BY 1,2,3,4,5,6,7,8,9,10)

WITH DATA
   PRIMARY INDEX(WEEK_IDNT)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (WEEK_IDNT)
     ,COLUMN(WEEK_IDNT)
     ON ia_wtd_date;


--Lets collect supp attributes    
--DROP TABLE sc_supp_gp
CREATE MULTISET VOLATILE TABLE ia_supp_gp AS( 
SELECT
	a.dept_num 
	,a.supplier_num 
	,a.supplier_group 
	,case when a.banner = 'FP' then 'NORDSTROM' ELSE 'NORDSTROM_RACK' end as SELLING_BRAND
	,a.buy_planner 
	,a.preferred_partner_desc 
	,a.areas_of_responsibility 
	,a.is_npg 
	,a.diversity_group 
	,a.nord_to_rack_transfer_rate 
FROM PRD_NAP_USR_VWS.SUPP_DEPT_MAP_DIM a
GROUP BY 1,2,3,4,5,6,7,8,9,10)
WITH DATA
   PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)
     ,COLUMN(DEPT_NUM)
     ,COLUMN(SUPPLIER_GROUP)
	ON ia_supp_gp;


--lets collect category attributes
CREATE MULTISET VOLATILE TABLE ia_cattr AS( 
SELECT 
	a.dept_num
	,a.class_num 
	,a.sbclass_num 
	,a.CATEGORY
	,a.category_planner_1 
	,a.category_planner_2 
	,a.category_group
	,a.seasonal_designation
	,a.rack_merch_zone
	,a.is_activewear
	,a.channel_category_roles_1 
	,a.channel_category_roles_2 
	,a.bargainista_dept_map 
from PRD_NAP_USR_VWS.CATG_SUBCLASS_MAP_DIM a
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)

WITH DATA
   PRIMARY INDEX(dept_num,CATEGORY)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(dept_num,CATEGORY)
     ,COLUMN(dept_num)
     ,COLUMN(CATEGORY)
     ON ia_cattr;


    --Lets bring in wtd rcpts at supp/cat
--drop table sc_wtd_rcpts
CREATE MULTISET VOLATILE TABLE ia_wtd_rcpts AS(
SELECT 
	g.DEPARTMENT_NUM 
	,g.SUPPLIER_GROUP
	,g.ALT_INV_MODEL
	,g.SELLING_COUNTRY
	,g.SELLING_BRAND
	,g.CHANNEL_IDNT
	,g.YEAR_NUM 
	,g.MONTH_ID
	,g.MONTH_LABEL
	,g.HALF_LABEL
	,g.QUARTER 
	,Coalesce(h.CATEGORY, i.CATEGORY, 'OTHER') CATEGORY
	,g.buy_planner 
	,g.preferred_partner_desc 
	,g.areas_of_responsibility 
	,g.is_npg 
	,g.diversity_group 
	,g.nord_to_rack_transfer_rate 
	,Coalesce(h.category_planner_1, i.category_planner_1, 'OTHER') category_planner_1
	,COALESCE(h.category_planner_2, i.category_planner_2, 'OTHER') category_planner_2
	,COALESCE(h.category_group, i.category_group,'OTHER') category_group
	,COALESCE(h.seasonal_designation,i.seasonal_designation,'OTHER') seasonal_designation
	,COALESCE(h.rack_merch_zone,i.rack_merch_zone,'OTHER') rack_merch_zone
	,COALESCE(h.is_activewear,i.is_activewear,'OTHER') is_activewear
	,COALESCE(h.channel_category_roles_1,i.channel_category_roles_1,'OTHER') channel_category_roles_1
	,COALESCE(h.channel_category_roles_2, i.channel_category_roles_2,'OTHER') channel_category_roles_2
	,COALESCE(h.bargainista_dept_map, i.bargainista_dept_map,'OTHER') as bargainista_dept_map
	,sum(g.INACTIVE_RCPTS_MTD_C) as INACTIVE_RCPTS_MTD_C
	--,sum(g.NRP_RCPTS_MTD_C) as NRP_RCPTS_MTD_C
	--,sum(g.RP_RCPTS_MTD_U) as RP_RCPTS_MTD_U
	--,sum(g.NRP_RCPTS_MTD_U) as NRP_RCPTS_MTD_U
FROM
	(	
	SELECT 
		d.DEPARTMENT_NUM 
		,d.SUPPLIER_NUM
		,d.SUPPLIER_NAME
		,d.SELLING_COUNTRY
		,d.SELLING_BRAND
		,d.YEAR_NUM 
		,d.MONTH_ID
		,d.MONTH_LABEL
		,d.HALF_LABEL
		,d.QUARTER
		,d.RP_IND 
		,d.NPG_IND
		,d.CHANNEL_IDNT
		,d.CLASS_NUM
		,d.SUBCLASS_NUM
		,COALESCE(e.SUPPLIER_GROUP,'OTHER') AS SUPPLIER_GROUP
		,CASE WHEN f.payto_vendor_num  = '5179609' THEN 'FANATICS ONLY' ELSE 'NON-FANATICS' END AS ALT_INV_MODEL
		,e.buy_planner 
		,e.preferred_partner_desc 
		,e.areas_of_responsibility 
		,e.is_npg 
		,e.diversity_group 
		,e.nord_to_rack_transfer_rate 
		,SUM(CASE WHEN d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_COST+d.RECEIPTS_CLEARANCE_COST+d.RECEIPTS_CROSSDOCK_REGULAR_COST+d.RECEIPTS_CROSSDOCK_CLEARANCE_COST ELSE 0
			END) AS INACTIVE_RCPTS_MTD_C
		--,SUM(CASE WHEN d.RP_IND = 'N' AND d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_COST+d.RECEIPTS_CLEARANCE_COST+d.RECEIPTS_CROSSDOCK_REGULAR_COST+d.RECEIPTS_CROSSDOCK_CLEARANCE_COST ELSE 0
		--	END) AS NRP_RCPTS_MTD_C
		--,SUM(CASE WHEN d.RP_IND = 'Y' AND d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_UNITS+d.RECEIPTS_CLEARANCE_UNITS+d.RECEIPTS_CROSSDOCK_REGULAR_UNITS+d.RECEIPTS_CROSSDOCK_CLEARANCE_UNITS ELSE 0
			--END) AS RP_RCPTS_MTD_U
		--,SUM(CASE WHEN d.RP_IND = 'N' AND d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_UNITS+d.RECEIPTS_CLEARANCE_UNITS+d.RECEIPTS_CROSSDOCK_REGULAR_UNITS+d.RECEIPTS_CROSSDOCK_CLEARANCE_UNITS ELSE 0
			--END) AS NRP_RCPTS_MTD_U	
	FROM(
		SELECT 
			a.RMS_SKU_NUM 
			,a.DEPARTMENT_NUM 
			,a.DEPARTMENT_DESC
			,a.CLASS_NUM
			,a.SUBCLASS_NUM
			,a.DIVISION_NUM 
			,a.DIVISION_DESC 
			,a.SUBDIVISION_NUM 
			,a.SUPPLIER_NUM 
			,a.SUPPLIER_NAME 
			,a.STORE_NUM
			,a.CHANNEL_NUM 
			,a.WEEK_NUM 
			,a.YEAR_NUM 
			,b.MONTH_ID
			,b.MONTH_LABEL
			,b.HALF_LABEL
			,b.QUARTER
			,a.MONTH_NUM
			,a.DROPSHIP_IND 
			,a.RP_IND 
			,a.NPG_IND 
			,c.CHANNEL_IDNT
			--,c.CLUSTER_NAME
			,c.SELLING_BRAND
			,c.SELLING_COUNTRY
			,a.RECEIPTS_REGULAR_COST
			,a.RECEIPTS_REGULAR_UNITS
			,a.RECEIPTS_CLEARANCE_COST
			,a.RECEIPTS_CLEARANCE_UNITS
			,a.RECEIPTS_CROSSDOCK_REGULAR_COST
			,a.RECEIPTS_CROSSDOCK_REGULAR_UNITS
			,a.RECEIPTS_CROSSDOCK_CLEARANCE_COST
			,a.RECEIPTS_CROSSDOCK_CLEARANCE_UNITS
		from PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW a
		JOIN ia_wtd_date b 
		ON a.WEEK_NUM = b.WEEK_IDNT
		JOIN ia_cluster_map c 
		ON a.CHANNEL_NUM = c.CHANNEL_IDNT
		--where a.DEPARTMENT_NUM = '882'
		--AND b.MONTH_LABEL = '2022 JAN'
		--AND a.DROPSHIP_IND = 'N'
		--AND a.RP_IND = 'Y'
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
		 )d
	LEFT JOIN ia_supp_gp e
	ON d.DEPARTMENT_NUM = e.DEPT_NUM
	AND d.SUPPLIER_NUM = e.supplier_num 
	and d.SELLING_BRAND = e.SELLING_BRAND
	LEFT JOIN (Select distinct payto_vendor_num,order_from_vendor_num from PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM
			where payto_vendor_num  in ('5179609')) f
	ON d.SUPPLIER_NUM = f.order_from_vendor_num 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23		
	)g
LEFT JOIN ia_cattr h 
ON g.DEPARTMENT_NUM = h.DEPT_NUM
AND g.CLASS_NUM = h.CLASS_NUM
AND g.SUBCLASS_NUM = h.SBCLASS_NUM
LEFT JOIN ia_cattr i
ON  g.DEPARTMENT_NUM = i.DEPT_NUM
AND g.CLASS_NUM = i.CLASS_NUM
AND i.SBCLASS_NUM = -1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27)

WITH DATA
   PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
     ,COLUMN(DEPARTMENT_NUM)
     ,COLUMN(SUPPLIER_GROUP)
     ,COLUMN(CATEGORY)
     ,COLUMN(MONTH_LABEL)
     ON ia_wtd_rcpts;
    

--drop table sc_oo_final
--lets add back in the hierarchy to the base data and sum up inactive/active rcpts as well as rp nrp
--Inactive channels 930,220 and 221
CREATE MULTISET VOLATILE TABLE ia_oo_final AS(
SELECT 
		g.DEPARTMENT_NUM
		,g.CHANNEL_NUM 
		,g.SELLING_COUNTRY
		,g.SELLING_BRAND
		,g.MONTH_LABEL
		,g.MONTH_ID
		,g.QUARTER
		,g.HALF_LABEL
		,g.FISCAL_YEAR_NUM
		,g.ALT_INV_MODEL
		,Coalesce(h.CATEGORY, i.CATEGORY, 'OTHER') CATEGORY
		,g.SUPPLIER_GROUP
		,Coalesce(h.category_planner_1, i.category_planner_1, 'OTHER') category_planner_1
		,COALESCE(h.category_planner_2, i.category_planner_2, 'OTHER') category_planner_2
		,COALESCE(h.category_group, i.category_group,'OTHER') category_group
		,COALESCE(h.seasonal_designation,i.seasonal_designation,'OTHER') seasonal_designation
		,COALESCE(h.rack_merch_zone,i.rack_merch_zone,'OTHER') rack_merch_zone
		,COALESCE(h.is_activewear,i.is_activewear,'OTHER') is_activewear
		,COALESCE(h.channel_category_roles_1,i.channel_category_roles_1,'OTHER') channel_category_roles_1
		,COALESCE(h.channel_category_roles_2, i.channel_category_roles_2,'OTHER') channel_category_roles_2
		,COALESCE(h.bargainista_dept_map, i.bargainista_dept_map,'OTHER') as bargainista_dept_map
		,g.buy_planner 
		,g.preferred_partner_desc 
		,g.areas_of_responsibility 
		,g.is_npg 
		,g.diversity_group 
		,g.nord_to_rack_transfer_rate
		,sum(g.OO_INACTIVE_COST) as INACTIVE_OO_C
		--,sum(g.RP_OO_ACTIVE_UNITS) as RP_OO_U
		--,sum(g.NON_RP_OO_ACTIVE_COST) as NRP_OO_C
		--,sum(g.NON_RP_OO_ACTIVE_UNITS) as NRP_OO_U
FROM(
	Select 		
			d.DEPARTMENT_NUM 
			,d.DIVISION_NUM 
			,d.SUBDIVISION_DESC 
			,d.CLASS_NUM 
			,d.SUBCLASS_NUM 
			,COALESCE(e.SUPPLIER_GROUP,'OTHER') AS SUPPLIER_GROUP
			,CASE WHEN f.payto_vendor_num  = '5179609' THEN 'FANATICS ONLY' ELSE 'NON-FANATICS' END AS ALT_INV_MODEL
			,d.WEEK_NUM 
			,d.MONTH_NUM 
			,d.MONTH_LABEL
			,d.MONTH_ID
			,d.QUARTER
			,d.HALF_LABEL
			,d.FISCAL_YEAR_NUM
			,d.CHANNEL_NUM 
			,d.SELLING_COUNTRY
			,d.SELLING_BRAND
			,e.buy_planner 
			,e.preferred_partner_desc 
			,e.areas_of_responsibility 
			,e.is_npg 
			,e.diversity_group 
			,e.nord_to_rack_transfer_rate 
			,SUM(d.OO_INACTIVE_COST) as OO_INACTIVE_COST
 from 
 	(		SELECT
			a.RMS_SKU_NUM 
			,a.PURCHASE_ORDER_NUMBER 
			,a.DEPARTMENT_NUM 
			,a.DIVISION_NUM 
			,a.SUBDIVISION_DESC 
			,a.CLASS_NUM 
			,a.SUBCLASS_NUM 
			,a.SUPPLIER_NUM 
			,a.WEEK_NUM 
			,a.MONTH_NUM 
			,a.MONTH_LABEL
			,b.MONTH_ID
			,b.QUARTER
			,b.HALF_LABEL
			,b.FISCAL_YEAR_NUM
			,a.STORE_NUM 
			,a.CHANNEL_NUM 
			,c.SELLING_COUNTRY
			,c.SELLING_BRAND
			,a.RP_IND
			,a.OO_INACTIVE_COST
		FROM PRD_NAP_USR_VWS.MERCH_APT_ON_ORDER_INSIGHT_FACT_VW a
	    JOIN ia_date b 
	    ON a.WEEK_NUM = b.week_IDNT
	    JOIN ia_cluster_map c 
	    ON a.CHANNEL_NUM = c.CHANNEL_IDNT
	    WHERE a.CHANNEL_NUM IN ('930','220','221')
	    --where a.month_num = '202302'
	    --and a.DEPARTMENT_NUM = '882'
	    ) d
	LEFT JOIN ia_supp_gp e
	ON d.DEPARTMENT_NUM = e.DEPT_NUM
	AND d.SUPPLIER_NUM = e.SUPPLIER_NUM
	AND d.SELLING_BRAND = e.SELLING_BRAND
	LEFT JOIN (Select distinct payto_vendor_num,order_from_vendor_num from PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM
	where payto_vendor_num  in ('5179609')) f
	ON d.SUPPLIER_NUM = f.order_from_vendor_num 
	group by 1,2,3,4,5,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) g
LEFT JOIN ia_cattr h
ON g.DEPARTMENT_NUM = h.DEPT_NUM
AND g.CLASS_NUM = h.class_num 
AND g.SUBCLASS_NUM = h.sbclass_num
LEFT JOIN ia_cattr i
ON  g.DEPARTMENT_NUM = i.DEPT_NUM
AND g.CLASS_NUM = i.CLASS_NUM
AND i.sbclass_num = -1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27)

WITH DATA
  PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
  ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
     ,COLUMN(DEPARTMENT_NUM)
     ,COLUMN(SUPPLIER_GROUP)
     ,COLUMN(CATEGORY)
     ,COLUMN(MONTH_LABEL)
     ON ia_oo_final;
    
   
CREATE MULTISET VOLATILE TABLE ia_cm AS(
SELECT	
		g.DEPT_ID
		,g.selling_country
		,g.selling_brand
		,g.ORG_ID
		,g.MONTH_ID
		,g.MONTH_LABEL
		,g.QUARTER 
		,g.HALF_LABEL
		,g.FISCAL_YEAR_NUM
		,Coalesce(h.CATEGORY, i.CATEGORY, 'OTHER') CATEGORY
		,g.SUPPLIER_GROUP
		,g.ALT_INV_MODEL
		,g.buy_planner 
		,g.preferred_partner_desc 
		,g.areas_of_responsibility 
		,g.is_npg 
		,g.diversity_group 
		,g.nord_to_rack_transfer_rate 
		,Coalesce(h.category_planner_1, i.category_planner_1, 'OTHER') category_planner_1
		,COALESCE(h.category_planner_2, i.category_planner_2, 'OTHER') category_planner_2
		,COALESCE(h.category_group, i.category_group,'OTHER') category_group
		,COALESCE(h.seasonal_designation,i.seasonal_designation,'OTHER') seasonal_designation
		,COALESCE(h.rack_merch_zone,i.rack_merch_zone,'OTHER') rack_merch_zone
		,COALESCE(h.is_activewear,i.is_activewear,'OTHER') is_activewear
		,COALESCE(h.channel_category_roles_1,i.channel_category_roles_1,'OTHER') channel_category_roles_1
		,COALESCE(h.channel_category_roles_2, i.channel_category_roles_2,'OTHER') channel_category_roles_2
		,COALESCE(h.bargainista_dept_map, i.bargainista_dept_map,'OTHER') as bargainista_dept_map
		,sum(g.INACTIVE_CM_C) as INACTIVE_CM_C
	FROM
		(
			SELECT 
			d.DEPT_ID
			,d.selling_country
			,d.selling_brand
			,d.MONTH_ID
			,d.MONTH_LABEL
			,d.QUARTER 
			,d.FISCAL_YEAR_NUM
			,d.HALF_LABEL
			,d.CLASS_ID
			,d.SUBCLASS_ID
			,COALESCE(e.SUPPLIER_GROUP,'OTHER') AS SUPPLIER_GROUP
			,CASE WHEN f.payto_vendor_num  = '5179609' THEN 'FANATICS ONLY' ELSE 'NON-FANATICS' END AS ALT_INV_MODEL
			,d.ORG_ID
			,e.buy_planner 
			,e.preferred_partner_desc 
			,e.areas_of_responsibility 
			,e.is_npg 
			,e.diversity_group 
			,e.nord_to_rack_transfer_rate 
			,SUM(CASE 
				WHEN d.PLAN_TYPE in ('RKPACKHOLD') AND d.SELLING_COUNTRY in ('US') THEN d.TTL_COST_US
				WHEN d.PLAN_TYPE in ('RKPACKHOLD') AND d.SELLING_COUNTRY in ('CA') THEN d.TTL_COST_CA  
				ELSE 0 END) AS INACTIVE_CM_C
			--,SUM(CASE 
			--	WHEN d.PLAN_TYPE in ('REPLNSHMNT') AND d.SELLING_COUNTRY in ('US') THEN d.TTL_COST_US
			--	WHEN d.PLAN_TYPE in ('REPLNSHMNT') AND d.SELLING_COUNTRY in ('CA') THEN d.TTL_COST_CA  
			--	ELSE 0 END) AS RP_CM_C
			--,SUM(CASE 
			--	WHEN d.PLAN_TYPE in ('BACKUP','FASHION','NRPREORDER') THEN d.RCPT_UNITS 
			--	ELSE 0 END) AS NRP_CM_U
			--,SUM(CASE 
			--	WHEN d.PLAN_TYPE in ('REPLNSHMNT') THEN d.RCPT_UNITS  
			--	ELSE 0 END) AS RP_CM_U
		FROM(
			SELECT
				a.DEPT_ID
				,a.STYLE_ID
				,a.STYLE_GROUP_ID
				,a.CLASS_id
				,a.SUBCLASS_id
				,a.SUPP_ID
				,a.SUPP_NAME
				,a.FISCAL_MONTH_ID
				,a.OTB_EOW_DATE
				,a.ORG_ID
				,a.VPN
				,a.NRF_COLOR_CODE
				,a.SUPP_COLOR
				,a.PLAN_TYPE
				,a.TTL_COST_US
				,a.TTL_COST_CA
				,a.RCPT_UNITS
				,a.CM_DOLLARS
				,c.MONTH_ID
				,c.MONTH_LABEL
				,c.QUARTER 
				,c.HALF_LABEL
				,c.FISCAL_YEAR_NUM
				,a.PO_KEY
				,a.PLAN_KEY
				,b.SELLING_BRAND
				,b.SELLING_COUNTRY 
			FROM t2dl_das_open_to_buy.AB_CM_ORDERS_CURRENT a
			JOIN ia_cluster_map b
			ON a.ORG_ID = b.CHANNEL_IDNT
			JOIN ia_date c
			on a.FISCAL_MONTH_ID = c.MONTH_IDNT
			--where a.DEPT_ID = '882'
			group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
			) d
			LEFT JOIN ia_supp_gp e
			ON d.DEPT_ID = e.DEPT_NUM
			AND d.SUPP_ID = e.SUPPLIER_NUM
			AND d.SELLING_BRAND = e.SELLING_BRAND
			LEFT JOIN (Select distinct payto_vendor_num,order_from_vendor_num from PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM
			where payto_vendor_num  in ('5179609')) f
			ON d.SUPP_ID = f.order_from_vendor_num
			group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)g
		LEFT JOIN ia_cattr h
		ON g.DEPT_ID = h.DEPT_NUM
		AND g.class_id = h.class_num 
		AND g.SUBCLASS_id = h.sbclass_num
		LEFT JOIN ia_cattr i
		ON  g.DEPT_ID = i.DEPT_NUM
		AND g.class_id = i.CLASS_NUM
		AND i.sbclass_num = -1
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27)

WITH DATA
   PRIMARY INDEX(DEPT_ID,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (DEPT_ID,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
     ,COLUMN(DEPT_ID)
     ,COLUMN(SUPPLIER_GROUP)
     ,COLUMN(CATEGORY)
     ,COLUMN(MONTH_LABEL)
     ON ia_cm;
    
    
--lets bring all the data together   
CREATE MULTISET VOLATILE TABLE ia_final AS(
SELECT 
		a.SELLING_COUNTRY
		,a.SELLING_BRAND
		,a.CHANNEL
		,a.SUPPLIER_GROUP 
		,a.CATEGORY 
		,a.DEPARTMENT_NUMBER
		,a.ALT_INV_MODEL
		,a.MONTH_ID
		,a.MONTH_LABEL
		,a.HALF
		,a.QUARTER
		,a.FISCAL_YEAR_NUM
		,a.BUY_PLANNER
		,a.PREFERRED_PARTNER_DESC
		,a.AREAS_OF_RESPOSIBILITY
		,a.NPG_IND 
		,a.DIVERSITY_GROUP 
		,a.NORD_TO_RACK_TRANSFER_RATE
		,a.CATEGORY_PLANNER_1
		,a.CATEGORY_PLANNER_2 
		,a.CATEGORY_GROUP
		,a.SEASONAL_DESIGNATION
		,a.RACK_MERCH_ZONE
		,a.IS_ACTIVEWEAR
		,a.CHANNEL_CATEGORY_ROLES_1
		,a.CHANNEL_CATEGORY_ROLES_2 
		,a.BARGAINISTA_DEPT_MAP
	  	,SUM(a.INACTIVE_RCPTS_MTD_C) AS INACTIVE_RCPTS_MTD_C
		,SUM(a.INACTIVE_OO_C) AS INACTIVE_OO_C
		,SUM(a.INACTIVE_CM_C) AS INACTIVE_CM_C
FROM(
		
		SELECT 
			CAST(SELLING_COUNTRY AS VARCHAR (2)) AS SELLING_COUNTRY
			,CAST(SELLING_BRAND AS VARCHAR(15)) AS SELLING_BRAND
			,CAST(CHANNEL_IDNT AS VARCHAR(5)) AS CHANNEL
			,CAST(SUPPLIER_GROUP AS VARCHAR(100)) AS SUPPLIER_GROUP 
			,CAST(CATEGORY AS VARCHAR(50)) AS CATEGORY 
			,CAST(DEPARTMENT_NUM AS INT) AS DEPARTMENT_NUMBER
			,CAST(ALT_INV_MODEL AS VARCHAR(15)) AS ALT_INV_MODEL
			,CAST(MONTH_ID AS VARCHAR(25)) AS MONTH_ID
			,CAST(MONTH_LABEL AS VARCHAR(15)) AS MONTH_LABEL
			,CAST(HALF_LABEL AS VARCHAR(20)) AS HALF
			,CAST(QUARTER AS VARCHAR(10)) AS QUARTER
			,CAST(YEAR_NUM AS VARCHAR(10)) AS FISCAL_YEAR_NUM
			,CAST(buy_planner AS VARCHAR(100)) AS BUY_PLANNER
			,CAST(preferred_partner_desc AS VARCHAR(100)) AS PREFERRED_PARTNER_DESC
			,CAST(areas_of_responsibility AS VARCHAR(100)) AS AREAS_OF_RESPOSIBILITY
			,CAST(is_npg AS VARCHAR(2)) AS NPG_IND 
			,CAST(diversity_group AS VARCHAR(100)) AS DIVERSITY_GROUP 
			,CAST(nord_to_rack_transfer_rate as VARCHAR(100)) as NORD_TO_RACK_TRANSFER_RATE
			,CAST(category_planner_1 AS VARCHAR(50)) AS CATEGORY_PLANNER_1
			,CAST(category_planner_2 AS VARCHAR(50)) AS CATEGORY_PLANNER_2 
			,CAST(category_group AS VARCHAR(50)) AS CATEGORY_GROUP
			,CAST(seasonal_designation AS VARCHAR(50)) AS SEASONAL_DESIGNATION
			,CAST(rack_merch_zone AS VARCHAR(50)) AS RACK_MERCH_ZONE
			,CAST(is_activewear AS VARCHAR(50)) AS IS_ACTIVEWEAR
			,CAST(channel_category_roles_1 AS VARCHAR(50)) AS CHANNEL_CATEGORY_ROLES_1
			,CAST(channel_category_roles_2 AS VARCHAR(50)) AS CHANNEL_CATEGORY_ROLES_2 
			,CAST(bargainista_dept_map AS VARCHAR(50)) AS BARGAINISTA_DEPT_MAP
		  	,CAST(INACTIVE_RCPTS_MTD_C AS DECIMAL(20, 4)) AS INACTIVE_RCPTS_MTD_C
		  	,CAST(0 AS DECIMAL(20, 4)) AS INACTIVE_OO_C
			,CAST(0 AS DECIMAL(20, 4)) AS INACTIVE_CM_C
		FROM ia_wtd_rcpts
		
		UNION ALL
		
		SELECT 
			CAST(SELLING_COUNTRY AS VARCHAR (2)) AS SELLING_COUNTRY
			,CAST(SELLING_BRAND AS VARCHAR(15)) AS SELLING_BRAND
			,CAST(CHANNEL_NUM AS VARCHAR(5)) AS CHANNEL
			,CAST(SUPPLIER_GROUP AS VARCHAR(100)) AS SUPPLIER_GROUP 
			,CAST(CATEGORY AS VARCHAR(50)) AS CATEGORY 
			,CAST(DEPARTMENT_NUM AS INT) AS DEPARTMENT_NUMBER
			,CAST(ALT_INV_MODEL AS VARCHAR(15)) AS ALT_INV_MODEL
			,CAST(MONTH_ID AS VARCHAR(25)) AS MONTH_ID
			,CAST(MONTH_LABEL AS VARCHAR(15)) AS MONTH_LABEL
			,CAST(HALF_LABEL AS VARCHAR(20)) AS HALF
			,CAST(QUARTER AS VARCHAR(10)) AS QUARTER
			,CAST(FISCAL_YEAR_NUM AS VARCHAR(10)) AS FISCAL_YEAR_NUM
			,CAST(buy_planner AS VARCHAR(100)) AS BUY_PLANNER
			,CAST(preferred_partner_desc AS VARCHAR(100)) AS PREFERRED_PARTNER_DESC
			,CAST(areas_of_responsibility AS VARCHAR(100)) AS AREAS_OF_RESPOSIBILITY
			,CAST(is_npg AS VARCHAR(2)) AS NPG_IND 
			,CAST(diversity_group AS VARCHAR(100)) AS DIVERSITY_GROUP 
			,CAST(nord_to_rack_transfer_rate as VARCHAR(100)) as NORD_TO_RACK_TRANSFER_RATE
			,CAST(category_planner_1 AS VARCHAR(50)) AS CATEGORY_PLANNER_1
			,CAST(category_planner_2 AS VARCHAR(50)) AS CATEGORY_PLANNER_2 
			,CAST(category_group AS VARCHAR(50)) AS CATEGORY_GROUP
			,CAST(seasonal_designation AS VARCHAR(50)) AS SEASONAL_DESIGNATION
			,CAST(rack_merch_zone AS VARCHAR(50)) AS RACK_MERCH_ZONE
			,CAST(is_activewear AS VARCHAR(50)) AS IS_ACTIVEWEAR
			,CAST(channel_category_roles_1 AS VARCHAR(50)) AS CHANNEL_CATEGORY_ROLES_1
			,CAST(channel_category_roles_2 AS VARCHAR(50)) AS CHANNEL_CATEGORY_ROLES_2 
			,CAST(bargainista_dept_map AS VARCHAR(50)) AS BARGAINISTA_DEPT_MAP
			,CAST(0 AS DECIMAL(20, 4)) AS INACTIVE_RCPTS_MTD_C
		  	,CAST(INACTIVE_OO_C AS DECIMAL(20, 4)) AS INACTIVE_OO_C
		  	,CAST(0 AS DECIMAL(20, 4)) AS INACTIVE_CM_C
		FROM ia_oo_final
	
	UNION ALL
	
	SELECT 
		CAST(SELLING_COUNTRY AS VARCHAR (2)) AS SELLING_COUNTRY
		,CAST(SELLING_BRAND AS VARCHAR(15)) AS SELLING_BRAND
		,CAST(ORG_ID AS VARCHAR(5)) AS CHANNEL
		,CAST(SUPPLIER_GROUP AS VARCHAR(100)) AS SUPPLIER_GROUP 
		,CAST(CATEGORY AS VARCHAR(50)) AS CATEGORY 
		,CAST(DEPT_ID AS INT) AS DEPARTMENT_NUMBER
		,CAST(ALT_INV_MODEL AS VARCHAR(15)) AS ALT_INV_MODEL
		,CAST(MONTH_ID AS VARCHAR(25)) AS MONTH_ID
		,CAST(MONTH_LABEL AS VARCHAR(15)) AS MONTH_LABEL
		,CAST(HALF_LABEL AS VARCHAR(20)) AS HALF
		,CAST(QUARTER AS VARCHAR(10)) AS QUARTER
		,CAST(FISCAL_YEAR_NUM AS VARCHAR(10)) AS FISCAL_YEAR_NUM
		,CAST(buy_planner AS VARCHAR(100)) AS BUY_PLANNER
		,CAST(preferred_partner_desc AS VARCHAR(100)) AS PREFERRED_PARTNER_DESC
		,CAST(areas_of_responsibility AS VARCHAR(100)) AS AREAS_OF_RESPOSIBILITY
		,CAST(is_npg AS VARCHAR(2)) AS NPG_IND 
		,CAST(diversity_group AS VARCHAR(100)) AS DIVERSITY_GROUP 
		,CAST(nord_to_rack_transfer_rate as VARCHAR(100)) as NORD_TO_RACK_TRANSFER_RATE
		,CAST(category_planner_1 AS VARCHAR(50)) AS CATEGORY_PLANNER_1
		,CAST(category_planner_2 AS VARCHAR(50)) AS CATEGORY_PLANNER_2 
		,CAST(category_group AS VARCHAR(50)) AS CATEGORY_GROUP
		,CAST(seasonal_designation AS VARCHAR(50)) AS SEASONAL_DESIGNATION
		,CAST(rack_merch_zone AS VARCHAR(50)) AS RACK_MERCH_ZONE
		,CAST(is_activewear AS VARCHAR(50)) AS IS_ACTIVEWEAR
		,CAST(channel_category_roles_1 AS VARCHAR(50)) AS CHANNEL_CATEGORY_ROLES_1
		,CAST(channel_category_roles_2 AS VARCHAR(50)) AS CHANNEL_CATEGORY_ROLES_2 
		,CAST(bargainista_dept_map AS VARCHAR(50)) AS BARGAINISTA_DEPT_MAP
	  	,CAST(0 AS DECIMAL(20, 4)) AS INACTIVE_RCPTS_MTD_C
	  	,CAST(0 AS DECIMAL(20, 4)) AS INACTIVE_OO_C
	  	,CAST(INACTIVE_CM_C AS DECIMAL(20, 4)) AS INACTIVE_CM_C
	FROM ia_cm
	) a 
--WHERE DEPARTMENT_NUMBER = '882'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27)

WITH DATA
   PRIMARY INDEX(DEPARTMENT_NUMBER,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (DEPARTMENT_NUMBER,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)
     ,COLUMN(DEPARTMENT_NUMBER)
     ,COLUMN(SUPPLIER_GROUP)
     ,COLUMN(CATEGORY)
     ,COLUMN(MONTH_LABEL)
     ON ia_final;
   
DELETE FROM T2DL_DAS_OPEN_TO_BUY.APT_INACTIVE_CURRENT;
    
INSERT INTO T2DL_DAS_OPEN_TO_BUY.APT_INACTIVE_CURRENT
	SELECT 
		a.SELLING_COUNTRY
		,a.SELLING_BRAND
		,TRIM(a.CHANNEL||','||c.CHANNEL_DESC) AS CHANNEL
		,a.SUPPLIER_GROUP 
		,a.CATEGORY 
		,TRIM(b.DIVISION_NUM||','||b.DIVISION_NAME) AS Division
		,TRIM(b.SUBDIVISION_NUM||','||b.SUBDIVISION_NAME) AS Subdivision
		,TRIM(a.DEPARTMENT_NUMBER||','||b.DEPT_NAME) AS Department
		,b.ACTIVE_STORE_IND AS ACTIVE_DIVISION
		,a.ALT_INV_MODEL
		,TRIM(a.MONTH_ID) as MONTH_ID
		,a.MONTH_LABEL
		,a.HALF
		,a.QUARTER
		,a.FISCAL_YEAR_NUM
		,a.BUY_PLANNER
		,a.PREFERRED_PARTNER_DESC
		,a.AREAS_OF_RESPOSIBILITY
		,a.NPG_IND
		,a.DIVERSITY_GROUP 
		,a.NORD_TO_RACK_TRANSFER_RATE
		,a.CATEGORY_PLANNER_1
		,a.CATEGORY_PLANNER_2 
		,a.CATEGORY_GROUP
		,a.SEASONAL_DESIGNATION
		,a.RACK_MERCH_ZONE
		,a.IS_ACTIVEWEAR
		,a.CHANNEL_CATEGORY_ROLES_1
		,a.CHANNEL_CATEGORY_ROLES_2 
		,a.BARGAINISTA_DEPT_MAP
		,a.INACTIVE_RCPTS_MTD_C
		,a.INACTIVE_OO_C
		,a.INACTIVE_CM_C
		,CURRENT_DATE AS PROCESS_DT 
		FROM ia_final a
JOIN prd_nap_usr_vws.department_dim b
on a.DEPARTMENT_NUMBER = b.dept_num
JOIN PRD_NAP_USR_VWS.STORE_DIM c 
ON a.CHANNEL = c.CHANNEL_NUM 
where a.INACTIVE_RCPTS_MTD_C <> 0
OR a.INACTIVE_OO_C <> 0
OR a.INACTIVE_CM_C <> 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34;

COLLECT STATS
     PRIMARY INDEX(Department,SELLING_BRAND, MONTH_ID,SUPPLIER_GROUP,CATEGORY)
     ,COLUMN(Department)
     ,COLUMN(SELLING_BRAND)
     ,COLUMN(MONTH_ID)
     ,COLUMN(SUPPLIER_GROUP)
     ,COLUMN(CATEGORY)
     ON T2DL_DAS_OPEN_TO_BUY.APT_INACTIVE_CURRENT;