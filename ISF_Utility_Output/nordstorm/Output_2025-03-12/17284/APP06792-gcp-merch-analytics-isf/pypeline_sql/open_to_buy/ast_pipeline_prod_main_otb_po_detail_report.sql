--This is the data that feeds the PO DETAIL REPORT
--It uses the data from T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL and brings in the CM data from AB
--Developed by: Isaac Wallick
--Updated last: 1/19/2024
CREATE MULTISET VOLATILE TABLE po_detail_base AS( 
	SELECT
	 	a.purchase_order_num 
	 	,a.status
	 	,a.edi_ind 
	 	,a.start_ship_date 
	 	,a.end_ship_date 
	 	,cast(b.latest_approval_event_tmstp_pacific as date) as latest_approval_date
	 	,a.open_to_buy_endofweek_date 
	 	,a.order_type 
	 	,b.internal_po_ind 
	 	,b.npg_ind 
	 	,b.po_type 
	 	,b.purchase_type 
	 FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_FACT a
	 left Join PRD_NAP_USR_VWS.PURCHASE_ORDER_HEADER_FACT b
	 on a.purchase_order_num = b.purchase_order_number 
	 WHERE  (
            (a.STATUS IN ( 'APPROVED','WORKSHEET')
            OR
            ( a.STATUS = 'CLOSED' AND (a.OPEN_TO_BUY_ENDOFWEEK_DATE >= current_date - 395)
              )
			)
		  AND (a.ORIGINAL_APPROVAL_DATE IS NOT NULL)
		  AND dropship_ind <> 't'
    )
)
WITH DATA
   PRIMARY INDEX(purchase_order_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (purchase_order_num)
     ,COLUMN(purchase_order_num)
     ON po_detail_base;
    
CREATE MULTISET VOLATILE TABLE cm_otb_date AS(   
SELECT 
     CAST(yr_454 || '-' || month_num || '-' || day_idnt AS date) AS day_date
    ,otb_eow_date
FROM (
		 SELECT DISTINCT
		     SUBSTR(otb_eow_date, 1, 2) AS day_idnt
		    ,SUBSTR(otb_eow_date, 4, 3) AS month_abrv
		    ,CAST(20 || SUBSTR(otb_eow_date, 8, 2) AS INTEGER) AS yr_454
		    ,otb_eow_date
		    ,CASE WHEN month_abrv = 'JAN' THEN '01'
                    WHEN month_abrv = 'FEB' THEN '02'
                    WHEN month_abrv = 'MAR' THEN '03'
                    WHEN month_abrv = 'APR' THEN '04'
                    WHEN month_abrv = 'MAY' THEN '05'
                    WHEN month_abrv = 'JUN' THEN '06'
                    WHEN month_abrv = 'JUL' THEN '07'
                    WHEN month_abrv = 'AUG' THEN '08'
                    WHEN month_abrv = 'SEP' THEN '09'
                    WHEN month_abrv = 'OCT' THEN '10'
                    WHEN month_abrv = 'NOV' THEN '11'
                    WHEN month_abrv = 'DEC' THEN '12'
                    END AS month_num
            
		FROM t2dl_das_open_to_buy.ab_cm_orders_current 

	) a
group by 1,2)
WITH DATA
   PRIMARY INDEX(day_date)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date)
     ON cm_otb_date;


CREATE MULTISET VOLATILE TABLE po_dtd_date AS(
select
	fiscal_year_num||fiscal_month_num||' '||month_abrv as "Month"
	,month_idnt 
	,month_label
	,day_date 
from PRD_NAP_USR_VWS.DAY_CAL_454_DIM
where day_date between CURRENT_DATE -180 and CURRENT_DATE
group by 1,2,3,4)
WITH DATA
   PRIMARY INDEX(day_date)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date)
     ON po_dtd_date;

    
 CREATE MULTISET VOLATILE TABLE po_mtd_date AS(
SELECT
     b.month_idnt 
FROM
	(SELECT 
	MAX(month_idnt) month_idnt
	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
	WHERE week_start_day_date <= CURRENT_DATE) a
JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM b
ON a.MONTH_IDNT = b.month_idnt
WHERE week_start_day_date <= CURRENT_DATE
GROUP BY 1)

WITH DATA
   PRIMARY INDEX(month_idnt)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (month_idnt)
     ,COLUMN(month_idnt)
     ON po_mtd_date;

CREATE MULTISET VOLATILE TABLE po_previous_date AS(
select 
	a.month_idnt 
from PRD_NAP_USR_VWS.DAY_CAL_454_DIM a
where a.day_date between CURRENT_DATE -180 and CURRENT_DATE
and a.month_idnt  < (select month_idnt from po_mtd_date group by 1)
group by 1 )

WITH DATA
   PRIMARY INDEX(month_idnt)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (month_idnt)
     ,COLUMN(month_idnt)
     ON po_previous_date;  
    
--drop table po_current_future_date;
CREATE MULTISET VOLATILE TABLE po_current_future_date AS(
SELECT
	fiscal_year_num||fiscal_month_num||' '||month_abrv as "Month"
	,MONTH_IDNT
	,CASE WHEN CURRENT_DATE BETWEEN month_start_day_date and month_end_day_date THEN 1 ELSE 0 END as curr_month_flag
	,quarter_label 
	,fiscal_year_num 
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM  
WHERE curr_month_flag =1 OR day_date >= current_date
group by 1,2,3,4,5)

WITH DATA
   PRIMARY INDEX(MONTH_IDNT)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (MONTH_IDNT)
     ,COLUMN(MONTH_IDNT)
     ON po_current_future_date;
    
--DROP TABLE po_supp_gp
CREATE MULTISET VOLATILE TABLE po_supp_gp AS( 
SELECT
	 a.dept_num 
	,a.supplier_num 
	,a.supplier_group 
	,case when a.banner = 'FP' then 'NORDSTROM' ELSE 'NORDSTROM_RACK' end as SELLING_BRAND 
FROM PRD_NAP_USR_VWS.SUPP_DEPT_MAP_DIM a
--where a.dept_num = '883'
GROUP BY 1,2,3,4)
WITH DATA
   PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)
   ON COMMIT PRESERVE ROWS;

 COLLECT STATS
     PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)
     ,COLUMN(DEPT_NUM)
     ,COLUMN(SUPPLIER_GROUP)
	ON po_supp_gp;

CREATE MULTISET VOLATILE TABLE po_cattr AS( 
SELECT 
	 a.dept_num
	,a.class_num 
	,a.sbclass_num 
	,a.CATEGORY
from PRD_NAP_USR_VWS.CATG_SUBCLASS_MAP_DIM a
--where a.dept_num = '883'
group by 1,2,3,4)

WITH DATA
   PRIMARY INDEX(dept_num,CATEGORY)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(dept_num,CATEGORY)
     ,COLUMN(dept_num)
     ,COLUMN(CATEGORY)
     ON po_cattr;
    
--drop table po_detail_loc
 CREATE MULTISET VOLATILE TABLE po_detail_loc AS(
		SELECT
			CHANNEL_NUM
			,channel_brand 
			,channel_desc 
		FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
		WHERE CHANNEL_BRAND in ('Nordstrom','Nordstrom_Rack')
		AND CHANNEL_NUM IN ('110', '120', '140', '210', '220', '240', '250', '260', '310', '930', '940', '990', '111', '121', '211', '221', '261', '311')
		AND CHANNEL_COUNTRY = 'US'
		AND store_name NOT like 'CLSD%' -- can be found as CLSD or CLOSED
  		AND store_name NOT like 'CLOSED%'
  		group by 1,2,3
		)

WITH DATA
   PRIMARY INDEX(CHANNEL_NUM)
   ON COMMIT PRESERVE ROWS;
  
--drop table po_detail_chnl_loc
CREATE MULTISET VOLATILE TABLE po_detail_chnl_loc AS(
		SELECT
			CHANNEL_NUM
			,channel_desc
			,channel_brand 
			,store_num 
		FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
		WHERE CHANNEL_BRAND in ('Nordstrom','Nordstrom_Rack')
		AND CHANNEL_NUM IN ('110', '120', '140', '210', '220', '240', '250', '260', '310', '930', '940', '990', '111', '121', '211', '221', '261', '311')
		AND CHANNEL_COUNTRY = 'US'
		AND store_name NOT like 'CLSD%' -- can be found as CLSD or CLOSED
  		AND store_name NOT like 'CLOSED%'
  		group by 1,2,3,4
		)

WITH DATA
   PRIMARY INDEX(store_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (store_num)
     ,COLUMN(CHANNEL_NUM)
     ON po_detail_chnl_loc;
     
CREATE MULTISET VOLATILE TABLE po_detail_rcpts AS( 
select 
	    g.purchase_order_num
		,g.CHANNEL_NUM
		,g.CHANNEL_DESC
		,g.CHANNEL_BRAND
		,g.division_num 
		,g.division_name 
		,g.subdivision_num 
		,g.subdivision_name 
		,g.dept_num 
		,g.dept_name 
		,g.class_num 
		,g.class_desc 
		,g.sbclass_num 
		,g.sbclass_desc
		,g.customer_choice
		,g.supp_num
		,g.supp_name
		,g.manufacturer_name
		,g.vpn
		,g.style_desc 
		,g.color_num 
		,g.supp_color 
		,g.status
		,case when g.edi_ind = 'T' then 'Y' else 'N' end as edi_ind
		,g.start_ship_date 
		,g.end_ship_date 
		,g.latest_approval_date
		,g.otb_eow_date
		,g.otb_month
		,g.otb_month_idnt
		,g.order_type 
		,case when g.internal_po_ind = 't' then 'Y' else 'N' end as internal_po_ind
		,case when g.npg_ind = 'T' then 'Y' else 'N' end as npg_ind
		,g.po_type
		,g.purchase_type
		,g."Month"
		,g.month_idnt 
		,g.fiscal_year_num
		,g.quarter_label
		,g.ttl_approved_qty
		,g.ttl_approved_c
		,g.ttl_approved_r
		,g.dc_received_qty
		,g.dc_received_c
		,max(g.UNIT_COST_AMT) as UNIT_COST_AMT
		,max(g.last_carrier_asn_date) as last_carrier_asn_date
	  	,max(g.last_supplier_asn_date) as last_supplier_asn_date
	  	,max(g.last_ship_activity_date) as last_ship_activity_date
	  	,max(g.last_outbound_activity_date) as last_outbound_activity_date
	  	,max(g.receipt_date) as receipt_date
		,sum(case when a.month_idnt is not null then g.RCPT_COST else 0 end) as RCPT_MTD_COST
		,sum(case when b.month_idnt is not null then g.RCPT_COST else 0 end) as RCPT_PREVIOUS_MTH_COST
		,sum(case when a.month_idnt is not null then g.RCPT_UNITS  else 0 end) as RCPT_MTD_UNITS
		,sum(case when b.month_idnt is not null then g.RCPT_UNITS else 0 end) as RCPT_PREVIOUS_MTH_UNITS
		,sum(g.RCPT_COST) as RCPT_COST
		,sum(g.RCPT_RETAIL) as RCPT_RETAIL
		,sum(g.RCPT_UNITS) as RCPT_UNITS
		,sum(g.QUANTITY_ORDERED) as QUANTITY_ORDERED
		,sum(g.QUANTITY_RECEIVED) as QUANTITY_RECEIVED
		,sum(g.QUANTITY_CANCELED) as QUANTITY_CANCELED
		,sum(g.QUANTITY_OPEN) as QUANTITY_OPEN
		,sum(g.TOTAL_ESTIMATED_LANDING_COST) as OO_COST
		,sum(g.TOTAL_ANTICIPATED_RETAIL_AMT) as OO_RETAIL
from T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL g 
left join po_mtd_date a
on g.month_idnt = a.month_idnt
left join po_previous_date b
on g.month_idnt = b.month_idnt 
--where purchase_order_num = '25272105'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44)

WITH DATA
   PRIMARY INDEX(purchase_order_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(purchase_order_num)
     ,COLUMN(purchase_order_num)
     ON po_detail_rcpts;
    
CREATE MULTISET VOLATILE TABLE po_supp_cat AS( 
select
c.*
,Coalesce(h.CATEGORY, i.CATEGORY, 'OTHER') CATEGORY
from 	
	(select 
		a.*
		,COALESCE(b.SUPPLIER_GROUP,'OTHER') AS SUPPLIER_GROUP
	from po_detail_rcpts a
	LEFT JOIN po_supp_gp b
		ON a.dept_num = b.dept_num
		AND a.supp_num = b.supplier_num
		AND a.channel_brand = b.selling_brand
		--where a.dept_num = '883'
		) c
	LEFT JOIN po_cattr h
		ON cast(c.DEPT_num as integer) = h.DEPT_NUM
		AND cast(c.CLASS_NUM as varchar(50)) = h.class_num 
		AND cast(c.sbclass_num as varchar(50)) = h.sbclass_num
	LEFT JOIN po_cattr i
	ON  cast(c.DEPT_num as integer) = i.DEPT_NUM
	AND cast(c.CLASS_NUM as varchar(50)) = i.CLASS_NUM
	AND cast(i.sbclass_num as varchar(50)) = -1
	--where c.dept_num = '883'
	
	)
WITH DATA
   PRIMARY INDEX(purchase_order_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(purchase_order_num)
     ,COLUMN(purchase_order_num)
     ON po_supp_cat;
 
--drop table po_cm;
--lets get the cm data
CREATE MULTISET VOLATILE TABLE po_cm AS( 
select 
		l.po_number 
		,i.status
		,i.edi_ind 
		,i.start_ship_date 
		,i.end_ship_date 
		,i.latest_approval_date
		,i.order_type 
		,i.internal_po_ind 
		,l.npg_ind 
		,i.po_type 
		,i.purchase_type
		,l.channel_num
		,l.channel_desc
		,l.channel_brand 
		,l.division_num 
		,l.division_name 
		,l.subdivision_num 
		,l.subdivision_name 
		,l.dept_id 
		,l.dept_name 
		,l.class_id 
		,l.subclass_id
		,l.supp_id 
		,l.supp_name
		,l.fiscal_month
	    ,l.otb_month
		,k.day_date as otb_eow_date
		,l.quarter_label 
		,l.fiscal_year_num 
		,l.fiscal_month_id 
		,l.vpn 
		,l.vpn_desc 
		,l.supp_color
		,l.SUPPLIER_GROUP
		,coalesce(g.CATEGORY, h.CATEGORY, 'OTHER') CATEGORY
		,l.nrf_color_code 
		,l.plan_season_desc
		,l.plan_type 
		,l.CM_C
		,l.CM_R
		,l.CM_U
from(
 SELECT 
			e.po_number 
			,e.channel_num
			,e.channel_desc
			,e.channel_brand 
			,e.division_num 
			,e.division_name 
			,e.subdivision_num 
			,e.subdivision_name 
			,e.dept_id 
			,e.dept_name 
			,e.class_id 
			,e.subclass_id
			,e.supp_id 
			,e.supp_name
			,e."Month" as fiscal_month
			,e."Month" as otb_month
			,e.otb_eow_date 
			,e.quarter_label 
			,e.fiscal_year_num 
			,e.fiscal_month_id 
			,e.vpn 
			,e.vpn_desc 
			,e.supp_color
			,coalesce(f.SUPPLIER_GROUP,'OTHER') AS SUPPLIER_GROUP
			,e.nrf_color_code 
			,e.plan_season_desc
			,e.plan_type
			,e.npg_ind 
			,e.CM_C
			,e.CM_R
			,e.CM_U
	from
		(
		select 
			a.po_number 
			,a.org_id 
			,b.channel_num
			,b.channel_desc
			,b.channel_brand
			,d.division_num  as division_num 
			,d.division_name  as division_name 
			,d.subdivision_num  as subdivision_num 
			,d.subdivision_name  as subdivision_name 
			,a.dept_id
			,d.dept_name  as dept_name 
			,a.class_id
			,a.subclass_id
			,a.supp_id 
			,a.supp_name
			,c."Month"
			,a.fiscal_month_id 
			,c.quarter_label 
			,c.fiscal_year_num 
			,a.otb_eow_date 
			,a.vpn 
			,a.vpn_desc 
			,a.supp_color 
			,a.nrf_color_code 
			,a.plan_season_desc
			,a.plan_type
			,a.npg_ind 
			,sum(ttl_cost_us) as CM_C
			,sum(rcpt_units*unit_rtl_us) as CM_R
			,sum(rcpt_units) as CM_U
		from T2DL_DAS_OPEN_TO_BUY.AB_CM_ORDERS_CURRENT a
		join po_detail_loc b 
		on a.org_id = b.channel_num
		join po_current_future_date c
		on a.fiscal_month_id = c.month_idnt
		left join PRD_NAP_USR_VWS.DEPARTMENT_DIM d
		on a.dept_id = d.dept_num
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27)e
	LEFT JOIN po_supp_gp f
		ON e.dept_id = f.dept_num
		AND e.supp_id = f.supplier_num
		AND e.channel_brand = f.selling_brand) l
	LEFT JOIN po_cattr g
		ON l.dept_id = g.DEPT_NUM
		AND l.class_id = g.class_num 
		AND l.subclass_id = g.sbclass_num
	LEFT JOIN po_cattr h
		ON  l.dept_id = h.DEPT_NUM
		AND l.class_id = h.CLASS_NUM
		AND h.sbclass_num = -1
	LEFT JOIN po_detail_base i
	 ON l.po_number = i.purchase_order_num
 	join cm_otb_date k
 	on l.otb_eow_date = k.otb_eow_date
 	)
	
WITH DATA
   PRIMARY INDEX(po_number)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(po_number)
     ,COLUMN(po_number)
     ON po_cm;

CREATE MULTISET VOLATILE TABLE po_supp AS( 
SELECT 
	 	purchase_order_num
	 	,CHANNEL_NUM
	 	,CHANNEL_DESC
		,CHANNEL_BRAND
		,division_num
		,division_name
		,subdivision_num
		,subdivision_name
		,dept_num
		,dept_name
		,class_num
		,sbclass_num
		,customer_choice
		,supp_num
		,supp_name
		,manufacturer_name
		,vpn
		,vpn_desc 
		,color_num
		,supp_color
		,SUPPLIER_GROUP
	    ,CATEGORY
		,status
		,case when edi_ind = 'T' or edi_ind = 'Y' then 'Y' else 'N' end as edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,otb_month
		,otb_month_idnt
		,order_type
		,case when internal_po_ind = 'T' or internal_po_ind = 'Y' then 'Y' else 'N' end as internal_po_ind
		,case when npg_ind = 'T' or npg_ind = 'Y' then 'Y' else 'N' end as npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,fiscal_year_num
		,quarter_label
		,plan_season_desc 
	  	,plan_type
	  	,ttl_approved_c
	  	,ttl_approved_r
	  	,ttl_approved_qty
	  	,dc_received_qty
	  	,dc_received_c
	  	,max(UNIT_COST_AMT) as UNIT_COST_AMT
		,max(last_carrier_asn_date) as last_carrier_asn_date
	  	,max(last_supplier_asn_date) as last_supplier_asn_date
	  	,max(last_ship_activity_date) as last_ship_activity_date
	  	,max(last_outbound_activity_date) as last_outbound_activity_date
	  	,max(receipt_date) as receipt_date
	  	,sum(RCPT_MTD_COST) as RCPT_MTD_COST
		,sum(RCPT_PREVIOUS_MTH_COST) as RCPT_PREVIOUS_MTH_COST
		,sum(RCPT_COST) as RCPT_COST
		,sum(RCPT_RETAIL) as RCPT_RETAIL
		,sum(RCPT_MTD_UNITS) as RCPT_MTD_UNITS
		,sum(RCPT_PREVIOUS_MTH_UNITS) as RCPT_PREVIOUS_MTH_UNITS
		,sum(RCPT_UNITS) as RCPT_UNITS
		,sum(QUANTITY_ORDERED) as QUANTITY_ORDERED
		,sum(QUANTITY_RECEIVED) as QUANTITY_RECEIVED
		,sum(QUANTITY_CANCELED) as QUANTITY_CANCELED
		,sum(QUANTITY_OPEN) as QUANTITY_OPEN
		,sum(OO_COST) as OO_COST
		,sum(OO_RETAIL) as OO_RETAIL
		,sum(CM_C) as CM_C
		,sum(CM_R) as CM_R
		,sum(CM_U) as CM_U

from (
select 
	cast(g.purchase_order_num as varchar(50)) as purchase_order_num
	,cast(g.CHANNEL_NUM as varchar(50)) as CHANNEL_NUM
	,cast(g.CHANNEL_DESC as varchar(50)) as CHANNEL_DESC
	,cast(g.CHANNEL_BRAND as varchar(50)) as CHANNEL_BRAND
	,cast(g.division_num as varchar(50)) as division_num
	,cast(g.division_name as varchar(50)) as division_name
	,cast(g.subdivision_num as varchar(50)) as subdivision_num
	,cast(g.subdivision_name as varchar(50)) as  subdivision_name
	,cast(g.dept_num as varchar(50)) as dept_num
	,cast(g.dept_name as varchar (50)) as dept_name
	,cast(g.class_num as varchar(50)) as class_num
	,cast(g.sbclass_num as varchar(50)) as sbclass_num	
	,cast(g.customer_choice as varchar(50)) as customer_choice
	,cast(g.supp_num as varchar(50)) as supp_num
	,cast(g.supp_name as varchar(50)) as supp_name
	,cast(g.manufacturer_name as varchar(50)) as manufacturer_name
	,cast(g.vpn as varchar(50)) as vpn
	,cast(g.style_desc as varchar(50)) as vpn_desc 
	,cast(g.color_num as varchar(50)) as color_num
	,cast(g.supp_color as varchar(50)) as supp_color
	,cast(g.SUPPLIER_GROUP as varchar(50)) as SUPPLIER_GROUP
    ,cast(g.CATEGORY as varchar(50)) as CATEGORY
	,cast(g.status as varchar(50)) as status
	,cast(g.edi_ind as varchar(50)) as edi_ind
	,cast(g.start_ship_date as date) as start_ship_date
	,cast(g.end_ship_date as date) as end_ship_date
	,cast(g.latest_approval_date as date) as latest_approval_date
	,cast(g.otb_eow_date as date) as otb_eow_date
	,cast(g.otb_month as varchar(50)) as otb_month
	,cast(g.otb_month_idnt as varchar(50)) as otb_month_idnt
	,cast(g.order_type as varchar(50)) as order_type
	,cast(g.internal_po_ind as varchar(50)) as internal_po_ind
	,cast(g.npg_ind as varchar(50)) as npg_ind
	,cast(g.po_type as varchar(50)) as po_type
	,cast(g.purchase_type as varchar(50)) as purchase_type
	,cast(g."Month" as varchar(50)) as "Month"
	,cast(g.month_idnt as varchar(50)) as month_idnt
	,cast(g.fiscal_year_num as varchar(50)) as fiscal_year_num
	,cast(g.quarter_label as varchar(50)) as quarter_label
	,cast(g.last_carrier_asn_date as date) as last_carrier_asn_date
	,cast(g.last_supplier_asn_date as date) as last_supplier_asn_date
	,cast(g.last_ship_activity_date as date) as last_ship_activity_date
	,cast(g.last_outbound_activity_date as date) as last_outbound_activity_date
	,cast(g.receipt_date as date) as receipt_date
	,CAST(g.RCPT_MTD_COST AS DECIMAL(20, 4)) as RCPT_MTD_COST
	,CAST(g.RCPT_PREVIOUS_MTH_COST AS DECIMAL(20, 4)) as RCPT_PREVIOUS_MTH_COST
	,CAST(g.RCPT_COST AS DECIMAL(20, 4)) as RCPT_COST
	,CAST(g.RCPT_RETAIL AS DECIMAL(20, 4)) as RCPT_RETAIL
	,CAST(g.RCPT_MTD_UNITS AS DECIMAL(20, 4)) as RCPT_MTD_UNITS
	,CAST(g.RCPT_PREVIOUS_MTH_UNITS AS DECIMAL(20, 4)) as RCPT_PREVIOUS_MTH_UNITS
	,CAST(g.RCPT_UNITS AS DECIMAL(20, 4)) as RCPT_UNITS
	,CAST(g.QUANTITY_ORDERED AS DECIMAL(20, 4)) as QUANTITY_ORDERED
	,CAST(g.QUANTITY_RECEIVED AS DECIMAL(20, 4)) as QUANTITY_RECEIVED
	,CAST(g.QUANTITY_CANCELED AS DECIMAL(20, 4)) as QUANTITY_CANCELED
	,CAST(g.QUANTITY_OPEN AS DECIMAL(20, 4)) as QUANTITY_OPEN
	,CAST(g.UNIT_COST_AMT AS DECIMAL(20, 4)) as UNIT_COST_AMT
	,CAST(g.OO_COST AS DECIMAL(20, 4)) as OO_COST
	,CAST(g.OO_RETAIL AS DECIMAL(20, 4)) as OO_RETAIL
	,CAST(g.ttl_approved_qty AS DECIMAL(20, 4)) as ttl_approved_qty
 	,CAST(g.ttl_approved_c AS DECIMAL(20, 4)) as ttl_approved_c
 	,CAST(g.ttl_approved_r AS DECIMAL(20, 4)) as ttl_approved_r
 	,CAST(g.dc_received_qty AS DECIMAL(20, 4)) as dc_received_qty
	,CAST(g.dc_received_c AS DECIMAL(20, 4)) as dc_received_c
	,cast(0 as varchar(50)) as plan_season_desc 
	,cast(0 as varchar(50)) as plan_type
	,CAST(0 AS DECIMAL(20, 4))as CM_C
    ,CAST(0 AS DECIMAL(20, 4)) as CM_R
	,CAST(0 AS DECIMAL(20, 4)) as CM_U
from po_supp_cat g

union all

select
		cast(j.po_number as varchar(50)) as purchase_order_num
		,cast(j.channel_num as varchar(50)) as channel_num
		,cast(j.channel_desc as varchar(50)) as channel_desc
		,cast(j.channel_brand as varchar(50)) as channel_brand
		,cast(j.division_num as varchar(50)) as division_num
		,cast(j.division_name as varchar(50)) as division_name
		,cast(j.subdivision_num as varchar(50)) as subdivision_num
		,cast(j.subdivision_name as varchar(50)) as subdivision_name
		,cast(j.dept_id as varchar(50)) as dept_num 
		,cast(j.dept_name as varchar(50)) as dept_name
		,cast(j.class_id as varchar(50)) as class_num 
		,cast(j.subclass_id as varchar(50)) as sbclass_num
		,cast(0 as varchar(50)) as customer_choice
		,cast(j.supp_id as varchar(50)) as supp_num
		,cast(j.supp_name as varchar(50)) as supp_name
		,cast(0 as varchar(50)) as manufacturer_name
		,cast(j.vpn as varchar(50)) as vpn
		,cast(j.vpn_desc as varchar(50)) as vpn_desc
		,cast(j.nrf_color_code as varchar(50)) as color_num
		,cast(j.supp_color as varchar(50)) as supp_color
		,cast(j.SUPPLIER_GROUP as varchar(50)) as SUPPLIER_GROUP
		,cast(j.CATEGORY as varchar(50)) as CATEGORY
		,cast(j.status as varchar(50)) as status
		,cast(j.edi_ind as varchar(50)) as edi_ind
		,cast(j.start_ship_date as date) as start_ship_date
		,cast(j.end_ship_date as date) as end_ship_date
		,cast(j.latest_approval_date as date) as latest_approval_date
		,cast(j.otb_eow_date as date) as otb_eow_date
		,cast(j.otb_month as varchar(50)) as otb_month
		,cast(j.fiscal_month_id as varchar(50)) as otb_month_idnt
		,cast(j.order_type as varchar(50)) as order_type
		,cast(j.internal_po_ind as varchar(50)) as internal_po_ind
		,cast(j.npg_ind as varchar(50)) as npg_ind
		,cast(j.po_type as varchar(50)) as po_type
		,cast(j.purchase_type as varchar(50)) as purchase_type 
		,cast(j.fiscal_month as varchar(50)) as "Month"
		,cast(j.fiscal_month_id as varchar(50)) as month_idnt
		,cast(j.fiscal_year_num as varchar(50)) as fiscal_year_num
		,cast(j.quarter_label as varchar(50)) as quarter_label
		,cast(NULL as date) as last_carrier_asn_date
	  	,cast(NULL as date) as last_supplier_asn_date
	  	,cast(NULL as date) as last_ship_activity_date
	  	,cast(NULL as date) as last_outbound_activity_date
	  	,cast(NULL as date) as receipt_date
		,cast(0 AS DECIMAL(20, 4)) as RCPT_MTD_COST
		,cast(0 AS DECIMAL(20, 4)) as RCPT_PREVIOUS_MTH_COST
		,cast(0 AS DECIMAL(20, 4)) as RCPT_RETAIL
		,cast(0 AS DECIMAL(20, 4)) as RCPT_COST
		,CAST(0 AS DECIMAL(20, 4)) as RCPT_MTD_UNITS
		,CAST(0 AS DECIMAL(20, 4)) as RCPT_PREVIOUS_MTH_UNITS
		,cast(0 AS DECIMAL(20, 4)) as RCPT_UNITS
		,cast(0 AS DECIMAL(20, 4)) as QUANTITY_ORDERED
		,cast(0 AS DECIMAL(20, 4)) as QUANTITY_RECEIVED
		,cast(0 AS DECIMAL(20, 4)) as QUANTITY_CANCELED
		,cast(0 AS DECIMAL(20, 4)) as QUANTITY_OPEN
		,cast(0 AS DECIMAL(20, 4)) as UNIT_COST_AMT
		,cast(0 AS DECIMAL(20, 4)) as OO_COST
		,cast(0 AS DECIMAL(20, 4)) as OO_RETAIL
		,cast(0 AS DECIMAL(20, 4)) as ttl_approved_qty
 		,cast(0 AS DECIMAL(20, 4)) as ttl_approved_c
 		,cast(0 AS DECIMAL(20, 4)) as ttl_approved_r
 		,cast(0 AS DECIMAL(20, 4)) as dc_received_qty
	  	,cast(0 AS DECIMAL(20, 4)) as dc_received_c
	    ,cast(j.plan_season_desc as varchar(50)) as plan_season_desc
		,cast(j.plan_type  as varchar(50)) as plan_type 
		,cast(j.CM_C AS DECIMAL(20, 4)) as CM_C
		,cast(j.CM_R AS DECIMAL(20, 4)) as CM_R
		,cast(j.CM_U AS DECIMAL(20, 4)) as CM_U
from po_cm j) b

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46
) 

WITH DATA
   PRIMARY INDEX(purchase_order_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(purchase_order_num)
     ,COLUMN(purchase_order_num)
     ON po_supp;
   
--drop table po_supp_final
CREATE MULTISET VOLATILE TABLE po_supp_final AS(     
  select 
		purchase_order_num
	 	,CHANNEL_NUM
	 	,CHANNEL_DESC
		,CHANNEL_BRAND
		,division_num
		,division_name
		,subdivision_num
		,subdivision_name
		,dept_num
		,dept_name
		,supp_num
		,supp_name
		,customer_choice
		,manufacturer_name
		,SUPPLIER_GROUP
		,case when status is null then 'CM' else status end as status
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,otb_month
		,otb_month_idnt
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,fiscal_year_num
		,quarter_label
		,plan_season_desc 
	  	,plan_type
	  	,ttl_approved_qty
	  	,ttl_approved_c
	  	,ttl_approved_r
	  	,g.dc_received_qty
	  	,g.dc_received_c
	  	,max(g.UNIT_COST_AMT) as UNIT_COST_AMT
	    ,max(g.last_carrier_asn_date) as last_carrier_asn_date
	  	,max(g.last_supplier_asn_date) as last_supplier_asn_date
	  	,max(g.last_ship_activity_date) as last_ship_activity_date
	  	,max(g.last_outbound_activity_date) as last_outbound_activity_date
	  	,max(g.receipt_date) as receipt_date
		,sum(g.RCPT_MTD_COST) as RCPT_MTD_COST
		,sum(g.RCPT_PREVIOUS_MTH_COST) as RCPT_PREVIOUS_MTH_COST
		,sum(g.RCPT_COST) as RCPT_COST
		,sum(g.RCPT_RETAIL) as RCPT_RETAIL
		,sum(g.RCPT_MTD_UNITS) as RCPT_MTD_UNITS
		,sum(g.RCPT_PREVIOUS_MTH_UNITS) as RCPT_PREVIOUS_MTH_UNITS
		,sum(g.RCPT_UNITS) as RCPT_UNITS
		,sum(g.QUANTITY_ORDERED) as QUANTITY_ORDERED
		,sum(g.QUANTITY_RECEIVED) as QUANTITY_RECEIVED
		,sum(g.QUANTITY_CANCELED) as QUANTITY_CANCELED
		,sum(g.QUANTITY_OPEN) as QUANTITY_OPEN
		,sum(g.OO_COST) as OO_COST
		,sum(g.OO_RETAIL) as OO_RETAIL
	 	,SUM(g.CM_C) as CM_C
	    ,SUM(g.CM_R) as CM_R
	    ,SUM(g.CM_U) as CM_U
	    ,current_date as process_dt
from  po_supp g 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
 ) 

WITH DATA
   PRIMARY INDEX(purchase_order_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(purchase_order_num)
     ,COLUMN(purchase_order_num)
     ON po_supp_final;   
 
--drop table po_vasn
CREATE MULTISET VOLATILE TABLE po_vasn AS (
select 
	g.purchase_order_num
	,max(g.vendor_ship_dt) as last_vasn_signal
	,sum(g.vasn_sku_qty) as vasn_sku_qty
	,sum((h.unit_cost * g.vasn_sku_qty) + (h.total_expenses_per_unit * g.vasn_sku_qty)+(h.total_duty_per_unit * g.vasn_sku_qty)) as vasn_cost
from(
			select
				c.purchase_order_num
				,c.ship_location_id
				,c.upc_num
				,d.rms_sku_num
				,c.vasn_sku_qty
				,c.vendor_ship_dt
		from 
			(select
				a.purchase_order_num
				,a.ship_location_id
				, SUBSTRING(a.upc_num,3,LENGTH(a.upc_num)) as upc_num
				,a.units_shipped as vasn_sku_qty
				,a.vendor_ship_dt
			from PRD_NAP_USR_VWS.VENDOR_ASN_FACT_VW a
			join po_detail_base b
			on a.purchase_order_num = b.purchase_order_num
			--where a.PURCHASE_ORDER_NUM = '40073120'
			) c
		join PRD_NAP_USR_VWS.PRODUCT_UPC_DIM d
		on c.upc_num = d.upc_num
		where d.prmy_upc_ind= 'Y'
		and d.channel_country= 'US'
) g
join PRD_NAP_USR_VWS.PURCHASE_ORDER_SHIPLOCATION_FACT h
on g.purchase_order_num = h.purchase_order_number
and g.rms_sku_num = h.rms_sku_num
and g.ship_location_id = h.ship_location_id
group by 1
	)
WITH DATA 
	PRIMARY INDEX (purchase_order_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num)
	,COLUMN (purchase_order_num) 
ON po_vasn;

CREATE MULTISET VOLATILE TABLE po_xref AS (
SELECT 
	a.purchase_order_num
	, a.crossreference_external_id as xref_po
from prd_nap_usr_vws.purchase_order_fact a
join po_detail_base b
on a.purchase_order_num = b.purchase_order_num
group by 1,2)

WITH DATA 
	PRIMARY INDEX (purchase_order_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num)
	,COLUMN (purchase_order_num) 
ON po_xref;

--drop table po_supp_stg
CREATE MULTISET VOLATILE TABLE po_supp_stg AS( 
select 
		a.purchase_order_num
		,h.xref_po
	 	,a.CHANNEL_NUM
	 	,a.CHANNEL_DESC
		,a.CHANNEL_BRAND
		,a.division_num
		,a.division_name
		,a.subdivision_num
		,a.subdivision_name
		,a.dept_num
		,a.dept_name
		,a.supp_num
		,a.supp_name
		,a.manufacturer_name
		,a.SUPPLIER_GROUP
		,a.status
		,a.edi_ind
		,a.start_ship_date
		,a.end_ship_date
		,a.latest_approval_date
		,a.otb_eow_date
		,a.otb_month
		,a.otb_month_idnt
		,a.order_type
		,a.internal_po_ind
		,a.npg_ind
		,a.po_type
		,a.purchase_type
		,a."Month"
		,a.month_idnt
		,a.fiscal_year_num
		,a.quarter_label
		,a.plan_season_desc 
	  	,a.plan_type
	  	,a.ttl_approved_qty
	  	,a.ttl_approved_c
	  	,a.ttl_approved_r
	  	,g.vasn_sku_qty
 		,g.vasn_cost
 		,a.dc_received_qty
 		,a.dc_received_c
	  	,max(a.UNIT_COST_AMT) as UNIT_COST_AMT
	    ,max(a.last_carrier_asn_date) as last_carrier_asn_date
	  	,max(a.last_supplier_asn_date) as last_supplier_asn_date
	  	,max(a.last_ship_activity_date) as last_ship_activity_date
	  	,max(a.last_outbound_activity_date) as last_outbound_activity_date
	  	,max(a.receipt_date) as receipt_date
		,sum(a.RCPT_MTD_COST) as RCPT_MTD_COST
		,sum(a.RCPT_PREVIOUS_MTH_COST) as RCPT_PREVIOUS_MTH_COST
		,sum(a.RCPT_COST) as RCPT_COST
		,sum(a.RCPT_RETAIL) as RCPT_RETAIL
		,sum(a.RCPT_MTD_UNITS) as RCPT_MTD_UNITS
		,sum(a.RCPT_PREVIOUS_MTH_UNITS) as RCPT_PREVIOUS_MTH_UNITS
		,sum(a.RCPT_UNITS) as RCPT_UNITS
		,sum(a.QUANTITY_ORDERED) as QUANTITY_ORDERED
		,sum(a.QUANTITY_RECEIVED) as QUANTITY_RECEIVED
		,sum(a.QUANTITY_CANCELED) as QUANTITY_CANCELED
		,sum(a.QUANTITY_OPEN) as QUANTITY_OPEN
		,sum(a.OO_COST) as OO_COST
		,sum(a.OO_RETAIL) as OO_RETAIL
	 	,SUM(a.CM_C) as CM_C
	    ,SUM(a.CM_R) as CM_R
	    ,SUM(a.CM_U) as CM_U
	    ,current_date as process_dt
from po_supp_final a
left join po_vasn g
on a.purchase_order_num = g.purchase_order_num
left join po_xref h
on a.purchase_order_num = h.purchase_order_num
--where a.PURCHASE_ORDER_NUM = '40040399'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
) 

WITH DATA
   PRIMARY INDEX(purchase_order_num)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(purchase_order_num)
     ,COLUMN(purchase_order_num)
     ON po_supp_stg;
    
DELETE FROM T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL_REPORT ;

INSERT INTO T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL_REPORT
select 
		d.purchase_order_num
		,d.xref_po
	 	,d.CHANNEL_NUM
	 	,d.CHANNEL_DESC
		,d.CHANNEL_BRAND
		,d.division_num
		,d.division_name
		,d.subdivision_num
		,d.subdivision_name
		,d.dept_num
		,d.dept_name
		,d.supp_num
		,d.supp_name
		,d.manufacturer_name
		,d.SUPPLIER_GROUP
		,d.status
		,d.otb_impact
		,d.edi_ind
		,d.start_ship_date
		,d.end_ship_date
		,d.latest_approval_date
		,d.otb_eow_date
		,d.otb_month
		,d.otb_month_idnt
		,d.order_type
		,d.internal_po_ind
		,d.npg_ind
		,d.po_type
		,d.purchase_type
		,d."Month"
		,d.month_idnt
		,d.fiscal_year_num
		,d.quarter_label
		,d.plan_season_desc 
	  	,d.plan_type
	  	,d.ttl_approved_qty
	  	,d.ttl_approved_c
	  	,d.ttl_approved_r
	  	,d.UNIT_COST_AMT
	    ,d.last_carrier_asn_date
	  	,d.last_supplier_asn_date
	  	,d.last_ship_activity_date
	  	,d.last_outbound_activity_date
	  	,d.receipt_date
		,d.RCPT_MTD_COST
		,d.RCPT_PREVIOUS_MTH_COST
		,d.RCPT_COST
		,d.RCPT_RETAIL
		,d.RCPT_MTD_UNITS
		,d.RCPT_PREVIOUS_MTH_UNITS
		,d.RCPT_UNITS
		,d.QUANTITY_ORDERED
		,d.QUANTITY_RECEIVED
		,d.QUANTITY_CANCELED
		,d.QUANTITY_OPEN
		,d.OO_COST
		,d.OO_RETAIL
		,sum(case when (d.vasn_sku_qty-d.carry_forward_u)<0 Then 0 else (d.vasn_sku_qty-d.carry_forward_u)end) as vasn_sku_qty
 		,sum(case when (d.vasn_cost-d.carry_forward_c)<0 then 0 else (d.vasn_cost-d.carry_forward_c) end)  as vasn_cost
 		,d.dc_received_qty
	  	,d.dc_received_c
	 	,d.CM_C
	    ,d.CM_R
	    ,d.CM_U
	    ,d.process_dt
From(select 
			c.purchase_order_num
			,c.xref_po
		 	,c.CHANNEL_NUM
		 	,c.CHANNEL_DESC
			,c.CHANNEL_BRAND
			,c.division_num
			,c.division_name
			,c.subdivision_num
			,c.subdivision_name
			,c.dept_num
			,c.dept_name
			,c.supp_num
			,c.supp_name
			,c.manufacturer_name
			,c.SUPPLIER_GROUP
			,c.status
			,c.otb_impact
			,c.edi_ind
			,c.start_ship_date
			,c.end_ship_date
			,c.latest_approval_date
			,c.otb_eow_date
			,c.otb_month
			,c.otb_month_idnt
			,c.order_type
			,c.internal_po_ind
			,c.npg_ind
			,c.po_type
			,c.purchase_type
			,c."Month"
			,c.month_idnt
			,c.fiscal_year_num
			,c.quarter_label
			,c.plan_season_desc 
		  	,c.plan_type
		  	,c.ttl_approved_qty
		  	,c.ttl_approved_c
		  	,c.ttl_approved_r
		  	,c.UNIT_COST_AMT
		    ,c.last_carrier_asn_date
		  	,c.last_supplier_asn_date
		  	,c.last_ship_activity_date
		  	,c.last_outbound_activity_date
		  	,c.receipt_date
			,c.RCPT_MTD_COST
			,c.RCPT_PREVIOUS_MTH_COST
			,c.RCPT_COST
			,c.RCPT_RETAIL
			,c.RCPT_MTD_UNITS
			,c.RCPT_PREVIOUS_MTH_UNITS
			,c.RCPT_UNITS
			,c.QUANTITY_ORDERED
			,c.QUANTITY_RECEIVED
			,c.QUANTITY_CANCELED
			,c.QUANTITY_OPEN
			,c.OO_COST
			,c.OO_RETAIL
			,sum(c.rcpt_u-c.rcpt_units) as carry_forward_u
	 		,sum(c.rcpt_c-c.rcpt_cost) as carry_forward_c
	 		,c.vasn_sku_qty
	 		,c.vasn_cost
	 		,c.dc_received_qty
		  	,c.dc_received_c
		 	,c.CM_C
		    ,c.CM_R
		    ,c.CM_U
		    ,c.process_dt
	FROM(
	select a.*
	,case when (a.RCPT_PREVIOUS_MTH_COST <> 0 or a.RCPT_MTD_COST <> 0) and a.month_idnt<a.otb_month_idnt and a.internal_po_ind ='N' then 'Shift In Future Mth'
				  when (a.RCPT_PREVIOUS_MTH_COST <> 0 or a.RCPT_MTD_COST <> 0) and a.month_idnt = a.otb_month_idnt and a.internal_po_ind ='N' then 'In Mth Rcvd'
				  when (a.RCPT_PREVIOUS_MTH_COST <> 0 or a.RCPT_MTD_COST <> 0) and a.month_idnt > a.otb_month_idnt and a.internal_po_ind ='N' then 'Shift In Prior Mth'
				  when (a.RCPT_PREVIOUS_MTH_COST <> 0 or a.RCPT_MTD_COST <> 0) and a.internal_po_ind ='Y' then 'Internal PO Rcvd'
		else ''
		end as otb_impact
	,sum(a.rcpt_units) over(partition by a.purchase_order_num order by a.month_idnt ROWS UNBOUNDED PRECEDING) as rcpt_u
	,sum(a.rcpt_cost) over(partition by a.purchase_order_num order by a.month_idnt ROWS UNBOUNDED PRECEDING) as rcpt_c
	from po_supp_stg a
	--where a.purchase_order_num = 40190040
	)c
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,60,61,62,63,64,65,66,67
)d
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,60,61,62,63,64,65
;
COLLECT STATS
		PRIMARY INDEX(purchase_order_num,month_idnt)
     	,COLUMN(purchase_order_num)
     	,COLUMN (OTB_MONTH_IDNT)
     	,COLUMN (MONTH_IDNT)
     on T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL_REPORT;
    
--drop table unkown_pos;
CREATE MULTISET VOLATILE TABLE unkown_pos AS(
SELECT 
	a.PORECEIPT_ORDER_NUMBER
	,a.SKU_NUM 
	,b.CHANNEL_NUM
	,b.CHANNEL_DESC
	,c.month_idnt
	,c."Month"
	,c.month_label
	,sum(a.RECEIPTS_COST+a.RECEIPTS_CROSSDOCK_COST) as RCPT_COST
	,sum(a.RECEIPTS_RETAIL+a.RECEIPTS_CROSSDOCK_RETAIL) as RCPT_RETAIL
	,sum(a.RECEIPTS_UNITS+ a.RECEIPTS_CROSSDOCK_UNITS) as RCPT_UNITS
FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a
JOIN po_detail_chnl_loc b
on a.STORE_NUM = b.store_num
join po_dtd_date c
on a.TRAN_DATE = c.day_date
WHERE a.TRAN_CODE = '30'
and a.DROPSHIP_IND = 'N'
group by 1,2,3,4,5,6,7)

WITH DATA
   PRIMARY INDEX(PORECEIPT_ORDER_NUMBER,SKU_NUM)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(PORECEIPT_ORDER_NUMBER,SKU_NUM)
     ,COLUMN(PORECEIPT_ORDER_NUMBER)
     ,COLUMN(SKU_NUM)
     ON unkown_pos;

DELETE FROM T2DL_DAS_OPEN_TO_BUY.UKNOWN_PO_DETAIL;

INSERT INTO T2DL_DAS_OPEN_TO_BUY.UKNOWN_PO_DETAIL 
SELECT
	e.transfer_id
	,e.CHANNEL_NUM
	,e.CHANNEL_DESC
	,e.month_idnt
	,e."Month"
	,e.month_label
	,e.div_num 
	,e.div_desc 
	,e.sdiv_num 
	,e.sdiv_desc
	,e.dept_num 
	,e.dept_desc 
	,e.class_num 
	,e.class_desc 
	,e.sbclass_num 
	,e.sbclass_desc 
	,e.rms_style_num 
	,e.supp_part_num as vpn
	,e.style_desc 
	,e.prmy_supp_num as supp_num
	,f.vendor_name as supp_name
	,e.supp_color 
	,e.color_num 
	,e.color_desc 
	,e.RCPT_COST
	,e.RCPT_RETAIL
	,e.RCPT_UNITS
	,CURRENT_DATE as process_dt 
FROM(
	SELECT 
		c.PORECEIPT_ORDER_NUMBER as transfer_id
		,c.CHANNEL_NUM
		,c.CHANNEL_DESC
		,c.month_idnt
		,c."Month"
		,c.month_label
		,d.div_num 
		,d.div_desc 
		,d.grp_num as sdiv_num 
		,d.grp_desc as sdiv_desc
		,d.dept_num 
		,d.dept_desc 
		,d.class_num 
		,d.class_desc 
		,d.sbclass_num 
		,d.sbclass_desc 
		,d.rms_style_num 
		,d.supp_part_num 
		,d.style_desc 
		,d.prmy_supp_num 
		,d.supp_color 
		,d.color_num 
		,d.color_desc 
		,SUM(c.RCPT_COST) as RCPT_COST
		,SUM(c.RCPT_RETAIL) as RCPT_RETAIL
		,SUM(c.RCPT_UNITS) as RCPT_UNITS
	FROM(
		SELECT
			a.PORECEIPT_ORDER_NUMBER
			,a.SKU_NUM 
			,a.CHANNEL_NUM
			,a.CHANNEL_DESC
			,a.month_idnt
			,a."Month"
			,a.month_label
			,a.RCPT_COST
			,a.RCPT_RETAIL
			,a.RCPT_UNITS
		FROM unkown_pos a
		left JOIN PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_DISTRIBUTELOCATION_FACT b
		ON a.PORECEIPT_ORDER_NUMBER = cast(b.external_distribution_id as varchar(100))
		WHERE b.external_distribution_id is null
		group by 1,2,3,4,5,6,7,8,9,10) c
	JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW d
	ON c.SKU_NUM = d.rms_sku_num
	where d.dept_desc not like '%INACT%'
	and d.channel_country = 'US'
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) e
	JOIN PRD_NAP_USR_VWS.VENDOR_DIM f
	ON e.prmy_supp_num = f.vendor_num;

COLLECT STATS
		PRIMARY INDEX(transfer_id,MONTH_IDNT)
     	,COLUMN(transfer_id)
     	,COLUMN (MONTH_IDNT)
     on T2DL_DAS_OPEN_TO_BUY.UKNOWN_PO_DETAIL;