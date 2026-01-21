--This is the base data for PO data that contains Rpcts$ and OO$ 
--this is for outbound data and is at PO Channel CC week
--we need to look bak 180 days 
--Developed by: Isaac Wallick
--Updated last: 1/19/2024
CREATE MULTISET VOLATILE TABLE po_rcpts_date AS (
	SELECT fiscal_year_num || fiscal_month_num || ' ' || month_abrv AS "Month"
	,month_idnt
	,week_idnt
	,fiscal_year_num
	,quarter_label
	,month_label
	,day_date FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date BETWEEN CURRENT_DATE - 180
		AND CURRENT_DATE GROUP BY 1,2,3,4,5,6,7
	)
WITH DATA 
	PRIMARY INDEX (day_date) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (day_date) ON po_rcpts_date;

--This gets us the dates for the elapsed days in the current week
CREATE MULTISET VOLATILE TABLE po_wtd AS( 
SELECT 
	b.fiscal_year_num || b.fiscal_month_num || ' ' || month_abrv AS "Month"
	,b.month_idnt
	,b.week_idnt
	,b.fiscal_year_num
	,b.quarter_label
	,b.month_label
	,b.day_date
FROM
	(SELECT DISTINCT 
	week_idnt FROM 
	prd_nap_usr_vws.day_cal_454_dim 
	WHERE day_date = CURRENT_DATE) a
JOIN prd_nap_usr_vws.day_cal_454_dim b
on a. week_idnt = b.week_idnt
where b.day_date < CURRENT_DATE 
)

WITH DATA
   PRIMARY INDEX(day_date)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date)
     ,COLUMN(WEEK_IDNT)
     ,COLUMN(MONTH_IDNT)
     ON po_wtd;
    
--this gets us the elapsed days for closed weeks
CREATE MULTISET VOLATILE TABLE po_pm AS(
SELECT 
	b.fiscal_year_num || b.fiscal_month_num || ' ' || month_abrv AS "Month"
	,b.month_idnt
	,b.week_idnt
	,b.fiscal_year_num
	,b.quarter_label
	,b.month_label
	,b.day_date
FROM
	(SELECT DISTINCT 
	week_idnt FROM 
	prd_nap_usr_vws.day_cal_454_dim 
	WHERE day_date between current_date -190 and CURRENT_DATE -7) a
JOIN prd_nap_usr_vws.day_cal_454_dim b
on a. week_idnt = b.week_idnt)

WITH DATA
   PRIMARY INDEX(day_date)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date)
     ,COLUMN(WEEK_IDNT)
     ,COLUMN(MONTH_IDNT)
     ON po_pm;

--only get US locations that are not closed
CREATE MULTISET VOLATILE TABLE po_loc AS (
	SELECT a.STORE_NUM
	,a.CHANNEL_NUM
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND
	,a.CHANNEL_COUNTRY
	,a.CHANNEL_NUM || ',' || a.CHANNEL_DESC AS CHANNEL FROM (
	SELECT STORE_NUM
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,CHANNEL_COUNTRY
	FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
	GROUP BY 1,2,3,4,5
	WHERE CHANNEL_BRAND IN ('Nordstrom','Nordstrom_Rack')
		AND CHANNEL_NUM IN ('110','120','210','250','260','310')
		AND CHANNEL_COUNTRY = 'US'
		AND store_name NOT LIKE 'CLSD%' -- can be found as CLSD or CLOSED
		AND store_name NOT LIKE 'CLOSED%'
	) a GROUP BY 1,2,3,4,5,6
	)
WITH DATA 
	PRIMARY INDEX (STORE_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (STORE_NUM)
	,COLUMN (STORE_NUM) ON po_loc;

--lets get list of pos we need to go back a year on closed POs since this is for outbound and all units may have not yet been rcvd at a astore
--drop table po_base
CREATE MULTISET VOLATILE TABLE po_base AS (
	SELECT a.purchase_order_num
	,a.STATUS
	,a.edi_ind
	,a.start_ship_date
	,a.end_ship_date
	,cast(b.latest_approval_event_tmstp_pacific AS DATE) AS latest_approval_date
	,a.open_to_buy_endofweek_date
	,a.order_type
	,b.internal_po_ind
	,b.npg_ind
	,b.po_type
	,b.purchase_type FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_FACT a LEFT JOIN PRD_NAP_USR_VWS.PURCHASE_ORDER_HEADER_FACT b ON a.purchase_order_num = b.purchase_order_number WHERE (
		(
			a.STATUS IN ('APPROVED','WORKSHEET')
			OR (
				a.STATUS = 'CLOSED'
				AND (a.OPEN_TO_BUY_ENDOFWEEK_DATE >= CURRENT_DATE - 395)
				)
			)
		AND (a.ORIGINAL_APPROVAL_DATE IS NOT NULL)
		AND dropship_ind <> 't'
		)
	)
WITH DATA 
	PRIMARY INDEX (purchase_order_num) 
ON COMMIT PRESERVE ROWS;

--lets get data that has the actual PO nums that have closed weeks and are 180 days in the past
--drop table nt_po
CREATE MULTISET VOLATILE TABLE nt_po_pm AS (
	SELECT d.purchase_order_num
	,d.STATUS
	,d.edi_ind
	,d.start_ship_date
	,d.end_ship_date
	,d.latest_approval_date
	,d.open_to_buy_endofweek_date AS otb_eow_date
	,d.order_type
	,d.internal_po_ind
	,d.npg_ind
	,d.po_type
	,d.purchase_type
	,e."Month"
	,e.month_idnt
	,e.week_idnt
	,e.fiscal_year_num
	,e.quarter_label
	,e.CHANNEL_NUM
	,e.CHANNEL_DESC
	,e.CHANNEL_BRAND
	,e.SKU_NUM
	,sum(e.RCPT_COST) AS RCPT_COST
	,sum(e.RCPT_RETAIL) AS RCPT_RETAIL
	,sum(e.RCPT_UNITS) AS RCPT_UNITS FROM po_base d JOIN (
	SELECT  a.PORECEIPT_ORDER_NUMBER AS PO_NUM
		,b."Month"
		,b.month_idnt
		,b.week_idnt
		,b.fiscal_year_num
		,b.quarter_label
		,c.CHANNEL_NUM
		,c.CHANNEL_DESC
		,c.CHANNEL_BRAND
		,a.SKU_NUM
		,sum(a.RECEIPTS_COST + a.RECEIPTS_CROSSDOCK_COST) AS RCPT_COST
		,sum(a.RECEIPTS_RETAIL + a.RECEIPTS_CROSSDOCK_RETAIL) AS RCPT_RETAIL
		,sum(a.RECEIPTS_UNITS + a.RECEIPTS_CROSSDOCK_UNITS) AS RCPT_UNITS
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a
	JOIN po_pm b ON a.TRAN_DATE = b.day_date
	JOIN po_loc c ON a.STORE_NUM = c.STORE_NUM
	WHERE a.DROPSHIP_IND = 'N'
	AND a.TRAN_CODE = '20'
	GROUP BY 1,2,3,4,5,6,7,8,9,10
	) e ON d.purchase_order_num = e.PO_NUM
	--where e.PO_NUM is null
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON nt_po_pm;

-- this gets us the backposting rcpt $s and units for wtd for actual po #
CREATE MULTISET VOLATILE TABLE nt_po_wtd AS (
	SELECT d.purchase_order_num
	,d.STATUS
	,d.edi_ind
	,d.start_ship_date
	,d.end_ship_date
	,d.latest_approval_date
	,d.open_to_buy_endofweek_date AS otb_eow_date
	,d.order_type
	,d.internal_po_ind
	,d.npg_ind
	,d.po_type
	,d.purchase_type
	,(select distinct "Month" from po_wtd) as "Month"
	,(select distinct month_idnt from po_wtd) as month_idnt
	,(select distinct week_idnt from po_wtd) as week_idnt
	,(select distinct fiscal_year_num from po_wtd) as fiscal_year_num
	,(select distinct quarter_label from po_wtd) as quarter_label
	,e.CHANNEL_NUM
	,e.CHANNEL_DESC
	,e.CHANNEL_BRAND
	,e.SKU_NUM
	,sum(e.RCPT_COST) AS RCPT_COST
	,sum(e.RCPT_RETAIL) AS RCPT_RETAIL
	,sum(e.RCPT_UNITS) AS RCPT_UNITS FROM po_base d JOIN (
	SELECT  a.PORECEIPT_ORDER_NUMBER AS PO_NUM
		,c.CHANNEL_NUM
		,c.CHANNEL_DESC
		,c.CHANNEL_BRAND
		,a.SKU_NUM
		,sum(a.RECEIPTS_COST + a.RECEIPTS_CROSSDOCK_COST) AS RCPT_COST
		,sum(a.RECEIPTS_RETAIL + a.RECEIPTS_CROSSDOCK_RETAIL) AS RCPT_RETAIL
		,sum(a.RECEIPTS_UNITS + a.RECEIPTS_CROSSDOCK_UNITS) AS RCPT_UNITS
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a
	JOIN po_loc c ON a.STORE_NUM = c.STORE_NUM
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_HIST psd
ON psd.rms_sku_num = A.SKU_NUM
AND c.CHANNEL_COUNTRY = psd.CHANNEL_COUNTRY
AND A.TRAN_DATE >= psd.eff_begin_tmstp
AND A.TRAN_DATE <  psd.eff_end_tmstp
	WHERE a.DROPSHIP_IND = 'N'
	AND a.TRAN_CODE = '20'
	AND
             (
               (
                 A.TRAN_DATE BETWEEN
                   (
                     SELECT MIN(day_date) FROM po_wtd
                   )
                             AND
                   (
                      SELECT MAX(day_date) FROM po_wtd
                   )
               )
              OR
               (
                 CAST(A.EVENT_TIME AS DATE AT SOURCE TIME ZONE) BETWEEN
                   (
                     SELECT MIN(day_date) FROM po_wtd
                   )
                             AND
                   (
                     SELECT MAX(day_date) FROM po_wtd
                   )
               )
             )
	GROUP BY 1,2,3,4,5
	) e ON d.purchase_order_num = e.PO_NUM
	--where e.PO_NUM is null
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON nt_po_wtd;

--now lets combine the closed weeks and the wtd data
CREATE MULTISET VOLATILE TABLE nt_po_rcpts AS (
	SELECT a.purchase_order_num
	,a.STATUS
	,a.edi_ind
	,a.start_ship_date
	,a.end_ship_date
	,a.latest_approval_date
	,a.otb_eow_date
	,a.order_type
	,a.internal_po_ind
	,a.npg_ind
	,a.po_type
	,a.purchase_type
	,a."Month"
	,a.month_idnt
	,a.week_idnt
	,a.fiscal_year_num
	,a.quarter_label
	,a.CHANNEL_NUM
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND
	,a.SKU_NUM
	,sum(a.RCPT_COST) AS RCPT_COST
	,sum(a.RCPT_RETAIL) AS RCPT_RETAIL
	,sum(a.RCPT_UNITS) AS RCPT_UNITS FROM (
	SELECT purchase_order_num
		,STATUS
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,RCPT_COST
		,RCPT_RETAIL
		,RCPT_UNITS
	FROM nt_po_pm
	
	UNION ALL
	
	SELECT purchase_order_num
		,STATUS
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,RCPT_COST
		,RCPT_RETAIL
		,RCPT_UNITS
	FROM nt_po_wtd
	) a GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
	)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON nt_po_rcpts;

--since a po rcpt num can be a transfer Id we need to isolate those for closed weeks
CREATE MULTISET VOLATILE TABLE trans_po_pm AS (
	SELECT a.PORECEIPT_ORDER_NUMBER AS TRANSFER_ID
	,b."Month"
	,b.month_idnt
	,b.week_idnt
	,b.fiscal_year_num
	,b.quarter_label
	,c.CHANNEL_NUM
	,c.CHANNEL_DESC
	,c.CHANNEL_BRAND
	,a.SKU_NUM
	,a.STORE_NUM
	,sum(a.RECEIPTS_COST + a.RECEIPTS_CROSSDOCK_COST) AS RCPT_COST
	,sum(a.RECEIPTS_RETAIL + a.RECEIPTS_CROSSDOCK_RETAIL) AS RCPT_RETAIL
	,sum(a.RECEIPTS_UNITS + a.RECEIPTS_CROSSDOCK_UNITS) AS RCPT_UNITS 
FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a 
JOIN po_pm b 
ON a.TRAN_DATE = b.day_date 
JOIN po_loc c 
ON a.STORE_NUM = c.STORE_NUM 
WHERE a.DROPSHIP_IND = 'N'
AND a.TRAN_CODE = '30' GROUP BY 1,2,3,4,5,6,7,8,9,10,11)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON trans_po_pm;

--we do that same thing here but for current week
CREATE MULTISET VOLATILE TABLE trans_po_wtd AS (
	SELECT a.PORECEIPT_ORDER_NUMBER AS TRANSFER_ID
	,(select distinct "Month" from po_wtd) as "Month"
	,(select distinct month_idnt from po_wtd) as month_idnt
	,(select distinct week_idnt from po_wtd) as week_idnt
	,(select distinct fiscal_year_num from po_wtd) as fiscal_year_num
	,(select distinct quarter_label from po_wtd) as quarter_label
	,c.CHANNEL_NUM
	,c.CHANNEL_DESC
	,c.CHANNEL_BRAND
	,a.SKU_NUM
	,a.STORE_NUM
	,sum(a.RECEIPTS_COST + a.RECEIPTS_CROSSDOCK_COST) AS RCPT_COST
	,sum(a.RECEIPTS_RETAIL + a.RECEIPTS_CROSSDOCK_RETAIL) AS RCPT_RETAIL
	,sum(a.RECEIPTS_UNITS + a.RECEIPTS_CROSSDOCK_UNITS) AS RCPT_UNITS 
FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a 
JOIN po_loc c 
ON a.STORE_NUM = c.STORE_NUM 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_HIST psd
ON psd.rms_sku_num = A.SKU_NUM
AND c.CHANNEL_COUNTRY = psd.CHANNEL_COUNTRY
AND A.TRAN_DATE >= psd.eff_begin_tmstp
AND A.TRAN_DATE <  psd.eff_end_tmstp
WHERE a.DROPSHIP_IND = 'N'
AND a.TRAN_CODE = '30' 
AND
             (
               (
                 A.TRAN_DATE BETWEEN
                   (
                     SELECT MIN(day_date) FROM po_wtd
                   )
                             AND
                   (
                      SELECT MAX(day_date) FROM po_wtd
                   )
               )
              OR
               (
                 CAST(A.EVENT_TIME AS DATE AT SOURCE TIME ZONE) BETWEEN
                   (
                     SELECT MIN(day_date) FROM po_wtd
                   )
                             AND
                   (
                     SELECT MAX(day_date) FROM po_wtd
                   )
               )
             )

GROUP BY 1,2,3,4,5,6,7,8,9,10,11)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON trans_po_wtd;

--now we combine all the trnasfer id data
CREATE MULTISET VOLATILE TABLE trans_po AS (
SELECT a.TRANSFER_ID
	,a."Month"
	,a.month_idnt
	,a.week_idnt
	,a.fiscal_year_num
	,a.quarter_label
	,a.CHANNEL_NUM
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND
	,a.SKU_NUM
	,a.STORE_NUM
	,sum(a.RCPT_COST) AS RCPT_COST
	,sum(a.RCPT_RETAIL) AS RCPT_RETAIL
	,sum(a.RCPT_UNITS) AS RCPT_UNITS FROM (
	SELECT TRANSFER_ID
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,STORE_NUM
		,RCPT_COST
		,RCPT_RETAIL
		,RCPT_UNITS
	FROM trans_po_pm
	
	UNION ALL
	
	SELECT TRANSFER_ID
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,STORE_NUM
		,RCPT_COST
		,RCPT_RETAIL
		,RCPT_UNITS
	FROM trans_po_wtd
	) a GROUP BY 1,2,3,4,5,6,7,8,9,10,11
	)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON trans_po;

-- since we dont have po number we need to join to a distribution table to get the po that is associated to the original PO
CREATE MULTISET VOLATILE TABLE trans_po_final AS (
	SELECT d.purchase_order_num
	,d.STATUS
	,d.edi_ind
	,d.start_ship_date
	,d.end_ship_date
	,d.latest_approval_date
	,d.open_to_buy_endofweek_date AS otb_eow_date
	,d.order_type
	,d.internal_po_ind
	,d.npg_ind
	,d.po_type
	,d.purchase_type
	,e."Month"
	,e.month_idnt
	,e.week_idnt
	,e.fiscal_year_num
	,e.quarter_label
	,e.CHANNEL_NUM
	,e.CHANNEL_DESC
	,e.CHANNEL_BRAND
	,e.SKU_NUM
	,sum(e.RCPT_COST) AS RCPT_COST
	,sum(e.RCPT_RETAIL) AS RCPT_RETAIL
	,sum(e.RCPT_UNITS) AS RCPT_UNITS FROM po_base d JOIN (
	SELECT c.purchase_order_num AS po_num
		,d."Month"
		,d.month_idnt
		,d.week_idnt
		,d.fiscal_year_num
		,d.quarter_label
		,d.CHANNEL_NUM
		,d.CHANNEL_DESC
		,d.CHANNEL_BRAND
		,d.SKU_NUM
		,sum(d.RCPT_COST) AS RCPT_COST
		,sum(d.RCPT_RETAIL) AS RCPT_RETAIL
		,sum(d.RCPT_UNITS) AS RCPT_UNITS
	FROM (
		SELECT a.purchase_order_num
			,b.TRANSFER_ID
			,b.SKU_NUM
			,b.STORE_NUM
		FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_DISTRIBUTELOCATION_FACT a
		JOIN trans_po b ON cast(a.external_distribution_id AS VARCHAR(100)) = b.TRANSFER_ID
			AND a.rms_sku_num = b.sku_num
			AND a.distribute_location_id = b.store_num
		GROUP BY 1,2,3,4
		) c
	JOIN trans_po d ON c.TRANSFER_ID = d.TRANSFER_ID
		AND c.sku_num = d.sku_num
		AND c.store_num = d.store_num
	GROUP BY 1,2,3,4,5,6,7,8,9,10
	) e ON d.purchase_order_num = e.PO_NUM 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON trans_po_final;

-- combine the PO data and the transfer id data to get our full rcpts data
CREATE MULTISET VOLATILE TABLE po_rcpts AS (
	SELECT a.purchase_order_num
	,a.STATUS
	,a.edi_ind
	,a.start_ship_date
	,a.end_ship_date
	,a.latest_approval_date
	,a.otb_eow_date
	,a.order_type
	,a.internal_po_ind
	,a.npg_ind
	,a.po_type
	,a.purchase_type
	,a."Month"
	,a.month_idnt
	,a.week_idnt
	,a.fiscal_year_num
	,a.quarter_label
	,a.CHANNEL_NUM
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND
	,a.SKU_NUM
	,sum(a.RCPT_COST) AS RCPT_COST
	,sum(a.RCPT_RETAIL) AS RCPT_RETAIL
	,sum(a.RCPT_UNITS) AS RCPT_UNITS FROM (
	SELECT purchase_order_num
		,STATUS
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,RCPT_COST
		,RCPT_RETAIL
		,RCPT_UNITS
	FROM trans_po_final
	
	UNION ALL
	
	SELECT purchase_order_num
		,STATUS
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,RCPT_COST
		,RCPT_RETAIL
		,RCPT_UNITS
	FROM nt_po_rcpts
	) a GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
	)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON po_rcpts;

--this gets us our OO data using the mfp carry forward logic
CREATE MULTISET VOLATILE TABLE po_oo_final AS (
	SELECT g.purchase_order_num
	,g.STATUS
	,g.edi_ind
	,g.start_ship_date
	,g.end_ship_date
	,g.latest_approval_date
	,g.open_to_buy_endofweek_date AS otb_eow_date
	,g.order_type
	,g.internal_po_ind
	,g.npg_ind
	,g.po_type
	,g.purchase_type
	,f."Month"
	,f.month_idnt
	,f.week_num AS week_idnt
	,f.fiscal_year_num
	,f.quarter_label
	,f.CHANNEL_NUM
	,f.CHANNEL_DESC
	,f.CHANNEL_BRAND
	,f.RMS_SKU_NUM AS SKU_NUM
	,max(f.UNIT_COST_AMT) as UNIT_COST_AMT
	,sum(f.QUANTITY_ORDERED) AS QUANTITY_ORDERED
	,sum(f.QUANTITY_RECEIVED) AS QUANTITY_RECEIVED
	,sum(f.QUANTITY_CANCELED) AS QUANTITY_CANCELED
	,sum(f.QUANTITY_OPEN) AS QUANTITY_OPEN
	,sum(f.TOTAL_ESTIMATED_LANDING_COST) AS TOTAL_ESTIMATED_LANDING_COST
	,sum(f.TOTAL_ANTICIPATED_RETAIL_AMT) AS TOTAL_ANTICIPATED_RETAIL_AMT FROM po_base g JOIN (
	SELECT e.PO_NUM
		,e.RMS_SKU_NUM
		,e.CHANNEL_NUM
		,e.CHANNEL_DESC
		,e.CHANNEL_BRAND
		,e.STORE_NUM
		,d.fiscal_year_num || d.fiscal_month_num || ' ' || d.month_abrv AS "Month"
		,d.month_idnt
		,d.fiscal_year_num
		,d.quarter_label
		,d.month_label
		,e.WEEK_NUM
		,e.UNIT_COST_AMT
		,sum(e.QUANTITY_ORDERED) AS QUANTITY_ORDERED
		,sum(e.QUANTITY_RECEIVED) AS QUANTITY_RECEIVED
		,sum(e.QUANTITY_CANCELED) AS QUANTITY_CANCELED
		,sum(e.QUANTITY_OPEN) AS QUANTITY_OPEN
		,sum(e.TOTAL_ESTIMATED_LANDING_COST) AS TOTAL_ESTIMATED_LANDING_COST
		,sum(e.TOTAL_ANTICIPATED_RETAIL_AMT) AS TOTAL_ANTICIPATED_RETAIL_AMT
	FROM (
		SELECT c.PURCHASE_ORDER_NUMBER AS PO_NUM
			,c.RMS_SKU_NUM
			,c.RMS_CASEPACK_NUM
			,c.CHANNEL_NUM
			,c.CHANNEL_DESC
			,c.CHANNEL_BRAND
			,c.STORE_NUM
			,CASE 
				WHEN c.WEEK_NUM < c.CURR_WK_IDNT
					THEN c.CURR_WK_IDNT
				ELSE c.WEEK_NUM
				END AS WEEK_NUM
			,c.ORDER_TYPE
			,c.LATEST_APPROVAL_DATE
			,c.QUANTITY_ORDERED
			,c.QUANTITY_RECEIVED
			,c.QUANTITY_CANCELED
			,c.QUANTITY_OPEN
			,c.UNIT_COST_AMT
			,c.TOTAL_ESTIMATED_LANDING_COST
			,c.TOTAL_ANTICIPATED_RETAIL_AMT
		FROM (
			SELECT a.PURCHASE_ORDER_NUMBER
				,a.RMS_SKU_NUM
				,a.RMS_CASEPACK_NUM
				,b.CHANNEL_NUM
				,b.CHANNEL_DESC
				,b.CHANNEL_BRAND
				,a.STORE_NUM
				,a.WEEK_NUM
				,(
					SELECT WEEK_IDNT
					FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
					WHERE DAY_DATE = CURRENT_DATE
					) AS CURR_WK_IDNT
				,a.ORDER_TYPE
				,a.LATEST_APPROVAL_DATE
				,a.QUANTITY_ORDERED
				,a.QUANTITY_RECEIVED
				,a.QUANTITY_CANCELED
				,a.QUANTITY_OPEN
				,a.UNIT_COST_AMT
				,a.TOTAL_ESTIMATED_LANDING_COST
				,a.TOTAL_ANTICIPATED_RETAIL_AMT
			FROM PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW a
			JOIN po_loc b ON a.STORE_NUM = b.STORE_NUM
			WHERE a.QUANTITY_OPEN > 0
				AND a.FIRST_APPROVAL_DATE IS NOT NULL
				AND (
					(
						a.STATUS = 'CLOSED'
						AND a.END_SHIP_DATE >= CURRENT_DATE - 45
						)
					OR a.STATUS IN ('APPROVED','WORKSHEET')
					)
				--and a.PURCHASE_ORDER_NUMBER='36998137'
			) c
		) e
	JOIN (
		SELECT week_idnt
			,month_idnt
			,fiscal_year_num
			,fiscal_month_num
			,month_abrv
			,quarter_label
			,month_label
		FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
		GROUP BY 1,2,3,4,5,6,7
		) d ON e.WEEK_NUM = d.week_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
	) f ON g.purchase_order_num = f.PO_NUM GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
	)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON po_oo_final;

-- Lets bring rcpts and oo together
--drop table po_rcpt_oo_final
CREATE MULTISET VOLATILE TABLE po_rcpt_oo_final AS (
	SELECT a.purchase_order_num
	,STATUS
	,edi_ind
	,start_ship_date
	,end_ship_date
	,latest_approval_date
	,otb_eow_date
	,order_type
	,internal_po_ind
	,npg_ind
	,po_type
	,purchase_type
	,"Month"
	,month_idnt
	,week_idnt
	,fiscal_year_num
	,quarter_label
	,CHANNEL_NUM
	,CHANNEL_DESC
	,CHANNEL_BRAND
	,SKU_NUM
	,max(UNIT_COST_AMT) as UNIT_COST_AMT
	,sum(RCPT_COST) AS RCPT_COST
	,sum(RCPT_RETAIL) AS RCPT_RETAIL
	,sum(RCPT_UNITS) AS RCPT_UNITS
	,sum(QUANTITY_ORDERED) AS QUANTITY_ORDERED
	,sum(QUANTITY_RECEIVED) AS QUANTITY_RECEIVED
	,sum(QUANTITY_CANCELED) AS QUANTITY_CANCELED
	,sum(QUANTITY_OPEN) AS QUANTITY_OPEN
	,sum(TOTAL_ESTIMATED_LANDING_COST) AS TOTAL_ESTIMATED_LANDING_COST
	,sum(TOTAL_ANTICIPATED_RETAIL_AMT) AS TOTAL_ANTICIPATED_RETAIL_AMT FROM (
	SELECT purchase_order_num
		,STATUS
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,CAST(RCPT_COST AS DECIMAL(20, 4)) AS RCPT_COST
		,CAST(RCPT_RETAIL AS DECIMAL(20, 4)) AS RCPT_RETAIL
		,CAST(RCPT_UNITS AS DECIMAL(20, 4)) AS RCPT_UNITS
		,CAST(0 AS DECIMAL(20, 4)) AS QUANTITY_ORDERED
		,CAST(0 AS DECIMAL(20, 4)) AS QUANTITY_RECEIVED
		,CAST(0 AS DECIMAL(20, 4)) AS QUANTITY_CANCELED
		,CAST(0 AS DECIMAL(20, 4)) AS QUANTITY_OPEN
		,CAST(0 AS DECIMAL(20, 4)) AS UNIT_COST_AMT
		,CAST(0 AS DECIMAL(20, 4)) AS TOTAL_ESTIMATED_LANDING_COST
		,CAST(0 AS DECIMAL(20, 4)) AS TOTAL_ANTICIPATED_RETAIL_AMT
	FROM po_rcpts
	
	UNION ALL
	
	SELECT purchase_order_num
		,STATUS
		,edi_ind
		,start_ship_date
		,end_ship_date
		,latest_approval_date
		,otb_eow_date
		,order_type
		,internal_po_ind
		,npg_ind
		,po_type
		,purchase_type
		,"Month"
		,month_idnt
		,week_idnt
		,fiscal_year_num
		,quarter_label
		,CHANNEL_NUM
		,CHANNEL_DESC
		,CHANNEL_BRAND
		,SKU_NUM
		,CAST(0 AS DECIMAL(20, 4)) asRCPT_COST
		,CAST(0 AS DECIMAL(20, 4)) AS RCPT_RETAIL
		,CAST(0 AS DECIMAL(20, 4)) AS RCPT_UNITS
		,CAST(QUANTITY_ORDERED AS DECIMAL(20, 4)) AS QUANTITY_ORDERED
		,CAST(QUANTITY_RECEIVED AS DECIMAL(20, 4)) AS QUANTITY_RECEIVED
		,CAST(QUANTITY_CANCELED AS DECIMAL(20, 4)) AS QUANTITY_CANCELED
		,CAST(QUANTITY_OPEN AS DECIMAL(20, 4)) AS QUANTITY_OPEN
		,CAST(UNIT_COST_AMT AS DECIMAL(20, 4)) AS UNIT_COST_AMT
		,CAST(TOTAL_ESTIMATED_LANDING_COST AS DECIMAL(20, 4)) AS TOTAL_ESTIMATED_LANDING_COST
		,CAST(TOTAL_ANTICIPATED_RETAIL_AMT AS DECIMAL(20, 4)) AS TOTAL_ANTICIPATED_RETAIL_AMT
	FROM po_oo_final
	) a
	--where purchase_order_num = '39992571'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
	)
WITH DATA 
	PRIMARY INDEX (SKU_NUM) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (SKU_NUM)
	,COLUMN (SKU_NUM) ON po_rcpt_oo_final;

--This gets the total approved qty, cost and retail associated to a PO
--drop table po_ttl_order
CREATE MULTISET VOLATILE TABLE po_ttl_order AS (
select 
	a.* 
	,d.ttl_approved_qty
	,d.ttl_approved_c
	,e.ttl_approved_r
from po_rcpt_oo_final a
left join (SELECT c.purchase_order_number
			,SUM(c.approved_quantity_ordered) AS ttl_approved_qty
			,SUM((c.unit_cost * c.quantity_ordered) + (c.total_expenses_per_unit * c.quantity_ordered)+(c.total_duty_per_unit * c.quantity_ordered)) ttl_approved_c
		  FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_SHIPLOCATION_FACT c
		  group by 1) d
on a.purchase_order_num = d.purchase_order_number
left join (select purchase_order_num 
			,sum(ordered_qty*unit_retail_amt) as ttl_approved_r
		   from PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT 
           --where purchase_order_num = 27105588
           group by 1 ) e
on a.purchase_order_num = e.purchase_order_num
--where a.purchase_order_num = '25272105'
)
WITH DATA 
	PRIMARY INDEX (purchase_order_num,sku_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num,sku_num)
	,COLUMN (purchase_order_num) 
	,COLUMN (sku_num)
ON po_ttl_order;

-- here we get everything that has been rcvd at a dc at a PO level
CREATE MULTISET VOLATILE TABLE po_dc_yesterday AS (
	SELECT rec.purchase_order_number
	,sum(rec.received_qty) AS dc_received_qty
	,sum(rec.received_qty * g.unit_cost) AS dc_received_c FROM (
	SELECT e.purchase_order_number AS purchase_order_number
		,e.rms_sku_num
		,cast(e.location_id AS VARCHAR(10)) AS ship_location_id
		,e.shipment_qty AS received_qty
		,cast(e.received_tmstp AS DATE) AS receipt_date
	FROM PRD_NAP_USR_VWS.WM_INBOUND_CARTON_FACT_VW e
	JOIN (
		SELECT purchase_order_num
		FROM po_base
		GROUP BY 1
		) f ON e.purchase_order_number = f.purchase_order_num
	WHERE ship_location_id NOT IN ('889','859','896','869','891','868')
		AND (
			rms_sku_num IS NULL
			OR rms_sku_num IS NOT NULL
			)
	) rec 
	LEFT JOIN PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_FACT g 
	ON rec.purchase_order_number = g.purchase_order_number
	AND rec.rms_sku_num = g.rms_sku_num
	JOIN po_rcpts_date h
	on rec.receipt_date = h.day_date
	--WHERE rec.purchase_order_number = '39648347'
	GROUP BY 1
	)
WITH DATA 
	PRIMARY INDEX (purchase_order_number) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_number)
	,COLUMN (purchase_order_number) ON po_dc_yesterday;

--lets get the latest date items on a po were rcvd at a dc
--drop table po_dc_rcpt_date
CREATE MULTISET VOLATILE TABLE po_dc_rcpt_date AS (
	SELECT rec.purchase_order_number
	,max(receipt_date) as receipt_date FROM (
	SELECT e.purchase_order_number AS purchase_order_number
		,e.rms_sku_num
		,cast(max(e.received_tmstp) AS DATE) AS receipt_date
	FROM PRD_NAP_USR_VWS.WM_INBOUND_CARTON_FACT_VW e
	JOIN (
		SELECT purchase_order_num
		FROM po_base
		GROUP BY 1
		) f ON e.purchase_order_number = f.purchase_order_num
	WHERE e.location_id NOT IN ('889','859','896','869','891','868')
		AND (
			rms_sku_num IS NULL
			OR trycast(rms_sku_num AS INTEGER) IS NOT NULL
			)
	--and e.purchase_order_number = '39870694'
	GROUP BY 1,2
	) rec
	GROUP BY 1
	)
	WITH DATA PRIMARY INDEX (purchase_order_number) ON

COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_number)
	,COLUMN (purchase_order_number) ON po_dc_rcpt_date;

--lets get the latest carrier and supplier ship dates
CREATE MULTISET VOLATILE TABLE po_dc_inbound_date AS (
	SELECT c.purchase_order_num
	,c.last_carrier_asn_date
	,c.last_supplier_asn_date
	,CASE 
		WHEN c.last_carrier_asn_date > c.last_supplier_asn_date
			THEN c.last_carrier_asn_date
		ELSE c.last_supplier_asn_date
		END AS last_ship_activity_date FROM (
	SELECT a.purchase_order_num
		,MAX(a.eta_date) AS last_carrier_asn_date
		,MAX(a.vendor_ship_date) AS last_supplier_asn_date
	FROM PRD_NAP_USR_VWS.CARRIER_ASN_FACT a
	JOIN (
		SELECT purchase_order_num
		FROM po_base
		GROUP BY 1
		) b ON a.purchase_order_num = b.purchase_order_num
	GROUP BY 1
	) c
	)
WITH DATA 
	PRIMARY INDEX (purchase_order_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num)
	,COLUMN (purchase_order_num) ON po_dc_inbound_date;

--lets get an idea of when an item on a po was rcvd to a store
CREATE MULTISET VOLATILE TABLE po_dc_outbound_date AS (
	SELECT f.po_num
	,f.sku_num
	,f.CHANNEL_NUM
	,max(f.tran_date) AS last_outbound_activity_date FROM (
	SELECT a.PORECEIPT_ORDER_NUMBER AS po_num
		,a.SKU_NUM
		,c.CHANNEL_NUM
		,a.TRAN_DATE
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a
	JOIN po_rcpts_date b ON a.TRAN_DATE = b.day_date
	JOIN po_loc c ON a.STORE_NUM = c.STORE_NUM
	WHERE a.DROPSHIP_IND = 'N'
		AND a.TRAN_CODE = '20'
	GROUP BY 1,2,3,4
	
	UNION ALL
	
	SELECT e.purchase_order_num AS po_num
		,d.SKU_NUM
		,d.CHANNEL_NUM
		,d.TRAN_DATE
	FROM (
		SELECT a.PORECEIPT_ORDER_NUMBER
			,a.SKU_NUM
			,c.store_num
			,c.channel_num
			,a.TRAN_DATE
		FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a
		JOIN po_rcpts_date b ON a.TRAN_DATE = b.day_date
		JOIN po_loc c ON a.STORE_NUM = c.STORE_NUM
		WHERE a.DROPSHIP_IND = 'N'
			AND a.TRAN_CODE = '30'
		GROUP BY 1,2,3,4,5
		) d
	JOIN PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_DISTRIBUTELOCATION_FACT e ON d.PORECEIPT_ORDER_NUMBER = cast(e.external_distribution_id AS VARCHAR(100))
		AND d.sku_num = e.rms_sku_num
		AND d.store_num = e.distribute_location_id
	GROUP BY 1,2,3,4
	) f
	GROUP BY 1,2,3
	)
WITH DATA 
	PRIMARY INDEX (po_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (po_num)
	,COLUMN (po_num) ON po_dc_outbound_date;

---bring everything together
--drop table po_detail_stg	  
CREATE MULTISET VOLATILE TABLE po_detail AS (
	SELECT a.*
	,dc_received_qty
	,dc_received_c
	,last_carrier_asn_date
	,last_supplier_asn_date
	,last_ship_activity_date
	,receipt_date
	,last_outbound_activity_date 
	FROM po_ttl_order a 
	LEFT JOIN po_dc_yesterday b 
	ON a.purchase_order_num = b.purchase_order_number
	LEFT JOIN po_dc_rcpt_date c 
	ON a.purchase_order_num = c.purchase_order_number 
	LEFT JOIN po_dc_inbound_date d 
	ON a.purchase_order_num = d.purchase_order_num 
	LEFT JOIN po_dc_outbound_date e 
	ON a.purchase_order_num = e.po_num
	AND a.sku_num = e.sku_num
	AND a.channel_num = e.channel_num
	)
WITH DATA 
	PRIMARY INDEX (purchase_order_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num)
	,COLUMN (purchase_order_num) ON po_detail;

--lets stage the above data and and some heir
CREATE MULTISET VOLATILE TABLE po_detail_stg AS (
SELECT g.purchase_order_num 
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
	,g.supp_num
	,g.supp_name
	,h.vendor_name AS manufacturer_name
	,g.vpn
	,g.style_desc
	,g.color_num
	,g.supp_color
	,g.customer_choice
	,g.epm_choice_num
	,g.STATUS
	,g.edi_ind
	,g.start_ship_date
	,g.end_ship_date
	,g.latest_approval_date
	,g.last_carrier_asn_date
	,g.last_supplier_asn_date
	,g.last_ship_activity_date
	,g.last_outbound_activity_date
	,g.receipt_date
	,g.otb_eow_date
	,g.otb_month
	,g.otb_month_idnt
	,g.order_type
	,g.internal_po_ind
	,g.npg_ind
	,g.po_type
	,g.purchase_type
	,g."Month"
	,g.month_idnt
	,g.week_idnt
	,g.fiscal_year_num
	,g.quarter_label
	,g.RCPT_COST
	,g.RCPT_RETAIL
	,g.RCPT_UNITS
	,g.QUANTITY_ORDERED
	,g.QUANTITY_RECEIVED
	,g.QUANTITY_CANCELED
	,g.QUANTITY_OPEN
	,g.UNIT_COST_AMT
	,g.TOTAL_ESTIMATED_LANDING_COST
	,g.TOTAL_ANTICIPATED_RETAIL_AMT
	,g.ttl_approved_qty
	,g.ttl_approved_c
	,g.ttl_approved_r
	,g.dc_received_qty
	,g.dc_received_c
	,current_date as process_dt
FROM (
	SELECT d.purchase_order_num
		,d.CHANNEL_NUM
		,d.CHANNEL_DESC
		,d.CHANNEL_BRAND
		,e.division_num
		,e.division_name
		,e.subdivision_num
		,e.subdivision_name
		,d.dept_num
		,e.dept_name
		,d.class_num
		,d.class_desc
		,d.sbclass_num
		,d.sbclass_desc
		,d.prmy_supp_num AS supp_num
		,f.vendor_name AS supp_name
		,d.supp_part_num AS vpn
		,d.manufacturer_num
		,d.style_desc
		,d.color_num
		,d.supp_color
		,d.customer_choice
		,d.epm_choice_num
		,d.STATUS
		,d.edi_ind
		,d.start_ship_date
		,d.end_ship_date
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
		,d.week_idnt
		,d.fiscal_year_num
		,d.quarter_label
		,d.ttl_approved_qty
		,d.ttl_approved_c
		,d.ttl_approved_r
		,d.dc_received_qty
		,d.dc_received_c
		,max(d.UNIT_COST_AMT) as UNIT_COST_AMT
		,max(d.latest_approval_date) AS latest_approval_date
		,max(d.last_carrier_asn_date) AS last_carrier_asn_date
		,max(d.last_supplier_asn_date) AS last_supplier_asn_date
		,max(d.last_ship_activity_date) AS last_ship_activity_date
		,max(d.last_outbound_activity_date) AS last_outbound_activity_date
		,max(d.receipt_date) AS receipt_date
		,sum(d.RCPT_COST) AS RCPT_COST
		,sum(d.RCPT_RETAIL) AS RCPT_RETAIL
		,sum(d.RCPT_UNITS) AS RCPT_UNITS
		,sum(d.QUANTITY_ORDERED) AS QUANTITY_ORDERED
		,sum(d.QUANTITY_RECEIVED) AS QUANTITY_RECEIVED
		,sum(d.QUANTITY_CANCELED) AS QUANTITY_CANCELED
		,sum(d.QUANTITY_OPEN) AS QUANTITY_OPEN
		,sum(d.TOTAL_ESTIMATED_LANDING_COST) AS TOTAL_ESTIMATED_LANDING_COST
		,sum(d.TOTAL_ANTICIPATED_RETAIL_AMT) AS TOTAL_ANTICIPATED_RETAIL_AMT
	FROM (
		SELECT a.purchase_order_num
			,a.sku_num
			,a.CHANNEL_NUM
			,a.CHANNEL_DESC
			,a.CHANNEL_BRAND
			,b.dept_num
			,b.class_num
			,b.class_desc
			,b.sbclass_num
			,b.sbclass_desc
			,b.prmy_supp_num
			,b.supp_part_num
			,b.manufacturer_num
			,b.style_desc
			,b.color_num
			,b.supp_color
			,TRIM(COALESCE(b.dept_num, 'UNKNOWN') || '_' || COALESCE(b.prmy_supp_num, 'UNKNOWN') || '_' || COALESCE(b.supp_part_num, 'UNKNOWN') || '_' || COALESCE(b.color_num, 'UNKNOWN')) AS customer_choice
			,b.epm_choice_num
			,a.STATUS
			,a.edi_ind
			,a.start_ship_date
			,a.end_ship_date
			,a.latest_approval_date
			,a.otb_eow_date
			,c.fiscal_year_num || c.fiscal_month_num || ' ' || c.month_abrv AS otb_month
			,c.month_idnt AS otb_month_idnt
			,a.order_type
			,a.internal_po_ind
			,a.last_carrier_asn_date
			,a.last_supplier_asn_date
			,a.last_ship_activity_date
			,a.last_outbound_activity_date
			,a.receipt_date
			,a.npg_ind
			,a.po_type
			,a.purchase_type
			,a."Month"
			,a.month_idnt
			,a.week_idnt
			,a.fiscal_year_num
			,a.quarter_label
			,a.RCPT_COST
			,a.RCPT_RETAIL
			,a.RCPT_UNITS
			,a.QUANTITY_ORDERED
			,a.QUANTITY_RECEIVED
			,a.QUANTITY_CANCELED
			,a.QUANTITY_OPEN
			,a.UNIT_COST_AMT
			,a.TOTAL_ESTIMATED_LANDING_COST
			,a.TOTAL_ANTICIPATED_RETAIL_AMT
			,a.ttl_approved_qty
			,a.ttl_approved_c
			,a.ttl_approved_r
			,a.dc_received_qty
			,a.dc_received_c
		FROM po_detail a
		JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW b ON a.SKU_NUM = b.rms_sku_num
		JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c ON a.otb_eow_date = c.day_date
		WHERE b.dept_desc NOT LIKE '%INACT%'
			AND b.channel_country = 'US'
		) d
	JOIN PRD_NAP_USR_VWS.DEPARTMENT_DIM e ON d.dept_num = e.dept_num
	JOIN PRD_NAP_USR_VWS.VENDOR_DIM f ON d.prmy_supp_num = f.vendor_num
	--where d.purchase_order_num = '39648347'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45
	) g
LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM h ON g.manufacturer_num = h.vendor_num)
WITH DATA 
	PRIMARY INDEX (purchase_order_num,customer_choice) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num,customer_choice)
	,COLUMN (purchase_order_num) 
	,COLUMN (customer_choice)
ON po_detail_stg;

--get the latest vasn signal and inventory associated to it
--drop table po_vasn
CREATE MULTISET VOLATILE TABLE po_vasn AS (
select 
	g.purchase_order_num
	,g.customer_choice
	,max(g.vendor_ship_dt) as last_vasn_signal
	,sum(g.vasn_sku_qty) as vasn_sku_qty
	,sum((h.unit_cost * g.vasn_sku_qty) + (h.total_expenses_per_unit * g.vasn_sku_qty)+(h.total_duty_per_unit * g.vasn_sku_qty)) as vasn_sku_cost
from(
	select 
			e.purchase_order_num
			,e.ship_location_id
			,e.upc_num
			,e.rms_sku_num
			,TRIM(COALESCE(f.dept_num, 'UNKNOWN') || '_' || COALESCE(f.prmy_supp_num, 'UNKNOWN') || '_' || COALESCE(f.supp_part_num, 'UNKNOWN') || '_' || COALESCE(f.color_num, 'UNKNOWN')) AS customer_choice
			,e.vasn_sku_qty
			,e.vendor_ship_dt
	from (
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
			join po_base b
			on a.purchase_order_num = b.purchase_order_num
			--where a.PURCHASE_ORDER_NUM = '40073120'
			) c
		join PRD_NAP_USR_VWS.PRODUCT_UPC_DIM d
		on c.upc_num = d.upc_num
		where d.prmy_upc_ind= 'Y'
		and d.channel_country= 'US'
	)e
	join PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW f
	on e.rms_sku_num = f.rms_sku_num
	where f.channel_country = 'US'
) g
join PRD_NAP_USR_VWS.PURCHASE_ORDER_SHIPLOCATION_FACT h
on g.purchase_order_num = h.purchase_order_number
and g.rms_sku_num = h.rms_sku_num
and g.ship_location_id = h.ship_location_id
group by 1,2
	)
WITH DATA 
	PRIMARY INDEX (purchase_order_num,customer_choice) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num,customer_choice)
	,COLUMN (purchase_order_num) 
	,COLUMN (customer_choice)
ON po_vasn;

--drop table po_detail_final
CREATE MULTISET VOLATILE TABLE po_detail_final AS (
Select a.*
	,b.LAST_VASN_SIGNAL
	,b.VASN_SKU_QTY
	,b.vasn_sku_cost as vasn_cost
from po_detail_stg a
left join po_vasn b
on a.purchase_order_num = b. purchase_order_num
and a.customer_choice = b.customer_choice
--where a.purchase_order_num = '40099252'
)
WITH DATA 
	PRIMARY INDEX (purchase_order_num,customer_choice) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (purchase_order_num,customer_choice)
	,COLUMN (purchase_order_num) 
	,COLUMN (customer_choice)
ON po_detail_final;

DELETE FROM T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL;

INSERT INTO T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL
SELECT g.purchase_order_num 
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
	,g.supp_num
	,g.supp_name
	,g.manufacturer_name
	,g.vpn
	,g.style_desc
	,g.color_num
	,g.supp_color
	,g.customer_choice
	,g.epm_choice_num
	,g.STATUS
	,g.edi_ind
	,g.start_ship_date
	,g.end_ship_date
	,g.latest_approval_date
	,g.last_carrier_asn_date
	,g.last_supplier_asn_date
	,g.last_ship_activity_date
	,g.last_outbound_activity_date
	,g.receipt_date
	,g.otb_eow_date
	,g.otb_month
	,g.otb_month_idnt
	,g.order_type
	,g.internal_po_ind
	,g.npg_ind
	,g.po_type
	,g.purchase_type
	,g."Month"
	,g.month_idnt
	,g.week_idnt
	,g.fiscal_year_num
	,g.quarter_label
	,g.RCPT_COST
	,g.RCPT_RETAIL
	,g.RCPT_UNITS
	,g.QUANTITY_ORDERED
	,g.QUANTITY_RECEIVED
	,g.QUANTITY_CANCELED
	,g.QUANTITY_OPEN
	,g.UNIT_COST_AMT
	,g.TOTAL_ESTIMATED_LANDING_COST
	,g.TOTAL_ANTICIPATED_RETAIL_AMT
	,g.ttl_approved_qty
	,g.ttl_approved_c
	,g.ttl_approved_r
	,g.vasn_sku_qty
	,g.vasn_cost
	,g.dc_received_qty
	,g.dc_received_c
	,current_date as process_dt
from po_detail_final g ;
COLLECT STATS PRIMARY INDEX (customer_choice)
	,COLUMN (customer_choice)
	,COLUMN (OTB_MONTH_IDNT)
	,COLUMN (MONTH_IDNT) ON T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL;

DELETE FROM T2DL_DAS_OPEN_TO_BUY.OUTBOUND_PO_SKU_LKP;

INSERT INTO T2DL_DAS_OPEN_TO_BUY.OUTBOUND_PO_SKU_LKP
SELECT 
	PURCHASE_ORDER_NUM
	,SKU_NUM
	,WEEK_IDNT
	,SUM(RCPT_COST) as RCPT_COST
	,SUM(RCPT_UNITS) as RCPT_UNITS
	,SUM(TOTAL_ESTIMATED_LANDING_COST) as TOTAL_ESTIMATED_LANDING_COST
	,SUM(QUANTITY_OPEN) as QUANTITY_OPEN
	,SUM(QUANTITY_ORDERED) as QUANTITY_ORDERED
	,CURRENT_DATE AS PROCESS_DT 
FROM po_detail
GROUP BY 1,2,3,9;

COLLECT STATS
PRIMARY INDEX (PURCHASE_ORDER_NUM,SKU_NUM,WEEK_IDNT)
,COLUMN(PURCHASE_ORDER_NUM)
,COLUMN(SKU_NUM)
,COLUMN(WEEK_IDNT)
ON T2DL_DAS_OPEN_TO_BUY.OUTBOUND_PO_SKU_LKP;	