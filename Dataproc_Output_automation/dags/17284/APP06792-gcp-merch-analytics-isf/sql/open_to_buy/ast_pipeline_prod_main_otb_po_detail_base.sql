--This is the base data for PO data that contains Rpcts$ and OO$ 
--this is for outbound data and is at PO Channel CC week
--we need to look bak 180 days 
--Developed by: Isaac Wallick
--Updated last: 1/19/2024



CREATE TEMPORARY TABLE IF NOT EXISTS po_rcpts_date
AS
SELECT FORMAT('%11d', fiscal_year_num) || FORMAT('%4d', fiscal_month_num) || ' ' || month_abrv AS month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 month_label,
 day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 180 DAY) AND (CURRENT_DATE('PST8PDT'))
GROUP BY month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 month_label,
 day_date;


--COLLECT STATS PRIMARY INDEX (day_date) ON po_rcpts_date


--This gets us the dates for the elapsed days in the current week


CREATE TEMPORARY TABLE IF NOT EXISTS po_wtd
AS
SELECT FORMAT('%11d', b.fiscal_year_num) || FORMAT('%4d', b.fiscal_month_num) || ' ' || b.month_abrv AS month,
 b.month_idnt,
 b.week_idnt,
 b.fiscal_year_num,
 b.quarter_label,
 b.month_label,
 b.day_date
FROM (SELECT DISTINCT week_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.week_idnt = b.week_idnt
WHERE b.day_date < CURRENT_DATE('PST8PDT');


--COLLECT STATS      PRIMARY INDEX (day_date)      ,COLUMN(WEEK_IDNT)      ,COLUMN(MONTH_IDNT)      ON po_wtd


--this gets us the elapsed days for closed weeks


CREATE TEMPORARY TABLE IF NOT EXISTS po_pm
AS
SELECT FORMAT('%11d', b.fiscal_year_num) || FORMAT('%4d', b.fiscal_month_num) || ' ' || b.month_abrv AS month,
 b.month_idnt,
 b.week_idnt,
 b.fiscal_year_num,
 b.quarter_label,
 b.month_label,
 b.day_date
FROM (SELECT DISTINCT week_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 190 DAY) AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 7 DAY))) AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.week_idnt = b.week_idnt;


--COLLECT STATS      PRIMARY INDEX (day_date)      ,COLUMN(WEEK_IDNT)      ,COLUMN(MONTH_IDNT)      ON po_pm


--only get US locations that are not closed


-- can be found as CLSD or CLOSED


CREATE TEMPORARY TABLE IF NOT EXISTS po_loc
AS
SELECT store_num,
 channel_num,
 channel_desc,
 channel_brand,
 channel_country,
   FORMAT('%11d', channel_num) || ',' || channel_desc AS channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 210, 250, 260, 310)
 AND LOWER(channel_country) = LOWER('US')
 AND LOWER(store_name) NOT LIKE LOWER('CLSD%')
 AND LOWER(store_name) NOT LIKE LOWER('CLOSED%')
GROUP BY store_num,
 channel_num,
 channel_desc,
 channel_brand,
 channel_country;


--COLLECT STATS PRIMARY INDEX (STORE_NUM) 	,COLUMN (STORE_NUM) ON po_loc


--lets get list of pos we need to go back a year on closed POs since this is for outbound and all units may have not yet been rcvd at a astore


--drop table po_base


CREATE TEMPORARY TABLE IF NOT EXISTS po_base
AS
SELECT a.purchase_order_num,
 a.status,
 a.edi_ind,
 a.start_ship_date,
 a.end_ship_date,
 CAST(b.latest_approval_event_tmstp_pacific AS DATE) AS latest_approval_date,
 a.open_to_buy_endofweek_date,
 a.order_type,
 b.internal_po_ind,
 b.npg_ind,
 b.po_type,
 b.purchase_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS b ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_number
   )
WHERE (LOWER(a.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')) OR LOWER(a.status) = LOWER('CLOSED') AND a.open_to_buy_endofweek_date
      >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY))
 AND LOWER(a.dropship_ind) <> LOWER('t')
 AND a.original_approval_date IS NOT NULL;


--lets get data that has the actual PO nums that have closed weeks and are 180 days in the past


--drop table nt_po


--where e.PO_NUM is null


CREATE TEMPORARY TABLE IF NOT EXISTS nt_po_pm
AS
SELECT d.purchase_order_num,
 d.status,
 d.edi_ind,
 d.start_ship_date,
 d.end_ship_date,
 d.latest_approval_date,
 d.open_to_buy_endofweek_date AS otb_eow_date,
 d.order_type,
 d.internal_po_ind,
 d.npg_ind,
 d.po_type,
 d.purchase_type,
 e.month,
 e.month_idnt,
 e.week_idnt,
 e.fiscal_year_num,
 e.quarter_label,
 e.channel_num,
 e.channel_desc,
 e.channel_brand,
 e.sku_num,
 SUM(e.rcpt_cost) AS rcpt_cost,
 SUM(e.rcpt_retail) AS rcpt_retail,
 SUM(e.rcpt_units) AS rcpt_units
FROM po_base AS d
 INNER JOIN (SELECT a.poreceipt_order_number AS po_num,
   b.month,
   b.month_idnt,
   b.week_idnt,
   b.fiscal_year_num,
   b.quarter_label,
   c.channel_num,
   c.channel_desc,
   c.channel_brand,
   a.sku_num,
   SUM(a.receipts_cost + a.receipts_crossdock_cost) AS rcpt_cost,
   SUM(a.receipts_retail + a.receipts_crossdock_retail) AS rcpt_retail,
   SUM(a.receipts_units + a.receipts_crossdock_units) AS rcpt_units
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a
   INNER JOIN po_pm AS b ON a.tran_date = b.day_date
   INNER JOIN po_loc AS c ON a.store_num = c.store_num
  WHERE LOWER(a.dropship_ind) = LOWER('N')
   AND a.tran_code = 20
  GROUP BY po_num,
   b.month,
   b.month_idnt,
   b.week_idnt,
   b.fiscal_year_num,
   b.quarter_label,
   c.channel_num,
   c.channel_desc,
   c.channel_brand,
   a.sku_num) AS e ON LOWER(d.purchase_order_num) = LOWER(e.po_num)
GROUP BY d.purchase_order_num,
 d.status,
 d.edi_ind,
 d.start_ship_date,
 d.end_ship_date,
 d.latest_approval_date,
 otb_eow_date,
 d.order_type,
 d.internal_po_ind,
 d.npg_ind,
 d.po_type,
 d.purchase_type,
 e.month,
 e.month_idnt,
 e.week_idnt,
 e.fiscal_year_num,
 e.quarter_label,
 e.channel_num,
 e.channel_desc,
 e.channel_brand,
 e.sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON nt_po_pm


-- this gets us the backposting rcpt $s and units for wtd for actual po #


--where e.PO_NUM is null


CREATE TEMPORARY TABLE IF NOT EXISTS nt_po_wtd
AS
SELECT d.purchase_order_num,
 d.status,
 d.edi_ind,
 d.start_ship_date,
 d.end_ship_date,
 d.latest_approval_date,
 d.open_to_buy_endofweek_date AS otb_eow_date,
 d.order_type,
 d.internal_po_ind,
 d.npg_ind,
 d.po_type,
 d.purchase_type,
  (SELECT DISTINCT month
  FROM po_wtd) AS month,
  (SELECT DISTINCT month_idnt
  FROM po_wtd) AS month_idnt,
  (SELECT DISTINCT week_idnt
  FROM po_wtd) AS week_idnt,
  (SELECT DISTINCT fiscal_year_num
  FROM po_wtd) AS fiscal_year_num,
  (SELECT DISTINCT quarter_label
  FROM po_wtd) AS quarter_label,
 e.channel_num,
 e.channel_desc,
 e.channel_brand,
 e.sku_num,
 SUM(e.rcpt_cost) AS rcpt_cost,
 SUM(e.rcpt_retail) AS rcpt_retail,
 SUM(e.rcpt_units) AS rcpt_units
FROM po_base AS d
 INNER JOIN (SELECT a.poreceipt_order_number AS po_num,
   c.channel_num,
   c.channel_desc,
   c.channel_brand,
   a.sku_num,
   SUM(a.receipts_cost + a.receipts_crossdock_cost) AS rcpt_cost,
   SUM(a.receipts_retail + a.receipts_crossdock_retail) AS rcpt_retail,
   SUM(a.receipts_units + a.receipts_crossdock_units) AS rcpt_units
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a
   INNER JOIN po_loc AS c ON a.store_num = c.store_num
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_hist AS psd ON LOWER(psd.rms_sku_num) = LOWER(a.sku_num) AND LOWER(c.channel_country
        ) = LOWER(psd.channel_country) AND a.tran_date >= CAST(psd.eff_begin_tmstp AS DATE) AND a.tran_date < CAST(psd.eff_end_tmstp AS DATE)
     
  WHERE LOWER(a.dropship_ind) = LOWER('N')
   AND a.tran_code = 20
   AND (a.tran_date BETWEEN (SELECT MIN(day_date)
       FROM po_wtd) AND (SELECT MAX(day_date)
       FROM po_wtd) OR CAST(a.event_time AS DATE) BETWEEN (SELECT MIN(day_date)
       FROM po_wtd) AND (SELECT MAX(day_date)
       FROM po_wtd))
  GROUP BY po_num,
   c.channel_num,
   c.channel_desc,
   c.channel_brand,
   a.sku_num) AS e ON LOWER(d.purchase_order_num) = LOWER(e.po_num)
GROUP BY d.purchase_order_num,
 d.status,
 d.edi_ind,
 d.start_ship_date,
 d.end_ship_date,
 d.latest_approval_date,
 otb_eow_date,
 d.order_type,
 d.internal_po_ind,
 d.npg_ind,
 d.po_type,
 d.purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 e.channel_num,
 e.channel_desc,
 e.channel_brand,
 e.sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON nt_po_wtd


--now lets combine the closed weeks and the wtd data


CREATE TEMPORARY TABLE IF NOT EXISTS nt_po_rcpts
AS
SELECT purchase_order_num,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num,
 SUM(rcpt_cost) AS rcpt_cost,
 SUM(rcpt_retail) AS rcpt_retail,
 SUM(rcpt_units) AS rcpt_units
FROM (SELECT purchase_order_num
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
		,Month
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
		,Month
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
   FROM nt_po_wtd) AS a
GROUP BY purchase_order_num,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON nt_po_rcpts


--since a po rcpt num can be a transfer Id we need to isolate those for closed weeks


CREATE TEMPORARY TABLE IF NOT EXISTS trans_po_pm
AS
SELECT a.poreceipt_order_number AS transfer_id,
 b.month,
 b.month_idnt,
 b.week_idnt,
 b.fiscal_year_num,
 b.quarter_label,
 c.channel_num,
 c.channel_desc,
 c.channel_brand,
 a.sku_num,
 a.store_num,
 SUM(a.receipts_cost + a.receipts_crossdock_cost) AS rcpt_cost,
 SUM(a.receipts_retail + a.receipts_crossdock_retail) AS rcpt_retail,
 SUM(a.receipts_units + a.receipts_crossdock_units) AS rcpt_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a
 INNER JOIN po_pm AS b ON a.tran_date = b.day_date
 INNER JOIN po_loc AS c ON a.store_num = c.store_num
WHERE LOWER(a.dropship_ind) = LOWER('N')
 AND a.tran_code = 30
GROUP BY transfer_id,
 b.month,
 b.month_idnt,
 b.week_idnt,
 b.fiscal_year_num,
 b.quarter_label,
 c.channel_num,
 c.channel_desc,
 c.channel_brand,
 a.sku_num,
 a.store_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON trans_po_pm


--we do that same thing here but for current week


CREATE TEMPORARY TABLE IF NOT EXISTS trans_po_wtd
AS
SELECT a.poreceipt_order_number AS transfer_id,
  (SELECT DISTINCT month
  FROM po_wtd) AS month,
  (SELECT DISTINCT month_idnt
  FROM po_wtd) AS month_idnt,
  (SELECT DISTINCT week_idnt
  FROM po_wtd) AS week_idnt,
  (SELECT DISTINCT fiscal_year_num
  FROM po_wtd) AS fiscal_year_num,
  (SELECT DISTINCT quarter_label
  FROM po_wtd) AS quarter_label,
 c.channel_num,
 c.channel_desc,
 c.channel_brand,
 a.sku_num,
 a.store_num,
 SUM(a.receipts_cost + a.receipts_crossdock_cost) AS rcpt_cost,
 SUM(a.receipts_retail + a.receipts_crossdock_retail) AS rcpt_retail,
 SUM(a.receipts_units + a.receipts_crossdock_units) AS rcpt_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a
 INNER JOIN po_loc AS c ON a.store_num = c.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_hist AS psd ON LOWER(psd.rms_sku_num) = LOWER(a.sku_num) AND LOWER(c.channel_country
      ) = LOWER(psd.channel_country) AND a.tran_date >= CAST(psd.eff_begin_tmstp AS DATE) AND a.tran_date < CAST(psd.eff_end_tmstp AS DATE)  
WHERE LOWER(a.dropship_ind) = LOWER('N')
 AND a.tran_code = 30
 AND (a.tran_date BETWEEN (SELECT MIN(day_date)
     FROM po_wtd) AND (SELECT MAX(day_date)
     FROM po_wtd) OR CAST(a.event_time AS DATE) BETWEEN (SELECT MIN(day_date)
     FROM po_wtd) AND (SELECT MAX(day_date)
     FROM po_wtd))
GROUP BY transfer_id,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 c.channel_num,
 c.channel_desc,
 c.channel_brand,
 a.sku_num,
 a.store_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON trans_po_wtd


--now we combine all the trnasfer id data


CREATE TEMPORARY TABLE IF NOT EXISTS trans_po
AS
SELECT transfer_id,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num,
 store_num,
 SUM(rcpt_cost) AS rcpt_cost,
 SUM(rcpt_retail) AS rcpt_retail,
 SUM(rcpt_units) AS rcpt_units
FROM (SELECT TRANSFER_ID
		,Month
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
		,Month
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
   FROM trans_po_wtd) AS a
GROUP BY transfer_id,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num,
 store_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON trans_po


-- since we dont have po number we need to join to a distribution table to get the po that is associated to the original PO


CREATE TEMPORARY TABLE IF NOT EXISTS trans_po_final
AS
SELECT d.purchase_order_num,
 d.status,
 d.edi_ind,
 d.start_ship_date,
 d.end_ship_date,
 d.latest_approval_date,
 d.open_to_buy_endofweek_date AS otb_eow_date,
 d.order_type,
 d.internal_po_ind,
 d.npg_ind,
 d.po_type,
 d.purchase_type,
 e.month,
 e.month_idnt,
 e.week_idnt,
 e.fiscal_year_num,
 e.quarter_label,
 e.channel_num,
 e.channel_desc,
 e.channel_brand,
 e.sku_num,
 SUM(e.rcpt_cost) AS rcpt_cost,
 SUM(e.rcpt_retail) AS rcpt_retail,
 SUM(e.rcpt_units) AS rcpt_units
FROM po_base AS d
 INNER JOIN (SELECT c.purchase_order_num AS po_num,
   d0.month,
   d0.month_idnt,
   d0.week_idnt,
   d0.fiscal_year_num,
   d0.quarter_label,
   d0.channel_num,
   d0.channel_desc,
   d0.channel_brand,
   d0.sku_num,
   SUM(d0.rcpt_cost) AS rcpt_cost,
   SUM(d0.rcpt_retail) AS rcpt_retail,
   SUM(d0.rcpt_units) AS rcpt_units
  FROM (SELECT a.purchase_order_num,
     b.transfer_id,
     b.sku_num,
     b.store_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS a
     INNER JOIN trans_po AS b ON LOWER(SUBSTR(CAST(a.external_distribution_id AS STRING), 1, 100)) = LOWER(b.transfer_id
         ) AND LOWER(a.rms_sku_num) = LOWER(b.sku_num) AND a.distribute_location_id = b.store_num
    GROUP BY a.purchase_order_num,
     b.transfer_id,
     b.sku_num,
     b.store_num) AS c
   INNER JOIN trans_po AS d0 ON LOWER(c.transfer_id) = LOWER(d0.transfer_id) AND LOWER(c.sku_num) = LOWER(d0.sku_num)
    AND c.store_num = d0.store_num
  GROUP BY po_num,
   d0.month,
   d0.month_idnt,
   d0.week_idnt,
   d0.fiscal_year_num,
   d0.quarter_label,
   d0.channel_num,
   d0.channel_desc,
   d0.channel_brand,
   d0.sku_num) AS e ON LOWER(d.purchase_order_num) = LOWER(e.po_num)
GROUP BY d.purchase_order_num,
 d.status,
 d.edi_ind,
 d.start_ship_date,
 d.end_ship_date,
 d.latest_approval_date,
 otb_eow_date,
 d.order_type,
 d.internal_po_ind,
 d.npg_ind,
 d.po_type,
 d.purchase_type,
 e.month,
 e.month_idnt,
 e.week_idnt,
 e.fiscal_year_num,
 e.quarter_label,
 e.channel_num,
 e.channel_desc,
 e.channel_brand,
 e.sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON trans_po_final


-- combine the PO data and the transfer id data to get our full rcpts data


CREATE TEMPORARY TABLE IF NOT EXISTS po_rcpts
AS
SELECT purchase_order_num,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num,
 SUM(rcpt_cost) AS rcpt_cost,
 SUM(rcpt_retail) AS rcpt_retail,
 SUM(rcpt_units) AS rcpt_units
FROM (SELECT purchase_order_num
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
		,Month
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
		,Month
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
   FROM nt_po_rcpts) AS a
GROUP BY purchase_order_num,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON po_rcpts


--this gets us our OO data using the mfp carry forward logic


--and a.PURCHASE_ORDER_NUMBER='36998137'


CREATE TEMPORARY TABLE IF NOT EXISTS po_oo_final
AS
SELECT g.purchase_order_num,
 g.status,
 g.edi_ind,
 g.start_ship_date,
 g.end_ship_date,
 g.latest_approval_date,
 g.open_to_buy_endofweek_date AS otb_eow_date,
 g.order_type,
 g.internal_po_ind,
 g.npg_ind,
 g.po_type,
 g.purchase_type,
 f.month,
 f.month_idnt,
 f.week_num AS week_idnt,
 f.fiscal_year_num,
 f.quarter_label,
 f.channel_num,
 f.channel_desc,
 f.channel_brand,
 f.rms_sku_num AS sku_num,
 MAX(f.unit_cost_amt) AS unit_cost_amt,
 SUM(f.quantity_ordered) AS quantity_ordered,
 SUM(f.quantity_received) AS quantity_received,
 SUM(f.quantity_canceled) AS quantity_canceled,
 SUM(f.quantity_open) AS quantity_open,
 SUM(f.total_estimated_landing_cost) AS total_estimated_landing_cost,
 SUM(f.total_anticipated_retail_amt) AS total_anticipated_retail_amt
FROM po_base AS g
 INNER JOIN (SELECT e.po_num,
   e.rms_sku_num,
   e.channel_num,
   e.channel_desc,
   e.channel_brand,
   e.store_num,
      FORMAT('%11d', d.fiscal_year_num) || FORMAT('%4d', d.fiscal_month_num) || ' ' || d.month_abrv AS month,
   d.month_idnt,
   d.fiscal_year_num,
   d.quarter_label,
   d.month_label,
   e.week_num,
   e.unit_cost_amt,
   SUM(e.quantity_ordered) AS quantity_ordered,
   SUM(e.quantity_received) AS quantity_received,
   SUM(e.quantity_canceled) AS quantity_canceled,
   SUM(e.quantity_open) AS quantity_open,
   SUM(e.total_estimated_landing_cost) AS total_estimated_landing_cost,
   SUM(e.total_anticipated_retail_amt) AS total_anticipated_retail_amt
  FROM (SELECT a.purchase_order_number AS po_num,
     a.rms_sku_num,
     a.rms_casepack_num,
     b.channel_num,
     b.channel_desc,
     b.channel_brand,
     a.store_num,
      CASE
      WHEN a.week_num < (SELECT week_idnt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
        WHERE day_date = CURRENT_DATE('PST8PDT'))
      THEN (SELECT week_idnt
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
       WHERE day_date = CURRENT_DATE('PST8PDT'))
      ELSE a.week_num
      END AS week_num,
     a.order_type,
     a.latest_approval_date,
     a.quantity_ordered,
     a.quantity_received,
     a.quantity_canceled,
     a.quantity_open,
     a.unit_cost_amt,
     a.total_estimated_landing_cost,
     a.total_anticipated_retail_amt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS a
     INNER JOIN po_loc AS b ON a.store_num = b.store_num
    WHERE a.quantity_open > 0
     AND (LOWER(a.status) = LOWER('CLOSED') AND a.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) OR LOWER(a.status
         ) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
     AND a.first_approval_date IS NOT NULL) AS e
   INNER JOIN (SELECT week_idnt,
     month_idnt,
     fiscal_year_num,
     fiscal_month_num,
     month_abrv,
     quarter_label,
     month_label
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    GROUP BY week_idnt,
     month_idnt,
     fiscal_year_num,
     fiscal_month_num,
     month_abrv,
     quarter_label,
     month_label) AS d ON e.week_num = d.week_idnt
  GROUP BY e.po_num,
   e.rms_sku_num,
   e.channel_num,
   e.channel_desc,
   e.channel_brand,
   e.store_num,
   month,
   d.month_idnt,
   d.fiscal_year_num,
   d.quarter_label,
   d.month_label,
   e.week_num,
   e.unit_cost_amt) AS f ON LOWER(g.purchase_order_num) = LOWER(f.po_num)
GROUP BY g.purchase_order_num,
 g.status,
 g.edi_ind,
 g.start_ship_date,
 g.end_ship_date,
 g.latest_approval_date,
 otb_eow_date,
 g.order_type,
 g.internal_po_ind,
 g.npg_ind,
 g.po_type,
 g.purchase_type,
 f.month,
 f.month_idnt,
 week_idnt,
 f.fiscal_year_num,
 f.quarter_label,
 f.channel_num,
 f.channel_desc,
 f.channel_brand,
 sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON po_oo_final


-- Lets bring rcpts and oo together


--drop table po_rcpt_oo_final


--where purchase_order_num = '39992571'


CREATE TEMPORARY TABLE IF NOT EXISTS po_rcpt_oo_final
AS
SELECT purchase_order_num,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num,
 MAX(unit_cost_amt) AS unit_cost_amt,
 SUM(rcpt_cost) AS rcpt_cost,
 SUM(rcpt_retail) AS rcpt_retail,
 SUM(rcpt_units) AS rcpt_units,
 SUM(quantity_ordered) AS quantity_ordered,
 SUM(quantity_received) AS quantity_received,
 SUM(quantity_canceled) AS quantity_canceled,
 SUM(quantity_open) AS quantity_open,
 SUM(total_estimated_landing_cost) AS total_estimated_landing_cost,
 SUM(total_anticipated_retail_amt) AS total_anticipated_retail_amt
FROM (SELECT purchase_order_num,
    status,
    edi_ind,
    start_ship_date,
    end_ship_date,
    latest_approval_date,
    otb_eow_date,
    order_type,
    internal_po_ind,
    npg_ind,
    po_type,
    purchase_type,
    month,
    month_idnt,
    week_idnt,
    fiscal_year_num,
    quarter_label,
    channel_num,
    channel_desc,
    channel_brand,
    sku_num,
    CAST(rcpt_cost AS NUMERIC) AS rcpt_cost,
    CAST(rcpt_retail AS NUMERIC) AS rcpt_retail,
    CAST(rcpt_units AS NUMERIC) AS rcpt_units,
    0 AS quantity_ordered,
    0 AS quantity_received,
    0 AS quantity_canceled,
    0 AS quantity_open,
    0 AS unit_cost_amt,
    0 AS total_estimated_landing_cost,
    0 AS total_anticipated_retail_amt
   FROM po_rcpts
   UNION ALL
   SELECT purchase_order_num,
    status,
    edi_ind,
    start_ship_date,
    end_ship_date,
    latest_approval_date,
    otb_eow_date,
    order_type,
    internal_po_ind,
    npg_ind,
    po_type,
    purchase_type,
    month,
    month_idnt,
    week_idnt,
    fiscal_year_num,
    quarter_label,
    channel_num,
    channel_desc,
    channel_brand,
    sku_num,
    0 AS asrcpt_cost,
    0 AS rcpt_retail,
    0 AS rcpt_units,
    CAST(quantity_ordered AS NUMERIC) AS quantity_ordered,
    CAST(quantity_received AS NUMERIC) AS quantity_received,
    CAST(quantity_canceled AS NUMERIC) AS quantity_canceled,
    CAST(quantity_open AS NUMERIC) AS quantity_open,
    ROUND(CAST(unit_cost_amt AS NUMERIC), 4) AS unit_cost_amt,
    ROUND(CAST(total_estimated_landing_cost AS NUMERIC), 4) AS total_estimated_landing_cost,
    ROUND(CAST(total_anticipated_retail_amt AS NUMERIC), 4) AS total_anticipated_retail_amt
   FROM po_oo_final) AS a
GROUP BY purchase_order_num,
 status,
 edi_ind,
 start_ship_date,
 end_ship_date,
 latest_approval_date,
 otb_eow_date,
 order_type,
 internal_po_ind,
 npg_ind,
 po_type,
 purchase_type,
 month,
 month_idnt,
 week_idnt,
 fiscal_year_num,
 quarter_label,
 channel_num,
 channel_desc,
 channel_brand,
 sku_num;


--COLLECT STATS PRIMARY INDEX (SKU_NUM) 	,COLUMN (SKU_NUM) ON po_rcpt_oo_final


--This gets the total approved qty, cost and retail associated to a PO


--drop table po_ttl_order


--where purchase_order_num = 27105588


--where a.purchase_order_num = '25272105'


CREATE TEMPORARY TABLE IF NOT EXISTS po_ttl_order
AS
SELECT a.purchase_order_num,
 a.status,
 a.edi_ind,
 a.start_ship_date,
 a.end_ship_date,
 a.latest_approval_date,
 a.otb_eow_date,
 a.order_type,
 a.internal_po_ind,
 a.npg_ind,
 a.po_type,
 a.purchase_type,
 a.month,
 a.month_idnt,
 a.week_idnt,
 a.fiscal_year_num,
 a.quarter_label,
 a.channel_num,
 a.channel_desc,
 a.channel_brand,
 a.sku_num,
 a.unit_cost_amt,
 a.rcpt_cost,
 a.rcpt_retail,
 a.rcpt_units,
 a.quantity_ordered,
 a.quantity_received,
 a.quantity_canceled,
 a.quantity_open,
 a.total_estimated_landing_cost,
 a.total_anticipated_retail_amt,
 d.ttl_approved_qty,
 d.ttl_approved_c,
 e.ttl_approved_r
FROM po_rcpt_oo_final AS a
 LEFT JOIN (SELECT purchase_order_number,
   SUM(approved_quantity_ordered) AS ttl_approved_qty,
   SUM(unit_cost * quantity_ordered + total_expenses_per_unit * quantity_ordered + total_duty_per_unit *
      quantity_ordered) AS ttl_approved_c
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact AS c
  GROUP BY purchase_order_number) AS d ON LOWER(a.purchase_order_num) = LOWER(d.purchase_order_number)
 LEFT JOIN (SELECT purchase_order_num,
   SUM(ordered_qty * unit_retail_amt) AS ttl_approved_r
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_shiplocation_fact
  GROUP BY purchase_order_num) AS e ON LOWER(a.purchase_order_num) = LOWER(e.purchase_order_num);


--COLLECT STATS PRIMARY INDEX (purchase_order_num,sku_num) 	,COLUMN (purchase_order_num)  	,COLUMN (sku_num) ON po_ttl_order


-- here we get everything that has been rcvd at a dc at a PO level


--WHERE rec.purchase_order_number = '39648347'


CREATE TEMPORARY TABLE po_dc_yesterday AS 
	SELECT rec.purchase_order_number
	,sum(rec.received_qty) AS dc_received_qty
	,sum(rec.received_qty * g.unit_cost) AS dc_received_c FROM (
	SELECT e.purchase_order_number AS purchase_order_number
		,e.rms_sku_num
		,cast(e.location_id AS STRING) AS ship_location_id
		,e.shipment_qty AS received_qty
		,cast(e.received_tmstp AS DATE) AS receipt_date
	FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.WM_INBOUND_CARTON_FACT_VW e
	JOIN (
		SELECT purchase_order_num
		FROM po_base
		GROUP BY 1
		) f ON e.purchase_order_number = f.purchase_order_num
	WHERE LOWER(cast(e.location_id AS STRING)) NOT IN (LOWER('889'),LOWER('859'),LOWER('896'),LOWER('869'),LOWER('891'),LOWER('868'))
		AND (
			rms_sku_num IS NULL
			OR rms_sku_num IS NOT NULL
			)
	) rec 
	LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.PURCHASE_ORDER_ITEM_FACT g 
	ON LOWER(rec.purchase_order_number) = LOWER(g.purchase_order_number)
	AND LOWER(rec.rms_sku_num) = LOWER(g.rms_sku_num)
	JOIN po_rcpts_date h
	on rec.receipt_date = h.day_date
	
	GROUP BY 1
	;

--COLLECT STATS PRIMARY INDEX (purchase_order_number) 	,COLUMN (purchase_order_number) ON po_dc_yesterday


--lets get the latest date items on a po were rcvd at a dc


--drop table po_dc_rcpt_date


--and e.purchase_order_number = '39870694'


CREATE TEMPORARY TABLE IF NOT EXISTS po_dc_rcpt_date
AS
SELECT purchase_order_number,
 MAX(receipt_date) AS receipt_date
FROM (SELECT e.purchase_order_number,
   CAST(MAX(e.received_tmstp) AS DATE) AS receipt_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw AS e
   INNER JOIN (SELECT purchase_order_num
    FROM po_base
    GROUP BY purchase_order_num) AS f ON LOWER(e.purchase_order_number) = LOWER(f.purchase_order_num)
  WHERE LOWER(e.location_id) NOT IN (LOWER('889'), LOWER('859'), LOWER('896'), LOWER('869'), LOWER('891'), LOWER('868'))
	AND (
			e.rms_sku_num IS NULL
			OR SAFE_CAST(TRUNC(CAST(rms_sku_num AS FLOAT64)) AS INTEGER) IS NOT NULL
			)
  GROUP BY e.purchase_order_number,
   e.rms_sku_num) AS rec
GROUP BY purchase_order_number;


--COLLECT STATS PRIMARY INDEX (purchase_order_number) 	,COLUMN (purchase_order_number) ON po_dc_rcpt_date


--lets get the latest carrier and supplier ship dates


CREATE TEMPORARY TABLE IF NOT EXISTS po_dc_inbound_date
AS
SELECT a.purchase_order_num,
 MAX(a.eta_date) AS last_carrier_asn_date,
 MAX(a.vendor_ship_date) AS last_supplier_asn_date,
  CASE
  WHEN MAX(a.eta_date) > MAX(a.vendor_ship_date)
  THEN MAX(a.eta_date)
  ELSE MAX(a.vendor_ship_date)
  END AS last_ship_activity_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.carrier_asn_fact AS a
 INNER JOIN (SELECT purchase_order_num
  FROM po_base
  GROUP BY purchase_order_num) AS b ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)
GROUP BY a.purchase_order_num;


--COLLECT STATS PRIMARY INDEX (purchase_order_num) 	,COLUMN (purchase_order_num) ON po_dc_inbound_date


--lets get an idea of when an item on a po was rcvd to a store


CREATE TEMPORARY TABLE IF NOT EXISTS po_dc_outbound_date
AS
SELECT po_num,
 sku_num,
 channel_num,
 MAX(tran_date) AS last_outbound_activity_date
FROM (SELECT a.poreceipt_order_number AS po_num,
    a.sku_num,
    c.channel_num,
    a.tran_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a
    INNER JOIN po_rcpts_date AS b ON a.tran_date = b.day_date
    INNER JOIN po_loc AS c ON a.store_num = c.store_num
   WHERE LOWER(a.dropship_ind) = LOWER('N')
    AND a.tran_code = 20
   GROUP BY po_num,
    a.sku_num,
    c.channel_num,
    a.tran_date
   UNION ALL
   SELECT e.purchase_order_num AS po_num,
    t4.sku_num,
    t4.channel_num,
    t4.tran_date
   FROM (SELECT a0.poreceipt_order_number,
      a0.sku_num,
      c0.store_num,
      c0.channel_num,
      a0.tran_date
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS a0
      INNER JOIN po_rcpts_date AS b0 ON a0.tran_date = b0.day_date
      INNER JOIN po_loc AS c0 ON a0.store_num = c0.store_num
     WHERE LOWER(a0.dropship_ind) = LOWER('N')
      AND a0.tran_code = 30
     GROUP BY a0.poreceipt_order_number,
      a0.sku_num,
      a0.tran_date,
      c0.store_num,
      c0.channel_num) AS t4
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS e ON LOWER(t4.poreceipt_order_number) =
       LOWER(SUBSTR(CAST(e.external_distribution_id AS STRING), 1, 100)) AND LOWER(t4.sku_num) = LOWER(e.rms_sku_num)
     AND t4.store_num = e.distribute_location_id
   GROUP BY po_num,
    t4.sku_num,
    t4.channel_num,
    t4.tran_date) AS f
GROUP BY po_num,
 sku_num,
 channel_num;


--COLLECT STATS PRIMARY INDEX (po_num) 	,COLUMN (po_num) ON po_dc_outbound_date


---bring everything together


--drop table po_detail_stg	  



CREATE TEMPORARY TABLE po_detail AS 
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
	ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_number)
	LEFT JOIN po_dc_rcpt_date c 
	ON LOWER(a.purchase_order_num) = LOWER(c.purchase_order_number) 
	LEFT JOIN po_dc_inbound_date d 
	ON LOWER(a.purchase_order_num) = LOWER(d.purchase_order_num) 
	LEFT JOIN po_dc_outbound_date e 
	ON LOWER(a.purchase_order_num) = LOWER(e.po_num)
	AND LOWER(a.sku_num) = LOWER(e.sku_num)
	AND a.channel_num = e.channel_num;
	


--COLLECT STATS PRIMARY INDEX (purchase_order_num) 	,COLUMN (purchase_order_num) ON po_detail


--lets stage the above data and and some heir


--where d.purchase_order_num = '39648347'



CREATE TEMPORARY TABLE po_detail_stg AS 
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
	,g.Month
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
	,current_date('PST8PDT') as process_dt
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
		,d.Month
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
			,TRIM(COALESCE(CAST(b.dept_num AS STRING), 'UNKNOWN') || '_' || COALESCE(CAST(b.prmy_supp_num AS STRING), 'UNKNOWN') || '_' || COALESCE(CAST(b.supp_part_num AS STRING), 'UNKNOWN') || '_' || COALESCE(CAST(b.color_num AS STRING), 'UNKNOWN')) AS customer_choice
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
			,a.Month
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
		JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw b ON a.SKU_NUM = b.rms_sku_num
		JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim c ON a.otb_eow_date = c.day_date
		WHERE LOWER(b.dept_desc) NOT LIKE LOWER('%INACT%')
			AND LOWER(b.channel_country) = LOWER('US')
		) d
	JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim e ON d.dept_num = e.dept_num
	JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim f ON LOWER(d.prmy_supp_num) = LOWER(f.vendor_num)
	
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45
	) g
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim h ON LOWER(g.manufacturer_num) = LOWER(h.vendor_num) ;


--COLLECT STATS PRIMARY INDEX (purchase_order_num,customer_choice) 	,COLUMN (purchase_order_num)  	,COLUMN (customer_choice) ON po_detail_stg


--get the latest vasn signal and inventory associated to it


--drop table po_vasn


--where a.PURCHASE_ORDER_NUM = '40073120'


CREATE TEMPORARY TABLE IF NOT EXISTS po_vasn
AS
SELECT g.purchase_order_num,
 g.customer_choice,
 MAX(g.vendor_ship_dt) AS last_vasn_signal,
 SUM(g.vasn_sku_qty) AS vasn_sku_qty,
 SUM(h.unit_cost * g.vasn_sku_qty + h.total_expenses_per_unit * g.vasn_sku_qty + h.total_duty_per_unit * g.vasn_sku_qty
  ) AS vasn_sku_cost
FROM (SELECT e.purchase_order_num,
   e.ship_location_id,
   e.upc_num,
   e.rms_sku_num,
   TRIM(COALESCE(FORMAT('%11d', f.dept_num), 'UNKNOWN') || '_' || COALESCE(f.prmy_supp_num, 'UNKNOWN') || '_' ||
       COALESCE(f.supp_part_num, 'UNKNOWN') || '_' || COALESCE(f.color_num, 'UNKNOWN')) AS customer_choice,
   e.vasn_sku_qty,
   e.vendor_ship_dt
  FROM (SELECT c.purchase_order_num,
     c.ship_location_id,
     c.upc_num,
     d.rms_sku_num,
     c.vasn_sku_qty,
     c.vendor_ship_dt
    FROM (SELECT a.purchase_order_num,
       a.ship_location_id,
       SUBSTR(a.upc_num, 3, LENGTH(a.upc_num)) AS upc_num,
       a.units_shipped AS vasn_sku_qty,
       a.vendor_ship_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_asn_fact_vw AS a
       INNER JOIN po_base AS b ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)) AS c
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS d ON LOWER(c.upc_num) = LOWER(d.upc_num)
    WHERE LOWER(d.prmy_upc_ind) = LOWER('Y')
     AND LOWER(d.channel_country) = LOWER('US')) AS e
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS f ON LOWER(e.rms_sku_num) = LOWER(f.rms_sku_num)
  WHERE LOWER(f.channel_country) = LOWER('US')) AS g
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact AS h ON LOWER(g.purchase_order_num) = LOWER(h.purchase_order_number
     ) AND LOWER(g.rms_sku_num) = LOWER(h.rms_sku_num) AND LOWER(g.ship_location_id) = LOWER(h.ship_location_id)
GROUP BY g.purchase_order_num,
 g.customer_choice;


--COLLECT STATS PRIMARY INDEX (purchase_order_num,customer_choice) 	,COLUMN (purchase_order_num)  	,COLUMN (customer_choice) ON po_vasn


--drop table po_detail_final


--where a.purchase_order_num = '40099252'


CREATE TEMPORARY TABLE po_detail_final AS 
Select a.*
	,b.LAST_VASN_SIGNAL
	,b.VASN_SKU_QTY
	,b.vasn_sku_cost as vasn_cost
from po_detail_stg a
left join po_vasn b
on  LOWER(a.purchase_order_num) = LOWER(b. purchase_order_num)
and LOWER(a.customer_choice) = LOWER(b.customer_choice);


--COLLECT STATS PRIMARY INDEX (purchase_order_num,customer_choice) 	,COLUMN (purchase_order_num)  	,COLUMN (customer_choice) ON po_detail_final


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail;



INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail
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
	,g.Month
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
	,current_date('PST8PDT') as process_dt
from po_detail_final g ;



--COLLECT STATS PRIMARY INDEX (customer_choice) 	,COLUMN (customer_choice) 	,COLUMN (OTB_MONTH_IDNT) 	,COLUMN (MONTH_IDNT) ON T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.outbound_po_sku_lkp;



INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.outbound_po_sku_lkp
SELECT 
	PURCHASE_ORDER_NUM
	,SKU_NUM
	,WEEK_IDNT
	,SUM(RCPT_COST) as RCPT_COST
	,SUM(RCPT_UNITS) as RCPT_UNITS
	,SUM(TOTAL_ESTIMATED_LANDING_COST) as TOTAL_ESTIMATED_LANDING_COST
	,SUM(QUANTITY_OPEN) as QUANTITY_OPEN
	,SUM(QUANTITY_ORDERED) as QUANTITY_ORDERED
	,CURRENT_DATE('PST8PDT') AS PROCESS_DT 
FROM po_detail
GROUP BY 1,2,3,9;


--COLLECT STATS PRIMARY INDEX (PURCHASE_ORDER_NUM,SKU_NUM,WEEK_IDNT) ,COLUMN(PURCHASE_ORDER_NUM) ,COLUMN(SKU_NUM) ,COLUMN(WEEK_IDNT) ON T2DL_DAS_OPEN_TO_BUY.OUTBOUND_PO_SKU_LKP