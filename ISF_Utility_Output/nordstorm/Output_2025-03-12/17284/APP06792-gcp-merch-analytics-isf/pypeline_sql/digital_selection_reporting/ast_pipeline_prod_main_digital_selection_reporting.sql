
/*
Purpose:        Creates base weekly table for selection reporting to be used in Digital Selection Reporting dashboard (Owned by Assortment Analytics team) and 
				Historical Plan reporting dashboard (owned by Thomas Peterson and Ivie Okieimen).  Includes both 
				realigned dates and true dates to support plan comparison reporting for Historical plan report.  
				Inserts data in {{environment_schema}} tables for digital selection reporting
                    digital_selection_reporting
Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
                 {{start_date}} as beginning of time period
                 {{end_date}} as end of time period
Author(s):       Sara Scott
*/

--Create calendar that blends realigned and true days
--drop table calendar_realigned
CREATE MULTISET VOLATILE TABLE calendar_realigned AS (
		SELECT 
			day_date
			, week_idnt
			, fiscal_week_num
			, month_idnt 
			, week_end_day_date
			, week_start_day_date
			, month_start_day_date
			, month_end_day_date
			, month_abrv
			, month_label
			, fiscal_month_num
			, quarter_idnt
			, fiscal_halfyear_num as half_idnt
			, fiscal_year_num as year_idnt
			, ty_ly_lly_ind as ty_ly_ind
			, CASE WHEN ytd_last_full_week_ind = 'Y' THEN fiscal_week_num  ELSE null end as last_full_week_ind
 


		FROM prd_nap_vws.REALIGNED_DATE_LKUP_VW rdlv 
		WHERE day_date <= current_date

	
		UNION ALL
		SELECT 
			day_date
			, week_idnt
			, fiscal_week_num
			, month_idnt 
			, week_end_day_date
			, week_start_day_date
			, month_start_day_date
			, month_end_day_date
			, month_abrv
			, month_label
			, fiscal_month_num
			, quarter_idnt
			, fiscal_halfyear_num as half_idnt
			, fiscal_year_num as year_idnt
			, 'llly' as ty_ly_lly_ind
			, CASE WHEN week_end_day_date <= CURRENT_DATE - 1 THEN fiscal_week_num ELSE null END AS last_full_week_ind


	    FROM prd_nap_usr_vws.DAY_CAL_454_DIM dcd 
		WHERE fiscal_year_num NOT IN (SELECT DISTINCT fiscal_year_num FROM prd_nap_vws.realigned_date_lkup_vw)
		and day_date <= current_date
		AND fiscal_year_num >= 2021
		
)

WITH DATA
PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;
--drop table calendar;
CREATE MULTISET VOLATILE TABLE calendar AS (

SELECT 
	cal.*
	, t.day_date as day_date_true
	, t.week_idnt as week_idnt_true
	, t.month_idnt as month_idnt_true
	, t.month_end_day_date as month_end_day_date_true
	, t.week_end_day_date as week_end_day_date_true
	FROM 
	calendar_realigned cal full outer join 

(
		SELECT 
			dcd.day_date
			, dcd.day_idnt
			, dcd.week_idnt
			, dcd.month_idnt 
			, dcd.month_end_day_date
			, dcd.week_end_day_date

				
		FROM prd_nap_usr_vws.DAY_CAL_454_DIM dcd 
		WHERE dcd.fiscal_year_num >= 2021
		and dcd.day_date <= current_Date
		) t 
		ON 
cal.day_date = t.day_date
WHERE cal.day_date BETWEEN {start_date} AND {end_date}


		
)

WITH DATA
PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;

--get live on site skus so that we can join to full metrics list to capture live on site skus that may not have demand, receipts, etc.
--drop table los;
CREATE MULTISET VOLATILE TABLE los AS (
SELECT
     cal.month_idnt
    , cal.month_idnt_true
    , month_abrv
	, month_label
	, fiscal_month_num
	, week_idnt
	, week_idnt_true
	, fiscal_week_num
	, quarter_idnt
	, year_idnt
	, cal.ty_ly_ind
	, last_full_week_ind
    ,los.channel_country
    ,los.channel_brand
    ,scl.customer_choice
    ,los.sku_id as sku_idnt
    ,MAX(los.rp_ind) AS rp_ind
    ,MAX(los.ds_ind) AS ds_ind
FROM t2dl_das_site_merch.live_on_site_daily los
JOIN calendar cal 
ON cal.day_date = los.day_date
JOIN t2dl_das_assortment_dim.sku_cc_lkp scl 
	ON los.sku_id = scl.sku_idnt
	and los.channel_country = scl.channel_country
WHERE los.channel_country = 'US'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
WITH DATA
PRIMARY INDEX (month_idnt, sku_idnt, customer_choice, channel_country, channel_brand)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(month_idnt, sku_idnt, customer_choice, channel_country, channel_brand)
    ,COLUMN (channel_country, channel_brand, customer_choice)
    ,COLUMN (month_idnt)
    ON los;


--Get receipts information at sku/week level
--DROP TABLE receipts_base;
CREATE MULTISET VOLATILE TABLE receipts_base AS (
WITH dates AS (
	SELECT 
		week_idnt_true
		,month_idnt
		,month_end_day_date
		,month_start_day_date
		,month_end_day_date_true
		,week_end_day_date
		,week_end_day_date_true
		,month_idnt_true
		,month_abrv
		,month_label
   		,fiscal_month_num
    	,week_idnt
    	,fiscal_week_num
    	,quarter_idnt
    	,half_idnt
    	,year_idnt
    	,ty_ly_ind
    	,last_full_week_ind
	FROM calendar
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
	)

SELECT
	r.sku_idnt
	, cal.month_idnt
	, cal.month_start_day_date
	, cal.month_end_day_date
	, cal.month_end_day_date_true
	, cal.week_end_day_date
	, cal.week_end_day_date_true
	, cal.month_idnt_true
	, month_abrv
	, month_label
	, fiscal_month_num
	, week_idnt
	, week_idnt_true
	, fiscal_week_num
	, quarter_idnt
	, half_idnt
	, year_idnt
	, ty_ly_ind
	, last_full_week_ind
	, channel_country
	, channel_brand
    , CASE WHEN ds_ind = 'Y' THEN 1 ELSE 0 END as ds_ind
	, CASE WHEN rp_ind = 'Y' THEN 1 ELSE 0 END as rp_ind
    , SUM(receipt_po_units + receipt_ds_units) AS rcpt_units
    , SUM(receipt_po_retail + receipt_ds_retail) AS rcpt_dollars
FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact r
JOIN prd_nap_usr_vws.price_store_dim_vw st
    ON r.store_num = st.store_num
JOIN dates cal 
	ON r.week_num = cal.week_idnt_true
WHERE selling_channel = 'ONLINE' -- DIGITAL only
	AND r.channel_num NOT IN (310, 922, 921, 920, 930, 940, 990) -- removing DC's (920 = US DC, 921 = Canada DC,  922 = OP Canada DC), 930 NQC, 990 FACO, 940 NPG, 310 RESERVE STOCK
	AND cal.month_idnt >= 202301
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
WITH DATA
PRIMARY INDEX (sku_idnt, week_idnt, channel_brand)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(sku_idnt, week_idnt, channel_brand)
    ON receipts_base;

 --get base metrics rolled up to sku/week - joined to calendar to get relaigned months   
--drop table metrics_base;

CREATE MULTISET VOLATILE TABLE metrics_base as 
(  

SELECT 
cal.month_idnt
, cal.month_idnt_true
, cal.month_abrv
, cal.month_label
, cal.month_start_day_date
, cal.month_end_day_date
, cal.month_end_day_date_true
, cal.week_end_day_date
, cal.week_end_day_date_true
, cal.week_idnt
, cal.week_idnt_true
, cal.fiscal_week_num
, cal.fiscal_month_num
, cal.quarter_idnt
, cal.year_idnt
, cal.ty_ly_ind
, cal.last_full_week_ind
, base.customer_choice
, base.sku_idnt
, base.channel_country 
, base.channel_brand
, SUM(COALESCE(net_sales_reg_units, 0)) as net_sales_reg_units
, SUM(COALESCE(net_sales_reg_dollars, 0)) as net_sales_reg_dollars
, SUM(COALESCE(net_sales_clr_units, 0)) as net_sales_clr_units
, SUM(COALESCE(net_sales_clr_dollars, 0)) as net_sales_clr_dollars
, SUM(COALESCE(net_sales_pro_units, 0)) as net_sales_pro_units
, SUM(COALESCE(net_sales_pro_dollars, 0)) as net_sales_pro_dollars
, MAX(new_flag) new_flag
, MAX(cf_flag) cf_flag
, MAX(rp_flag) rp_flag
, MAX(dropship_flag) dropship_flag
, SUM(demand_dollars) AS demand_dollars
, SUM(demand_units) as demand_units
, SUM(sales_dollars) as sales_dollars 
, SUM(sales_units) as sales_units 
, SUM(net_sales_dollars) as net_sales_dollars
, SUM(net_sales_units) as net_sales_units
, SUM(return_dollars) as return_dollars
, SUM(return_units) as return_units
, CAST(0 AS DECIMAL(10,0)) AS receipt_units
, CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
, SUM(COALESCE(product_views,0)) as product_views
, SUM(COALESCE(instock_views,0)) as instock_views
, SUM(CASE WHEN week_end_day_date = base.day_date then eoh_dollars else 0 end) as eoh_dollars
, SUM(CASE WHEN week_end_day_date = base.day_date then eoh_units else 0 end) as eoh_units
, SUM(CASE WHEN week_end_day_date_true = base.day_date then eoh_dollars else 0 end) as eoh_dollars_true
, SUM(CASE WHEN week_end_day_date_true = base.day_date then eoh_units else 0 end) as eoh_units_true

FROM T2DL_DAS_Selection.selection_productivity_base base
JOIN calendar cal
ON base.day_date = cal.day_date
WHERE channel = 'DIGITAL'
and base.channel_country = 'US'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21--,22
)
WITH DATA 
PRIMARY INDEX (week_idnt, sku_idnt, channel_brand)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (week_idnt, sku_idnt, channel_brand)
    ON metrics_base;


 --drop table cc_full  
DELETE FROM {environment_schema}.digital_selection_reporting_weekly_base{env_suffix} WHERE week_idnt IN (SELECT DISTINCT week_idnt FROM calendar);  
INSERT INTO {environment_schema}.digital_selection_reporting_weekly_base{env_suffix} 
   
   
SELECT 
 	COALESCE(base.week_idnt, los.week_idnt) as week_idnt
	, COALESCE(base.week_idnt_true, los.week_idnt_true) as week_idnt_true
	, COALESCE(base.fiscal_week_num, los.fiscal_week_num) as fiscal_week_num
	, COALESCE(base.month_idnt, los.month_idnt) as month_idnt
	, COALESCE(base.month_idnt_true, los.month_idnt_true) as month_idnt_true
	, COALESCE(base.month_abrv, los.month_abrv) as month_abrv
	, COALESCE(base.month_label, los.month_label) as month_label
	, COALESCE(base.fiscal_month_num, los.fiscal_month_num) as fiscal_month_num
	, month_start_day_date
	, month_end_day_date
	, month_end_day_date_true
	, week_end_day_date
	, week_end_day_date_true
	, COALESCE(base.quarter_idnt, los.quarter_idnt) as quarter_idnt
	, COALESCE(base.year_idnt, los.year_idnt) as year_idnt
	, COALESCE(base.ty_ly_ind, los.ty_ly_ind) as ty_ly_ind
	, COALESCE(base.last_full_week_ind, los.last_full_week_ind) as last_full_week_ind
	, COALESCE(base.customer_choice, los.customer_choice) as customer_choice
	, COALESCE(base.sku_idnt, los.sku_idnt) as sku_idnt
	, COALESCE(base.channel_country , los.channel_country) as channel_country
	, COALESCE(base.channel_brand, los.channel_brand) as channel_brand
	, COALESCE(new_flag,0) as new_flag
	, COALESCE(cf_flag,0) as cf_flag
	, COALESCE(base.rp_flag,los.rp_ind) as rp_flag
	, COALESCE(base.dropship_flag, los.ds_ind) dropship_flag
	, CASE WHEN los.sku_idnt is not null then 1 else 0 end as los_flag
	, COALESCE(demand_dollars,0) AS demand_dollars
	, COALESCE(demand_units,0) as demand_units
	, COALESCE(sales_dollars,0) as sales_dollars 
	, COALESCE(sales_units,0) as sales_units 
	, COALESCE(net_sales_dollars,0) as net_sales_dollars
	, COALESCE(net_sales_units,0) as net_sales_units
	, COALESCE(return_dollars,0) as return_dollars
	, COALESCE(return_units,0) as return_units
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	, CAST(0 AS DECIMAL(10,0)) AS receipt_units
	, COALESCE(product_views,0) as product_views
	, COALESCE(instock_views,0) as instock_views
	, COALESCE(eoh_dollars,0) as eoh_dollars
	, COALESCE(eoh_units,0) as eoh_units
	, COALESCE(eoh_dollars_true, 0) as eoh_dollars_true
	, COALESCE(eoh_units_true, 0) as eoh_units_true
	, COALESCE(net_sales_reg_dollars, 0) as net_sales_reg_dollars
	, COALESCE(net_sales_reg_units, 0) as net_sales_reg_units
	, COALESCE(net_sales_clr_dollars, 0) as net_sales_clr_dollars
	, COALESCE(net_sales_clr_units, 0) as net_sales_clr_units
	, COALESCE(net_sales_pro_dollars, 0) as net_sales_pro_dollars
	, COALESCE(net_sales_pro_units, 0) as net_sales_pro_units

FROM metrics_base base
FULL OUTER JOIN los 
	ON base.customer_choice = los.customer_choice
	AND base.channel_brand = los.channel_brand
	AND base.channel_country = los.channel_country
	AND base.month_idnt = los.month_idnt
 	AND base.week_idnt = los.week_idnt
	AND base.sku_idnt = los.sku_idnt
	AND base.rp_flag = los.rp_ind
	AND base.dropship_flag = los.ds_ind

UNION ALL 

SELECT 
 	week_idnt
	, week_idnt_true
	, fiscal_week_num
	, r.month_idnt
	, r.month_idnt_true
	, r.month_abrv
	, r.month_label
	, fiscal_month_num
	, month_start_day_date
	, month_end_day_date
	, month_end_day_date_true
	, week_end_day_date
	, week_end_day_date_true
	, r.quarter_idnt 
	, r.year_idnt
	, ty_ly_ind
	, last_full_week_ind
	, s.customer_choice
	, r.sku_idnt
	, r.channel_country
	, r.channel_brand
	, CASE WHEN cc_type = 'NEW' THEN 1 ELSE 0 END as new_flag
	, COALESCE(CASE WHEN cc_type = 'CF' THEN 1 ELSE 0 END,0) as cf_flag
	, MAX(rp_ind) as rp_ind
	, MAX(ds_ind) as ds_ind
	, CAST(0 AS INTEGER) as los_flag
	, CAST(0 AS DECIMAL(12,2)) AS demand_dollars
	, CAST(0 AS DECIMAL(10,0)) AS demand_units
	, CAST(0 AS DECIMAL(12,2)) AS sales_dollars
	, CAST(0 AS DECIMAL(10,0)) AS sales_units
	, CAST(0 AS DECIMAL(12,2)) as net_sales_dollars
	, CAST(0 AS DECIMAL(10,0)) as net_sales_units
	, CAST(0 AS DECIMAL(12,2)) as return_dollars
	, CAST(0 AS DECIMAL(10,0)) as return_units
	, SUM(COALESCE(rcpt_dollars, 0)) as receipt_dollars
	, SUM(COALESCE(rcpt_units, 0)) as receipt_units
	, CAST(0 AS DECIMAL(38,6)) AS product_views
	, CAST(0 AS DECIMAL(38,6)) AS instock_views
	, CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
	, CAST(0 AS DECIMAL(10,0)) AS eoh_units
	, CAST(0 AS DECIMAL(12,2)) AS eoh_dollars_true
	, CAST(0 AS DECIMAL(10,0)) AS eoh_units_true
	, CAST(0 AS DECIMAL(20,2)) as net_sales_reg_dollars
	, CAST(0 AS DECIMAL(12,0)) as net_sales_reg_units
	, CAST(0 AS DECIMAL(20,2)) as net_sales_clr_dollars
	, CAST(0 AS DECIMAL(12,0)) as net_sales_clr_units
	, CAST(0 AS DECIMAL(20,2)) as net_sales_pro_dollars
	, CAST(0 AS DECIMAL(12,0)) as net_sales_pro_units
FROM receipts_base r
JOIN t2dl_das_assortment_dim.sku_cc_lkp  s
  ON r.sku_idnt = s.sku_idnt 
  AND r.channel_country = s.channel_country
LEFT JOIN t2dl_das_assortment_dim.cc_type c
  ON c.customer_choice = s.customer_choice
  AND c.mnth_idnt = r.month_idnt
  AND c.channel_country = r.channel_country
  AND c.channel_brand = r.channel_brand
  AND c.channel = 'DIGITAL'
WHERE c.channel_country = 'US'
  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23;

COLLECT STATS
     PRIMARY INDEX(week_idnt, sku_idnt, channel_brand)
     ON {environment_schema}.digital_selection_reporting_weekly_base{env_suffix};