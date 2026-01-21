/*
Purpose:        Inserts data in table `cc_type` for CC Type
                A CC is considered `NEW` if the CC is first receipted in the month and not in the prior 4 months
                Supporting temp tables:  
                  - receipts_prep: collect receipt history for appropriate receipt types
                  - receipts_monthly: connect receipt location to business unit to get country/brand/channel
Variable(s):    {{environment_schema}} T2DL_DAS_ASSORTMENT_DIM (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' table suffix for prod testing
                {{start_date}} date to start for output data
                {{end_date}} date to end for output data
Author(s):      Sara Riker & Christine Buckler
Updated:        2024-08-23 by Asiyah Fox: filtered receipt locations to align to reporting
*/


CREATE MULTISET VOLATILE TABLE locations AS 
(
SELECT 
	DISTINCT store_num
FROM prd_nap_usr_vws.price_store_dim_vw
WHERE selling_channel = 'ONLINE' -- DIGITAL only
	AND channel_num NOT IN 
		(
		310 --RS NORDSTROM US ONLINE
		,920 -- DC NORDSTROM US ONLINE
		,921 -- DC NORDSTROM CA ONLINE
		,922 -- DC OP CANADA NORDSTROM CA ONLINE
		,930 -- NQC NORDSTROM US STORE/UNKNOWN
		,940 -- NPG NORDSTROM US STORE
		,990 -- FACO NORDSTROM US STORE/UNKNOWN
		)
)
WITH DATA
	PRIMARY INDEX (store_num)
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX(store_num)
	ON locations
;


--Realigned calendar 
CREATE MULTISET VOLATILE TABLE calendar_realigned AS (
		SELECT 
			day_date
			, week_idnt
			, month_idnt
			, week_end_day_date
			, week_start_day_date
			, month_start_day_date
			, month_end_day_date
      , month_start_week_idnt
      , month_end_week_idnt
			, fiscal_month_num as mnth_454
      , fiscal_quarter_num as qtr_454
			, fiscal_year_num as yr_454


		FROM prd_nap_vws.REALIGNED_DATE_LKUP_VW rdlv 
		WHERE day_date <= current_date

	
		UNION ALL
		SELECT 
			day_date
			, week_idnt
			, month_idnt 
			, week_end_day_date
			, week_start_day_date
			, month_start_day_date
			, month_end_day_date
      , month_start_week_idnt
      , month_end_week_idnt
			, fiscal_month_num as mnth_454
      , fiscal_quarter_num as qtr_454
			, fiscal_year_num as yr_454


		FROM prd_nap_usr_vws.DAY_CAL_454_DIM dcd 
		WHERE fiscal_year_num NOT IN (SELECT DISTINCT fiscal_year_num FROM prd_nap_vws.realigned_date_lkup_vw)
		and day_date <= current_date
)

WITH DATA
PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;
--drop table calendar_true
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
		WHERE dcd.fiscal_year_num >= 2019
		and dcd.day_date <= current_Date
		) t 
		ON 
cal.day_date = t.day_date

		
)

WITH DATA
PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;

-- Aggregate monthly receipts in date range
CREATE MULTISET VOLATILE TABLE receipts_prep AS (
--grabbing NAP receipts for 202301 onward and MADM receipts for 202212 and prior (there was a bug identified in the NAP receipts table for data prior to 2023
--which is why we are using MADM data)
	WITH dates AS (
	SELECT 
		week_idnt_true
		  ,week_idnt as week_num
		  ,month_idnt AS mnth_idnt
		  ,month_start_day_date AS mnth_start_day_date
      ,month_start_week_idnt
		  ,mnth_454
   		,qtr_454
    	,yr_454
	FROM calendar
	WHERE mnth_idnt >= 201901
	GROUP BY 1,2,3,4,5,6,7,8
	)
,receipts_base AS (
SELECT 
sku_idnt  AS sku_idnt
	  ,d.week_num
	  , d.mnth_idnt
	  , d.mnth_start_day_date
	  , rcpt.store_num
	  ,SUM(rcpt.receipt_po_units + rcpt.receipt_ds_units) as rcpt_tot_units
FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact rcpt
INNER JOIN locations loc
	ON rcpt.store_num = loc.store_num
JOIN dates d ON rcpt.week_num = d.week_idnt_true 
WHERE  d.mnth_idnt >= 202301
GROUP BY 1,2,3,4,5

UNION ALL

SELECT 
sku_idnt  AS sku_idnt
	  ,d.week_num
	  , d.mnth_idnt
	  , d.mnth_start_day_date
	  , rcpt.store_num
	  ,SUM(rcpt.receipt_po_units + rcpt.receipt_ds_units) as rcpt_tot_units
FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm rcpt
INNER JOIN locations loc
	ON rcpt.store_num = loc.store_num
JOIN dates d ON rcpt.week_num = d.week_idnt_true 
WHERE  d.mnth_idnt < 202301
GROUP BY 1,2,3,4,5
)

SELECT
     sku_idnt  AS sku_idnt
	  ,week_num
    ,mnth_idnt
    ,mnth_start_day_date
	  ,store_num
	  ,rcpt_tot_units
FROM receipts_base rcpt
WHERE rcpt.week_num BETWEEN (SELECT DISTINCT month_start_week_idnt
                            FROM calendar
                            WHERE month_idnt = (SELECT CAST(CONCAT(CAST(CASE WHEN mnth_454 >= 5 THEN yr_454 
                                                                        ELSE (yr_454 - 1) END AS VARCHAR(4))
                                                                  ,LPAD(CAST(CASE WHEN mnth_454 >= 5 THEN mnth_454 - 4 
                                                                      ELSE (mnth_454 + 8) END AS VARCHAR(2)), 2, '0')
                                                                  ) AS INTEGER)
                                                                -- 4 month extension to look for new at the beginning
                                                FROM calendar 
                                                WHERE day_date = {start_date}))
                       AND (SELECT month_end_week_idnt
                            FROM calendar
                            WHERE day_date = {end_date})
AND rcpt_tot_units <> 0
)
WITH DATA
PRIMARY INDEX (sku_idnt, week_num, store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (sku_idnt, week_num, store_num)
    ,COLUMN (sku_idnt)
    ,COLUMN (week_num)
    ,COLUMN (store_num)
    ,COLUMN (week_num, store_num)
    ON receipts_prep;

CREATE MULTISET VOLATILE TABLE receipts_monthly AS (
SELECT
     mnth_idnt
    ,mnth_start_day_date
    ,rp.sku_idnt
    ,channel_country
    ,channel_brand
    ,CASE WHEN selling_channel = 'ONLINE' THEN 'DIGITAL' ELSE selling_channel END as channel 
    ,SUM(rcpt_tot_units) as receipt_units
    ,DENSE_RANK() OVER (ORDER BY mnth_idnt) AS new_cf_rank
FROM receipts_prep rp
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON rp.store_num = st.store_num
GROUP BY mnth_idnt, mnth_start_day_date, rp.sku_idnt, channel_country, channel_brand, channel
)
WITH DATA
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(sku_idnt)
    ,COLUMN (mnth_idnt, channel_country, channel)
    ,COLUMN (mnth_idnt, channel_country, channel_brand, new_cf_rank)
    ,COLUMN (channel_country, sku_idnt)
    ON receipts_monthly;

DELETE FROM {environment_schema}.cc_type_realigned{env_suffix}
WHERE mnth_idnt BETWEEN (SELECT DISTINCT month_idnt
                        FROM calendar 
                        WHERE day_date = {start_date})
                   AND (SELECT DISTINCT month_idnt
                        FROM calendar
                        WHERE day_date = {end_date});

INSERT INTO {environment_schema}.cc_type_realigned{env_suffix}
WITH los_ccs AS (
    SELECT DISTINCT
         d.month_idnt AS mnth_idnt
        ,l.channel_country
        ,l.channel_brand
        ,cc.customer_choice
        ,1 AS los_flag
    FROM t2dl_das_site_merch.live_on_site_daily l
    JOIN t2dl_das_assortment_dim.sku_cc_lkp cc
      ON l.sku_id = cc.sku_idnt
     AND l.channel_country = cc.channel_country
    JOIN calendar d
      ON d.day_date = l.day_date
),
rcpts AS (
    SELECT
         rcpt.mnth_idnt
        ,rcpt.mnth_start_day_date
        ,new_cf_rank
        ,rcpt.channel_country
        ,rcpt.channel_brand
        ,rcpt.channel
        ,cc.customer_choice
        ,SUM(receipt_units) AS rcpt_units
        ,LAG(new_cf_rank) OVER (PARTITION BY cc.customer_choice, rcpt.channel_country, rcpt.channel_brand, rcpt.channel ORDER BY new_cf_rank) as mnth_lag
    FROM receipts_monthly rcpt
    JOIN t2dl_das_assortment_dim.sku_cc_lkp cc
      ON cc.sku_idnt = rcpt.sku_idnt
     AND cc.channel_country = rcpt.channel_country
    GROUP BY 1,2,3,4,5,6,7
)
SELECT
     r.mnth_idnt
    ,r.mnth_start_day_date
    ,r.channel_country
    ,r.channel_brand
    ,r.channel
    ,r.customer_choice
    ,COALESCE(l.los_flag, 0) AS los_flag
    ,CASE WHEN new_cf_rank - mnth_lag <= 4 THEN 'CF'
          ELSE 'NEW' END AS cc_type -- considered NEW if receipted within last 4 months
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM rcpts r
LEFT JOIN los_ccs l
  ON r.mnth_idnt = l.mnth_idnt
 AND r.customer_choice = l.customer_choice
 AND r.channel_country = l.channel_country
 AND r.channel_brand = l.channel_brand
WHERE r.mnth_idnt BETWEEN (SELECT DISTINCT month_idnt
                            FROM calendar
                            WHERE day_date = {start_date})
                       AND (SELECT DISTINCT month_idnt
                            FROM calendar
                            WHERE day_date = {end_date});

COLLECT STATS
     PRIMARY INDEX (mnth_idnt, customer_choice)
    ,COLUMN (mnth_idnt, channel_country, customer_choice)
    ,COLUMN (mnth_idnt, channel_country, channel_brand, customer_choice)
    ,COLUMN (mnth_idnt, channel_country,channel_brand, channel, customer_choice)
    ,COLUMN (customer_choice)
    ,COLUMN (channel)
    ,COLUMN (PARTITION)
    ,COLUMN (PARTITION, mnth_idnt, customer_choice)
    ON {environment_schema}.cc_type_realigned{env_suffix};