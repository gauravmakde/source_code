

CREATE TEMPORARY TABLE IF NOT EXISTS sku_01
AS
SELECT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
GROUP BY rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS store_01
AS
SELECT store_num,
 price_store_num,
 store_type_code,
  CASE
  WHEN price_store_num IN (808, 867, 835, 1) OR price_store_num = - 1 AND channel_num = 120
  THEN 'FL'
  WHEN price_store_num IN (844, 828, 338) OR price_store_num = - 1 AND channel_num = 250
  THEN 'RK'
  ELSE NULL
  END AS store_type_code_new,
  CASE
  WHEN price_store_num IN (808, 828, 338, 1)
  THEN 'USA'
  WHEN price_store_num = - 1 AND LOWER(channel_country) = LOWER('US')
  THEN 'USA'
  WHEN price_store_num IN (844, 867, 835)
  THEN 'CAN'
  WHEN price_store_num = - 1 AND LOWER(channel_country) = LOWER('CA')
  THEN 'CAN'
  ELSE NULL
  END AS store_country_code,
  CASE
  WHEN price_store_num = - 1 AND channel_num = 120 OR price_store_num = - 1 AND channel_num = 250
  THEN 'ONLINE'
  ELSE selling_channel
  END AS selling_channel,
 channel_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
GROUP BY store_num,
 price_store_num,
 store_type_code,
 store_type_code_new,
 store_country_code,
 selling_channel,
 channel_num;

--COLLECT STATS 	 PRIMARY INDEX (STORE_NUM) 	,COLUMN (store_num, store_type_code, store_country_code, selling_channel) 	,COLUMN (store_num) 	,COLUMN (selling_channel) 		ON STORE_01
--HELP STATISTICS STORE_01;
-- 2021.07.23 - Create the price layout with store type code
-- 2021.09.08 - Changed out the join to reflect the representative doors
-- 2022.01.15 - Add in Ownership Price Amount to Value Promo Inventory
-- 2022.01.20 - Add in selling channel
---- DROP TABLE PRICE_01;
--,b.pricing_start_tmstp
--,b.pricing_end_tmstp
--,pricing_start_tmstp
--,pricing_end_tmstp
--,14,15

CREATE TEMPORARY TABLE IF NOT EXISTS price_01
AS
SELECT b.rms_sku_num,
 a.store_type_code,
 a.store_country_code,
 a.selling_channel,
 b.channel_country,
 b.ownership_price_type,
 b.ownership_price_amt,
 b.regular_price_amt,
 b.current_price_amt,
 b.current_price_currency_code,
 b.current_price_type,
 b.eff_begin_tmstp,
 b.eff_end_tmstp
FROM (SELECT price_store_num,
   store_type_code_new AS store_type_code,
   store_country_code,
   selling_channel
  FROM store_01
  GROUP BY price_store_num,
   store_type_code,
   store_country_code,
   selling_channel) AS a
 INNER JOIN (SELECT rms_sku_num,
   store_num,
    CASE
    WHEN LOWER(channel_country) = LOWER('US')
    THEN 'USA'
    WHEN LOWER(channel_country) = LOWER('CA')
    THEN 'CAN'
    ELSE NULL
    END AS channel_country,
    CASE
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
    THEN 'R'
    ELSE ownership_retail_price_type_code
    END AS ownership_price_type,
   ownership_retail_price_amt AS ownership_price_amt,
   regular_price_amt,
   selling_retail_price_amt AS current_price_amt,
   selling_retail_currency_code AS current_price_currency_code,
    CASE
    WHEN LOWER(selling_retail_price_type_code) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(selling_retail_price_type_code) = LOWER('REGULAR')
    THEN 'R'
    WHEN LOWER(selling_retail_price_type_code) = LOWER('PROMOTION')
    THEN 'P'
    ELSE selling_retail_price_type_code
    END AS current_price_type,
   eff_begin_tmstp,
   eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim
  WHERE CAST(eff_begin_tmstp AS DATE) < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS b ON a.price_store_num = CAST(b.store_num AS FLOAT64)  
GROUP BY b.rms_sku_num,
 a.store_type_code,
 a.store_country_code,
 a.selling_channel,
 b.channel_country,
 b.ownership_price_type,
 b.ownership_price_amt,
 b.regular_price_amt,
 b.current_price_amt,
 b.current_price_currency_code,
 b.current_price_type,
 b.eff_begin_tmstp,
 b.eff_end_tmstp;
--	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,EFF_BEGIN_TMSTP, EFF_END_TMSTP)
--	,INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,EFF_BEGIN_TMSTP)
-- ,COLUMN (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,EFF_BEGIN_TMSTP, EFF_END_TMSTP)
--	,COLUMN (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,EFF_BEGIN_TMSTP, EFF_END_TMSTP)
--COLLECT STATS 	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,eff_begin_tmstp, eff_end_tmstp)   	,COLUMN (RMS_SKU_NUM) 	 	,COLUMN(EFF_BEGIN_TMSTP, EFF_END_TMSTP)  		ON PRICE_01
--HELP STATISTICS PRICE_01;
--------------- END - Baseline Data Pulls
--------------- Start - Transactions
-- Two Parts
-- One for transactions associated to SKU's
-- ONe for transactions not associated to SKU's
-- 2021.07.23 - Isolate and optimize the base transaction data (The ones with sku association at transactions
-- 2022.07.18 - Initial Implementation of COGS
-- 2022.07.22 - Alteration of COGS to change logic for returns/exchanges to the date in which the trans type occurred vs. original transaction tracing
---- DROP TABLE TRANS_BASE_01;
-- 2021.08.11 - Since we're calculating on the tran_time for price type, for returns specifically, we need to identify the price of the original transaction, not the return
-- 2021.09.08 - There are situations where the return transaction can't associate to the original transaction leaving the business date null. In these situations, we will have to go with what the transaction of the return is as we have no other reference point for this.
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' AND a.LINE_NET_USD_AMT <= 0 THEN COALESCE(CAST(a.ORIGINAL_BUSINESS_DATE AS TIMESTAMP), a.TRAN_TIME)
-- 2022.07.25 - New Tran Time for the purposes of COGS.
-- This is different than the tran_time above because for returns/exchanges, we are considering WAC for the time in which the transaction type occurred, not original state.
-- 2021.08.11 - We want to capture the original_ringing_store_num for returns to track the price type
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
-- 2022.07.25 - We want to capture the ringing_store_num for returns/exchanges for the purposes of COGS calculation
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
-- 2022.01.15 - Flag items with special ad hoc promo adjustments to track the price type
-- 2023.4.13 - change price type logic to match with merch table
-- 2022.01.15 - Add in tran line number to capture correct qty for multiple of same sku in one transaction
-- 2022.12.27 - Add in dropship indicator to properly join in wac on business date for these orders
-- Only capture merch related transactions
-- 2021.09.08 - Incorporating a UPC check. This is due to restaurant and gift boxing where "sku's" exist but with no correlating UPC.
-- If the downstream consumers want this, just remove the UPC check in this build.
-- This will not remove any merch merch related items where there is a correlating UPC, for instance, mugs, etc.
-- Gift Box items also come in SKU_NUM = 6 where there is an UPC of 000000000
-- Remove Last Chance Doors
CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_01a
AS
SELECT sku_num,
 business_day_date,
 tran_date,
  CASE
  WHEN LOWER(tran_type_code) = LOWER('RETN')
  THEN COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(original_business_date AS DATETIME)) AS DATETIME), tran_time
   )
  ELSE CAST(tran_time AS DATETIME)
  END AS tran_time,
 tran_time AS tran_time_new,
 intent_store_num,
  CASE
  WHEN LOWER(tran_type_code) = LOWER('RETN')
  THEN COALESCE(original_ringing_store_num, intent_store_num)
  ELSE intent_store_num
  END AS intent_store_num_new,
  CASE
  WHEN LOWER(tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
  THEN COALESCE(original_ringing_store_num, ringing_store_num, intent_store_num)
  ELSE COALESCE(fulfilling_store_num, ringing_store_num, intent_store_num)
  END AS intent_store_num_cogs,
 tran_type_code,
  CASE
  WHEN line_item_promo_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS promo_flag,
 line_net_usd_amt,
 line_net_amt,
 line_item_quantity,
 line_item_seq_num,
  CASE
  WHEN LOWER(line_item_fulfillment_type) = LOWER('VendorDropShip')
  THEN 1
  ELSE 0
  END AS dropship_fulfilled
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS a
WHERE CAST(upc_num AS FLOAT64) > 0
 AND business_day_date BETWEEN  {{params.start_date}} AND {{params.end_date}}
 AND LOWER(line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND intent_store_num NOT IN (260, 770)
 AND sku_num IS NOT NULL
GROUP BY sku_num,
 business_day_date,
 tran_date,
 tran_time,
 tran_time_new,
 intent_store_num,
 intent_store_num_new,
 intent_store_num_cogs,
 tran_type_code,
 promo_flag,
 line_net_usd_amt,
 line_net_amt,
 line_item_quantity,
 line_item_seq_num,
 dropship_fulfilled;
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, TRAN_TIME) 	,COLUMN (SKU_NUM, TRAN_TIME) 		ON TRANS_BASE_01A
-- 2021.07.26 - Isolate and optimize the base transaction data (The ones without sku association at transaction)
-- 2021.08.11 - Since we're calculating on the tran_time for price type, for returns specifically, we need to identify the price of the original transaction, not the return
-- 2022.07.25 - New Tran Time for the purposes of COGS.
-- This is different than the tran_time above because for returns/exchanges, we are considering WAC for the time in which the transaction type occurred, not original state.
-- 2021.08.11 - We want to capture the original_ringing_store_num for returns to track the price type
-- 2022.07.25 - We want to capture the ringing_store_num for returns/exchanges for the purposes of COGS calculation
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
-- 2022.01.15 - Flag items with special ad hoc promo adjustments to track the price type
-- 2023.4.13 - change price type logic to match with merch table
-- 2022.01.15 - Add in tran line number to capture correct qty for multiple of same sku in one transaction
-- 2022.12.27 - Add in dropship indicator to properly join in wac on business date for these orders
-- Only capture merch related transactions
-- Remove Last Chance Doors
INSERT INTO trans_base_01a
(SELECT b.rms_sku_num,
  a.business_day_date,
  a.tran_date,
   CASE
   WHEN LOWER(a.tran_type_code) = LOWER('RETN')
   THEN COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(a.original_business_date AS DATETIME)) AS DATETIME), a.tran_time
    )
   ELSE CAST(a.tran_time AS DATETIME)
   END AS tran_time,
  a.tran_time AS tran_time_new,
  a.intent_store_num,
   CASE
   WHEN LOWER(a.tran_type_code) = LOWER('RETN')
   THEN COALESCE(a.original_ringing_store_num, a.intent_store_num)
   ELSE a.intent_store_num
   END AS intent_store_num_new,
   CASE
   WHEN LOWER(a.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
   THEN COALESCE(a.original_ringing_store_num, a.ringing_store_num, a.intent_store_num)
   ELSE COALESCE(a.fulfilling_store_num, a.ringing_store_num, a.intent_store_num)
   END AS intent_store_num_cogs,
  a.tran_type_code,
   CASE
   WHEN a.line_item_promo_id IS NOT NULL
   THEN 1
   ELSE 0
   END AS promo_flag,
  a.line_net_usd_amt,
  a.line_net_amt,
  a.line_item_quantity,
  a.line_item_seq_num,
   CASE
   WHEN LOWER(a.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 1
   ELSE 0
   END AS dropship_fulfilled
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS b ON LOWER(a.upc_num) = LOWER(b.upc_num)
 WHERE a.sku_num IS NULL
  AND a.business_day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
  AND LOWER(a.line_item_merch_nonmerch_ind) = LOWER('MERCH')
  AND a.intent_store_num NOT IN (260, 770)
 GROUP BY b.rms_sku_num,
  a.business_day_date,
  a.tran_date,
  tran_time,
  tran_time_new,
  a.intent_store_num,
  intent_store_num_new,
  intent_store_num_cogs,
  a.tran_type_code,
  promo_flag,
  a.line_net_usd_amt,
  a.line_net_amt,
  a.line_item_quantity,
  a.line_item_seq_num,
  dropship_fulfilled);
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM,TRAN_TIME), 	INDEX (INTENT_STORE_NUM), 	COLUMN (SKU_NUM), 	COLUMN (SKU_NUM,TRAN_TIME), 	COLUMN (INTENT_STORE_NUM) 		ON TRANS_BASE_01A
/*
 * On 2022.07.19, for returns/exchanges, it was decided by stakeholders that WAC calculations would be based on the return date vs. original transaction date
 * As such, the extra processing to identify the returns is no longer needed.
-- 2022.06.06 - Identify the returns and exchanges and their original sales timestamp
-- DROP TABLE TRANS_RETURN_01;
CREATE MULTISET VOLATILE TABLE trans_return_01
AS
(
select a.sku_num
	,a.business_day_date
	,a.tran_time
	,b.tran_time tran_time_original
	,a.intent_store_num
	,a.intent_store_num_new
	,a.tran_type_code
	,a.original_tran_num
	,a.original_transaction_identifier
	,a.original_business_date
from trans_base_01A a left join
	`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw b
		on a.sku_num = b.sku_num
			and a.original_business_date = b.business_day_date
			and a.original_transaction_identifier = b.transaction_identifier
where a.tran_type_code in ('EXCH','RETN')
group by 1,2,3,4,5,6,7,8,9,10
)
WITH DATA
PRIMARY INDEX (SKU_NUM,TRAN_TIME)
INDEX (INTENT_STORE_NUM)
ON COMMIT PRESERVE ROWS;
COLLECT STATS
	PRIMARY INDEX ( SKU_NUM, TRAN_TIME )
	,COLUMN ( SKU_NUM, TRAN_TIME )
		ON TRANS_RETURN_01;
-- 2022.06.09 - Creation of Sales TRAN with original transaction's timestamp for returns/exchanges
-- DROP TABLE TRANS_BASE_01;
CREATE MULTISET VOLATILE TABLE TRANS_BASE_01B
AS
(
select a.sku_num
	,a.business_day_date
	,a.tran_date
	,a.tran_time
	,b.tran_time_original
	,a.intent_store_num
	,a.intent_store_num_new
	,a.tran_type_code
	,a.promo_flag
	,a.line_net_usd_amt
	,a.line_net_amt
	,a.line_item_quantity
	,a.line_item_seq_num
	,a.original_tran_num
	,a.original_transaction_identifier
	,a.original_business_date
from trans_base_01a a left join
	trans_return_01 b
		on a.sku_num = b.sku_num
			and a.business_day_date = b.business_day_date
			and a.tran_time = b.tran_time
			and a.intent_store_num = b.intent_store_num
			and a.tran_type_code = b.tran_type_code
			and a.original_transaction_identifier = b.original_transaction_identifier
)
WITH DATA
PRIMARY INDEX ( SKU_NUM, TRAN_TIME )
INDEX ( INTENT_STORE_NUM )
ON COMMIT PRESERVE ROWS;
COLLECT STATS
	PRIMARY INDEX (SKU_NUM,TRAN_TIME),
	INDEX (INTENT_STORE_NUM),
	COLUMN (SKU_NUM),
	COLUMN (SKU_NUM,TRAN_TIME),
	COLUMN (INTENT_STORE_NUM)
		ON TRANS_BASE_01B;
*/
-- Outstanding question - What timezone is this start_date/end_date at? Currency
-- Outstanding question - What timezone is the WAC data at?
-- WAC is dependent on the time in which the original transaction occurred.
-- For Returns/Exchanges, if the return/exchange can be sourced back to the original transaction, the WAC is taken from that day/original location
-- If not, then it is by default taken from the return/exchange time of transaction and the location as well as we have no other method of identifying the original sale
-- drop table trans_base_01;
--	,a.tran_time_original
--	,a.original_tran_num
--	,a.original_transaction_identifier
--	,a.original_business_date
-- 2022.10.18 - Using market rate
--	,a.market_rate
-- 2022.10.18 - Using market rate
--	,CASE
--		when a.weighted_average_cost_currency_code = 'CAD' then a.weighted_average_cost * a.market_rate
--		else a.weighted_average_cost
--	 END weighted_average_cost_new
--		,a.tran_time_original
--		,a.original_tran_num
--		,a.original_transaction_identifier
--		,a.original_business_date
-- 2022.10.18 - Using market rate
--		,b.market_rate
-- 2022.10.18 - Using market rate
--			when a.tran_type_code = 'EXCH' and a.line_net_usd_amt <=0 then c.weighted_average_cost * -1
--			when a.tran_type_code = 'EXCH' and a.line_net_usd_amt > 0 then c.weighted_average_cost
-- 2022.10.18 - Using market rate
--left join
-- We don't need the entirety of the exchange rate, just CAD to USD conversion and the default start date of when our pipeline began.
--		(select start_date
--			,end_date
--			,market_rate
--		 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.currency_exchg_rate_dim
--		-- 2022.07.22 - Dropping the date range as the source table is not partitioned by any date and is hindering the processing time instead of helping.
----		 where start_date between '2019-01-01' and current_date
--		 where from_country_currency_code = 'CAD'
--		 	and to_country_currency_code = 'USD'
--		 group by 1,2,3) b
--		on cast(a.tran_time  as date) between b.start_date and b.end_date left join
----			on coalesce(cast(a.tran_time_original as date),cast(a.tran_time as date)) between b.start_date and b.end_date
-- There are timestamp time zone conversion issue her. To match to actual timestamps, we're decreasing the timestamp by 7 hours
-- Timestamp for eff_end_tmstp is less interval 1 millisecond to eliminate duplications of WAC if timestamp to millisecond matches
--				and coalesce(a.tran_time_original,a.tran_time) - interval '7' hour between c.weighted_average_cost_changed_tmstp and c.eff_end_tmstp 
--				and a.intent_store_num_new = c.location_num
--	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_01
AS
SELECT a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 a.intent_store_num_new,
 a.tran_type_code,
 a.promo_flag,
 a.line_net_usd_amt,
 a.line_net_amt,
 a.line_item_quantity,
 a.line_item_seq_num,
 c.weighted_average_cost_currency_code,
  CASE
  WHEN LOWER(a.tran_type_code) = LOWER('SALE') AND a.dropship_fulfilled = 1
  THEN COALESCE(c.weighted_average_cost, d.weighted_average_cost)
  WHEN LOWER(a.tran_type_code) = LOWER('SALE')
  THEN c.weighted_average_cost
  WHEN LOWER(a.tran_type_code) = LOWER('RETN')
  THEN c.weighted_average_cost * - 1
  WHEN LOWER(a.tran_type_code) = LOWER('EXCH') AND a.line_net_amt <= 0
  THEN c.weighted_average_cost * - 1
  ELSE c.weighted_average_cost
  END AS weighted_average_cost
FROM trans_base_01a AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.weighted_average_cost_dim AS c 
 ON LOWER(a.sku_num) = LOWER(c.sku_num) 
 AND DATETIME_ADD(a.tran_time ,INTERVAL 8 HOUR) BETWEEN CAST(c.weighted_average_cost_changed_tmstp AS DATETIME) AND CAST((CAST(c.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
      AND a.intent_store_num_cogs = CAST(c.location_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.weighted_average_cost_date_dim AS d ON LOWER(a.sku_num) = LOWER(d.sku_num) AND a.business_day_date
    BETWEEN d.eff_begin_dt AND (DATE_SUB(d.eff_end_dt, INTERVAL 1 DAY)) AND a.intent_store_num_cogs = CAST(d.location_num AS FLOAT64);
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM,TRAN_TIME), 	INDEX (INTENT_STORE_NUM), 	COLUMN (SKU_NUM), 	COLUMN (SKU_NUM,TRAN_TIME), 	COLUMN (INTENT_STORE_NUM) 		ON TRANS_BASE_01
-- 2021.07.23 - Identify for each transaction which store_type it belongs to
-- Since we are taking original transaction identifier's off, this will drop records from total as they appear as duplicates but are not. As such, they need to be summed to correlate to what occurred.
---- DROP TABLE TRANS_BASE_02;
-- 2022.06.10 - weighted average cost new is the converted and uniform USD weighted average cost
-- 2022.10.18 - Using market rate
--	,a.WEIGHTED_AVERAGE_COST_NEW AS WEIGHTED_AVERAGE_COST
--PRIMARY INDEX (SKU_NUM)
CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_02
AS
SELECT a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 b.price_store_num,
 b.selling_channel,
 a.tran_type_code,
 SUM(a.line_net_usd_amt) AS line_net_usd_amt,
 SUM(a.line_net_amt) AS line_net_amt,
 SUM(a.line_item_quantity) AS line_item_quantity,
 a.line_item_seq_num,
 a.promo_flag,
 b.store_type_code,
 b.store_country_code,
 a.weighted_average_cost
FROM trans_base_01 AS a
 LEFT JOIN store_01 AS b ON a.intent_store_num = b.store_num
GROUP BY a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 b.price_store_num,
 b.selling_channel,
 a.tran_type_code,
 a.line_item_seq_num,
 a.promo_flag,
 b.store_type_code,
 b.store_country_code,
 a.weighted_average_cost;
--	PRIMARY INDEX (SKU_NUM)
--	,COLUMN (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME)
--	,COLUMN (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME) 	,INDEX (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)  	,COLUMN  (SKU_NUM)   		ON TRANS_BASE_02
--HELP STATISTICS TRANS_BASE_02;
-- 2021.07.23 - Associate price type to each transaction based on product's item price categorized by store channel
-- This step really could just be the aggregate step, however, it's being placed in more granularity for troubleshooting purposes
---- DROP TABLE TRANS_BASE_03;
--	,b.store_num
--	,a.store_type_code
--	,b.channel_country
--	,b.ownership_price_type
-- 2022.01.15 - If an item is promo on clearance, count as clearance. Also code items with ad hoc adjustments as promo
-- 2023.4.13 - updating price type logic to align more closely with merch table, but still bucketing Clearance on Promo into Clearance and retaining visibility into N/A--which we typically roll into Regular on the frontend
--	,b.pricing_start_tmstp
--	,b.pricing_end_tmstp
--	,b.eff_begin_tmstp
--	,b.eff_end_tmstp
CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_03
AS
SELECT a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 a.price_store_num,
 a.tran_type_code,
 a.line_net_usd_amt,
 a.line_net_amt,
 a.line_item_quantity,
 a.line_item_seq_num,
 a.store_type_code,
 b.rms_sku_num,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt,
 b.selling_retail_currency_code AS current_price_currency_code,
  CASE
  WHEN LOWER(b.ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  WHEN a.promo_flag = 1
  THEN 'P'
  ELSE COALESCE(CASE
    WHEN LOWER(b.selling_retail_price_type_code) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
    THEN 'R'
    WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
    THEN 'P'
    ELSE b.selling_retail_price_type_code
    END, 'N/A')
  END AS current_price_type,
 a.weighted_average_cost,
  a.weighted_average_cost * a.line_item_quantity AS cost_of_goods_sold
FROM trans_base_02 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b ON LOWER(a.sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
      = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) AND a.tran_time BETWEEN CAST(b.eff_begin_tmstp AS DATETIME)
   AND CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
GROUP BY a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 a.price_store_num,
 a.tran_type_code,
 a.line_net_usd_amt,
 a.line_net_amt,
 a.line_item_quantity,
 a.line_item_seq_num,
 a.store_type_code,
 b.rms_sku_num,
 b.regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 a.weighted_average_cost;
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, TRAN_TIME)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, TRAN_TIME) 	,COLUMN (SKU_NUM) 	,COLUMN (SKU_NUM, BUSINESS_DAY_DATE)  		ON TRANS_BASE_03
-- Validation - How many SKU's are unidentifiable
--select count(*) from trans_base_03 where sku_num is NULL'
-- 2021.08.02 - Creation of Final Transaction related data for final aggregates and creation of row_id
-- DROP TABLE TRANS_BASE_FINAL;
--2022.10.18 - Using market rate currency
--	,sum(line_net_usd_amt) net_usd_sls_dollar
--	,SUM(LINE_NET_AMT) NET_SLS_dollar
-- capture only when there is a return recorded
--2022.10.18 - Using market rate currency
--		when min(line_net_usd_amt) < 0 then (min(line_net_usd_amt) * -1)
--		end return_usd_dollar
--	,MIN(LINE_NET_AMT) RETURN_dollar
/* I was originally splitting out sales vs. returns so that you could calculate between the two, however, MFP aggregates both in sales measurements
	,max(CASE
		when tran_type_code = 'SALE' then line_item_quantity
		else null
		end) sls_units
*/
--	,max(line_net_amt) net_sls_dollar
--	,min(line_net_amt) return_dollar
--		,TRAN_DATE
--		,TRAN_TIME
--2022.10.18 - Using market rate currency
--			when tran_type_code = 'EXCH' and line_net_usd_amt >= 0 then 'SALE'
--			when tran_type_code = 'EXCH' and line_net_usd_amt < 0 then 'RETN'
--2022.10.18 - Using market rate currency
--		,sum(line_net_usd_amt) line_net_usd_amt
--	where business_day_date between '2021-07-18' and '2021-07-24'
--		and line_item_merch_nonmerch_ind = 'MERCH'
CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_final
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', business_day_date) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_' ||
      TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 business_day_date,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(line_net_amt) AS net_sls_dollar,
  CASE
  WHEN MIN(line_net_amt) < 0
  THEN MIN(line_net_amt) * - 1
  ELSE NULL
  END AS return_dollar,
 SUM(CASE
   WHEN LOWER(tran_type_code) = LOWER('RETN')
   THEN line_item_quantity * - 1
   ELSE line_item_quantity
   END) AS sls_units,
 MAX(CASE
   WHEN LOWER(tran_type_code) = LOWER('RETN')
   THEN line_item_quantity
   ELSE NULL
   END) AS return_units,
 SUM(cost_of_goods_sold) AS cost_of_goods_sold
FROM (SELECT sku_num,
   business_day_date,
   intent_store_num,
   store_type_code,
    CASE
    WHEN LOWER(tran_type_code) = LOWER('EXCH') AND line_net_amt >= 0
    THEN 'SALE'
    WHEN LOWER(tran_type_code) = LOWER('EXCH') AND line_net_amt < 0
    THEN 'RETN'
    ELSE tran_type_code
    END AS tran_type_code,
   regular_price_amt,
   current_price_amt,
   current_price_currency_code,
   current_price_type,
   SUM(line_net_amt) AS line_net_amt,
   SUM(line_item_quantity) AS line_item_quantity,
   SUM(cost_of_goods_sold) AS cost_of_goods_sold
  FROM trans_base_03
  GROUP BY sku_num,
   business_day_date,
   intent_store_num,
   store_type_code,
   tran_type_code,
   regular_price_amt,
   current_price_amt,
   current_price_currency_code,
   current_price_type) AS t0
GROUP BY row_id,
 sku_num,
 business_day_date,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 	,COLUMN (SKU_NUM) 		ON TRANS_BASE_FINAL
--------------- End - Transactions
--------------- Start - Error Logging - Transactions
-- Identify any records of which will be considered an error for this process
-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
-- Only one record will be determined to pass on until it is resolved
-- 2021.08.17 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'TRANS_BASE_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as  process_timestamp_tz
 FROM trans_base_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) 
AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);
--------------- End - Error Logging - Transactions
--------------- Begin: Demand - Dollars/Units
-- 2021.09.22 - Creation of the baseline demand dataset
---- DROP TABLE DEMAND_BASE_01;
-----Converted till here
-- 2022.01.05 - Only show node when its a pick-up order and should be attributed to stores
-- 2024.05.02 - Adding Marketplace for location attribution
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
-- 2022.01.05 - Identify items with special ad hoc promo adjustments (doesnt include employee discounts) to bucket as promo price later
-- 2022.10.18 - Using market rate
--	,case when coalesce(order_line_promotion_discount_amount_usd,0) - coalesce(order_line_employee_discount_amount_usd,0) > 0 then 1 else 0 end as promo_flag
-- 2022.01.05 - Add source store to identify DTCs and attribut to stores
-- 2021.10.21 - Incorporated country code separation to enable identification between N.COM vs. N.CA
-- 2023.11.04 - Fraud Transactions identification 
-- 2023.11.04 - Same day cancellations identification
CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_01
AS
SELECT sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
  CASE
  WHEN LOWER(delivery_method_code) = LOWER('PICK')
  THEN destination_node_num
  ELSE NULL
  END AS destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount, 0) - COALESCE(order_line_employee_discount_amount, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(COALESCE(fraud_cancel_ind, 'N')) <> LOWER('Y')
 AND order_date_pacific <> COALESCE(canceled_date_pacific, DATE '1900-01-01')
GROUP BY sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id;
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON DEMAND_BASE_01
-- 2021.09.22 - Identify the Demand Units and dollar
---- DROP TABLE DEMAND_BASE_02;
-- 2021.09.22 - TRANSACTIONS WHERE THEY ARE 808 OR 828 ARE REPRESENTED BY NULL VALUES. As such, we need to place a numeric value to it to associate it for price
-- 2024.05.02 - Marketplace location
--BOPUS goes to stores
-- 2022.01.15 - Incorporated source platform separation to split out DTC orders and attribute to source location
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
-- 2022.01.15 Create price store num to join price data in on
-- 2024.05.02 - Marketplace price store
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
-- 2022.01.05 - Add in Order Line Id to Track multiple skus per order correctly
CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_02
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.prc_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.promo_flag,
 a.order_line_id,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   order_date_pacific,
   destination_node_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 5405
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 808
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('US')
    THEN 808
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('CA')
    THEN 867
    WHEN LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS prc_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific,
   shipped_date_pacific,
   promo_flag,
   order_line_id
  FROM demand_base_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.prc_store_num = b.store_num;
--	,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)  		ON DEMAND_BASE_02
-- 2021.09.22 - Identification of Price at point in time for each transaction
-- -- DROP TABLE DEMAND_BASE_03;
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
-- 2022.01.05 - Make sure ad hoc adjustments go under promo and promo under clearance goes under clearance
CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_03
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.price_store_num,
 a.order_line_id,
 b.rms_sku_num,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt,
 b.selling_retail_currency_code AS current_price_currency_code,
  CASE
  WHEN a.promo_flag = 1 AND LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'P'
  WHEN LOWER(b.ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  ELSE CASE
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('CLEARANCE')
   THEN 'C'
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
   THEN 'R'
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
   THEN 'P'
   ELSE b.selling_retail_price_type_code
   END
  END AS current_price_type
FROM demand_base_02 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b 
 ON LOWER(a.sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) 
 AND a.order_tmstp_pacific  BETWEEN CAST(b.eff_begin_tmstp AS DATETIME) AND  CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
GROUP BY a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.price_store_num,
 a.order_line_id,
 b.rms_sku_num,
 b.regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)  		ON DEMAND_BASE_03
-- 2021.09.22 - Creation of Final Demand dollar/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE DEMAND_BASE_FINAL;
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) DEMAND_DOLLARS
---2024-03-05 to filter blank SKUs
CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_final
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', order_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_'
      || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS demand_dollars,
 SUM(order_line_quantity) AS demand_units
FROM demand_base_03
WHERE LOWER(sku_num) <> LOWER('')
GROUP BY row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON DEMAND_BASE_FINAL
--------------- End - Demand - Units/Dollars
--------------- Start - Error Logging - Demand - Units/Dollars
-- Identify any records of which will be considered an error for this process
-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
-- Only one record will be determined to pass on until it is resolved
-- 2021.09.22 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'DEMAND_BASE_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM demand_base_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);
--------------- End - Error Logging - Demand - Units/Dollars
--------------- Begin: Demand - Dropship
-- 2022.01.05 - Identification of Dropship Items Based on Order Table
---- DROP TABLE DEMAND_DROPSHIP_01;
-- 2022.01.05 - Only show node when its a pick-up order and should be attributed to stores
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
-- 2022.01.05 - Identify items with special ad hoc promo adjustments (doesnt include employee discounts) to bucket as promo price later
-- 2022.10.18 - Using market rate
--	,case when coalesce(order_line_promotion_discount_amount_usd,0) - coalesce(order_line_employee_discount_amount_usd,0) > 0 then 1 else 0 end as promo_flag
-- 2022.01.05 - Add source store to identify DTCs and attribut to stores
-- 2021.10.21 - Incorporated country code separation to enable identification between N.COM vs. N.CA
-- 2023.11.04 - Fraud transactions identification
-- 2023.11.04 - Same day cancellations identification
CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_01
AS
SELECT sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
  CASE
  WHEN LOWER(delivery_method_code) = LOWER('PICK')
  THEN destination_node_num
  ELSE NULL
  END AS destination_node_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount, 0) - COALESCE(order_line_employee_discount_amount, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(first_released_node_type_code) = LOWER('DS')
 AND LOWER(COALESCE(fraud_cancel_ind, 'N')) <> LOWER('Y')
 AND order_date_pacific <> COALESCE(canceled_date_pacific, DATE '1900-01-01')
GROUP BY sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
 destination_node_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id;
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON DEMAND_DROPSHIP_01
-- 2021.09.22 - Identify the Demand Units and dollar
---- DROP TABLE DEMAND_DROPSHIP_02;
-- 2021.09.22 - TRANSACTIONS WHERE THEY ARE 808 OR 828 ARE REPRESENTED BY NULL VALUES. As such, we need to place a numeric value to it to associate it for price
-- 2022.01.15 - Incorporated source platform separation to split out DTC orders and attribute to source location
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
-- 2022.01.15 Create price store num to join price data in on
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
-- 2022.01.05 - Add in Order Line Id to Track multiple skus per order correctly
CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_02
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.prc_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.promo_flag,
 a.order_line_id,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   order_date_pacific,
   destination_node_num,
    CASE
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
    CASE
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('US')
    THEN 808
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('CA')
    THEN 867
    WHEN LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS prc_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific,
   shipped_date_pacific,
   promo_flag,
   order_line_id
  FROM demand_dropship_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.prc_store_num = b.store_num;
--	,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)  		ON DEMAND_DROPSHIP_02
-- 2021.09.22 - Identification of Price at point in time for each transaction
-- -- DROP TABLE DEMAND_DROPSHIP_03;
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
-- 2022.01.05 - Make sure ad hoc adjustments go under promo and promo under clearance goes under clearance
CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_03
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.price_store_num,
 a.order_line_id,
 b.rms_sku_num,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt,
 b.selling_retail_currency_code AS current_price_currency_code,
  CASE
  WHEN a.promo_flag = 1 AND LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'P'
  WHEN LOWER(b.ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  ELSE CASE
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('CLEARANCE')
   THEN 'C'
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
   THEN 'R'
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
   THEN 'P'
   ELSE b.selling_retail_price_type_code
   END
  END AS current_price_type
FROM demand_dropship_02 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b ON LOWER(a.sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
      = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) AND a.order_tmstp_pacific
   BETWEEN CAST(b.eff_begin_tmstp AS DATETIME) AND  CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
GROUP BY a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.price_store_num,
 a.order_line_id,
 b.rms_sku_num,
 b.regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)  		ON DEMAND_DROPSHIP_03
-- 2021.09.22 - Creation of Final Demand dollar/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE DEMAND_DROPSHIP_FINAL;
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) AS DEMAND_DROPSHIP_DOLLARS
---2024-03-05 to filter blank SKUs
CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_final
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', order_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_'
      || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS demand_dropship_dollars,
 SUM(order_line_quantity) AS demand_dropship_units
FROM demand_dropship_03
WHERE LOWER(sku_num) <> LOWER('')
GROUP BY row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON DEMAND_DROPSHIP_FINAL
--------------- End - Demand - Dropship
--------------- Start - Error Logging - Demand - Dropship
-- Identify any records of which will be considered an error for this process
-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
-- Only one record will be determined to pass on until it is resolved
-- 2021.09.22 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'DEMAND_DROPSHIP_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM demand_dropship_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);
--------------- End - Error Logging - Demand - Dropship
--------------- Begin: Demand - CANCELS
-- 2021.09.22 - Identification of the cancelled orders dataset
-- 2022.04.07 - Alteration to demand cancels to include exclusion of fraud and same day cancels
-- DROP TABLE DEMAND_CANCEL_01;
-- 2024.05.02 - Adding Marketplace for location attribution
-- 2022.10.18 - Using market rate
--    ,order_line_current_amount_usd
/* Exclude Cancels because of Fraud */
/* Exclude Same-Day cancels */
CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_01
AS
SELECT sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 canceled_tmstp_pacific,
 canceled_date_pacific,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 cancel_reason_code,
 fraud_cancel_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE canceled_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND canceled_date_pacific <> order_date_pacific
 AND LOWER(cancel_reason_code) NOT LIKE LOWER('%FRAUD%')
GROUP BY sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 canceled_tmstp_pacific,
 canceled_date_pacific,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 cancel_reason_code,
 fraud_cancel_ind;
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON DEMAND_CANCEL_01
-- 2021.09.22 - Identify the Price Types for Price association for each transaction at time of purchase
---- DROP TABLE DEMAND_CANCEL_02;
-- 2021.09.22 - TRANSACTIONS WHERE THEY ARE 808 OR 828 ARE REPRESENTED BY NULL VALUES. As such, we need to place a numeric value to it to associate it for price
-- 2024.05.02 - Marketplace location
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_02
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.canceled_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   canceled_tmstp_pacific,
   destination_node_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 5405
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific
  FROM demand_cancel_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.intent_store_num = b.store_num;
-- ,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC) 	 	,COLUMN (ORDER_TMSTP_PACIFIC) 		ON DEMAND_CANCEL_02
-- 2021.09.22 - Identification of Price at point in time for each transaction
-- -- DROP TABLE DEMAND_CANCEL_03;
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_03
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.canceled_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.price_store_num,
 b.rms_sku_num,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt,
 b.selling_retail_currency_code AS current_price_currency_code,
  CASE
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'P'
  ELSE b.selling_retail_price_type_code
  END AS current_price_type
FROM demand_cancel_02 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b ON LOWER(a.sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
      = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) AND a.order_tmstp_pacific
   BETWEEN CAST(b.eff_begin_tmstp AS DATETIME) AND CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
GROUP BY a.sku_num,
 a.order_tmstp_pacific,
 a.canceled_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.price_store_num,
 b.rms_sku_num,
 b.regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)  		ON DEMAND_CANCEL_03
-- 2021.09.22 - Creation of Final Demand Cancel dollar/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE DEMAND_CANCEL_FINAL;
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) DEMAND_CANCEL_DOLLARS
CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_final
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', canceled_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) ||
       '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 canceled_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS demand_cancel_dollars,
 SUM(order_line_quantity) AS demand_cancel_units
FROM demand_cancel_03
GROUP BY row_id,
 sku_num,
 canceled_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON DEMAND_CANCEL_FINAL
--------------- End - Demand - CANCELS
--------------- Start - Error Logging - Demand - Cancelled
-- Identify any records of which will be considered an error for this process
-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
-- Only one record will be determined to pass on until it is resolved
-- 2021.09.22 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'DEMAND_CANCEL_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM demand_cancel_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);
--------------- End - Error Logging - Demand - Cancelled
--------------- Begin: Store Fulfill - Dollars/Units
-- 2021.10.13 - Creation of baseline order's that were fulfilled by stores
-- drop table store_fulfill_01;
--	,sku_type
--	,delivery_method_code
--	,destination_node_num
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
--	,shipped_tmstp_pacific
--	,shipped_date_pacific
--	,order_line_id
CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_01
AS
SELECT sku_num,
 rms_sku_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 fulfilled_node_num,
 fulfilled_node_type_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE fulfilled_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND fulfilled_node_num IS NOT NULL
GROUP BY sku_num,
 rms_sku_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 fulfilled_node_num,
 fulfilled_node_type_code;
--collect stats 	primary index ( sku_num, fulfilled_tmstp_pacific ) 	,column ( sku_num, fulfilled_tmstp_pacific ) 		on store_fulfill_01
-- 2021.10.13 - Identify the store fulfill units & dollar's and prep for price association
-- drop table store_fulfill_02;
-- 2022.10.18 - Using market rate
--		,order_line_current_amount_usd
CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_02
AS
SELECT a.sku_num,
 a.fulfilled_tmstp_pacific,
 a.fulfilled_date_pacific,
 a.fulfilled_node_num,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.order_tmstp_pacific,
 b.price_store_num,
 b.store_type_code,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   fulfilled_tmstp_pacific,
   fulfilled_date_pacific,
   fulfilled_node_num,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific,
   order_tmstp_pacific
  FROM store_fulfill_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.fulfilled_node_num = b.store_num;
--	,column ( sku_num, price_store_num, selling_channel, fulfilled_tmstp_pacific )
--collect stats 	primary index ( sku_num, price_store_num, selling_channel, fulfilled_tmstp_pacific )  		on store_fulfill_02
-- 2021.10.13 - Identification of PRice at point in time for each transaction
-- drop table store_fulfill_03;
-- 2022.10.18 - Using market rate
--	,a.order_line_current_amount_usd
--	,a.order_tmstp_pacific
CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_03
AS
SELECT a.sku_num,
 a.fulfilled_tmstp_pacific,
 a.fulfilled_date_pacific,
 a.fulfilled_node_num,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.price_store_num,
 b.rms_sku_num,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt,
 b.selling_retail_currency_code AS current_price_currency_code,
  CASE
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'P'
  ELSE b.selling_retail_price_type_code
  END AS current_price_type
FROM store_fulfill_02 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b ON LOWER(a.sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
      = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) AND a.order_tmstp_pacific
   BETWEEN CAST(b.eff_begin_tmstp AS DATETIME) AND CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME);
--collect stats 	primary index ( sku_num, fulfilled_node_num, fulfilled_tmstp_pacific ) 		on store_fulfill_03
-- 2021.10.13 - Creation of final store fulfill dollar/units related dataset for final aggregate and creation of row_id
-- drop table store_fulfill_final;
-- 2022.10.18 - Using market rate
--	,sum(order_line_current_amount_usd) store_fulfill_dollars
CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_final
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', fulfilled_date_pacific) || '_' || TRIM(FORMAT('%11d', fulfilled_node_num)) ||
       '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 fulfilled_date_pacific,
 fulfilled_node_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS store_fulfill_dollars,
 SUM(order_line_quantity) AS store_fulfill_units
FROM store_fulfill_03
GROUP BY row_id,
 sku_num,
 fulfilled_date_pacific,
 fulfilled_node_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--collect stats 	primary index ( row_id ) 	,column ( row_id ) 		on store_fulfill_final
--------------- End: Store Fulfill - Dollars/Units
--------------- Start - Error Logging - Store Fulfill - Units/Dollars
-- Identify any records of which will be considered an error for this process
-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
-- Only one record will be determined to pass on until it is resolved
-- 2021.10.12 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'STORE_FULFILL_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM store_fulfill_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) 
AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);
--------------- End - Error Logging - Store Fulfill - Units/Dollars
--------------- Begin: Order Ship
-- 2021.09.23 - Identification of the Shipped orders dataset
-- DROP TABLE ORDER_SHIP_01;
-- 2022.03.15 - Only show node when its a pick-up order and should be attributed to stores
-- 2024.05.02 - Adding Marketplace for location attribution
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
-- 2022.03.15 - Identify items with special ad hoc promo adjustments (doesnt include employee discounts) to bucket as promo price later
-- 2022.10.18 - Using market rate
--    ,CASE WHEN COALESCE(order_line_promotion_discount_amount_usd,0) - COALESCE(order_line_employee_discount_amount_usd,0) > 0 THEN 1 ELSE 0 END AS promo_flag
-- 2022.03.15 - Add source store to identify DTCs and attribute to stores
-- 2022.03.15 - Add in Order Line Id to Track multiple skus per order correctly
CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_01
AS
SELECT sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 sku_type,
 delivery_method_code,
  CASE
  WHEN LOWER(delivery_method_code) = LOWER('PICK')
  THEN destination_node_num
  ELSE NULL
  END AS destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount, 0) - COALESCE(order_line_employee_discount_amount, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
 source_store_num,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 order_line_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE shipped_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
 promo_flag,
 source_store_num,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 order_line_id;
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON ORDER_SHIP_01
-- 2021.09.23 - Identify the Price Types for Price association for each transaction at time of purchase
---- DROP TABLE ORDER_SHIP_02;
-- 2024.05.02 - Marketplace location
-- 2022.03.15 - Incorporated source platform separation to split out DTC orders and attribute to source location
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
-- 2022.03.15 Create price store num to join price data in on
-- 2024.05.02 - Marketplace price store
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
-- 2022.03.15 - Add in Order Line Id to Track multiple skus per order correctly
CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_02
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.shipped_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.prc_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.shipped_date_pacific,
 a.promo_flag,
 a.order_line_id,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   shipped_tmstp_pacific,
   destination_node_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 5405
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 808
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('US')
    THEN 808
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('CA')
    THEN 867
    WHEN LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS prc_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   shipped_date_pacific,
   promo_flag,
   order_line_id
  FROM order_ship_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.prc_store_num = b.store_num;
--	,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)  		ON ORDER_SHIP_02
-- 2021.09.23 - Identification of Price at point in time for each transaction
-- -- DROP TABLE ORDER_SHIP_03;
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
-- 2022.03.15 - Make sure ad hoc adjustments go under promo and promo under clearance goes under clearance
CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_03
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.shipped_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.shipped_date_pacific,
 a.price_store_num,
 a.order_line_id,
 b.rms_sku_num,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt,
 b.selling_retail_currency_code AS current_price_currency_code,
  CASE
  WHEN a.promo_flag = 1 AND LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'P'
  WHEN LOWER(b.ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  ELSE CASE
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('CLEARANCE')
   THEN 'C'
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
   THEN 'R'
   WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
   THEN 'P'
   ELSE b.selling_retail_price_type_code
   END
  END AS current_price_type
FROM order_ship_02 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b 
 ON LOWER(a.sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
      = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) AND a.order_tmstp_pacific
   BETWEEN CAST(b.eff_begin_tmstp AS DATETIME) AND  CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
GROUP BY a.sku_num,
 a.order_tmstp_pacific,
 a.shipped_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.shipped_date_pacific,
 a.price_store_num,
 a.order_line_id,
 b.rms_sku_num,
 b.regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)  		ON ORDER_SHIP_03
-- 2021.09.22 - Creation of Final Order Ship dollar/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE ORDER_SHIP_FINAL;
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) SHIPPED_DOLLARS
---2024-03-05 to filter blank SKUs
CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_final
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', shipped_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_'
        || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 shipped_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS shipped_dollars,
 SUM(order_line_quantity) AS shipped_units
FROM order_ship_03
WHERE LOWER(sku_num) <> LOWER('')
GROUP BY row_id,
 sku_num,
 shipped_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;
--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON ORDER_SHIP_FINAL
--------------- End - Order Ship
--------------- Start - Error Logging - Order Ship
-- Identify any records of which will be considered an error for this process
-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
-- Only one record will be determined to pass on until it is resolved
-- 2021.09.23 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'ORDER_SHIP_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM order_ship_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);
--------------- End - Error Logging - Order Ship
--------------- Inventory (SOH)
-- 2021.08.02 - Build out of SOH for each execution
-- 2022.01.25 - Added BOH
-- 2022.02.09 - Created a sku_eoh table to make sku_soh_01 more performant
-- 2022.02.09 - NULL-ed out BOH because of bugs
-- 2022.02.17 - Reverted back to structure before adding BOH on 1.25, added BOH/EOH logic
-- 2023.05.01 - Added new Inventory Source Table, excluded DS and DS_OP location types from SOH due to duplication issue
-- -- DROP TABLE SKU_SOH_01;
--2023-12-13 derived field from Logical fact table
--2023-12-13 direct field from Logical fact table
-- Added location_type to associate dropship inventory
--2023-12-13 changed PHYSICAL_FACT to LOGICAL_FACT table
CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_01
AS
SELECT rms_sku_id,
 snapshot_date,
 DATE_ADD(snapshot_date, INTERVAL 1 DAY) AS snapshot_date_boh,
 value_updated_time,
 location_id,
  COALESCE(stock_on_hand_qty, 0) - COALESCE(unavailable_qty, 0) AS immediately_sellable_qty,
 COALESCE(stock_on_hand_qty, 0) AS stock_on_hand_qty,
 COALESCE(unavailable_qty, 0) AS nonsellable_qty,
 location_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_logical_fact
WHERE snapshot_date BETWEEN {{params.start_date_soh}} AND {{params.end_date}}
GROUP BY rms_sku_id,
 snapshot_date,
 snapshot_date_boh,
 value_updated_time,
 location_id,
 immediately_sellable_qty,
 stock_on_hand_qty,
 nonsellable_qty,
 location_type;
--COLLECT STATS 	PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) 	,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) 	,COLUMN (RMS_SKU_ID) 	,COLUMN (LOCATION_ID) 		ON SKU_SOH_01
---- DROP TABLE sku_soh_02;
-- NEW --
--WHERE a.location_type IS NULL                            ----2023-12-13 location_type has no Null values in logical_fact table
CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_02
AS
SELECT a.rms_sku_id,
 b.store_type_code,
 b.store_type_code_new,
 b.store_country_code,
 b.selling_channel,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time,
 a.location_id,
 a.immediately_sellable_qty,
 a.stock_on_hand_qty,
 a.nonsellable_qty,
 a.location_type
FROM sku_soh_01 AS a
 LEFT JOIN store_01 AS b ON CAST(a.location_id AS FLOAT64) = b.store_num
GROUP BY a.rms_sku_id,
 b.store_type_code,
 b.store_type_code_new,
 b.store_country_code,
 b.selling_channel,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time,
 a.location_id,
 a.immediately_sellable_qty,
 a.stock_on_hand_qty,
 a.nonsellable_qty,
 a.location_type;
--collect stats 	primary index ( rms_sku_id, store_type_code, store_country_code, snapshot_date) 	,column ( rms_sku_id, store_type_code, store_country_code, snapshot_date) 		on sku_soh_02
-- 2021.08.09 - Incorporate STORE TYPE in to SOH
-- 2021.08.10 - Incorporated dropship override WHERE location_id's equate to vendorr #
-- 2022.02.17 - Added row_id and snapshot date columns for BOH
-- 2022.02.23 - Added min() function to price type and amount to avoid duplicate rows
-- drop table sku_soh_final;
-- 2021.10.26 - Incorporated SOH dollars as we're switching from sellable SOH to SOH as it aligns better with MADM measurements
-- 2022.01.15 - For inventory, use ownership price type instead of current
-- If there is no (0) SOH for immediate, stock_on_hand or nonsellable, remove to not waste space
--where rms_sku_id = '25256294'
--	and location_id = '551'
--order by rms_sku_id
--sample 100
CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_final
AS
SELECT a.rms_sku_id || '_' || FORMAT_DATE('%F', a.snapshot_date) || '_' || TRIM(a.location_id) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
          , b.regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', MIN(b.current_price_amt)),
       1, 10), '')) || '_' || COALESCE(MIN(b.ownership_price_type), '') AS row_id,
           a.rms_sku_id || '_' || FORMAT_DATE('%F', a.snapshot_date_boh) || '_' || TRIM(a.location_id) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
          , b.regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', MIN(b.current_price_amt)),
       1, 10), '')) || '_' || COALESCE(MIN(b.ownership_price_type), '') AS row_id_boh,
 a.rms_sku_id,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time,
 a.location_id,
 a.immediately_sellable_qty,
  a.immediately_sellable_qty * b.ownership_price_amt AS immediately_sellable_dollars,
 a.stock_on_hand_qty,
  a.stock_on_hand_qty * b.ownership_price_amt AS stock_on_hand_dollars,
 a.nonsellable_qty,
 a.store_type_code,
 a.store_country_code,
 MIN(b.ownership_price_type) AS current_price_type,
 MIN(b.current_price_amt) AS current_price_amt,
 b.regular_price_amt,
 a.location_type
FROM sku_soh_02 AS a
 LEFT JOIN price_01 AS b ON LOWER(a.rms_sku_id) = LOWER(b.rms_sku_num) AND LOWER(a.store_type_code_new) = LOWER(b.store_type_code
       ) AND LOWER(a.store_country_code) = LOWER(b.channel_country) AND LOWER(a.selling_channel) = LOWER(b.selling_channel
     ) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(a.snapshot_date AS DATETIME)) AS DATETIME) BETWEEN  CAST(b.eff_begin_tmstp AS DATETIME) AND CAST((CAST(b.eff_end_tmstp AS TIMESTAMP) - interval '0.001' second ) AS DATETIME)
WHERE a.immediately_sellable_qty <> 0
 OR a.stock_on_hand_qty <> 0
 OR a.nonsellable_qty <> 0
GROUP BY a.rms_sku_id,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time,
 a.location_id,
 a.immediately_sellable_qty,
 immediately_sellable_dollars,
 a.stock_on_hand_qty,
 stock_on_hand_dollars,
 a.nonsellable_qty,
 a.store_type_code,
 a.store_country_code,
 b.regular_price_amt,
 a.location_type;
--collect stats 	primary index (row_id) 	,column (row_id) 		on sku_soh_final
/*
-- Validation
	select *
	from sku_soh_final
	where strtok(row_id,'_',6) is null
	sample 100;
*/
--------------- End - SOH
--------------- Start - Error Logging - SOH
-- 2021.08.17 - Error Logs
-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM sku_soh_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);


-- 2022.02.25 - Error Logs (BOH)
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id_boh AS row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id_boh , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL_BOH' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM sku_soh_final
 GROUP BY row_id
 HAVING COUNT(row_id_boh) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);

-- 2022.02.28 - Error Logs To Catch Erroneous SOH (EOH) records
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL_BAD' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM sku_soh_final
 WHERE stock_on_hand_dollars > 9999999999.99) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);

-- 2022.02.28 - Error Logs To Catch Erroneous SOH (BOH) records
MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} AS a
USING (SELECT row_id_boh AS row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id_boh , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL_BAD_BOH' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
 FROM sku_soh_final
 WHERE stock_on_hand_dollars > 9999999999.99) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = b.process_timestamp,
 process_timestamp_tz = b.process_timestamp_tz
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, b.process_timestamp, b.process_timestamp_tz);

CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline_union
AS
((((((SELECT row_id
      FROM trans_base_final
      UNION DISTINCT
      SELECT row_id
      FROM demand_base_final)
     UNION DISTINCT
     SELECT row_id
     FROM demand_dropship_final)
    UNION DISTINCT
    SELECT row_id
    FROM demand_cancel_final)
   UNION DISTINCT
   SELECT row_id
   FROM order_ship_final)
  UNION DISTINCT
  SELECT row_id
  FROM sku_soh_final)
 UNION DISTINCT
 SELECT row_id_boh AS row_id
 FROM sku_soh_final)
UNION DISTINCT
SELECT row_id
FROM store_fulfill_final;
--COLLECT STATS COLUMN(row_id) ON final_baseline_union
-- drop table final_baseline_01;

CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline_01
AS
SELECT row_id,
  CASE
  WHEN REGEXP_EXTRACT_ALL(row_id, '[^_]+')[SAFE_OFFSET(0)] IS NULL OR REGEXP_EXTRACT_ALL(row_id, '[^_]+')[SAFE_OFFSET(1
       )] IS NULL OR REGEXP_EXTRACT_ALL(row_id, '[^_]+')[SAFE_OFFSET(2)] IS NULL OR REGEXP_EXTRACT_ALL(row_id, '[^_]+')
    [SAFE_OFFSET(5)] IS NULL
  THEN 'Y'
  ELSE 'N'
  END AS incomplete_ind
FROM (SELECT a.row_id
  FROM final_baseline_union AS a
  LEFT JOIN (SELECT row_id
    FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day_error_logs
    WHERE process_dt = CURRENT_DATE('PST8PDT')
    GROUP BY row_id) AS b 
  ON LOWER(a.row_id) = LOWER(b.row_id)
  WHERE b.row_id IS NULL) AS t2
WHERE PARSE_DATE('%F', REGEXP_EXTRACT_ALL(t2.row_id , r'[^_]+') [OFFSET ( 1 ) ]) BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND t2.row_id IS NOT NULL;

CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline_02
AS
SELECT row_id,
 REGEXP_EXTRACT_ALL(row_id, '[^_]+')[SAFE_OFFSET(0)] AS sku_num,
 PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS day_dt,
 REGEXP_EXTRACT_ALL(row_id, '[^_]+')[SAFE_OFFSET(5)] AS price_type,
 REGEXP_EXTRACT_ALL(row_id, '[^_]+')[SAFE_OFFSET(2)] AS loc_idnt,
 incomplete_ind
FROM final_baseline_01 AS a;

CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline
AS
SELECT a.row_id,
 a.sku_num,
 a.day_dt,
 a.price_type,
 a.loc_idnt,
 b.sls_units AS sales_units,
 b.net_sls_dollar AS sales_dollars,
 b.return_units,
 b.return_dollar AS return_dollars,
 b.cost_of_goods_sold,
  CASE
  WHEN st.channel_num IN (110, 111, 210, 211)
  THEN COALESCE(b.sls_units, 0) + COALESCE(b.return_units, 0)
  ELSE CAST(h.demand_units AS BIGNUMERIC)
  END AS demand_units,
  CASE
  WHEN st.channel_num IN (110, 111, 210, 211)
  THEN COALESCE(b.net_sls_dollar, 0) + COALESCE(b.return_dollar, 0)
  ELSE h.demand_dollars
  END AS demand_dollars,
 i.demand_cancel_units,
 i.demand_cancel_dollars,
 j.demand_dropship_units,
 j.demand_dropship_dollars,
 k.shipped_units,
 k.shipped_dollars,
 m.store_fulfill_units,
 m.store_fulfill_dollars,
 c.stock_on_hand_qty AS eoh_units,
 c.stock_on_hand_dollars AS eoh_dollars,
 n.stock_on_hand_qty AS boh_units,
 n.stock_on_hand_dollars AS boh_dollars,
 c.nonsellable_qty AS nonsellable_units,
 NULL AS receipt_units,
 NULL AS receipt_dollars,
 NULL AS receipt_po_units,
 NULL AS receipt_po_dollars,
 NULL AS receipt_pah_units,
 NULL AS receipt_pah_dollars,
 NULL AS receipt_dropship_units,
 NULL AS receipt_dropship_dollars,
 NULL AS receipt_reservestock_units,
 NULL AS receipt_reservestock_dollars,
 COALESCE(b.current_price_amt, c.current_price_amt, h.current_price_amt, i.current_price_amt, j.current_price_amt, k.current_price_amt
  , m.current_price_amt, n.current_price_amt) AS current_price,
 COALESCE(b.regular_price_amt, c.regular_price_amt, h.regular_price_amt, i.regular_price_amt, j.regular_price_amt, k.regular_price_amt
  , m.regular_price_amt, n.regular_price_amt) AS regular_price,
 a.incomplete_ind,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS update_timestamp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP ) AS process_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as update_timestamp_tz,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as process_timestamp_tz
FROM final_baseline_02 AS a
 LEFT JOIN trans_base_final AS b ON LOWER(a.row_id) = LOWER(b.row_id)
 LEFT JOIN sku_soh_final AS c ON LOWER(a.row_id) = LOWER(c.row_id)
 LEFT JOIN demand_base_final AS h ON LOWER(a.row_id) = LOWER(h.row_id)
 LEFT JOIN demand_cancel_final AS i ON LOWER(a.row_id) = LOWER(i.row_id)
 LEFT JOIN demand_dropship_final AS j ON LOWER(a.row_id) = LOWER(j.row_id)
 LEFT JOIN order_ship_final AS k ON LOWER(a.row_id) = LOWER(k.row_id)
 LEFT JOIN store_fulfill_final AS m ON LOWER(a.row_id) = LOWER(m.row_id)
 LEFT JOIN sku_soh_final AS n ON LOWER(a.row_id) = LOWER(n.row_id_boh) AND a.row_id IS NOT NULL
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CAST(CASE
    WHEN a.loc_idnt = ''
    THEN '0'
    ELSE a.loc_idnt
    END AS INTEGER) = st.store_num
GROUP BY a.row_id,
 a.sku_num,
 a.day_dt,
 a.price_type,
 a.loc_idnt,
 sales_units,
 sales_dollars,
 b.return_units,
 return_dollars,
 b.cost_of_goods_sold,
 demand_units,
 demand_dollars,
 i.demand_cancel_units,
 i.demand_cancel_dollars,
 j.demand_dropship_units,
 j.demand_dropship_dollars,
 k.shipped_units,
 k.shipped_dollars,
 m.store_fulfill_units,
 m.store_fulfill_dollars,
 eoh_units,
 eoh_dollars,
 boh_units,
 boh_dollars,
 nonsellable_units,
 receipt_units,
 current_price,
 regular_price,
 a.incomplete_ind,
 update_timestamp,
 receipt_dollars,
 receipt_po_units,
 receipt_po_dollars,
 receipt_pah_units,
 receipt_pah_dollars,
 receipt_dropship_units,
 receipt_dropship_dollars,
 receipt_reservestock_units,
 receipt_reservestock_dollars,
 process_timestamp;
