--------------- Start - Baseline Data Pulls
	-- 2021.07.23 - Create initial baseline datasets for references
	-- 2022.02.08 - Change all VOLATILE TABLES to MULTISET for performance  

/********* NOTE ******* Begin
 * This portion is only for the development phase of the project. Remove this before transitioning from T3 to T2
 */

---------- NOTE ------- END

-- 2021.07.20 - Capture the holistic unique sku list as baseline
-- -- DROP TABLE SKU_01;
create multiset volatile table sku_01 
as
(
	select rms_sku_num
--		,epm_sku_num
	from prd_nap_usr_vws.product_sku_dim
	group by 1
)
with DATA
PRIMARY INDEX (RMS_SKU_NUM)
on commit preserve rows;
-- 2021.07.26: Total Unique SKU's: 46,533,501

-- select count(distinct rms_sku_num) from sku_01;
-- 46,455,108

-- 2021.07.23 - Create store reference data
---- DROP TABLE STORE_01;
create multiset volatile TABLE STORE_01
AS
(
SELECT store_num
	,price_store_num
	,store_type_code
-- 2021.08.11 - Had to begin incorporating store type code translations as price type doesn't determine based on them
	-- product_price_dim only holds FC, RK, FL values. For all others, they will be converted to FL or RK
/* 2021.09.08 - Swapped logic to go against the Price representative stores vs. BU's. As such, this process is sunset
	,case
		when store_type_code not in ('FC') and BUSINESS_UNIT_NUM in (1000,4000,6000,9000) THEN 'FL'
		WHEN STORE_TYPE_CODE NOT IN ('FC') AND BUSINESS_UNIT_NUM IN (2000,5000,6500,9500) THEN 'RK'
		ELSE STORE_TYPE_CODE
	 END STORE_TYPE_CODE_NEW
*/
	,CASE
		when price_store_num in (808,867,835,1) then 'FL'
		when price_store_num = -1 and channel_num = 120 then 'FL'
		when price_Store_num in (844,828,338) then 'RK'
		when price_store_num = -1 and channel_num = 250 then 'RK'
		else NULL
	 end STORE_TYPE_CODE_NEW
/* 2021.09.08 - Swapped logic to go against the Price representative stores vs. BU's. As such, this process is sunset
 * 	In theory, this process for the store country doesn't matter though on which method you go with
	,CASE
		WHEN STORE_COUNTRY_CODE = 'US' THEN 'USA'
		WHEN STORE_COUNTRY_CODE = 'CA' THEN 'CAN'
		ELSE NULL
		END STORE_COUNTRY_CODE
*/
	,CASE
		when price_store_num in (808,828,338,1) then 'USA'
		when price_store_num = -1 and channel_country = 'US' then 'USA'
		when price_Store_num in (844,867,835) then 'CAN'
		when price_store_num = -1 and channel_country = 'CA' then 'CAN'
		else NULL
	 end STORE_COUNTRY_CODE
-- 2022.05.17 - Incorporating changes to include Shared Inventory changes
	 ,CASE
		 when price_store_num = -1 and channel_num = 120 then 'ONLINE'
		 when price_store_num = -1 and channel_num = 250 then 'ONLINE'
		 else SELLING_CHANNEL
	 END SELLING_CHANNEL
-- 2021.09.01 - Incorporated Channel Num to identify for PAH and Reserve Stock
	 ,channel_num
FROM prd_nap_usr_vws.price_store_dim_vw
group by 1,2,3,4,5,6,7
)
WITH DATA
PRIMARY INDEX (STORE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	 PRIMARY INDEX (STORE_NUM)
	,COLUMN (store_num, store_type_code, store_country_code, selling_channel)
	,COLUMN (store_num)
	,COLUMN (selling_channel)
		ON STORE_01;

--HELP STATISTICS STORE_01;

-- 2021.07.23 - Create the price layout with store type code
	-- 2021.09.08 - Changed out the join to reflect the representative doors
	-- 2022.01.15 - Add in Ownership Price Amount to Value Promo Inventory
	-- 2022.01.20 - Add in selling channel
---- DROP TABLE PRICE_01;
create multiset volatile TABLE price_01 AS
(
SELECT 
	 b.rms_sku_num
	,a.store_type_code
	,a.store_country_code
    ,a.selling_channel
	,b.channel_country
	,b.ownership_price_type
	,b.ownership_price_amt
	,b.regular_price_amt
	,b.current_price_amt
	,b.current_price_currency_code
	,b.current_price_type
	,b.pricing_start_tmstp
	,b.pricing_end_tmstp
	,b.eff_begin_tmstp
	,b.eff_end_tmstp
FROM (
		SELECT 
			 price_store_num
			,store_type_code_new AS store_type_code
			,store_country_code
            ,selling_channel
		FROM store_01
		GROUP BY 1,2,3,4
	) a 
JOIN (
		SELECT 
			 rms_sku_num
			,store_num
			,channel_country
			,ownership_price_type
			,ownership_price_amt
			,regular_price_amt
			,current_price_amt
			,current_price_currency_code
			,current_price_type
			,pricing_start_tmstp
			,pricing_end_tmstp
			,eff_begin_tmstp
			,eff_end_tmstp
		FROM prd_nap_usr_vws.product_price_forecast_dim
		WHERE CAST(eff_begin_tmstp as DATE) < CURRENT_DATE + 1
	) b
  ON a.price_store_num = b.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
WITH DATA
PRIMARY INDEX (rms_sku_num, store_type_code, channel_country, selling_channel, eff_begin_tmstp, eff_end_tmstp)
ON COMMIT preserve ROWS;

COLLECT STATS
	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,eff_begin_tmstp, eff_end_tmstp)
--	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,EFF_BEGIN_TMSTP, EFF_END_TMSTP)
--	,INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,EFF_BEGIN_TMSTP)
	,COLUMN (RMS_SKU_NUM)
	-- ,COLUMN (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,EFF_BEGIN_TMSTP, EFF_END_TMSTP)
	,COLUMN(EFF_BEGIN_TMSTP, EFF_END_TMSTP)
--	,COLUMN (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,EFF_BEGIN_TMSTP, EFF_END_TMSTP)
		ON PRICE_01;

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
create multiset volatile TABLE TRANS_BASE_01A
AS
(
SELECT a.SKU_NUM
	,a.BUSINESS_DAY_DATE
	,a.TRAN_DATE
-- 2021.08.11 - Since we're calculating on the tran_time for price type, for returns specifically, we need to identify the price of the original transaction, not the return
	-- 2021.09.08 - There are situations where the return transaction can't associate to the original transaction leaving the business date null. In these situations, we will have to go with what the transaction of the return is as we have no other reference point for this.
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(CAST(a.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),a.TRAN_TIME)
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' AND a.LINE_NET_USD_AMT <= 0 THEN COALESCE(CAST(a.ORIGINAL_BUSINESS_DATE AS TIMESTAMP), a.TRAN_TIME)
		ELSE a.TRAN_TIME
	 END TRAN_TIME
-- 2022.07.25 - New Tran Time for the purposes of COGS.
	 -- This is different than the tran_time above because for returns/exchanges, we are considering WAC for the time in which the transaction type occurred, not original state.
	,a.TRAN_TIME as TRAN_TIME_NEW
	,a.INTENT_STORE_NUM
-- 2021.08.11 - We want to capture the original_ringing_store_num for returns to track the price type
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
		ELSE a.INTENT_STORE_NUM
	 END INTENT_STORE_NUM_NEW
-- 2022.07.25 - We want to capture the ringing_store_num for returns/exchanges for the purposes of COGS calculation
	,CASE
		WHEN a.TRAN_TYPE_CODE in ('RETN','EXCH') THEN COALESCE(a.RINGING_STORE_NUM, a.INTENT_STORE_NUM)
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
		ELSE a.INTENT_STORE_NUM
	 END INTENT_STORE_NUM_COGS
	,a.TRAN_TYPE_CODE
-- 2022.01.15 - Flag items with special ad hoc promo adjustments to track the price type
	,CASE WHEN a.LINE_ITEM_PROMO_ID IS NOT NULL OR MERCH_PRICE_ADJUST_REASON IN (22,24) THEN 1 ELSE 0 END AS PROMO_FLAG
	,a.LINE_NET_USD_AMT
	,a.LINE_NET_AMT
	,a.LINE_ITEM_QUANTITY
-- 2022.01.15 - Add in tran line number to capture correct qty for multiple of same sku in one transaction
	,a.LINE_ITEM_SEQ_NUM
-- 2022.06.09 - Addition of return components to trace original sale
--	,a.original_tran_num
--	,a.original_transaction_identifier
--	,original_business_date
--	,CAST(NULL as decimal(38,9)) as weighted_average_cost
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW a 
-- Only capture merch related transactions
-- 2021.09.08 - Incorporating a UPC check. This is due to restaurant and gift boxing where "sku's" exist but with no correlating UPC.
	-- If the downstream consumers want this, just remove the UPC check in this build.
	-- This will not remove any merch merch related items where there is a correlating UPC, for instance, mugs, etc.
	-- Gift Box items also come in SKU_NUM = 6 where there is an UPC of 000000000
WHERE a.SKU_NUM IS NOT NULL
	AND a.UPC_NUM IS NOT NULL
	AND a.UPC_NUM > 0
	AND a.BUSINESS_DAY_DATE BETWEEN {start_date} AND {end_date}
	AND LINE_ITEM_MERCH_NONMERCH_IND = 'MERCH'
-- Remove Last Chance Doors
	AND a.INTENT_STORE_NUM NOT IN (260,770)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
WITH DATA
PRIMARY INDEX (SKU_NUM,TRAN_TIME)
INDEX (INTENT_STORE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, TRAN_TIME)
	,COLUMN (SKU_NUM, TRAN_TIME)
		ON TRANS_BASE_01A;

-- 2021.07.26 - Isolate and optimize the base transaction data (The ones without sku association at transaction)
INSERT INTO TRANS_BASE_01A
SELECT b.SKU_NUM AS RMS_SKU_NUM
	,a.BUSINESS_DAY_DATE
	,a.TRAN_DATE
-- 2021.08.11 - Since we're calculating on the tran_time for price type, for returns specifically, we need to identify the price of the original transaction, not the return
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(CAST(a.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),a.TRAN_TIME)
		ELSE a.TRAN_TIME
	 END TRAN_TIME
-- 2022.07.25 - New Tran Time for the purposes of COGS.
	 -- This is different than the tran_time above because for returns/exchanges, we are considering WAC for the time in which the transaction type occurred, not original state.
	,a.TRAN_TIME as TRAN_TIME_NEW
	,a.INTENT_STORE_NUM
-- 2021.08.11 - We want to capture the original_ringing_store_num for returns to track the price type
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM,a.INTENT_STORE_NUM)
		ELSE a.INTENT_STORE_NUM
	 END INTENT_STORE_NUM_NEW
-- 2022.07.25 - We want to capture the ringing_store_num for returns/exchanges for the purposes of COGS calculation
	,CASE
		WHEN a.TRAN_TYPE_CODE in ('RETN','EXCH') THEN COALESCE(a.RINGING_STORE_NUM, a.INTENT_STORE_NUM)
--		WHEN a.TRAN_TYPE_CODE = 'EXCH' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
		ELSE a.INTENT_STORE_NUM
	 END INTENT_STORE_NUM_COGS
	,a.TRAN_TYPE_CODE
-- 2022.01.15 - Flag items with special ad hoc promo adjustments to track the price type
	,CASE WHEN a.LINE_ITEM_PROMO_ID IS NOT NULL OR MERCH_PRICE_ADJUST_REASON IN (22,24) THEN 1 ELSE 0 END AS PROMO_FLAG
	,a.LINE_NET_USD_AMT
	,a.LINE_NET_AMT
	,a.LINE_ITEM_QUANTITY
-- 2022.01.15 - Add in tran line number to capture correct qty for multiple of same sku in one transaction
	,a.LINE_ITEM_SEQ_NUM
-- 2022.06.09 - Addition of return components to trace original sale
--	,a.original_tran_num
--	,a.original_transaction_identifier
--	,original_business_date
--	,NULL as weighted_average_cost
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW a LEFT JOIN
	PRD_NAP_USR_VWS.PRODUCT_UPC b
		ON a.UPC_NUM = b.UPC_NUM
-- Only capture merch related transactions
WHERE a.SKU_NUM IS NULL
	and a.BUSINESS_DAY_DATE BETWEEN {start_date} AND {end_date}
	AND LINE_ITEM_MERCH_NONMERCH_IND = 'MERCH'
-- Remove Last Chance Doors
	AND a.INTENT_STORE_NUM NOT IN (260,770)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM,TRAN_TIME),
	INDEX (INTENT_STORE_NUM),
	COLUMN (SKU_NUM),
	COLUMN (SKU_NUM,TRAN_TIME),
	COLUMN (INTENT_STORE_NUM)
		ON TRANS_BASE_01A;

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
	prd_nap_usr_vws.retail_tran_detail_fact_vw b 
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
CREATE MULTISET VOLATILE TABLE TRANS_BASE_01
AS 
(
 select a.sku_num
	,a.business_day_date
	,a.tran_date
	,a.tran_time
--	,a.tran_time_original
	,a.intent_store_num
	,a.intent_store_num_new
	,a.tran_type_code
	,a.promo_flag
	,a.line_net_usd_amt
	,a.line_net_amt
	,a.line_item_quantity
	,a.line_item_seq_num
--	,a.original_tran_num
--	,a.original_transaction_identifier
--	,a.original_business_date
-- 2022.10.18 - Using market rate
--	,a.market_rate
	,a.weighted_average_cost_currency_code
	,a.weighted_average_cost
-- 2022.10.18 - Using market rate
--	,CASE 
--		when a.weighted_average_cost_currency_code = 'CAD' then a.weighted_average_cost * a.market_rate
--		else a.weighted_average_cost
--	 END weighted_average_cost_new	
from (
	select a.sku_num
		,a.business_day_date
		,a.tran_date
		,a.tran_time
--		,a.tran_time_original
		,a.intent_store_num
		,a.intent_store_num_new
		,a.tran_type_code
		,a.promo_flag
		,a.line_net_usd_amt
		,a.line_net_amt
		,a.line_item_quantity
		,a.line_item_seq_num
--		,a.original_tran_num
--		,a.original_transaction_identifier
--		,a.original_business_date
-- 2022.10.18 - Using market rate
--		,b.market_rate
		,c.weighted_average_cost_currency_code
		,CASE 
			when a.tran_type_code = 'SALE' then c.weighted_average_cost
			when a.tran_type_code = 'RETN' then c.weighted_average_cost * -1
-- 2022.10.18 - Using market rate
--			when a.tran_type_code = 'EXCH' and a.line_net_usd_amt <=0 then c.weighted_average_cost * -1
--			when a.tran_type_code = 'EXCH' and a.line_net_usd_amt > 0 then c.weighted_average_cost
			when a.tran_type_code = 'EXCH' and a.line_net_amt <=0 then c.weighted_average_cost * -1
			when a.tran_type_code = 'EXCH' and a.line_net_amt > 0 then c.weighted_average_cost
			else c.weighted_average_cost
		END as weighted_average_cost
	from trans_base_01a a
-- 2022.10.18 - Using market rate
	--left join
		-- We don't need the entirety of the exchange rate, just CAD to USD conversion and the default start date of when our pipeline began.
--		(select start_date
--			,end_date
--			,market_rate
--		 from prd_nap_usr_vws.currency_exchg_rate_dim
--		-- 2022.07.22 - Dropping the date range as the source table is not partitioned by any date and is hindering the processing time instead of helping.
----		 where start_date between '2019-01-01' and current_date
--		 where from_country_currency_code = 'CAD'
--		 	and to_country_currency_code = 'USD'
--		 group by 1,2,3) b 
--		on cast(a.tran_time  as date) between b.start_date and b.end_date left join 
----			on coalesce(cast(a.tran_time_original as date),cast(a.tran_time as date)) between b.start_date and b.end_date 
		LEFT JOIN 
		prd_nap_usr_vws.weighted_average_cost_dim c 
			on a.sku_num = c.sku_num
				-- There are timestamp time zone conversion issue her. To match to actual timestamps, we're decreasing the timestamp by 7 hours
				-- Timestamp for eff_end_tmstp is less interval 1 millisecond to eliminate duplications of WAC if timestamp to millisecond matches
				and a.tran_time_new + interval '7' hour between c.weighted_average_cost_changed_tmstp and c.eff_end_tmstp - interval '0.001' second 
--				and coalesce(a.tran_time_original,a.tran_time) - interval '7' hour between c.weighted_average_cost_changed_tmstp and c.eff_end_tmstp - interval '0.001' second 
--				and a.intent_store_num_new = c.location_num
				and a.intent_store_num_cogs = c.location_num
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
	) a
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
		ON TRANS_BASE_01;
	
-- 2021.07.23 - Identify for each transaction which store_type it belongs to
	-- Since we are taking original transaction identifier's off, this will drop records from total as they appear as duplicates but are not. As such, they need to be summed to correlate to what occurred.
---- DROP TABLE TRANS_BASE_02;
create multiset volatile TABLE TRANS_BASE_02
AS
(
SELECT a.SKU_NUM
	,a.BUSINESS_DAY_DATE
	,a.TRAN_DATE
	,a.TRAN_TIME
	,a.INTENT_STORE_NUM
	,b.PRICE_STORE_NUM
	,b.selling_channel
	,a.TRAN_TYPE_CODE
	,SUM(a.LINE_NET_USD_AMT) LINE_NET_USD_AMT
	,SUM(a.LINE_NET_AMT) LINE_NET_AMT
	,SUM(a.LINE_ITEM_QUANTITY) LINE_ITEM_QUANTITY
	,a.LINE_ITEM_SEQ_NUM
	,a.PROMO_FLAG
	,b.STORE_TYPE_CODE
	,b.STORE_COUNTRY_CODE
	-- 2022.06.10 - weighted average cost new is the converted and uniform USD weighted average cost
-- 2022.10.18 - Using market rate
--	,a.WEIGHTED_AVERAGE_COST_NEW AS WEIGHTED_AVERAGE_COST
	,a.WEIGHTED_AVERAGE_COST
FROM TRANS_BASE_01 a left join
	STORE_01 b
		ON a.INTENT_STORE_NUM = b.STORE_NUM
GROUP BY 1,2,3,4,5,6,7,8,12,13,14,15,16
)
WITH DATA
--PRIMARY INDEX (SKU_NUM)
PRIMARY INDEX (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME)
INDEX (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME)
	,INDEX (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)
--	PRIMARY INDEX (SKU_NUM)
	,COLUMN  (SKU_NUM)
--	,COLUMN (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME)
--	,COLUMN (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)
		ON TRANS_BASE_02;	
	
--HELP STATISTICS TRANS_BASE_02;

-- 2021.07.23 - Associate price type to each transaction based on product's item price categorized by store channel
	-- This step really could just be the aggregate step, however, it's being placed in more granularity for troubleshooting purposes
---- DROP TABLE TRANS_BASE_03;
create multiset volatile TABLE TRANS_BASE_03
AS
(
SELECT a.SKU_NUM
	,a.BUSINESS_DAY_DATE
	,a.TRAN_DATE
	,a.TRAN_TIME
	,a.INTENT_STORE_NUM
	,a.price_store_num
	,a.TRAN_TYPE_CODE
	,a.LINE_NET_USD_AMT
	,a.LINE_NET_AMT
	,a.LINE_ITEM_QUANTITY
	,a.LINE_ITEM_SEQ_NUM
	,a.STORE_TYPE_CODE
	,b.rms_sku_num
--	,b.store_num
--	,a.store_type_code
--	,b.channel_country
--	,b.ownership_price_type
	,b.regular_price_amt
	,b.current_price_amt
	,b.current_price_currency_code
-- 2022.01.15 - If an item is promo on clearance, count as clearance. Also code items with ad hoc adjustments as promo
	,case when b.ownership_price_type = 'C' then b.ownership_price_type
	   when a.PROMO_FLAG = 1 and a.TRAN_TYPE_CODE = 'SALE' and b.current_price_type = 'R' then 'P'
	   else b.current_price_type
	 end as current_price_type
--	,b.pricing_start_tmstp
--	,b.pricing_end_tmstp
--	,b.eff_begin_tmstp
--	,b.eff_end_tmstp
	,a.WEIGHTED_AVERAGE_COST
	,a.WEIGHTED_AVERAGE_COST * a.LINE_ITEM_QUANTITY AS COST_OF_GOODS_SOLD
FROM TRANS_BASE_02 a 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM b
		ON a.SKU_NUM = b.RMS_SKU_NUM
			AND a.price_store_num = b.store_num
			AND a.selling_channel = b.selling_channel
      AND a.TRAN_TIME BETWEEN b.eff_begin_tmstp and b.eff_end_tmstp - interval '0.001' second
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,18
)
WITH DATA
PRIMARY INDEX (SKU_NUM,INTENT_STORE_NUM,TRAN_TIME)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, TRAN_TIME)
	,COLUMN (SKU_NUM)
	,COLUMN (SKU_NUM, BUSINESS_DAY_DATE)
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, TRAN_TIME)
		ON TRANS_BASE_03;

-- Validation - How many SKU's are unidentifiable
--select count(*) from trans_base_03 where sku_num is NULL'

-- 2021.08.02 - Creation of Final Transaction related data for final aggregates and creation of row_id
-- DROP TABLE TRANS_BASE_FINAL;
create multiset volatile TABLE TRANS_BASE_FINAL
AS
(
select SKU_NUM||'_'||CAST(BUSINESS_DAY_DATE AS DATE FORMAT 'YYYY-MM-DD')||'_'||TRIM(INTENT_STORE_NUM)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(CURRENT_PRICE_TYPE,'') AS ROW_ID
	,SKU_NUM
	,business_day_date
	,intent_store_num
	,REGULAR_PRICE_AMT
	,CURRENT_PRICE_AMT
	,CURRENT_PRICE_CURRENCY_CODE
	,CURRENT_PRICE_TYPE
	--2022.10.18 - Using market rate currency
--	,sum(line_net_usd_amt) net_usd_sls_$
	,sum(line_net_amt) net_sls_$
--	,SUM(LINE_NET_AMT) NET_SLS_$
-- capture only when there is a return recorded
	,case
	--2022.10.18 - Using market rate currency
--		when min(line_net_usd_amt) < 0 then (min(line_net_usd_amt) * -1)
		when min(line_net_amt) < 0 then (min(line_net_amt) * -1)
		else null
--		end return_usd_$
		end return_$
--	,MIN(LINE_NET_AMT) RETURN_$
/* I was originally splitting out sales vs. returns so that you could calculate between the two, however, MFP aggregates both in sales measurements
	,max(CASE
		when tran_type_code = 'SALE' then line_item_quantity
		else null
		end) sls_units
*/
	,sum(case
		when tran_type_code = 'RETN' then (line_item_quantity * -1)
		else line_item_quantity
	 end) sls_units
	--	,max(line_net_amt) net_sls_$
--	,min(line_net_amt) return_$
	,max(case
		when tran_type_code = 'RETN' then line_item_quantity
		else null
		end)return_units
	,sum(cost_of_goods_sold) cost_of_goods_sold
from (
	select SKU_NUM
		,business_day_date
--		,TRAN_DATE
--		,TRAN_TIME
		,intent_store_num
		,store_type_code
		,CASE
			--2022.10.18 - Using market rate currency
--			when tran_type_code = 'EXCH' and line_net_usd_amt >= 0 then 'SALE'
--			when tran_type_code = 'EXCH' and line_net_usd_amt < 0 then 'RETN'
			when tran_type_code = 'EXCH' and line_net_amt >= 0 then 'SALE'
			when tran_type_code = 'EXCH' and line_net_amt < 0 then 'RETN'
			else tran_type_code
			end tran_type_code
		,REGULAR_PRICE_AMT
		,CURRENT_PRICE_AMT
		,CURRENT_PRICE_CURRENCY_CODE
		,CURRENT_PRICE_TYPE
		--2022.10.18 - Using market rate currency
--		,sum(line_net_usd_amt) line_net_usd_amt
		,sum(line_net_amt) AS line_net_amt
		,sum(line_item_quantity) AS line_item_quantity
		,sum(cost_of_goods_sold) AS cost_of_goods_sold
	from TRANS_BASE_03
--	where business_day_date between '2021-07-18' and '2021-07-24'
--		and line_item_merch_nonmerch_ind = 'MERCH'
	group by 1,2,3,4,5,6,7,8,9) a
group by 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
	,COLUMN (SKU_NUM)
		ON TRANS_BASE_FINAL;

--------------- End - Transactions

--------------- Start - Error Logging - Transactions
	-- Identify any records of which will be considered an error for this process
		-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
			-- Only one record will be determined to pass on until it is resolved

-- 2021.08.17 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'TRANS_BASE_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM trans_base_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Transactions


--------------- Begin: Demand - Dollars/Units

-- 2021.09.22 - Creation of the baseline demand dataset
---- DROP TABLE DEMAND_BASE_01;
create multiset volatile TABLE DEMAND_BASE_01
AS
(
select sku_num
	,rms_sku_num
	,sku_type
	,delivery_method_code
-- 2022.01.05 - Only show node when its a pick-up order and should be attributed to stores
	,case when delivery_method_code = 'PICK' THEN destination_node_num ELSE NULL END AS destination_node_num
	,order_num
	,order_line_num
	,order_tmstp_pacific
	,order_date_pacific
	,order_line_quantity
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
	,order_line_current_amount
-- 2022.01.05 - Identify items with special ad hoc promo adjustments (doesnt include employee discounts) to bucket as promo price later
-- 2022.10.18 - Using market rate
--	,case when coalesce(order_line_promotion_discount_amount_usd,0) - coalesce(order_line_employee_discount_amount_usd,0) > 0 then 1 else 0 end as promo_flag
	,case when coalesce(order_line_promotion_discount_amount,0) - coalesce(order_line_employee_discount_amount,0) > 0 then 1 else 0 end as promo_flag
	,source_channel_code
-- 2022.01.05 - Add source store to identify DTCs and attribut to stores
	,source_store_num
-- 2021.10.21 - Incorporated country code separation to enable identification between N.COM vs. N.CA
	,source_channel_country_code
	,source_platform_code
	,canceled_date_pacific
	,cancel_reason_code
	,fraud_cancel_ind
	,shipped_tmstp_pacific
	,shipped_date_pacific
	,fulfilled_tmstp_pacific
	,fulfilled_date_pacific
	,order_line_id
from prd_nap_usr_vws.order_line_detail_fact
where ORDER_DATE_PACIFIC BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)
WITH DATA
PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
INDEX (DESTINATION_NODE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,INDEX (DESTINATION_NODE_NUM)
		ON DEMAND_BASE_01;

-- 2021.09.22 - Identify the Demand Units and $
---- DROP TABLE DEMAND_BASE_02;
create multiset volatile TABLE DEMAND_BASE_02
AS
(
SELECT a.*
	,b.PRICE_STORE_NUM
	,b.SELLING_CHANNEL
FROM
	(SELECT a.RMS_SKU_NUM AS SKU_NUM
		,a.ORDER_TMSTP_PACIFIC
		,a.ORDER_DATE_PACIFIC
		,a.DESTINATION_NODE_NUM
-- 2021.09.22 - TRANSACTIONS WHERE THEY ARE 808 OR 828 ARE REPRESENTED BY NULL VALUES. As such, we need to place a numeric value to it to associate it for price
		,CASE
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
-- 2022.01.15 - Incorporated source platform separation to split out DTC orders and attribute to source location
      WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
-- 2022.01.15 Create price store num to join price data in on
		,CASE
		  WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN SOURCE_CHANNEL_CODE = 'RACK' THEN 828
		 END PRC_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.CANCELED_DATE_PACIFIC
		,a.SHIPPED_DATE_PACIFIC
		,a.PROMO_FLAG
-- 2022.01.05 - Add in Order Line Id to Track multiple skus per order correctly
	  ,a.ORDER_LINE_ID
	FROM DEMAND_BASE_01 a) a LEFT JOIN
		STORE_01 b
			ON a.PRC_STORE_NUM = b.STORE_NUM
)
WITH DATA
PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
		ON DEMAND_BASE_02;

-- 2021.09.22 - Identification of Price at point in time for each transaction
-- -- DROP TABLE DEMAND_BASE_03;
create multiset volatile TABLE DEMAND_BASE_03
AS
(
SELECT a.SKU_NUM
	,a.ORDER_TMSTP_PACIFIC
	,a.ORDER_DATE_PACIFIC
	,a.DESTINATION_NODE_NUM
	,a.INTENT_STORE_NUM
	,a.DELIVERY_METHOD_CODE
	,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.CANCELED_DATE_PACIFIC
	,a.SHIPPED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,a.ORDER_LINE_ID
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.CURRENT_PRICE_AMT
	,b.CURRENT_PRICE_CURRENCY_CODE
-- 2022.01.05 - Make sure ad hoc adjustments go under promo and promo under clearance goes under clearance
	,CASE WHEN PROMO_FLAG = 1 AND b.current_price_type = 'R' then 'P'
	   WHEN b.ownership_price_type = 'C' then b.ownership_price_type
	   ELSE b.current_price_type
	 END AS current_price_type
FROM DEMAND_BASE_02 a 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM b
		ON a.SKU_NUM = b.RMS_SKU_NUM
			AND a.price_store_num = b.store_num
			AND a.selling_channel = b.selling_channel
			AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
WITH DATA
PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
		ON DEMAND_BASE_03;

-- 2021.09.22 - Creation of Final Demand $/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE DEMAND_BASE_FINAL;
create multiset volatile TABLE DEMAND_BASE_FINAL
AS
(
SELECT SKU_NUM||'_'||CAST(ORDER_DATE_PACIFIC AS DATE FORMAT 'YYYY-MM-DD')||'_'||TRIM(INTENT_STORE_NUM)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(CURRENT_PRICE_TYPE,'') AS ROW_ID
	,SKU_NUM
	,ORDER_DATE_PACIFIC
	,INTENT_STORE_NUM
	,REGULAR_PRICE_AMT
	,CURRENT_PRICE_AMT
	,CURRENT_PRICE_CURRENCY_CODE
	,CURRENT_PRICE_TYPE
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) DEMAND_DOLLARS
	,SUM(ORDER_LINE_CURRENT_AMOUNT) DEMAND_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) DEMAND_UNITS
FROM DEMAND_BASE_03
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON DEMAND_BASE_FINAL;

--------------- End - Demand - Units/Dollars


--------------- Start - Error Logging - Demand - Units/Dollars
	-- Identify any records of which will be considered an error for this process
		-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
			-- Only one record will be determined to pass on until it is resolved

-- 2021.09.22 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'DEMAND_BASE_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM demand_base_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Demand - Units/Dollars


--------------- Begin: Demand - Dropship

-- 2022.01.05 - Identification of Dropship Items Based on Order Table
---- DROP TABLE DEMAND_DROPSHIP_01;
create multiset volatile TABLE DEMAND_DROPSHIP_01
AS
(
select sku_num
	,rms_sku_num
	,sku_type
	,delivery_method_code
-- 2022.01.05 - Only show node when its a pick-up order and should be attributed to stores
	,case when delivery_method_code = 'PICK' THEN destination_node_num ELSE NULL END AS destination_node_num
	,order_num
	,order_line_num
	,order_tmstp_pacific
	,order_date_pacific
	,order_line_quantity
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
	,order_line_current_amount
-- 2022.01.05 - Identify items with special ad hoc promo adjustments (doesnt include employee discounts) to bucket as promo price later
-- 2022.10.18 - Using market rate
--	,case when coalesce(order_line_promotion_discount_amount_usd,0) - coalesce(order_line_employee_discount_amount_usd,0) > 0 then 1 else 0 end as promo_flag
	,case when coalesce(order_line_promotion_discount_amount,0) - coalesce(order_line_employee_discount_amount,0) > 0 then 1 else 0 end as promo_flag
	,source_channel_code
-- 2022.01.05 - Add source store to identify DTCs and attribut to stores
	,source_store_num
-- 2021.10.21 - Incorporated country code separation to enable identification between N.COM vs. N.CA
	,source_channel_country_code
	,source_platform_code
	,canceled_date_pacific
	,cancel_reason_code
	,fraud_cancel_ind
	,shipped_tmstp_pacific
	,shipped_date_pacific
	,fulfilled_tmstp_pacific
	,fulfilled_date_pacific
	,order_line_id
from prd_nap_usr_vws.order_line_detail_fact
where ORDER_DATE_PACIFIC BETWEEN {start_date} AND {end_date}
  and first_released_node_type_code = 'DS'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)
WITH DATA
PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
INDEX (DESTINATION_NODE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,INDEX (DESTINATION_NODE_NUM)
		ON DEMAND_DROPSHIP_01;

-- 2021.09.22 - Identify the Demand Units and $
---- DROP TABLE DEMAND_DROPSHIP_02;
create multiset volatile TABLE DEMAND_DROPSHIP_02
AS
(
SELECT a.*
	,b.PRICE_STORE_NUM
	,b.SELLING_CHANNEL
FROM
	(SELECT a.RMS_SKU_NUM AS SKU_NUM
		,a.ORDER_TMSTP_PACIFIC
		,a.ORDER_DATE_PACIFIC
		,a.DESTINATION_NODE_NUM
-- 2021.09.22 - TRANSACTIONS WHERE THEY ARE 808 OR 828 ARE REPRESENTED BY NULL VALUES. As such, we need to place a numeric value to it to associate it for price
		,CASE
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
-- 2022.01.15 - Incorporated source platform separation to split out DTC orders and attribute to source location
      WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
-- 2022.01.15 Create price store num to join price data in on
		,CASE
		  WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN SOURCE_CHANNEL_CODE = 'RACK' THEN 828
		 END PRC_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.CANCELED_DATE_PACIFIC
		,a.SHIPPED_DATE_PACIFIC
		,a.PROMO_FLAG
-- 2022.01.05 - Add in Order Line Id to Track multiple skus per order correctly
	  ,a.ORDER_LINE_ID
	FROM DEMAND_DROPSHIP_01 a) a LEFT JOIN
		STORE_01 b
			ON a.PRC_STORE_NUM = b.STORE_NUM
)
WITH DATA
PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
		ON DEMAND_DROPSHIP_02;

-- 2021.09.22 - Identification of Price at point in time for each transaction
-- -- DROP TABLE DEMAND_DROPSHIP_03;
create multiset volatile TABLE DEMAND_DROPSHIP_03
AS
(
SELECT a.SKU_NUM
	,a.ORDER_TMSTP_PACIFIC
	,a.ORDER_DATE_PACIFIC
	,a.DESTINATION_NODE_NUM
	,a.INTENT_STORE_NUM
	,a.DELIVERY_METHOD_CODE
	,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.CANCELED_DATE_PACIFIC
	,a.SHIPPED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,a.ORDER_LINE_ID
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.CURRENT_PRICE_AMT
	,b.CURRENT_PRICE_CURRENCY_CODE
-- 2022.01.05 - Make sure ad hoc adjustments go under promo and promo under clearance goes under clearance
	,CASE WHEN PROMO_FLAG = 1 AND b.current_price_type = 'R' then 'P'
	   WHEN b.ownership_price_type = 'C' then b.ownership_price_type
	   ELSE b.current_price_type
	 END AS current_price_type
FROM DEMAND_DROPSHIP_02 a 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM b
		ON a.SKU_NUM = b.RMS_SKU_NUM
			AND a.price_store_num = b.store_num
			AND a.selling_channel = b.selling_channel
			AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
WITH DATA
PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
		ON DEMAND_DROPSHIP_03;

-- 2021.09.22 - Creation of Final Demand $/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE DEMAND_DROPSHIP_FINAL;
create multiset volatile TABLE DEMAND_DROPSHIP_FINAL
AS
(
SELECT SKU_NUM||'_'||CAST(ORDER_DATE_PACIFIC AS DATE FORMAT 'YYYY-MM-DD')||'_'||TRIM(INTENT_STORE_NUM)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(CURRENT_PRICE_TYPE,'') AS ROW_ID
	,SKU_NUM
	,ORDER_DATE_PACIFIC
	,INTENT_STORE_NUM
	,REGULAR_PRICE_AMT
	,CURRENT_PRICE_AMT
	,CURRENT_PRICE_CURRENCY_CODE
	,CURRENT_PRICE_TYPE
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) AS DEMAND_DROPSHIP_DOLLARS
	,SUM(ORDER_LINE_CURRENT_AMOUNT) AS DEMAND_DROPSHIP_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) AS DEMAND_DROPSHIP_UNITS
FROM DEMAND_DROPSHIP_03
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON DEMAND_DROPSHIP_FINAL;


--------------- End - Demand - Dropship


--------------- Start - Error Logging - Demand - Dropship
	-- Identify any records of which will be considered an error for this process
		-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
			-- Only one record will be determined to pass on until it is resolved

-- 2021.09.22 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'DEMAND_DROPSHIP_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM demand_dropship_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Demand - Dropship


--------------- Begin: Demand - CANCELS

-- 2021.09.22 - Identification of the cancelled orders dataset
	-- 2022.04.07 - Alteration to demand cancels to include exclusion of fraud and same day cancels
---- DROP TABLE DEMAND_CANCEL_01;
create multiset volatile TABLE DEMAND_CANCEL_01
AS
(
select sku_num
    ,rms_sku_num
    ,order_tmstp_pacific
    ,CANCELED_TMSTP_PACIFIC
    ,canceled_date_pacific
    ,sku_type
    ,delivery_method_code
    ,destination_node_num
    ,order_num
    ,order_line_num
    ,order_line_quantity
-- 2022.10.18 - Using market rate
--    ,order_line_current_amount_usd
    ,order_line_current_amount
    ,source_channel_code
    ,source_channel_country_code
    ,source_platform_code
    ,cancel_reason_code
    ,fraud_cancel_ind
from prd_nap_usr_vws.order_line_detail_fact
where canceled_date_pacific BETWEEN {start_date} AND {end_date}
    /* Exclude Cancels because of Fraud */
    and cancel_reason_code not like '%FRAUD%'
   /* Exclude Same-Day cancels */
   and canceled_date_pacific <> order_date_pacific
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
WITH DATA
PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
INDEX (DESTINATION_NODE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,INDEX (DESTINATION_NODE_NUM)
		ON DEMAND_CANCEL_01;

-- 2021.09.22 - Identify the Price Types for Price association for each transaction at time of purchase
---- DROP TABLE DEMAND_CANCEL_02;
CREATE MULTISET VOLATILE TABLE DEMAND_CANCEL_02
AS
(
SELECT a.*
	,b.PRICE_STORE_NUM
	,b.SELLING_CHANNEL
FROM
	(SELECT a.RMS_SKU_NUM AS SKU_NUM
		,a.ORDER_TMSTP_PACIFIC
		,a.CANCELED_TMSTP_PACIFIC
		,a.DESTINATION_NODE_NUM
-- 2021.09.22 - TRANSACTIONS WHERE THEY ARE 808 OR 828 ARE REPRESENTED BY NULL VALUES. As such, we need to place a numeric value to it to associate it for price
		,CASE
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.CANCELED_DATE_PACIFIC
	FROM DEMAND_CANCEL_01 a) a 
	LEFT JOIN STORE_01 b
	  ON a.INTENT_STORE_NUM = b.STORE_NUM
)
WITH DATA
PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
	-- ,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
	,COLUMN (ORDER_TMSTP_PACIFIC)
		ON DEMAND_CANCEL_02;

-- 2021.09.22 - Identification of Price at point in time for each transaction
-- -- DROP TABLE DEMAND_CANCEL_03;
CREATE MULTISET VOLATILE TABLE DEMAND_CANCEL_03
AS
(
SELECT a.SKU_NUM
	,a.ORDER_TMSTP_PACIFIC
	,a.CANCELED_TMSTP_PACIFIC
	,a.DESTINATION_NODE_NUM
	,a.INTENT_STORE_NUM
	,a.DELIVERY_METHOD_CODE
	,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.CANCELED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.CURRENT_PRICE_AMT
	,b.CURRENT_PRICE_CURRENCY_CODE
	,b.CURRENT_PRICE_TYPE
FROM DEMAND_CANCEL_02 a 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM b
		ON a.SKU_NUM = b.RMS_SKU_NUM
			AND a.price_store_num = b.store_num
			AND a.selling_channel = b.selling_channel
			AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
WITH DATA
PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
		ON DEMAND_CANCEL_03;

-- 2021.09.22 - Creation of Final Demand Cancel $/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE DEMAND_CANCEL_FINAL;
CREATE MULTISET VOLATILE TABLE DEMAND_CANCEL_FINAL
AS
(
SELECT SKU_NUM||'_'||CAST(CANCELED_DATE_PACIFIC AS DATE FORMAT 'YYYY-MM-DD')||'_'||TRIM(INTENT_STORE_NUM)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(CURRENT_PRICE_TYPE,'') AS ROW_ID
	,SKU_NUM
	,CANCELED_DATE_PACIFIC
	,INTENT_STORE_NUM
	,REGULAR_PRICE_AMT
	,CURRENT_PRICE_AMT
	,CURRENT_PRICE_CURRENCY_CODE
	,CURRENT_PRICE_TYPE
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) DEMAND_CANCEL_DOLLARS
	,SUM(ORDER_LINE_CURRENT_AMOUNT) DEMAND_CANCEL_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) DEMAND_CANCEL_UNITS
FROM DEMAND_CANCEL_03
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON DEMAND_CANCEL_FINAL;

--------------- End - Demand - CANCELS


--------------- Start - Error Logging - Demand - Cancelled
	-- Identify any records of which will be considered an error for this process
		-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
			-- Only one record will be determined to pass on until it is resolved

-- 2021.09.22 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'DEMAND_CANCEL_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM demand_cancel_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Demand - Cancelled


--------------- Begin: Store Fulfill - Dollars/Units

-- 2021.10.13 - Creation of baseline order's that were fulfilled by stores
-- drop table store_fulfill_01;
create multiset volatile table store_fulfill_01
as (
select sku_num
	,rms_sku_num
--	,sku_type
--	,delivery_method_code
--	,destination_node_num
	,order_num
	,order_line_num
	,order_tmstp_pacific
	,order_date_pacific
	,order_line_quantity
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
	,order_line_current_amount
	,source_channel_code
	,source_platform_code
	,canceled_date_pacific
	,cancel_reason_code
	,fraud_cancel_ind
--	,shipped_tmstp_pacific
--	,shipped_date_pacific
	,fulfilled_tmstp_pacific
	,fulfilled_date_pacific
	,fulfilled_node_num
	,fulfilled_node_type_code
--	,order_line_id
from prd_nap_usr_vws.order_line_detail_fact
where fulfilled_date_pacific BETWEEN {start_date} AND {end_date}
	and fulfilled_node_num is not null
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
with DATA
primary index ( sku_num, fulfilled_tmstp_pacific )
on commit preserve rows;

collect stats
	primary index ( sku_num, fulfilled_tmstp_pacific )
	,column ( sku_num, fulfilled_tmstp_pacific )
		on store_fulfill_01;

-- 2021.10.13 - Identify the store fulfill units & $'s and prep for price association
-- drop table store_fulfill_02;
create multiset volatile table store_fulfill_02
as (
select a.*
	,b.price_store_num
	,b.store_type_code
	,b.selling_channel
from
	(select a.rms_sku_num as sku_num
		,a.fulfilled_tmstp_pacific
		,a.fulfilled_date_pacific
		,a.fulfilled_node_num
		,a.order_line_quantity
-- 2022.10.18 - Using market rate
--		,order_line_current_amount_usd
		,order_line_current_amount
		,source_channel_code
		,canceled_date_pacific
		,order_tmstp_pacific
	from store_fulfill_01 a) a left join
		store_01 b
			on a.fulfilled_node_num = b.store_num
)
with data
primary index ( sku_num, price_store_num, selling_channel, fulfilled_tmstp_pacific )
on commit preserve rows;

collect stats
	primary index ( sku_num, price_store_num, selling_channel, fulfilled_tmstp_pacific )
--	,column ( sku_num, price_store_num, selling_channel, fulfilled_tmstp_pacific )
		on store_fulfill_02;

-- 2021.10.13 - Identification of PRice at point in time for each transaction
-- drop table store_fulfill_03;
create multiset volatile table store_fulfill_03
as (
select a.sku_num
	,a.fulfilled_tmstp_pacific
	,a.fulfilled_date_pacific
	,a.fulfilled_node_num
	,a.order_line_quantity
-- 2022.10.18 - Using market rate
--	,a.order_line_current_amount_usd
	,a.order_line_current_amount
	,a.source_channel_code
	,a.canceled_date_pacific
	,a.price_store_num
--	,a.order_tmstp_pacific
	,b.rms_sku_num
	,b.regular_price_amt
	,b.current_price_amt
	,b.current_price_currency_code
	,b.current_price_type
from store_fulfill_02 a 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM b
		on a.sku_num = b.rms_sku_num
			and a.price_store_num = b.store_num
			and a.selling_channel = b.selling_channel
			and a.order_tmstp_pacific between b.eff_begin_tmstp and b.eff_end_tmstp - interval '0.001' second
)
with DATA
primary index ( sku_num, fulfilled_node_num, fulfilled_tmstp_pacific )
on commit preserve rows;

collect stats
	primary index ( sku_num, fulfilled_node_num, fulfilled_tmstp_pacific )
		on store_fulfill_03;

-- 2021.10.13 - Creation of final store fulfill $/units related dataset for final aggregate and creation of row_id
-- drop table store_fulfill_final;
create multiset volatile table store_fulfill_final
as (
select SKU_NUM||'_'||CAST(fulfilled_date_pacific AS DATE FORMAT 'YYYY-MM-DD')||'_'||TRIM(fulfilled_node_num)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(CURRENT_PRICE_TYPE,'') AS ROW_ID
	,sku_num
	,fulfilled_date_pacific
	,fulfilled_node_num
	,regular_price_amt
	,current_price_amt
	,current_price_currency_code
	,current_price_type
-- 2022.10.18 - Using market rate
--	,sum(order_line_current_amount_usd) store_fulfill_dollars
	,sum(order_line_current_amount) AS store_fulfill_dollars
	,sum(order_line_quantity) AS store_fulfill_units
from store_fulfill_03
group by 1,2,3,4,5,6,7,8
)
with DATA
primary index ( row_id )
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
		on store_fulfill_final;

--------------- End: Store Fulfill - Dollars/Units


--------------- Start - Error Logging - Store Fulfill - Units/Dollars
	-- Identify any records of which will be considered an error for this process
		-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
			-- Only one record will be determined to pass on until it is resolved

-- 2021.10.12 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'STORE_FULFILL_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM store_fulfill_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Store Fulfill - Units/Dollars


--------------- Begin: Order Ship

-- 2021.09.23 - Identification of the Shipped orders dataset
-- DROP TABLE ORDER_SHIP_01;
create multiset volatile TABLE ORDER_SHIP_01
AS
(
select sku_num
	,rms_sku_num
	,order_tmstp_pacific
	,SHIPPED_TMSTP_PACIFIC
	,SHIPPED_date_pacific
	,sku_type
	,delivery_method_code
-- 2022.03.15 - Only show node when its a pick-up order and should be attributed to stores
    ,CASE WHEN delivery_method_code = 'PICK' THEN destination_node_num ELSE NULL END AS destination_node_num
	,order_num
	,order_line_num
	,order_line_quantity
-- 2022.10.18 - Using market rate
--	,order_line_current_amount_usd
	,order_line_current_amount
-- 2022.03.15 - Identify items with special ad hoc promo adjustments (doesnt include employee discounts) to bucket as promo price later
-- 2022.10.18 - Using market rate
--    ,CASE WHEN COALESCE(order_line_promotion_discount_amount_usd,0) - COALESCE(order_line_employee_discount_amount_usd,0) > 0 THEN 1 ELSE 0 END AS promo_flag
    ,CASE WHEN COALESCE(order_line_promotion_discount_amount,0) - COALESCE(order_line_employee_discount_amount,0) > 0 THEN 1 ELSE 0 END AS promo_flag
-- 2022.03.15 - Add source store to identify DTCs and attribute to stores
    ,source_store_num
	,source_channel_code
	,source_channel_country_code
	,source_platform_code
-- 2022.03.15 - Add in Order Line Id to Track multiple skus per order correctly
	,order_line_id
from prd_nap_usr_vws.order_line_detail_fact
where shipped_date_pacific BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
WITH DATA
PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
INDEX (DESTINATION_NODE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,INDEX (DESTINATION_NODE_NUM)
		ON ORDER_SHIP_01;

-- 2021.09.23 - Identify the Price Types for Price association for each transaction at time of purchase
---- DROP TABLE ORDER_SHIP_02;
create multiset volatile TABLE ORDER_SHIP_02
AS
(
SELECT a.*
	,b.PRICE_STORE_NUM
	,b.SELLING_CHANNEL
FROM
	(SELECT a.RMS_SKU_NUM AS SKU_NUM
		,a.ORDER_TMSTP_PACIFIC
		,a.SHIPPED_TMSTP_PACIFIC
		,a.DESTINATION_NODE_NUM
		,CASE
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
-- 2022.03.15 - Incorporated source platform separation to split out DTC orders and attribute to source location
			WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
-- 2021.10.21 - Incorporated source channel country separation to split out N.COM vs. N.CA
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
-- 2022.03.15 Create price store num to join price data in on
		,CASE
		  WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN SOURCE_CHANNEL_CODE = 'RACK' THEN 828
		 END PRC_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--		,a.ORDER_LINE_CURRENT_AMOUNT_USD
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.SHIPPED_DATE_PACIFIC
		,a.PROMO_FLAG
-- 2022.03.15 - Add in Order Line Id to Track multiple skus per order correctly
		,a.ORDER_LINE_ID
	FROM ORDER_SHIP_01 a) a LEFT JOIN
		STORE_01 b
			ON a.PRC_STORE_NUM = b.STORE_NUM
)
WITH DATA
PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, PRICE_STORE_NUM, SELLING_CHANNEL, ORDER_TMSTP_PACIFIC)
		ON ORDER_SHIP_02;

-- 2021.09.23 - Identification of Price at point in time for each transaction
-- -- DROP TABLE ORDER_SHIP_03;
create multiset volatile TABLE ORDER_SHIP_03
AS
(
SELECT a.SKU_NUM
	,a.ORDER_TMSTP_PACIFIC
	,a.SHIPPED_TMSTP_PACIFIC
	,a.DESTINATION_NODE_NUM
	,a.INTENT_STORE_NUM
	,a.DELIVERY_METHOD_CODE
	,a.ORDER_LINE_QUANTITY
-- 2022.10.18 - Using market rate
--	,a.ORDER_LINE_CURRENT_AMOUNT_USD
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.SHIPPED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,a.ORDER_LINE_ID
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.CURRENT_PRICE_AMT
	,b.CURRENT_PRICE_CURRENCY_CODE
-- 2022.03.15 - Make sure ad hoc adjustments go under promo and promo under clearance goes under clearance
	,CASE WHEN PROMO_FLAG = 1 AND b.current_price_type = 'R' then 'P'
	   WHEN b.ownership_price_type = 'C' then b.ownership_price_type
	   ELSE b.current_price_type
	 END AS current_price_type
FROM ORDER_SHIP_02 a 
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM b
		ON a.SKU_NUM = b.RMS_SKU_NUM
			AND a.price_store_num = b.store_num
			AND a.selling_channel = b.selling_channel
			AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
WITH DATA
PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
--	,COLUMN (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC)
		ON ORDER_SHIP_03;

-- 2021.09.22 - Creation of Final Order Ship $/Units related dataset for final aggregate and creation of row_id
-- -- DROP TABLE ORDER_SHIP_FINAL;
create multiset volatile TABLE ORDER_SHIP_FINAL
AS
(
SELECT SKU_NUM||'_'||CAST(SHIPPED_DATE_PACIFIC AS DATE FORMAT 'YYYY-MM-DD')||'_'||TRIM(INTENT_STORE_NUM)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(CURRENT_PRICE_TYPE,'') AS ROW_ID
	,SKU_NUM
	,SHIPPED_DATE_PACIFIC
	,INTENT_STORE_NUM
	,REGULAR_PRICE_AMT
	,CURRENT_PRICE_AMT
	,CURRENT_PRICE_CURRENCY_CODE
	,CURRENT_PRICE_TYPE
-- 2022.10.18 - Using market rate
--	,SUM(ORDER_LINE_CURRENT_AMOUNT_USD) SHIPPED_DOLLARS
	,SUM(ORDER_LINE_CURRENT_AMOUNT) AS SHIPPED_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) AS SHIPPED_UNITS
FROM ORDER_SHIP_03
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON ORDER_SHIP_FINAL;


--------------- End - Order Ship


--------------- Start - Error Logging - Order Ship
	-- Identify any records of which will be considered an error for this process
		-- Any records of which the ROW_ID is duplicated is considered for this pipeline as an error.
			-- Only one record will be determined to pass on until it is resolved

-- 2021.09.23 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'ORDER_SHIP_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM order_ship_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Order Ship


--------------- Inventory (SOH)

-- 2021.08.02 - Build out of SOH for each execution
-- 2022.01.25 - Added BOH
-- 2022.02.09 - Created a sku_eoh table to make sku_soh_01 more performant 
-- 2022.02.09 - NULL-ed out BOH because of bugs 
-- 2022.02.17 - Reverted back to structure before adding BOH on 1.25, added BOH/EOH logic
-- -- DROP TABLE SKU_SOH_01;

CREATE MULTISET VOLATILE TABLE SKU_SOH_01
AS
(
select RMS_SKU_ID
	,snapshot_date
	,snapshot_date+1 snapshot_date_boh
	,value_updated_time
	,location_id
	,coalesce(immediately_sellable_qty,0) immediately_sellable_qty
	,coalesce(stock_on_hand_qty,0) stock_on_hand_qty
	,coalesce(stock_on_hand_qty,0)-coalesce(immediately_sellable_qty,0) nonsellable_qty
-- Added location_type to associate dropship inventory
	,location_type
from prd_nap_usr_vws.inventory_stock_quantity_by_day_fact
where snapshot_date between {start_date_soh} AND {end_date}
GROUP BY 1,2,3,4,5,6,7,8,9
)
WITH DATA
PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID)
	,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID)
	,COLUMN (RMS_SKU_ID)
	,COLUMN (LOCATION_ID)
		ON SKU_SOH_01;


---- DROP TABLE sku_soh_02;
create multiset volatile table sku_soh_02
as
(
select a.rms_sku_id
	,CASE
		when a.location_type = 'DS' and a.store_type_code is null then 'FL'
		when a.location_type = 'DS_OP' and a.store_type_code is null then 'RK'
		else a.store_type_code
	 end store_type_code
	,CASE
		when a.location_type = 'DS' and a.store_type_code_new is null then 'FL'
		when a.location_type = 'DS_OP' and a.store_type_code is null then 'RK'
		else store_type_code_new
	 end store_type_code_new
-- This case statement could just be overridden with USA but I'm leaving it here so if we need to distinguish Canada at some point, it's right here
	,case
		when a.location_type = 'DS' and a.store_country_code is null then 'USA'
		when a.location_type = 'DS_OP' and a.store_country_code is null then 'USA'
		else a.store_country_code
	 end store_country_code
	,a.selling_channel
	,a.snapshot_date
	,a.snapshot_date_boh
	,a.value_updated_time
	,a.location_id
	,a.immediately_sellable_qty
	,a.stock_on_hand_qty
	,a.nonsellable_qty
	,a.location_type
from (
	select a.rms_sku_id
		,b.store_type_code
		,b.store_type_code_new
		,b.store_country_code
		,b.selling_channel
		,a.snapshot_date
		,a.snapshot_date_boh
		,a.value_updated_time
		,a.location_id
		,a.immediately_sellable_qty
		,a.stock_on_hand_qty
		,a.nonsellable_qty
		,a.location_type
	from sku_soh_01 a left join
		store_01 b
			on a.location_id = b.store_num
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13) a
)
with DATA
primary index (rms_sku_id,store_type_code,store_country_code,snapshot_date)
on commit preserve rows;

collect stats 
	primary index ( rms_sku_id, store_type_code, store_country_code, snapshot_date)
	,column ( rms_sku_id, store_type_code, store_country_code, snapshot_date)
		on sku_soh_02;

-- 2021.08.09 - Incorporate STORE TYPE in to SOH
	-- 2021.08.10 - Incorporated dropship override WHERE location_id's equate to vendorr #
-- 2022.02.17 - Added row_id and snapshot date columns for BOH 
-- 2022.02.23 - Added min() function to price type and amount to avoid duplicate rows
-- drop table sku_soh_final;
create multiset volatile table sku_soh_final
as
(
select rms_sku_id||'_'||cast(snapshot_date as date format 'YYYY-MM-DD')||'_'||TRIM(location_id)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(current_price_type,'') row_id
	,rms_sku_id||'_'||cast(snapshot_date_boh as date format 'YYYY-MM-DD')||'_'||TRIM(location_id)||'_'||TRIM(COALESCE(CAST(REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||COALESCE(current_price_type,'') row_id_boh
	,rms_sku_id 
	,snapshot_date
	,snapshot_date_boh 
	,value_updated_time 
	,location_id 
	,immediately_sellable_qty 
	,immediately_sellable_dollars 
	,stock_on_hand_qty 
	,stock_on_hand_dollars 
	,nonsellable_qty 
	,store_type_code 
	,store_country_code 
	,current_price_type 
	,current_price_amt 
	,regular_price_amt 
	,location_type 
from (
	select a.rms_sku_id
		,a.snapshot_date
		,a.snapshot_date_boh
		,a.value_updated_time
		,a.location_id
		,a.immediately_sellable_qty
		,(a.immediately_sellable_qty * b.ownership_price_amt) as immediately_sellable_dollars
		,a.stock_on_hand_qty
	-- 2021.10.26 - Incorporated SOH dollars as we're switching from sellable SOH to SOH as it aligns better with MADM measurements
		,(a.stock_on_hand_qty * b.ownership_price_amt) stock_on_hand_dollars
		,a.nonsellable_qty
		,a.store_type_code
		,a.store_country_code
	-- 2022.01.15 - For inventory, use ownership price type instead of current
		,min(b.ownership_price_type) as current_price_type
		,min(b.current_price_amt) current_price_amt
		,b.regular_price_amt
		,a.location_type
	from sku_soh_02 a left JOIN
		price_01 b
			on a.rms_sku_id = b.rms_sku_num
				and a.store_type_code_new = b.store_type_code
				and a.store_country_code = b.channel_country
				and a.selling_channel = b.selling_channel
				and cast(a.snapshot_date as timestamp) between b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
	-- If there is no (0) SOH for immediate, stock_on_hand or nonsellable, remove to not waste space
	where a.immediately_sellable_qty <> 0
		or a.stock_on_hand_qty <> 0
		or a.nonsellable_qty <> 0 
	group by 1,2,3,4,5,6,7,8,9,10,11,12,15,16) a
--where rms_sku_id = '25256294'
--	and location_id = '551'
--order by rms_sku_id
--sample 100
)
with DATA
primary index (ROW_ID)
on commit preserve rows;

collect stats
	primary index (row_id)
	,column (row_id)
		on sku_soh_final;

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

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'SKU_SOH_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM sku_soh_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

-- 2022.02.25 - Error Logs (BOH)

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'SKU_SOH_FINAL_BOH' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID_BOH as ROW_ID
	FROM sku_soh_final
	GROUP BY 1
	HAVING COUNT(ROW_ID_BOH) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

-- 2022.02.28 - Error Logs To Catch Erroneous SOH (EOH) records 
MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'SKU_SOH_FINAL_BAD' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM sku_soh_final
	where stock_on_hand_dollars > 9999999999.99) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

-- 2022.02.28 - Error Logs To Catch Erroneous SOH (BOH) records 
MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'SKU_SOH_FINAL_BAD_BOH' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID_BOH as ROW_ID
	FROM sku_soh_final
	where stock_on_hand_dollars > 9999999999.99) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - SOH


--------------- Start - EOH

-- 2021.09.28 - EOH Calculation - Identify all BOH run per day for the delta period and move back the snapshot date by -1 for update to final dataset
	-- Any issues found with the SKU_SOH_FINAL will already be excluded from the EOH calculation. The error log to follow will only be on new issues
-- -- DROP TABLE SKU_EOH_FINAL;
/* 2021.11.05 - This component of EOH calculation has been removed as the request was to replicate BOH as EOH for each day
create multiset volatile TABLE SKU_EOH_FINAL
AS
(
SELECT a.rms_sku_id||'_'||cast(a.snapshot_date as date format 'YYYY-MM-DD')||'_'||TRIM(a.location_id) row_id
	,RMS_SKU_ID
	,(SNAPSHOT_DATE - 1) SNAPSHOT_DATE
	,LOCATION_ID
-- 2021.10.26 - Switching Sellable SOH to SOH to better align with MADM measurements
	,STOCK_ON_HAND_QTY
	,STOCK_ON_HAND_DOLLARS
FROM SKU_SOH_FINAL a LEFT JOIN
	{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS b
		on a.row_id = b.row_id
where b.row_id is null
GROUP BY 1,2,3,4,5,6
)
WITH DATA
PRIMARY INDEX ( ROW_ID )
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX ( ROW_ID )
	,COLUMN ( ROW_ID )
	,COLUMN ( RMS_SKU_ID, SNAPSHOT_DATE, LOCATION_ID )
		ON SKU_EOH_FINAL;
*/
--------------- End - EOH

--------------- Start - Error Logging - EOH
-- 2021.09.28 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.
/* 2021.11.05 - This component of EOH calculation has been removed as the request was to replicate BOH as EOH for each day
MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'SKU_EOH_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM sku_eoh_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);
*/
--------------- End - Error Logging - EOH


--------------- Receipts

	----------- PO Receipts

-- 2021.08.04 - (RMS14) Identify what record's have been receipted on at SKU
--drop table receipt_01_rms14;
create multiset volatile table receipt_01_rms14
as
(
select a.purchase_order_num
	,a.rms_sku_num
	,b.to_location_id
--	,a.carton_id as carton_id_inbound
--	,b.carton_id as carton_id_transfer
--	,a.external_reference_id
--	,b.bill_of_lading
	,sum(a.shipment_qty) as shipment_qty
	,a.received_date
	,a.received_tmstp
	,a.source_system
	,a.tological_type
	,b.operation_type
	,b.transfer_source
	,b.ship_date
	,b.bill_of_lading
	,b.asn_number
	,b.from_location_id
	,b.from_location_type
	,b.to_location_type
	,sum(b.ship_qty) as ship_qty
	,sum(b.ship_cancelled_qty) as ship_cancelled_qty
	,b.receipt_date
	,b.receipt_tmstp
	,sum(b.receipt_qty) as receipt_qty
	,sum(b.receipt_cancelled_qty) as receipt_cancelled_qty
from prd_nap_usr_vws.rms14_inbound_shipment_fact a left join
	prd_nap_usr_vws.rms14_transfer_shipment_receipt_fact b
		on a.rms_sku_num = b.rms_sku_num
where receipt_date between {start_date} AND {end_date}
--where receipt_date between {start_date} AND {end_date}
--	and a.rms_sku_num = '37631080'
	and (a.carton_id = b.carton_id
		or a.external_reference_id = substring(b.bill_of_lading,4,10))
group by 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,19,20
)
with DATA
primary index (purchase_order_num,rms_sku_num,to_location_id)
on commit preserve rows;

collect stats
	primary index (purchase_order_num,rms_sku_num,to_location_id)
	,column (purchase_order_num,rms_sku_num,to_location_id)
		on receipt_01_rms14;


-- 2021.08.03 - Identify all PO's with receipts within time window analyzed
-- drop table receipt_02_po;
create multiset volatile table receipt_02_po
as
(
select a.purchase_order_num
	,a.rms_sku_num
	,a.to_location_id
	,a.shipment_qty
	,a.received_date
	,a.received_tmstp
	,a.source_system
	,a.tological_type
	,a.operation_type
	,a.transfer_source
	,a.ship_date
	,a.bill_of_lading
	,a.asn_number
	,a.from_location_id
	,a.from_location_type
	,a.to_location_type
	,a.ship_qty
	,a.ship_cancelled_qty
	,a.receipt_date
	,a.receipt_tmstp
	,a.receipt_qty
	,a.receipt_cancelled_qty
--	,b.purchase_order_num
	,b.status
	,b.close_date
	,b.currency
	,b.department_id
	,b.dropship_ind
	,b.edi_ind
	,b.edi_sent_ind
	,b.end_ship_date
	,b.nordstrom_com_ind
	,b.exclude_from_availability_ind
	,b.latest_ship_date
	,b.instore_scheduled_date
	,b.packing_method
	,b.pickup_date
	,b.plan_season_id
	,b.promotion_id
	,b.purchaseorder_type
	,b.purchase_type
	,b.start_ship_date
	,b.open_to_buy_endofweek_date
from receipt_01_rms14 a left join
	prd_nap_usr_vws.purchase_order_fact b
		on a.purchase_order_num = b.purchase_order_num
		--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)
with DATA
primary index (purchase_order_num,rms_sku_num,to_location_id)
on commit preserve rows;

COLLECT STATS
	PRIMARY INDEX (purchase_order_num,rms_sku_num,to_location_id)
	,COLUMN (purchase_order_num,rms_sku_num,to_location_id)
		ON receipt_02_po;

-- 2021.08.04 - Identify all PO activity of the PO's open and approved to track receipts
--drop table receipt_02_po_location;
create multiset volatile table receipt_02_po_location
as
(
select a.purchase_order_num
	,a.rms_sku_num
	,a.to_location_id
	,a.shipment_qty
	,a.received_date
	,a.received_tmstp
	,a.source_system
	,a.tological_type
	,a.operation_type
	,a.transfer_source
	,a.ship_date
	,a.bill_of_lading
	,a.asn_number
	,a.from_location_id
	,a.from_location_type
	,a.to_location_type
	,a.ship_qty
	,a.ship_cancelled_qty
	,a.receipt_date
	,a.receipt_tmstp
	,a.receipt_qty
	,a.receipt_cancelled_qty
	,a.status
	,a.close_date as po_close_date
	,a.currency
	,a.department_id
	,a.dropship_ind
	,a.edi_ind
	,a.edi_sent_ind
	,a.end_ship_date
	,a.nordstrom_com_ind
	,a.exclude_from_availability_ind
	,a.latest_ship_date
	,a.instore_scheduled_date
	,a.packing_method
	,a.pickup_date
	,a.plan_season_id
	,a.promotion_id
	,a.purchaseorder_type
	,a.purchase_type
	,a.start_ship_date
	,a.open_to_buy_endofweek_date
	,b.distribution_id
--	,b.allocated_qty
--	,b.canceled_qty as distribute_canceled_qty
--	,b.prescaled_qty as distribute_prescaled_qty
--	,b.received_qty as distribute_received_qty
--	,b.transferred_qty
	,b.ship_location_id
	,b.case_pack_ind
	,b.supplier_pack_size
	,b.initial_unit_cost_amt
	,b.unit_cost_amt as distribute_unit_cost_amt
	,c.cancel_code
	,c.cancel_date
	,c.close_date as shiplocation_close_date
	,c.distribution_status
	,c.estimatedin_stock_date
--	,c.canceled_qty as shiplocation_canceled_qty
--	,c.ordered_qty
--	,c.prescaled_qty as shiplocation_prescaled_qty
--	,c.received_qty as shiplocation_received_qty
	,c.release_date
	,c.initial_unit_cost
	,c.unit_retail_amt
	,c.supplier_packsize
	,c.unit_cost_amt
from receipt_02_po a left join
	prd_nap_usr_vws.purchase_order_item_distributelocation_fact b
		on a.purchase_order_num = b.purchase_order_num
			and a.rms_sku_num = b.rms_sku_num
			and a.to_location_id = b.distribute_location_id JOIN
	prd_nap_usr_vws.purchase_order_item_shiplocation_fact c
		on b.purchase_order_num = c.purchase_order_num
			and b.rms_sku_num = c.rms_sku_num
			and b.distribution_id = c.distribution_id
--			and b.ship_location_id = c.ship_location_id
--			and a.department_id = b.department_id
)
with data
primary index (purchase_order_num,rms_sku_num,to_location_id)
on commit preserve rows;

collect stats
	primary index (purchase_order_num,rms_sku_num,to_location_id)
	,column (purchase_order_num,rms_sku_num,to_location_id)
		on receipt_02_po_location;

	--------------- DC RECEIPT CAPTURE FOR PO'S

-- 2021.08.24 - Identification of any sku's receipted at DC's - This has been incorporated to match MFP's KPI's in MADM
-- -- DROP TABLE RECEIPT_DC_01_RMS14;
create multiset volatile TABLE RECEIPT_DC_01_RMS14
AS
(
	select PURCHASE_ORDER_NUM
		,RMS_SKU_NUM
	--	,TOFACILITY_ID
	-- For dc's, we're identifying based on the facility location the product is landing
		-- 2021.08.26 - For facilities such as 808 and 828, we need to incorporate without the substring as
		,CASE
			WHEN tofacility_id > 999 then cast(substr(trim(tofacility_id),3) as int)
			else tofacility_id
		 end as to_location_id
-- For DC receipts, the shipment qty becomes the receipt qty
		,SUM(SHIPMENT_QTY) RECEIPT_QTY
-- The received date of at the DC is the receipt date for the DC records
		,RECEIVED_DATE as RECEIPT_DATE
		,RECEIVED_TMSTP AS RECEIPT_TMSTP
		,SOURCE_SYSTEM
		,TOLOGICAL_TYPE
	from prd_nap_usr_vws.rms14_inbound_shipment_fact
	where received_date between {start_date} AND {end_date}
		and tofacility_type = 'W'
	GROUP BY 1,2,3,5,6,7,8
)
WITH DATA
PRIMARY INDEX (PURCHASE_ORDER_NUM,RMS_SKU_NUM,TO_LOCATION_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (PURCHASE_ORDER_NUM,RMS_SKU_NUM,TO_LOCATION_ID)
	,COLUMN (PURCHASE_ORDER_NUM,RMS_SKU_NUM,TO_LOCATION_ID)
		ON RECEIPT_DC_01_RMS14;

-- 2021.08.24 - The PO data for initial receipt at DC doesn't necessarily contain the distribution records yet based on routing
	-- As such, we are tracing for the unit retail based on the ship location to the DC itself
-- -- DROP TABLE RECEIPT_DC_02_PO_LOCATION;
create multiset volatile TABLE RECEIPT_DC_02_PO_LOCATION
AS
(
SELECT a.PURCHASE_ORDER_NUM
	,a.RMS_SKU_NUM
	,c.STORE_TYPE_CODE
	,c.STORE_TYPE_CODE_NEW
	,c.STORE_COUNTRY_CODE
	,a.TO_LOCATION_ID
	,a.RECEIPT_QTY
	,a.RECEIPT_DATE
	,a.RECEIPT_TMSTP
	,a.SOURCE_SYSTEM
	,a.TOLOGICAL_TYPE
	,b.UNIT_RETAIL_AMT
FROM RECEIPT_DC_01_RMS14 a LEFT JOIN
	(SELECT PURCHASE_ORDER_NUM
		,RMS_SKU_NUM
		,UNIT_RETAIL_AMT
	 FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT
	 GROUP BY 1,2,3) b
		ON a.PURCHASE_ORDER_NUM = b.PURCHASE_ORDER_NUM
			AND a.RMS_SKU_NUM = b.RMS_SKU_NUM LEFT JOIN
	STORE_01 c
		on a.TO_LOCATION_ID = c.STORE_NUM
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
WITH DATA
PRIMARY INDEX (PURCHASE_ORDER_NUM,RMS_SKU_NUM,TO_LOCATION_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (PURCHASE_ORDER_NUM,RMS_SKU_NUM,TO_LOCATION_ID)
	,COLUMN (PURCHASE_ORDER_NUM,RMS_SKU_NUM,TO_LOCATION_ID)
		ON RECEIPT_DC_02_PO_LOCATION;

-- 2021.08.09 - Create final Receipt (PO) data and creation of ROW_ID
	-- VALIDATE: We may need to pull in more from above as filtering criteria
	-- 2021.08.24 - Incoporated DC PO's in to the final volatile dataset
-- drop table receipt_po_final;
create multiset volatile table receipt_po_final
as
(
select a.rms_sku_num||'_'||cast(a.receipt_date as date format 'YYYY-MM-DD')||'_'||trim(a.to_location_id)||'_'||TRIM(COALESCE(CAST(b.REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(b.CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||coalesce(b.current_price_type,'') row_id
	,a.rms_sku_num
	,a.store_type_code
	,a.store_country_code
	,a.receipt_date
	,a.to_location_id
	,a.receipt_qty
	,a.unit_retail_amt
	,a.receipt_qty * a.unit_retail_amt receipt_po_dollars
	,b.current_price_type
	,b.regular_price_amt
	,b.current_price_amt
from
	(select rms_sku_num
		,store_type_code
		,store_country_code
		,receipt_date
		,to_location_id
		,receipt_qty
		,unit_retail_amt
	from
		(select a.rms_sku_num
			,b.store_type_code
			,b.store_country_code
			,a.receipt_date
			,a.to_location_id
			,a.receipt_qty
			,a.unit_retail_amt
		from
			(select rms_sku_num
				,receipt_date
				,to_location_id
				,sum(receipt_qty) - sum(receipt_cancelled_qty) receipt_qty
				,unit_retail_amt
			from receipt_02_po_location
			group by 1,2,3,5) a left join
			store_01 b
				on a.to_location_id = b.store_num
		union
		select rms_sku_num
			,store_type_code
			,store_country_code
			,receipt_date
			,to_location_id
			,receipt_qty
			,unit_retail_amt
		from receipt_dc_02_po_location
		group by 1,2,3,4,5,6,7) a ) a left JOIN
	price_01 b
		on a.rms_sku_num = b.rms_sku_num
			and a.store_type_code = b.store_type_code
			and a.store_country_code = b.channel_country
			and cast(a.receipt_date as timestamp) between b.eff_begin_tmstp and b.eff_end_tmstp
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
with DATA
primary index (row_id)
on commit preserve rows;
-- Validate: That there were in fact multiple PO's per same location for the same sku
	-- validate what the store_type_code's are for the ones where current_price_type is null (SS, OF)

collect stats
	primary index (row_id)
	,column (row_id)
		on receipt_po_final;

	--------------- End - PO Receipts

--------------- Start - Error Logging - PO Receipts
-- 2021.08.17 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'RECEIPT_PO_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM receipt_po_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - PO RECEIPTS


	----------- START - Receipts - Pack and Hold

-- 2021.09.02 - Identification of receipts for pack and hold
-- -- DROP TABLE RECEIPT_PAH_FINAL;
create multiset volatile TABLE RECEIPT_PAH_FINAL
AS (
SELECT a.rms_sku_num||'_'||cast(a.receipt_date as date format 'YYYY-MM-DD')||'_'||trim(a.to_location_id)||'_'||TRIM(COALESCE(CAST(b.REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(b.CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||coalesce(b.current_price_type,'') row_id
	,a.RMS_SKU_NUM
	,a.STORE_TYPE_CODE
	,a.STORE_COUNTRY_CODE
	,a.RECEIPT_DATE
	,a.TO_LOCATION_ID
	,a.RECEIPT_QTY
	,a.unit_retail_amt
	,a.RECEIPT_QTY * a.UNIT_RETAIL_AMT RECEIPT_PAH_DOLLARS
	,b.current_price_type
	,b.regular_price_amt
	,b.current_price_amt
FROM
	(SELECT b.RMS_SKU_NUM
		,b.STORE_TYPE_CODE
-- 2021.09.01 - We will override the DC store type with RK for Pack and Hold to capture the price within the JOIN
		,'RK' STORE_TYPE_CODE_NEW
		,b.STORE_COUNTRY_CODE
		,b.RECEIPT_DATE
		,b.TO_LOCATION_ID
		,b.RECEIPT_QTY
		,b.unit_retail_amt
	FROM
		(SELECT STORE_NUM
		FROM STORE_01
	-- 220 (US), 221 (CAN)
		WHERE CHANNEL_NUM IN (220,221)
		GROUP BY 1) a JOIN
		receipt_dc_02_po_location b
			ON A.STORE_NUM = B.TO_LOCATION_ID) a LEFT JOIN
	PRICE_01 b
		ON a.RMS_SKU_NUM = b.rms_sku_num
			and a.STORE_TYPE_CODE_NEW = b.STORE_TYPE_CODE
			and a.STORE_COUNTRY_CODE = b.CHANNEL_COUNTRY
			and cast(a.receipt_date as timestamp) between b.eff_begin_tmstp and b.eff_end_tmstp
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON RECEIPT_PAH_FINAL;

--------------- End - Receipts - Pack and Hold

--------------- Start - Error Logging - Pack and Hold Receipts
-- 2021.09.02 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'RECEIPT_PAH_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM receipt_pah_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Pack and Hold RECEIPTS


	----------- START - Receipts - Reserve Stock
-- 2021.09.01 - Identification of receipts for Reserve Stock
create multiset volatile TABLE RECEIPT_RS_FINAL
AS (
SELECT a.rms_sku_num||'_'||cast(a.receipt_date as date format 'YYYY-MM-DD')||'_'||trim(a.to_location_id)||'_'||TRIM(COALESCE(CAST(b.REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(b.CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||coalesce(b.current_price_type,'') row_id
	,a.RMS_SKU_NUM
	,a.STORE_TYPE_CODE
	,a.STORE_COUNTRY_CODE
	,a.RECEIPT_DATE
	,a.TO_LOCATION_ID
	,a.RECEIPT_QTY
	,a.unit_retail_amt
	,a.RECEIPT_QTY * a.UNIT_RETAIL_AMT RECEIPT_RS_DOLLARS
	,b.current_price_type
	,b.regular_price_amt
	,b.current_price_amt
FROM
	(SELECT b.RMS_SKU_NUM
		,b.STORE_TYPE_CODE
-- 2021.09.01 - We will override the DC store type with RK for Pack and Hold to capture the price within the JOIN
		,a.STORE_TYPE_CODE_NEW
		,b.STORE_COUNTRY_CODE
		,b.RECEIPT_DATE
		,b.TO_LOCATION_ID
		,b.RECEIPT_QTY
		,b.unit_retail_amt
	FROM
		(SELECT STORE_NUM
			,CASE
				WHEN CHANNEL_NUM IN (310,311) THEN 'FLS'
				WHEN CHANNEL_NUM IN (260,261) THEN 'RK'
				ELSE NULL
			 END STORE_TYPE_CODE_NEW
		FROM STORE_01
	-- 310 (FP RS US), 311 (FP RS CAN), 260 (OP RS US), 261 (OP RS CAN)
		WHERE CHANNEL_NUM IN (310,311,260,261)
		GROUP BY 1,2) a JOIN
		receipt_dc_02_po_location b
			ON A.STORE_NUM = B.TO_LOCATION_ID) a LEFT JOIN
	PRICE_01 b
		ON a.RMS_SKU_NUM = b.rms_sku_num
			and a.STORE_TYPE_CODE_NEW = b.STORE_TYPE_CODE
			and a.STORE_COUNTRY_CODE = b.CHANNEL_COUNTRY
			and cast(a.receipt_date as timestamp) between b.eff_begin_tmstp and b.eff_end_tmstp
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON RECEIPT_RS_FINAL;

--------------- End - Receipts - Reserve Stock

--------------- Start - Error Logging - Reserve Stock Receipts
-- 2021.09.03 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'RECEIPT_RS_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM receipt_rs_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Reserve Stock RECEIPTS


	----------- Start - Dropship PO Receipts
-- 2021.08.05 - Identification of all dropship orders based on written date of the PO
--drop table receipt_dropship_01_po_header;
create multiset volatile table receipt_dropship_01_po_header
as
(
select purchase_order_num
	,status
-- 2021.08.24 - Incorporating written date to track what the unit pricing was at the time of order placement
	,written_date
	,close_date
	,currency
	,department_id
	,dropship_ind
	,edi_ind
	,edi_sent_ind
	,end_ship_date
	,nordstrom_com_ind
	,exclude_from_availability_ind
	,latest_ship_date
	,instore_scheduled_date
	,packing_method
	,pickup_date
	,plan_season_id
	,promotion_id
	,purchaseorder_type
	,purchase_type
	,start_ship_date
	,open_to_buy_endofweek_date
from prd_nap_usr_vws.purchase_order_fact
where close_date between {start_date} AND {end_date}
	and dropship_ind = 't'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
with DATA
primary index (purchase_order_num)
on commit preserve rows;

COLLECT STATS
	PRIMARY INDEX (PURCHASE_ORDER_NUM)
	,COLUMN (PURCHASE_ORDER_NUM)
		ON receipt_dropship_01_po_header;

-- 2021.08.05 - Identify the details of the dropship PO's generated
-- drop table receipt_dropship_02_po_details;
create multiset volatile table receipt_dropship_02_po_details
as
(
select a.purchase_order_num
	,b.rms_sku_num
	,b.distribute_location_id
	,a.status
	,a.written_date
	,a.close_date as po_close_date
	,a.currency
	,a.department_id
	,a.dropship_ind
	,a.edi_ind
	,a.edi_sent_ind
	,a.end_ship_date
	,a.nordstrom_com_ind
	,a.exclude_from_availability_ind
	,a.latest_ship_date
	,a.instore_scheduled_date
	,a.packing_method
	,a.pickup_date
	,a.plan_season_id
	,a.promotion_id
	,a.purchaseorder_type
	,a.purchase_type
	,a.start_ship_date
	,a.open_to_buy_endofweek_date
	,b.distribution_id
	,b.allocated_qty
	,b.canceled_qty as distribute_canceled_qty
	,b.prescaled_qty as distribute_prescaled_qty
	,b.received_qty as distribute_received_qty
	,b.transferred_qty
	,b.ship_location_id
	,b.case_pack_ind
	,b.supplier_pack_size
	,b.initial_unit_cost_amt
	,b.unit_cost_amt as distribute_unit_cost_amt
	,c.cancel_code
	,c.cancel_date
	,c.close_date as shiplocation_close_date
	,c.distribution_status
	,c.estimatedin_stock_date
	,c.canceled_qty as shiplocation_canceled_qty
	,c.ordered_qty
	,c.prescaled_qty as shiplocation_prescaled_qty
	,c.received_qty as shiplocation_received_qty
	,c.release_date
	,c.initial_unit_cost
	,c.unit_retail_amt
	,c.supplier_packsize
	,c.unit_cost_amt
from receipt_dropship_01_po_header a join
	prd_nap_usr_vws.purchase_order_item_distributelocation_fact b
		on a.purchase_order_num = b.purchase_order_num JOIN
	prd_nap_usr_vws.purchase_order_item_shiplocation_fact c
		on b.purchase_order_num = c.purchase_order_num
			and b.rms_sku_num = c.rms_sku_num
--			and b.distribution_id = c.distribution_id
--			and b.ship_location_id = c.ship_location_id
--where b.rms_sku_num = '37631080'
--			and a.department_id = b.department_id
)
with data
primary index (purchase_order_num,rms_sku_num,distribute_location_id)
on commit preserve rows;

collect stats
	primary index (purchase_order_num,rms_sku_num,distribute_location_id)
	,column (purchase_order_num,rms_sku_num,distribute_location_id)
		on receipt_dropship_02_po_details;

-- Validation - Check to see if distribute_location_id is ever different from ship_location_id

-- 2021.08.24 - Create final dataset for dropship receipts
---- DROP TABLE RECEIPT_DS_FINAL;
create multiset volatile TABLE RECEIPT_DS_FINAL
AS
(
SELECT a.rms_sku_num||'_'||cast(a.receipt_date as date format 'YYYY-MM-DD')||'_'||trim(a.ship_location_id)||'_'||TRIM(COALESCE(CAST(b.REGULAR_PRICE_AMT AS VARCHAR(10)),''))||'_'||TRIM(COALESCE(CAST(b.CURRENT_PRICE_AMT AS VARCHAR(10)),''))||'_'||coalesce(b.current_price_type,'') row_id
	,a.rms_sku_num
	,a.store_type_code
	,a.store_country_code
	,a.receipt_date
	,a.ship_location_id
-- Due to the written date for tracking price at time of order, we're rolling this up one last time due to differences in written dates of po's
	,sum(a.receipt_qty) as receipt_qty
	,a.unit_retail_amt
	,sum(a.receipt_qty) * a.unit_retail_amt receipt_po_dollars
	,b.current_price_type
	,b.regular_price_amt
	,b.current_price_amt
FROM
	(select a.rms_sku_num
		,b.store_type_code
		,b.store_country_code
		,a.written_date
		,a.receipt_date
		,a.distribute_location_id
		,a.ship_location_id
		,a.receipt_qty
		,a.unit_retail_amt
	from
		(select rms_sku_num
			,written_date
			,po_close_date as receipt_date
			,distribute_location_id
			,ship_location_id
			,sum(shiplocation_received_qty) - sum(coalesce(shiplocation_canceled_qty,0)) receipt_qty
			,unit_retail_amt
		from receipt_dropship_02_po_details
		group by 1,2,3,4,5,7) a left join
		store_01 b
			on a.ship_location_id = b.store_num) a left JOIN
	price_01 b
		on a.rms_sku_num = b.rms_sku_num
			and a.store_type_code = b.store_type_code
			and a.store_country_code = b.channel_country
			and cast(a.written_date as timestamp) between b.eff_begin_tmstp and b.eff_end_tmstp
group by 1,2,3,4,5,6,8,10,11,12
)
with DATA
primary index (row_id)
on commit preserve rows;

collect stats
	primary index (row_id)
	,column (row_id)
		on receipt_ds_final;

--------------- Start - Error Logging - Dropship Receipts
-- 2021.08.24 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'RECEIPT_DS_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM receipt_ds_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Dropship Receipts

	----------- START - Receipts - Total Receipts
	-- These are already established aggregates from the individual Receipt components
		-- PO Receipts and Reserve Stock Receipts collide with each other. There needs to be a separate aggregation (coalesce) to ensure there isn't inflation counting
	-- Because of PO/Reserve Stock Receipts at times coinciding with each other, instead of stacking and aggregating vertically, this design is to lay the kpi's flat horizontally and then aggregate accordingly

-- Identify the unique row_id's that make up any given execution's receipt kpi's
-- drop table receipt_tot_final;
create multiset volatile table receipt_tot_final
as (
select a.row_id
	,(coalesce(coalesce(b.receipt_qty,0),coalesce(e.receipt_qty,0))+coalesce(c.receipt_qty,0)+coalesce(d.receipt_qty,0)) as receipt_units
	,(coalesce(coalesce(b.receipt_po_dollars,0),coalesce(e.receipt_rs_dollars,0))+coalesce(c.receipt_po_dollars,0)+coalesce(d.receipt_pah_dollars,0)) as receipt_dollars
--	,b.receipt_qty as receipt_po_units
--	,b.receipt_po_dollars
--	,c.receipt_qty as receipt_dropship_units
--	,c.receipt_po_dollars as receipt_dropship_dollars
--	,d.receipt_qty as receipt_pah_units
--	,d.receipt_pah_dollars
--	,e.receipt_qty as receipt_reservestock_units
--	,e.receipt_rs_dollars as receipt_reservestock_dollars
from
	(select row_id
	from (
		select row_id
		from receipt_po_final
		group by 1
		UNION
		select row_id
		from receipt_ds_final
		group by 1
		UNION
		select row_id
		from receipt_pah_final
		group by 1
		union
		select row_id
		from receipt_rs_final
		group by 1
	) a) a left join
	receipt_po_final b
		on a.row_id = b.row_id left JOIN
	receipt_ds_final c
		on a.row_id = c.row_id left JOIN
	receipt_pah_final d
		on a.row_id = d.row_id left JOIN
	receipt_rs_final e
		on a.row_id = e.row_id
)
with data
primary index ( row_id )
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
		on receipt_tot_final;

	----------- End - Receipts - Total Receipts

--------------- Start - Error Logging - Total Receipts
-- 2021.10.12 - Error Logs
	-- If any row_id's exist more than once per any processing load, it will be determined as a failure and should be sent to the error logs
	-- 2021.10.25 - Swap of the error logs from insert to a merge to reduce the physical imprint of the redundant records through the delta executions.

MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS AS a
USING
(SELECT ROW_ID
	,CAST(STRTOK(ROW_ID,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS BUS_DT
	,'RECEIPT_TOT_FINAL' AS DATASET_ISSUE
	,CAST(CURRENT_TIMESTAMP AS DATE) PROCESS_DT
	,CURRENT_TIMESTAMP as PROCESS_TIMESTAMP
FROM
	(SELECT ROW_ID
	FROM receipt_tot_final
	GROUP BY 1
	HAVING COUNT(ROW_ID) > 1) a) AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.BUS_DT = b.BUS_DT
	AND a.DATASET_ISSUE = b.DATASET_ISSUE
WHEN MATCHED
THEN UPDATE
	SET
		PROCESS_DT = b.PROCESS_DT
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.BUS_DT
	,b.DATASET_ISSUE
	,b.PROCESS_DT
	,b.PROCESS_TIMESTAMP
	);

--------------- End - Error Logging - Total Receipts

--------------- Aggregatate Final Data
-- 2021.08.09 - Identify all record's that are being processed in totality across all domain data sets
	-- This dataset will be updated for each new data domain built out
	-- 2021.08.17 - Implemented a removal of any records found to be an error (Breaking the rules of design). All these records can be found in error logs but they will not pass through here.
	-- 2021.08.24 - Added dropship receipts

CREATE MULTISET VOLATILE TABLE final_baseline_union AS (
SELECT row_id
FROM trans_base_final
        
UNION

SELECT row_id
FROM demand_base_final
        
UNION

SELECT row_id
FROM demand_dropship_final
        
UNION

SELECT row_id
FROM demand_cancel_final
        
UNION

SELECT row_id
FROM order_ship_final
        
UNION

SELECT row_id
FROM sku_soh_final

UNION

SELECT row_id_boh AS row_id
FROM sku_soh_final

UNION

SELECT row_id
FROM receipt_po_final

UNION

SELECT row_id
FROM receipt_ds_final

UNION

SELECT row_id
FROM receipt_pah_final

UNION

SELECT row_id
FROM receipt_rs_final

UNION

SELECT row_id
FROM receipt_tot_final

UNION

SELECT row_id
FROM store_fulfill_final

) WITH DATA
UNIQUE PRIMARY INDEX (row_id)
ON COMMIT PRESERVE ROWS;
        
COLLECT STATS COLUMN(row_id) ON final_baseline_union;


-- drop table final_baseline_01;
CREATE MULTISET VOLATILE TABLE final_baseline_01 AS
(
SELECT 
	 row_id
 	,CASE WHEN StrTok(row_id,'_',1) IS NULL OR StrTok(row_id,'_',2) IS NULL OR StrTok(row_id,'_',3) IS NULL OR StrTok(row_id,'_',6) IS NULL THEN 'Y'
          ELSE 'N'
     	  END INCOMPLETE_IND
FROM (
    SELECT 
		 a.row_id
    FROM final_baseline_union a 
    LEFT JOIN (
				SELECT 
					 ROW_ID
         		FROM T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS
         		WHERE PROCESS_DT = Current_Date
         		GROUP BY 1
			) b
      ON a.ROW_ID = b.ROW_ID
    WHERE b.ROW_ID IS NULL
    ) a

WHERE ROW_ID IS NOT NULL
  AND Cast(StrTok(a.row_id,'_',2) AS DATE FORMAT 'YYYY-MM-DD') BETWEEN {start_date} and {end_date}
)
WITH DATA
UNIQUE PRIMARY INDEX (row_id)
ON COMMIT PRESERVE ROWS;

collect stats
	unique primary index (row_id)
	,column (row_id)
		on final_baseline_01;

CREATE MULTISET VOLATILE TABLE final_baseline_02 AS (
SELECT
	 a.row_id
    ,StrTok(a.row_id,'_',1) sku_num
    ,Cast(StrTok(a.row_id,'_',2) AS DATE FORMAT 'YYYY-MM-DD') AS day_dt
    ,StrTok(a.row_id,'_',6) AS price_type
    ,StrTok(a.row_id,'_',3) AS loc_idnt
    ,a.incomplete_ind
FROM final_baseline_01 a
) WITH DATA
PRIMARY INDEX (row_id)
ON COMMIT PRESERVE ROWS;

COLLECT STATS ON final_baseline_02 COLUMN(row_id);

/*
-- Validation - The row_id must be unique
select row_id
from trans_base_final
group by 1
having count(row_id) > 1
sample 100;

select row_id
from sku_soh_final
where location_type is null
group by 1
having count(row_id) > 1
sample 100;

select row_id
from receipt_po_final
group by 1
having count(row_id) > 1
sample 100;
*/

-- 2021.08.10 - Validation:How many incomplete record's are there within a 3 day span of sku's
/* select count(*) from final_baseline_01 where incomplete_ind = 'Y'
 * 8,161,341 as of the data built out thus far on 2021.08.10
 */

-- 2021.08.09 - Build out of each record for the day
-- 2022.02.17 - Removed boh and eoh dropship dollars and units from final_baseline
-- drop table final_baseline;
CREATE MULTISET VOLATILE TABLE final_baseline
AS
(
SELECT 
a.row_id
    ,a.sku_num
    ,a.day_dt
    ,a.price_type
    ,a. loc_idnt
    ,b.sls_units AS sales_units
    --2022.10.18 - Using market rate
--  ,b.net_usd_sls_$ as sales_dollars
    ,b.net_sls_$ AS sales_dollars
    ,b.return_units AS return_units
    --2022.10.18 - Using market rate
--  ,b.return_usd_$ as return_dollars
    ,b.return_$ AS return_dollars
    ,b.cost_of_goods_sold
    ,h.demand_units
    ,h.demand_dollars
    ,i.demand_cancel_units
    ,i.demand_cancel_dollars
    ,j.demand_dropship_units
    ,j.demand_dropship_dollars
    ,k.shipped_units
    ,k.shipped_dollars
    ,m.store_fulfill_units
    ,m.store_fulfill_dollars
    ,c.stock_on_hand_qty AS eoh_units
    ,c.stock_on_hand_dollars AS eoh_dollars
    ,n.stock_on_hand_qty AS boh_units
    ,n.stock_on_hand_dollars AS boh_dollars
    ,c.nonsellable_qty AS nonsellable_units
    ,l.receipt_units
    ,l.receipt_dollars
    ,d.receipt_qty AS receipt_po_units
    ,d.receipt_po_dollars AS receipt_po_dollars
    ,f.receipt_qty AS receipt_pah_units
    ,f.receipt_pah_dollars AS receipt_pah_dollars
    ,e.receipt_qty AS receipt_dropship_units
    ,e.receipt_po_dollars AS receipt_dropship_dollars
    ,g.receipt_qty AS receipt_reservestock_units
    ,g.receipt_rs_dollars AS receipt_reservestock_dollars
    ,Coalesce(b.current_price_amt,c.current_price_amt,d.current_price_amt,e.current_price_amt,f.current_price_amt,g.current_price_amt,h.current_price_amt,i.current_price_amt,j.current_price_amt,k.current_price_amt,m.current_price_amt,n.current_price_amt) current_price
    ,Coalesce(b.regular_price_amt,c.regular_price_amt,d.regular_price_amt,e.regular_price_amt,f.regular_price_amt,g.regular_price_amt,h.regular_price_amt,i.regular_price_amt,j.regular_price_amt,k.regular_price_amt,m.regular_price_amt,n.regular_price_amt) regular_price
    ,a.incomplete_ind
-- 2021.08.10: Timestamp is recorded in PST and is as desired as of 2021.08.10
    ,Current_Timestamp AS update_timestamp
    ,Current_Timestamp AS process_timestamp
FROM final_baseline_02 a
    LEFT JOIN
    trans_base_final b
        ON a.row_id = b.row_id LEFT JOIN
    sku_soh_final c
        ON a.row_id = c.row_id LEFT JOIN
    receipt_po_final d
        ON a.row_id = d.row_id LEFT JOIN
-- 2021.08.24 - Incorporated dropship receipts
    receipt_ds_final e
        ON a.row_id = e.row_id LEFT JOIN
-- 2021.09.02 - Incorporated pah receipts
    receipt_pah_final f
        ON a.row_id = f.row_id LEFT JOIN
-- 2021.09.03 - Incorporated rs receipts
    receipt_rs_final g
        ON a.row_id = g.row_id LEFT JOIN
-- 2021.09.22 - Incorporated demand units/dollars
    demand_base_final h
        ON a.row_id = h.row_id LEFT JOIN
-- 2021.09.22 - Incorporated demand cancel units/dollars
    demand_cancel_final i
        ON a.row_id = i.row_id LEFT JOIN
-- 2021.09.23 - Incorporated demand dropship units/dollars
    demand_dropship_final j
        ON a.row_id = j.row_id LEFT JOIN
-- 2021.09.23 - Incorporated order ship units/dollars
    order_ship_final k
        ON a.row_id = k.row_id LEFT JOIN
-- 2021.10.12 - Incorporated Total Receipts
    receipt_tot_final l
        ON a.row_id = l.row_id LEFT JOIN
-- 2021.10.13 - Incorporated store fulfill
    store_fulfill_final m
        ON a.row_id = m.row_id LEFT JOIN
-- 2022.02.17 - Incorporated BOH units/dollars
    sku_soh_final n
        ON a.row_id = n.row_id_boh 
        AND a.row_id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
)
WITH DATA
UNIQUE PRIMARY INDEX (row_id)
ON COMMIT PRESERVE ROWS;

collect stats
	primary index (row_id)
	,column (row_id)
		on final_baseline;


/* Validations

-- Validation Check - Make sure each dataset is unique.
select row_id
from trans_base_final
group by 1
having count(row_id) > 1
sample 100;

select row_id
from sku_soh_final
-- 2021.08.16 - Dropship will be removed until we can figure out how to handle the duplicated offprice/fullprice soh
where location_type is NULL
group by 1
having count(row_id) > 1
sample 100;

select row_id
from receipt_po_final
group by 1
having count(row_id) > 1
sample 100;

select *
from receipt_po_final
where row_id in ('96198838_2021-08-14_34_79.00_79.00_R','96198838_2021-08-14_240_79.00_79.00_R')

select *
from receipt_02_po
where rms_sku_num = '96198838'
order by to_location_id;

select puchase_order_num
	,rms_sku_num
	,to_location_id
	,shipment_qty
	,
from receipt_02_po_location
where rms_sku_num = '96198838'
order by to_location_id
sample 100;

select *
from receipt_po_final
where rms_sku_num = '96198838'
order by row_id
sample 100;


select b.*
from
	(select row_id
	from final_baseline
	group by 1
	having count(row_id) > 1
	sample 10) a JOIN
	final_baseline b
		on a.row_id = b.row_id
order by b.row_id
sample 100;

*/

-------------------- Start - Final
	-- Identify Updates vs. New and update or insert records accordingly to Datalabs

-- 2022.09.22 - As this is a daily backfill of the existing delta process but for an extended period, we are cleaning out and creating days 16-31 brand new
DELETE FROM {environment_schema}.SKU_LOC_PRICETYPE_DAY WHERE DAY_DT between {start_date} and {end_date};

-- 2021.08.10 - Update or insert records based on existing or new
-- 2022.02.17 - Removed boh and eoh dropship dollars and units 
	-- This concept is based on the row_id being whole. The problem today is that the record can be incomplete and will need to accommodate for this elsewhere
MERGE INTO
{environment_schema}.SKU_LOC_PRICETYPE_DAY AS a
USING
FINAL_BASELINE AS b
ON
	a.ROW_ID = b.ROW_ID
	AND a.DAY_DT = b.DAY_DT
WHEN MATCHED
THEN UPDATE
	SET
		SALES_UNITS = b.SALES_UNITS
		,SALES_DOLLARS = b. SALES_DOLLARS
		,RETURN_UNITS = b.RETURN_UNITS
		,RETURN_DOLLARS = b.RETURN_DOLLARS
		,DEMAND_UNITS = b.DEMAND_UNITS
		,DEMAND_DOLLARS = b.DEMAND_DOLLARS
		,DEMAND_CANCEL_UNITS = b.DEMAND_CANCEL_UNITS
		,DEMAND_CANCEL_DOLLARS = b.DEMAND_CANCEL_DOLLARS
		,DEMAND_DROPSHIP_UNITS = b.DEMAND_DROPSHIP_UNITS
		,DEMAND_DROPSHIP_DOLLARS = b.DEMAND_DROPSHIP_DOLLARS
		,SHIPPED_UNITS = b.SHIPPED_UNITS
		,SHIPPED_DOLLARS = b.SHIPPED_DOLLARS
		,STORE_FULFILL_UNITS = b.STORE_FULFILL_UNITS
		,STORE_FULFILL_DOLLARS = b.STORE_FULFILL_DOLLARS
		,EOH_UNITS = b.EOH_UNITS
		,EOH_DOLLARS = b.EOH_DOLLARS
		,BOH_UNITS = b.BOH_UNITS
		,BOH_DOLLARS = b.BOH_DOLLARS
		,NONSELLABLE_UNITS = b.NONSELLABLE_UNITS
		,RECEIPT_UNITS = b.RECEIPT_UNITS
		,RECEIPT_DOLLARS = b.RECEIPT_DOLLARS
		,RECEIPT_PO_UNITS = b.RECEIPT_PO_UNITS
		,RECEIPT_PO_DOLLARS = b.RECEIPT_PO_DOLLARS
		,RECEIPT_PAH_UNITS = b.RECEIPT_PAH_UNITS
		,RECEIPT_PAH_DOLLARS = b.RECEIPT_PAH_DOLLARS
		,RECEIPT_DROPSHIP_UNITS = b.RECEIPT_DROPSHIP_UNITS
		,RECEIPT_DROPSHIP_DOLLARS = b.RECEIPT_DROPSHIP_DOLLARS
		,RECEIPT_RESERVESTOCK_UNITS = b.RECEIPT_RESERVESTOCK_UNITS
		,RECEIPT_RESERVESTOCK_DOLLARS = b.RECEIPT_RESERVESTOCK_DOLLARS
		,CURRENT_PRICE = b.CURRENT_PRICE
		,REGULAR_PRICE = b.REGULAR_PRICE
		,INCOMPLETE_IND = b.INCOMPLETE_IND
		,UPDATE_TIMESTAMP = b.UPDATE_TIMESTAMP
		,COST_OF_GOODS_SOLD = b.COST_OF_GOODS_SOLD
--		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP
WHEN NOT MATCHED
THEN INSERT
	VALUES
	(
	b.ROW_ID
	,b.SKU_NUM
	,b.DAY_DT
	,b.PRICE_TYPE
	,b.LOC_IDNT
	,b.SALES_UNITS
	,b.SALES_DOLLARS
	,b.RETURN_UNITS
	,b.RETURN_DOLLARS
	,b.DEMAND_UNITS
	,b.DEMAND_DOLLARS
	,b.DEMAND_CANCEL_UNITS
	,b.DEMAND_CANCEL_DOLLARS
	,b.DEMAND_DROPSHIP_UNITS
	,b.DEMAND_DROPSHIP_DOLLARS
	,b.SHIPPED_UNITS
	,b.SHIPPED_DOLLARS
	,b.STORE_FULFILL_UNITS
	,b.STORE_FULFILL_DOLLARS
	,b.EOH_UNITS
	,b.EOH_DOLLARS
	,b.BOH_UNITS
	,b.BOH_DOLLARS
	,NULL
	,NULL
	,NULL
	,NULL
	,b.NONSELLABLE_UNITS
	,b.RECEIPT_UNITS
	,b.RECEIPT_DOLLARS
	,b.RECEIPT_PO_UNITS
	,b.RECEIPT_PO_DOLLARS
	,b.RECEIPT_PAH_UNITS
	,b.RECEIPT_PAH_DOLLARS
	,b.RECEIPT_DROPSHIP_UNITS
	,b.RECEIPT_DROPSHIP_DOLLARS
	,b.RECEIPT_RESERVESTOCK_UNITS
	,b.RECEIPT_RESERVESTOCK_DOLLARS
	,b.CURRENT_PRICE
	,b.REGULAR_PRICE
	,b.INCOMPLETE_IND
	,b.UPDATE_TIMESTAMP
	,b.PROCESS_TIMESTAMP
	,NULL 
	,b.COST_OF_GOODS_SOLD 
	,NULL 
	);

-- 2021.09.28 - Update EOH for any records within the delta timeframe based on the records processed today.
	-- This process will restate updates based on the last 4 days for EOH as the delta period is for 3
/* 2021.11.05 - This component of EOH calculation has been removed as the request was to replicate BOH as EOH for each day
UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY
FROM
	(SELECT a.*
	 FROM SKU_EOH_FINAL a LEFT JOIN
	 	(SELECT ROW_ID
		 FROM {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS
		 WHERE PROCESS_DT = CURRENT_DATE
		 GROUP BY 1) b
		 ON a.ROW_ID = b.ROW_ID
	 WHERE b.ROW_ID IS NULL) b
	SET EOH_UNITS = b.STOCK_ON_HAND_QTY
		,EOH_DOLLARS = b.STOCK_ON_HAND_DOLLARS
WHERE SKU_LOC_PRICETYPE_DAY.SKU_IDNT = b.RMS_SKU_ID
	AND SKU_LOC_PRICETYPE_DAY.DAY_DT = b.SNAPSHOT_DATE
	AND SKU_LOC_PRICETYPE_DAY.LOC_IDNT = b.LOCATION_ID
	AND SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date};
*/

--------------- BEGIN: WAC Incorporation
-- Entire WAC Design re-designed and completed on 2022.08.01

-- 2022.04.26 - Identification of WAC data for time period of the delta operation
-- DROP TABLE WAC_01;
CREATE MULTISET VOLATILE TABLE WAC_01
AS 
(
	select sku_num 
		,location_num
		,weighted_average_cost_currency_code
		,weighted_average_cost
		,eff_begin_dt
		,eff_end_dt
		,row_number() over (partition by sku_num, location_num order by eff_end_dt desc) rn
	from PRD_NAP_USR_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM
	-- This date will be variable based on the delta operations of the pipeline
	where eff_end_dt >= {start_date}
)
with data
primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
on commit preserve rows;

/*
collect stats
	primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
	,column (sku_num, location_num, eff_begin_dt, eff_end_dt )
	,column ( rn, eff_end_dt )
		on wac_01;
*/

-- 2022.07.29 - Extract out all row_id's less price type for the time frame being pulled
create multiset volatile table sku_loc_01
as (
	select sku_idnt
		,day_dt
		,loc_idnt
		,row_id
	from {environment_schema}.SKU_LOC_PRICETYPE_DAY slpdv 
	where day_dt between {start_date} and {end_date}
	group by 1,2,3,4
)
with data
primary index ( sku_idnt, day_dt, loc_idnt )
on commit preserve rows;

collect stats
	primary index ( sku_idnt, day_dt, loc_idnt )
	,column ( sku_idnt, day_dt, loc_idnt )
		on sku_loc_01;

-- 2022.07.29 - Identify only CA sku's and prep for CAD/USD conversion
--create multiset volatile table wac_02_can
--as (
--	select a.sku_idnt
--		,a.day_dt
--		,a.loc_idnt
--		,a.row_id
--		,b.weighted_average_cost_currency_code
--		,c.market_rate
--	from sku_loc_01 a 
--		inner join 
--			(select location_num
--				,weighted_average_cost_currency_code
--			from wac_01  
--			where weighted_average_cost_currency_code = 'CAD'
--			group by 1,2) b
--			on a.loc_idnt = b.location_num
--		inner join 
--			(select start_date
--				,end_date
--				,market_rate
--				,from_country_currency_code 
--			 from prd_nap_usr_vws.currency_exchg_rate_dim
--			 where from_country_currency_code = 'CAD'
--			 	and to_country_currency_code = 'USD'
--			 group by 1,2,3,4) c 
--			 on a.day_dt between c.start_date and c.end_date
--			 	and b.weighted_average_cost_currency_code = c.from_country_currency_code
--)
--with DATA 
--primary index ( sku_idnt, day_dt, loc_idnt )
--on commit preserve rows;
--
--collect stats
--	primary index ( sku_idnt, day_dt, loc_idnt )
--	,column ( sku_idnt, day_dt, loc_idnt )
--	,column ( row_id )
--		on wac_02_can;
--	
-- 2022.07.29 - From the data that is to be updated, associate specifically Canadian locations with the currency market rate for final execution
create multiset volatile table sku_loc_final
as (
select sku_idnt
	,day_dt
	,loc_idnt
	,row_id
-- 2022.10.18 - Using market rate
--	,market_rate
from sku_loc_01
--	(select a.sku_idnt
--		,a.day_dt
--		,a.loc_idnt
--		,a.row_id
--		,cast(NULL as DECIMAL(8,4)) market_rate
--	from sku_loc_01 a
--		left join wac_02_can b 
--			on a.row_id = b.row_id
--	where b.row_id is null
--	union all
--	select sku_idnt
--		,day_dt
--		,loc_idnt
--		,row_id
--		,market_rate
--	from wac_02_can) a
group by 1,2,3,4
)
with data
primary index ( sku_idnt, day_dt, loc_idnt )
on commit preserve rows;

collect stats
	primary index ( sku_idnt, day_dt, loc_idnt )
	,column ( sku_idnt, day_dt, loc_idnt )
		on sku_loc_final;
	
-- 2022.07.29 - Identify the WAC for each row_id and convert CAD to USD where relevant	
-- 2022.10.18 - Using market rate
create multiset volatile table wac_final
as (
	select row_id
		,sku_idnt
		,day_dt
		,loc_idnt
--		,market_rate
		,weighted_average_cost
-- 2022.10.18 - Using market rate
		,weighted_average_cost AS weighted_average_cost_new
	from sku_loc_final a 
		inner join wac_01 b 
			on a.sku_idnt = b.sku_num
				and a.loc_idnt = b.location_num
	where a.day_dt between b.eff_begin_dt and eff_end_dt - 1
	group by 1,2,3,4,5,6
)
with DATA 
primary index ( row_id )
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '{partition_start_date}' AND DATE '{partition_end_date}' EACH INTERVAL '1' DAY , NO RANGE)
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
	,column ( day_dt )
		on wac_final;

-- 2022.05.02 - Update the final dataset with WAC values where matched
	-- 2022.07.29 - Adjust for adjusted build to accommodate CAD/US conversion in transit
UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY 
FROM wac_final b
	SET 
		weighted_average_cost = b.weighted_average_cost_new
		,update_timestamp = current_timestamp
WHERE SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date}
	and SKU_LOC_PRICETYPE_DAY.ROW_ID = b.ROW_ID;
	
	
-------------------- WAC THAT ENDS
	-- Overview: The WAC logic above is with the premise that sku/loc/day continually persists over time, i.e. whether with an arbitrary end date or new rows representing differing states of WAC
	-- Currently, there haven't been any sku/loc/day's that end without a current/future state WAC presented. While this may not exist today, we are building this in in the chance it does exist in the future

-- 2022.05.02 - Identify if any of the records that have a WAC value ending doesn't have a new WAC value starting
	-- This is a safeguard statement. While this doesn't exist today, if a WAC value ends without a new value with new timelines begins on tha same day
		-- we want to capture that with the actual effective end date. Otherwise, we end up with a gap situation
CREATE MULTISET VOLATILE TABLE WAC_ENDING
AS 
(
	SELECT *
	FROM WAC_01
	WHERE rn = 1
		and eff_end_dt <> '9999-12-31'
)
WITH DATA 
PRIMARY INDEX ( SKU_NUM, LOCATION_NUM, EFF_BEGIN_DT, EFF_END_DT )
ON COMMIT PRESERVE ROWS;

COLLECT stats
	primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
	,column (sku_num, location_num, eff_begin_dt, eff_end_dt )
		on WAC_ENDING;
	
-- 2022.07.29 - Identify the WAC for each row_id and convert CAD to USD where relevant	
create multiset volatile table wac_ending_final
as (
	select row_id
		,sku_idnt
		,day_dt
		,loc_idnt
--		,market_rate
		,weighted_average_cost
-- 2022.10.18 - Using market rate
--		,weighted_average_cost * coalesce(market_rate,1) weighted_average_cost_new
		,weighted_average_cost AS weighted_average_cost_new
	from sku_loc_final a 
		inner join wac_ending b 
			on a.sku_idnt = b.sku_num
				and a.loc_idnt = b.location_num
	where a.day_dt between b.eff_begin_dt and eff_end_dt
	group by 1,2,3,4,5,6
)
with DATA 
primary index ( row_id )
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '{partition_start_date}' AND DATE '{partition_end_date}' EACH INTERVAL '1' DAY , NO RANGE)
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
	,column ( day_dt )
		on wac_ending_final;

-- 2022.05.02 - Update the WAC value with any gap scenario's if exists
UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY 
FROM WAC_ENDING_FINAL b
	SET 
		weighted_average_cost = b.weighted_average_cost
		,update_timestamp = current_timestamp
WHERE SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date}
	and SKU_LOC_PRICETYPE_DAY.ROW_ID = b.ROW_ID;

--------------- END: WAC Incorporation

--------------- BEGIN: Gross Margin

-- 2022.08.02 - Implementation of product_margin
	-- We are not changing the field name from gross_margin to product_margin as the alter statement would be too much cpu for this.
	-- Rather, we will handle this through the table's view
update {environment_schema}.sku_loc_pricetype_day
	set gross_margin = sku_loc_pricetype_day.sales_dollars - sku_loc_pricetype_day.cost_of_goods_sold
where sku_loc_pricetype_day.day_dt between {start_date} and {end_date}
	and sku_loc_pricetype_day.sales_dollars is not null 
	and sku_loc_pricetype_day.cost_of_goods_sold is not null;

--------------- END: Gross Margin

--------------- BEGIN: Final Stats on permanent output datasets

COLLECT stats
	PRIMARY INDEX ( row_id )
	,COLUMN ( row_id )
	,COLUMN ( day_dt )
	,COLUMN ( sku_idnt, loc_idnt )
	,COLUMN ( PARTITION )
		ON {environment_schema}.SKU_LOC_PRICETYPE_DAY;

collect stats
	primary index ( row_id)
	,column ( row_id )
	,column ( bus_dt )
		on {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS;

--------------- END: Final Stats on permanent output datasets
