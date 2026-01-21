-- begin
DELETE FROM {environment_schema}.SKU_LOC_PRICETYPE_DAY WHERE DAY_DT BETWEEN {start_date} AND {end_date};
DELETE FROM {environment_schema}.SKU_LOC_PRICETYPE_DAY_dev ALL;
DELETE FROM {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS_dev ALL;
DELETE FROM {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS ALL;

create multiset volatile table sku_01
as
(
	select rms_sku_num
	from prd_nap_usr_vws.product_sku_dim
	group by 1
)
with DATA
PRIMARY INDEX (RMS_SKU_NUM)
on commit preserve rows;

create multiset volatile TABLE STORE_01
AS
(
SELECT store_num
	,price_store_num
	,store_type_code
	,CASE
		when price_store_num in (808,867,835,1) then 'FL'
		when price_store_num = -1 and channel_num = 120 then 'FL'
		when price_Store_num in (844,828,338) then 'RK'
		when price_store_num = -1 and channel_num = 250 then 'RK'
		else NULL
	 end STORE_TYPE_CODE_NEW
	,CASE
		when price_store_num in (808,828,338,1) then 'USA'
		when price_store_num = -1 and channel_country = 'US' then 'USA'
		when price_Store_num in (844,867,835) then 'CAN'
		when price_store_num = -1 and channel_country = 'CA' then 'CAN'
		else NULL
	 end STORE_COUNTRY_CODE
	 ,CASE
		 when price_store_num = -1 and channel_num = 120 then 'ONLINE'
		 when price_store_num = -1 and channel_num = 250 then 'ONLINE'
		 else SELLING_CHANNEL
	 END SELLING_CHANNEL
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
	--,b.pricing_start_tmstp
	--,b.pricing_end_tmstp
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
			,case when channel_country = 'US' then 'USA' when channel_country = 'CA' then 'CAN' end as channel_country
			,case when ownership_retail_price_type_code = 'CLEARANCE' then 'C' when ownership_retail_price_type_code = 'REGULAR' then 'R' else ownership_retail_price_type_code end as ownership_price_type
			,ownership_retail_price_amt as ownership_price_amt
			,regular_price_amt
			,selling_retail_price_amt as current_price_amt
			,selling_retail_currency_code as current_price_currency_code
			,case when selling_retail_price_type_code = 'CLEARANCE' then 'C' when selling_retail_price_type_code = 'REGULAR' then 'R' when selling_retail_price_type_code = 'PROMOTION' then 'P' else selling_retail_price_type_code end as current_price_type
			--,pricing_start_tmstp
			--,pricing_end_tmstp
			,eff_begin_tmstp
			,eff_end_tmstp
		FROM prd_nap_usr_vws.PRODUCT_PRICE_TIMELINE_DIM
		WHERE CAST(eff_begin_tmstp as DATE) < CURRENT_DATE + 1
	) b
  ON a.price_store_num = b.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13--,14,15
)
WITH DATA
PRIMARY INDEX (rms_sku_num, store_type_code, channel_country, selling_channel, eff_begin_tmstp, eff_end_tmstp)
ON COMMIT preserve ROWS;

COLLECT STATS
	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,eff_begin_tmstp, eff_end_tmstp)
	,COLUMN (RMS_SKU_NUM)
	,COLUMN(EFF_BEGIN_TMSTP, EFF_END_TMSTP)
		ON PRICE_01;

create multiset volatile TABLE TRANS_BASE_01A
AS
(
SELECT a.SKU_NUM
	,a.BUSINESS_DAY_DATE
	,a.TRAN_DATE
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(CAST(a.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),a.TRAN_TIME)
		ELSE a.TRAN_TIME
	 END TRAN_TIME
	,a.TRAN_TIME as TRAN_TIME_NEW
	,a.INTENT_STORE_NUM
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.INTENT_STORE_NUM)
		ELSE a.INTENT_STORE_NUM
	 END INTENT_STORE_NUM_NEW
	,CASE
		WHEN a.TRAN_TYPE_CODE in ('RETN','EXCH') THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.RINGING_STORE_NUM, a.INTENT_STORE_NUM)
		ELSE COALESCE(a.FULFILLING_STORE_NUM, a.RINGING_STORE_NUM, a.INTENT_STORE_NUM)
	 END INTENT_STORE_NUM_COGS
	,a.TRAN_TYPE_CODE
	,CASE WHEN a.LINE_ITEM_PROMO_ID IS NOT NULL THEN 1 ELSE 0 END AS PROMO_FLAG
	,a.LINE_NET_USD_AMT
	,a.LINE_NET_AMT
	,a.LINE_ITEM_QUANTITY
	,a.LINE_ITEM_SEQ_NUM
	,CASE WHEN a.LINE_ITEM_FULFILLMENT_TYPE = 'VendorDropShip' THEN 1 ELSE 0 END as DROPSHIP_FULFILLED
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW a
WHERE a.SKU_NUM IS NOT NULL
	AND a.UPC_NUM IS NOT NULL
	AND a.UPC_NUM > 0
	AND a.BUSINESS_DAY_DATE BETWEEN {start_date} AND {end_date}
	AND LINE_ITEM_MERCH_NONMERCH_IND = 'MERCH'
	AND a.INTENT_STORE_NUM NOT IN (260,770)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
WITH DATA
PRIMARY INDEX (SKU_NUM,TRAN_TIME)
INDEX (INTENT_STORE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, TRAN_TIME)
	,COLUMN (SKU_NUM, TRAN_TIME)
		ON TRANS_BASE_01A;

INSERT INTO TRANS_BASE_01A
SELECT b.RMS_SKU_NUM
	,a.BUSINESS_DAY_DATE
	,a.TRAN_DATE
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(CAST(a.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),a.TRAN_TIME)
		ELSE a.TRAN_TIME
	 END TRAN_TIME
	,a.TRAN_TIME as TRAN_TIME_NEW
	,a.INTENT_STORE_NUM
	,CASE
		WHEN a.TRAN_TYPE_CODE = 'RETN' THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM,a.INTENT_STORE_NUM)
		ELSE a.INTENT_STORE_NUM
	 END INTENT_STORE_NUM_NEW
	,CASE
		WHEN a.TRAN_TYPE_CODE in ('RETN','EXCH') THEN COALESCE(a.ORIGINAL_RINGING_STORE_NUM, a.RINGING_STORE_NUM, a.INTENT_STORE_NUM)
		ELSE COALESCE(a.FULFILLING_STORE_NUM, a.RINGING_STORE_NUM, a.INTENT_STORE_NUM)
	 END INTENT_STORE_NUM_COGS
	,a.TRAN_TYPE_CODE
	,CASE WHEN a.LINE_ITEM_PROMO_ID IS NOT NULL THEN 1 ELSE 0 END AS PROMO_FLAG
	,a.LINE_NET_USD_AMT
	,a.LINE_NET_AMT
	,a.LINE_ITEM_QUANTITY
	,a.LINE_ITEM_SEQ_NUM
	,CASE WHEN a.LINE_ITEM_FULFILLMENT_TYPE = 'VendorDropShip' THEN 1 ELSE 0 END as DROPSHIP_FULFILLED
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW a
LEFT JOIN
	PRD_NAP_USR_VWS.PRODUCT_UPC_DIM b
		ON a.UPC_NUM = b.UPC_NUM
WHERE a.SKU_NUM IS NULL
	and a.BUSINESS_DAY_DATE BETWEEN {start_date} AND {end_date}
	AND LINE_ITEM_MERCH_NONMERCH_IND = 'MERCH'
	AND a.INTENT_STORE_NUM NOT IN (260,770)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM,TRAN_TIME),
	INDEX (INTENT_STORE_NUM),
	COLUMN (SKU_NUM),
	COLUMN (SKU_NUM,TRAN_TIME),
	COLUMN (INTENT_STORE_NUM)
		ON TRANS_BASE_01A;

CREATE MULTISET VOLATILE TABLE TRANS_BASE_01
AS
(
 select a.sku_num
	,a.business_day_date
	,a.tran_date
	,a.tran_time
	,a.intent_store_num
	,a.intent_store_num_new
	,a.tran_type_code
	,a.promo_flag
	,a.line_net_usd_amt
	,a.line_net_amt
	,a.line_item_quantity
	,a.line_item_seq_num
	,a.weighted_average_cost_currency_code
	,a.weighted_average_cost
from (
	select a.sku_num
		,a.business_day_date
		,a.tran_date
		,a.tran_time
		,a.intent_store_num
		,a.intent_store_num_new
		,a.tran_type_code
		,a.promo_flag
		,a.line_net_usd_amt
		,a.line_net_amt
		,a.line_item_quantity
		,a.line_item_seq_num
		,c.weighted_average_cost_currency_code
		,CASE
			when a.tran_type_code = 'SALE' and dropship_fulfilled = 1 THEN coalesce(c.weighted_average_cost, d.weighted_average_cost)
			when a.tran_type_code = 'SALE' then c.weighted_average_cost
			when a.tran_type_code = 'RETN' then c.weighted_average_cost * -1
			when a.tran_type_code = 'EXCH' and a.line_net_amt <=0 then c.weighted_average_cost * -1
			when a.tran_type_code = 'EXCH' and a.line_net_amt > 0 then c.weighted_average_cost
			else c.weighted_average_cost
		END as weighted_average_cost
	from trans_base_01a a
		LEFT JOIN
		prd_nap_usr_vws.weighted_average_cost_dim c
			on a.sku_num = c.sku_num
				and a.tran_time + interval '8' hour between c.weighted_average_cost_changed_tmstp and c.eff_end_tmstp - interval '0.001' second
				and a.intent_store_num_cogs = c.location_num
		LEFT JOIN
		prd_nap_usr_vws.weighted_average_cost_date_dim d
		  on a.sku_num = d.sku_num
		    and a.business_day_date between d.eff_begin_dt and d.eff_end_dt - 1
		    and a.intent_store_num_cogs = d.location_num
--	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
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
	,a.WEIGHTED_AVERAGE_COST
FROM TRANS_BASE_01 a left join
	STORE_01 b
		ON a.INTENT_STORE_NUM = b.STORE_NUM
GROUP BY 1,2,3,4,5,6,7,8,12,13,14,15,16
)
WITH DATA
PRIMARY INDEX (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME)
INDEX (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM,INTENT_STORE_NUM,STORE_COUNTRY_CODE,TRAN_TIME)
	,INDEX (SKU_NUM,STORE_TYPE_CODE,TRAN_TIME)
	,COLUMN  (SKU_NUM)
		ON TRANS_BASE_02;

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
	,b.regular_price_amt
	,b.selling_retail_price_amt as current_price_amt
	,b.selling_retail_currency_code as current_price_currency_code
	,CASE
		 WHEN b.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
		 WHEN a.PROMO_FLAG = 1 THEN 'P'
		 ELSE COALESCE (case when selling_retail_price_type_code = 'CLEARANCE' then 'C' when selling_retail_price_type_code = 'REGULAR' then 'R' when selling_retail_price_type_code = 'PROMOTION' then 'P' else selling_retail_price_type_code end,'N/A')
	 END AS current_price_type
	,a.WEIGHTED_AVERAGE_COST
	,a.WEIGHTED_AVERAGE_COST * a.LINE_ITEM_QUANTITY AS COST_OF_GOODS_SOLD
FROM TRANS_BASE_02 a
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
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
		ON TRANS_BASE_03;

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
	,sum(line_net_amt) net_sls_$
	,case
		when min(line_net_amt) < 0 then (min(line_net_amt) * -1)
		else null
		end return_$
	,sum(case
		when tran_type_code = 'RETN' then (line_item_quantity * -1)
		else line_item_quantity
	 end) sls_units
	,max(case
		when tran_type_code = 'RETN' then line_item_quantity
		else null
		end)return_units
	,sum(cost_of_goods_sold) cost_of_goods_sold
from (
	select SKU_NUM
		,business_day_date
		,intent_store_num
		,store_type_code
		,CASE
			when tran_type_code = 'EXCH' and line_net_amt >= 0 then 'SALE'
			when tran_type_code = 'EXCH' and line_net_amt < 0 then 'RETN'
			else tran_type_code
			end tran_type_code
		,REGULAR_PRICE_AMT
		,CURRENT_PRICE_AMT
		,CURRENT_PRICE_CURRENCY_CODE
		,CURRENT_PRICE_TYPE
		,sum(line_net_amt) AS line_net_amt
		,sum(line_item_quantity) AS line_item_quantity
		,sum(cost_of_goods_sold) AS cost_of_goods_sold
	from TRANS_BASE_03
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

	------ DROP INTERMEDIATE TRANS TABLES -----
	DROP TABLE TRANS_BASE_01A;
	DROP TABLE TRANS_BASE_01;
	DROP TABLE TRANS_BASE_02;
	DROP TABLE TRANS_BASE_03;
	--------------------------------------------

create multiset volatile TABLE DEMAND_BASE_01
AS
(
select sku_num
	,rms_sku_num
	,sku_type
	,delivery_method_code
	,case when delivery_method_code = 'PICK' THEN destination_node_num ELSE NULL END AS destination_node_num
	,partner_relationship_type_code
	,order_num
	,order_line_num
	,order_tmstp_pacific
	,order_date_pacific
	,order_line_quantity
	,order_line_current_amount
	,case when coalesce(order_line_promotion_discount_amount,0) - coalesce(order_line_employee_discount_amount,0) > 0 then 1 else 0 end as promo_flag
	,source_channel_code
	,source_store_num
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
	AND COALESCE(fraud_cancel_ind, 'N') <> 'Y' -- 2023.11.07 - Fraud Transactions identification 
	AND order_date_pacific <> COALESCE(canceled_date_pacific, CAST('1900-01-01' AS DATE)) -- 2023.11.07 - Same day cancellations identification
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
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
		,CASE
			WHEN PARTNER_RELATIONSHIP_TYPE_CODE = 'ECONCESSION' THEN 5405
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
     		WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
		,CASE
		 	WHEN PARTNER_RELATIONSHIP_TYPE_CODE = 'ECONCESSION' THEN 808
 			WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN SOURCE_CHANNEL_CODE = 'RACK' THEN 828
		 END PRC_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.CANCELED_DATE_PACIFIC
		,a.SHIPPED_DATE_PACIFIC
		,a.PROMO_FLAG
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
		ON DEMAND_BASE_02;

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
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.CANCELED_DATE_PACIFIC
	,a.SHIPPED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,a.ORDER_LINE_ID
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.selling_retail_price_amt as current_price_amt
	,b.selling_retail_currency_code as current_price_currency_code
	,CASE WHEN PROMO_FLAG = 1 AND b.selling_retail_price_type_code = 'REGULAR' then 'P'
	   WHEN b.ownership_retail_price_type_code = 'CLEARANCE' then 'C'
	   ELSE (case when selling_retail_price_type_code = 'CLEARANCE' then 'C' when selling_retail_price_type_code = 'REGULAR' then 'R' when selling_retail_price_type_code = 'PROMOTION' then 'P' else selling_retail_price_type_code end)
	 END AS current_price_type
FROM DEMAND_BASE_02 a
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
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
		ON DEMAND_BASE_03;

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
	,SUM(ORDER_LINE_CURRENT_AMOUNT) DEMAND_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) DEMAND_UNITS
FROM DEMAND_BASE_03
WHERE SKU_NUM <> ''          ---2024-03-05 to filter blank SKUs
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON DEMAND_BASE_FINAL;

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

	------ DROP INTERMEDIATE DEMAND BASE TABLES -----
	DROP TABLE DEMAND_BASE_01;
	DROP TABLE DEMAND_BASE_02;
	DROP TABLE DEMAND_BASE_03;
	--------------------------------------------------

create multiset volatile TABLE DEMAND_DROPSHIP_01
AS
(
select sku_num
	,rms_sku_num
	,sku_type
	,delivery_method_code
	,case when delivery_method_code = 'PICK' THEN destination_node_num ELSE NULL END AS destination_node_num
	,order_num
	,order_line_num
	,order_tmstp_pacific
	,order_date_pacific
	,order_line_quantity
	,order_line_current_amount
	,case when coalesce(order_line_promotion_discount_amount,0) - coalesce(order_line_employee_discount_amount,0) > 0 then 1 else 0 end as promo_flag
	,source_channel_code
	,source_store_num
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
  AND COALESCE(fraud_cancel_ind, 'N') <> 'Y' -- 2023.11.07 - Fraud transactions identification
  AND order_date_pacific <> COALESCE(canceled_date_pacific, CAST('1900-01-01' AS DATE)) -- 2023.11.07 - Same day cancellations identification
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
		,CASE
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
      WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
		,CASE
		  WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN SOURCE_CHANNEL_CODE = 'RACK' THEN 828
		 END PRC_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.CANCELED_DATE_PACIFIC
		,a.SHIPPED_DATE_PACIFIC
		,a.PROMO_FLAG
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
		ON DEMAND_DROPSHIP_02;

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
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.CANCELED_DATE_PACIFIC
	,a.SHIPPED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,a.ORDER_LINE_ID
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.selling_retail_price_amt as current_price_amt
	,b.selling_retail_currency_code as current_price_currency_code
	,CASE WHEN PROMO_FLAG = 1 AND b.selling_retail_price_type_code = 'REGULAR' then 'P'
	   WHEN b.ownership_retail_price_type_code = 'CLEARANCE' then 'C'
	   ELSE (case when selling_retail_price_type_code = 'CLEARANCE' then 'C' when selling_retail_price_type_code = 'REGULAR' then 'R' when selling_retail_price_type_code = 'PROMOTION' then 'P' else selling_retail_price_type_code end)
	 END AS current_price_type
FROM DEMAND_DROPSHIP_02 a
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
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
		ON DEMAND_DROPSHIP_03;

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
	,SUM(ORDER_LINE_CURRENT_AMOUNT) AS DEMAND_DROPSHIP_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) AS DEMAND_DROPSHIP_UNITS
FROM DEMAND_DROPSHIP_03
WHERE SKU_NUM <> ''          ---2024-03-05 to filter blank SKUs
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON DEMAND_DROPSHIP_FINAL;

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

	------ DROP INTERMEDIATE DEMAND DS TABLES -----
	DROP TABLE DEMAND_DROPSHIP_01;
	DROP TABLE DEMAND_DROPSHIP_02;
	DROP TABLE DEMAND_DROPSHIP_03;
	-----------------------------------------------

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
    ,partner_relationship_type_code
	,order_num
    ,order_line_num
    ,order_line_quantity
    ,order_line_current_amount
    ,source_channel_code
    ,source_channel_country_code
    ,source_platform_code
    ,cancel_reason_code
    ,fraud_cancel_ind
from prd_nap_usr_vws.order_line_detail_fact
where canceled_date_pacific BETWEEN {start_date} AND {end_date}
    and cancel_reason_code not like '%FRAUD%'
   and canceled_date_pacific <> order_date_pacific
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
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
		,CASE
			WHEN PARTNER_RELATIONSHIP_TYPE_CODE = 'ECONCESSION' THEN 5405
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' and SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
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
	,COLUMN (ORDER_TMSTP_PACIFIC)
		ON DEMAND_CANCEL_02;

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
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.CANCELED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.selling_retail_price_amt as current_price_amt
	,b.selling_retail_currency_code as current_price_currency_code
	,case when b.selling_retail_price_type_code = 'CLEARANCE' then 'C' when b.selling_retail_price_type_code = 'REGULAR' then 'R' when b.selling_retail_price_type_code = 'PROMOTION' then 'P' else b.selling_retail_price_type_code end as CURRENT_PRICE_TYPE
FROM DEMAND_CANCEL_02 a
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
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
		ON DEMAND_CANCEL_03;

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

	------ DROP INTERMEDIATE DEMAND CANCEL TABLES -----
	DROP TABLE DEMAND_CANCEL_01;
	DROP TABLE DEMAND_CANCEL_02;
	DROP TABLE DEMAND_CANCEL_03;
	--------------------------------------------------

create multiset volatile table store_fulfill_01
as (
select sku_num
	,rms_sku_num
	,order_num
	,order_line_num
	,order_tmstp_pacific
	,order_date_pacific
	,order_line_quantity
	,order_line_current_amount
	,source_channel_code
	,source_platform_code
	,canceled_date_pacific
	,cancel_reason_code
	,fraud_cancel_ind
	,fulfilled_tmstp_pacific
	,fulfilled_date_pacific
	,fulfilled_node_num
	,fulfilled_node_type_code
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
		on store_fulfill_02;

create multiset volatile table store_fulfill_03
as (
select a.sku_num
	,a.fulfilled_tmstp_pacific
	,a.fulfilled_date_pacific
	,a.fulfilled_node_num
	,a.order_line_quantity
	,a.order_line_current_amount
	,a.source_channel_code
	,a.canceled_date_pacific
	,a.price_store_num
	,b.rms_sku_num
	,b.regular_price_amt
	,b.selling_retail_price_amt as current_price_amt
	,b.selling_retail_currency_code as current_price_currency_code
	,case when b.selling_retail_price_type_code = 'CLEARANCE' then 'C' when b.selling_retail_price_type_code = 'REGULAR' then 'R' when b.selling_retail_price_type_code = 'PROMOTION' then 'P' else b.selling_retail_price_type_code end as CURRENT_PRICE_TYPE
from store_fulfill_02 a
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
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

	------ DROP INTERMEDIATE STORE FULFILL TABLES -----
	DROP TABLE store_fulfill_01;
	DROP TABLE store_fulfill_02;
	DROP TABLE store_fulfill_03;
	--------------------------------------------------

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
    ,CASE WHEN delivery_method_code = 'PICK' THEN destination_node_num ELSE NULL END AS destination_node_num
	,partner_relationship_type_code
	,order_num
	,order_line_num
	,order_line_quantity
	,order_line_current_amount
    ,CASE WHEN COALESCE(order_line_promotion_discount_amount,0) - COALESCE(order_line_employee_discount_amount,0) > 0 THEN 1 ELSE 0 END AS promo_flag
    ,source_store_num
	,source_channel_code
	,source_channel_country_code
	,source_platform_code
	,order_line_id
from prd_nap_usr_vws.order_line_detail_fact
where shipped_date_pacific BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
WITH DATA
PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
INDEX (DESTINATION_NODE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC)
	,INDEX (DESTINATION_NODE_NUM)
		ON ORDER_SHIP_01;

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
			WHEN PARTNER_RELATIONSHIP_TYPE_CODE = 'ECONCESSION' THEN 5405
			WHEN DESTINATION_NODE_NUM IS NOT NULL THEN DESTINATION_NODE_NUM
			WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN DESTINATION_NODE_NUM IS NULL AND SOURCE_CHANNEL_CODE = 'RACK' THEN 828
			ELSE NULL
		 END INTENT_STORE_NUM
		,CASE
			WHEN PARTNER_RELATIONSHIP_TYPE_CODE = 'ECONCESSION' THEN 808
		  	WHEN SOURCE_PLATFORM_CODE = 'POS' THEN SOURCE_STORE_NUM
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'US' THEN 808
			WHEN SOURCE_CHANNEL_CODE = 'FULL_LINE' AND SOURCE_CHANNEL_COUNTRY_CODE = 'CA' THEN 867
			WHEN SOURCE_CHANNEL_CODE = 'RACK' THEN 828
		 END PRC_STORE_NUM
		,a.DELIVERY_METHOD_CODE
		,a.ORDER_LINE_QUANTITY
		,a.ORDER_LINE_CURRENT_AMOUNT
		,a.SOURCE_CHANNEL_CODE
		,a.SHIPPED_DATE_PACIFIC
		,a.PROMO_FLAG
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
		ON ORDER_SHIP_02;

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
	,a.ORDER_LINE_CURRENT_AMOUNT
	,a.SOURCE_CHANNEL_CODE
	,a.SHIPPED_DATE_PACIFIC
	,a.PRICE_STORE_NUM
	,a.ORDER_LINE_ID
	,b.RMS_SKU_NUM
	,b.REGULAR_PRICE_AMT
	,b.selling_retail_price_amt as current_price_amt
	,b.selling_retail_currency_code as current_price_currency_code
	,CASE WHEN PROMO_FLAG = 1 AND b.selling_retail_price_type_code = 'REGULAR' then 'P'
	   WHEN b.ownership_retail_price_type_code = 'CLEARANCE' then 'C'
	   ELSE (case when selling_retail_price_type_code = 'CLEARANCE' then 'C' when selling_retail_price_type_code = 'REGULAR' then 'R' when selling_retail_price_type_code = 'PROMOTION' then 'P' else selling_retail_price_type_code end)
	 END AS current_price_type
FROM ORDER_SHIP_02 a
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
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
		ON ORDER_SHIP_03;

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
	,SUM(ORDER_LINE_CURRENT_AMOUNT) AS SHIPPED_DOLLARS
	,SUM(ORDER_LINE_QUANTITY) AS SHIPPED_UNITS
FROM ORDER_SHIP_03
WHERE SKU_NUM <> ''          ---2024-03-05 to filter blank SKUs
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
PRIMARY INDEX (ROW_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (ROW_ID)
	,COLUMN (ROW_ID)
		ON ORDER_SHIP_FINAL;

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

	------ DROP INTERMEDIATE ORDER SHIP TABLES -----
	DROP TABLE ORDER_SHIP_01;
	DROP TABLE ORDER_SHIP_02;
	DROP TABLE ORDER_SHIP_03;
	--------------------------------------------------

CREATE MULTISET VOLATILE TABLE SKU_SOH_LOG
AS
(
select RMS_SKU_ID
	,snapshot_date
	,snapshot_date+1 snapshot_date_boh
	,value_updated_time
	,location_id
	,coalesce(stock_on_hand_qty,0)-coalesce(unavailable_qty,0) immediately_sellable_qty   --2023-12-14 derived field from Logical fact table
	,coalesce(stock_on_hand_qty,0) stock_on_hand_qty
	,coalesce(unavailable_qty,0) nonsellable_qty                                          --2023-12-14 direct field from Logical fact table
-- Added location_type to associate dropship inventory
	,location_type
from PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT      --2023-12-14 changed PHYSICAL_FACT to LOGICAL_FACT table
where (snapshot_date between {start_date_soh} AND {end_date})
and SNAPSHOT_DATE >= '2022-10-26'                 --2024-02-01 Logical Fact has data from 2022-10-26
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
		ON SKU_SOH_LOG;

CREATE MULTISET VOLATILE TABLE SKU_SOH_PHY                     ---2024-02-01 Using Physical Fact table for data before 2022-10-26
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
,location_type
from PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT
where (snapshot_date between {start_date_soh} AND {end_date})
and SNAPSHOT_DATE <'2022-10-26'                      --2024-02-01 Using Physical Fact table for data before 2022-10-26
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
ON SKU_SOH_PHY;

-----------sku_soh_01------------ 

CREATE MULTISET VOLATILE TABLE SKU_SOH_01             --2024-02-01 Combining Physical & Logical Fact to create SKU_SOH_01 table
AS
(
Select *
from SKU_SOH_LOG
UNION
select *
from SKU_SOH_PHY a
WHERE a.location_type IS NULL)
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
SELECT
	a.rms_sku_id
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
FROM sku_soh_01 a
LEFT JOIN
	store_01 b
		on a.location_id = b.store_num
	-- NEW --
	--WHERE a.location_type IS NULL                            ----2023-12-14 location_type has no Null values in logical_fact table
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
with DATA
primary index (rms_sku_id,store_type_code,store_country_code,snapshot_date)
on commit preserve rows;

collect stats
	primary index ( rms_sku_id, store_type_code, store_country_code, snapshot_date)
	,column ( rms_sku_id, store_type_code, store_country_code, snapshot_date)
		on sku_soh_02;

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
		,(a.stock_on_hand_qty * b.ownership_price_amt) stock_on_hand_dollars
		,a.nonsellable_qty
		,a.store_type_code
		,a.store_country_code
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
	where a.immediately_sellable_qty <> 0
		or a.stock_on_hand_qty <> 0
		or a.nonsellable_qty <> 0
	group by 1,2,3,4,5,6,7,8,9,10,11,12,15,16) a
)
with DATA
primary index (ROW_ID)
on commit preserve rows;

collect stats
	primary index (row_id)
	,column (row_id)
		on sku_soh_final;

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

	------ DROP INTERMEDIATE SKU SOH TABLES -----
	DROP TABLE SKU_SOH_LOG;
	DROP TABLE SKU_SOH_PHY;
	DROP TABLE SKU_SOH_01;
	DROP TABLE SKU_SOH_02;
	--------------------------------------------------


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

-- SELECT row_id
-- FROM receipt_po_final
--
-- UNION
--
-- SELECT row_id
-- FROM receipt_ds_final
--
-- UNION
--
-- SELECT row_id
-- FROM receipt_pah_final
--
-- UNION
--
-- SELECT row_id
-- FROM receipt_rs_final
--
-- UNION
--
-- SELECT row_id
-- FROM receipt_tot_final
--
-- UNION

SELECT row_id
FROM store_fulfill_final

) WITH DATA
UNIQUE PRIMARY INDEX (row_id)
ON COMMIT PRESERVE ROWS;

COLLECT STATS COLUMN(row_id) ON final_baseline_union;

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

CREATE MULTISET VOLATILE TABLE final_baseline
AS
(
SELECT
a.row_id
    ,a.sku_num
    ,a.day_dt
    ,a.price_type
    ,a.loc_idnt
    ,b.sls_units AS sales_units
    ,b.net_sls_$ AS sales_dollars
    ,b.return_units AS return_units
    ,b.return_$ AS return_dollars
    ,b.cost_of_goods_sold
    ,CASE WHEN st.channel_num IN (110,111,210,211) THEN COALESCE(b.sls_units,0) + COALESCE(b.return_units,0)
		ELSE h.demand_units
		END AS demand_units
    ,CASE WHEN st.channel_num IN (110,111,210,211) THEN COALESCE(b.net_sls_$,0) + COALESCE(b.return_$,0)
		ELSE h.demand_dollars
		END AS demand_dollars
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
    ,NULL as receipt_units
    ,NULL as receipt_dollars
    ,NULL AS receipt_po_units
    ,NULL AS receipt_po_dollars
    ,NULL AS receipt_pah_units
    ,NULL AS receipt_pah_dollars
    ,NULL AS receipt_dropship_units
    ,NULL AS receipt_dropship_dollars
    ,NULL AS receipt_reservestock_units
    ,NULL AS receipt_reservestock_dollars
    ,Coalesce(b.current_price_amt,c.current_price_amt/*,d.current_price_amt,e.current_price_amt,f.current_price_amt,g.current_price_amt*/,h.current_price_amt,i.current_price_amt,j.current_price_amt,k.current_price_amt,m.current_price_amt,n.current_price_amt) current_price
    ,Coalesce(b.regular_price_amt,c.regular_price_amt/*,d.regular_price_amt,e.regular_price_amt,f.regular_price_amt,g.regular_price_amt*/,h.regular_price_amt,i.regular_price_amt,j.regular_price_amt,k.regular_price_amt,m.regular_price_amt,n.regular_price_amt) regular_price
    ,a.incomplete_ind
    ,Current_Timestamp AS update_timestamp
    ,Current_Timestamp AS process_timestamp
FROM final_baseline_02 a
    LEFT JOIN
    trans_base_final b
        ON a.row_id = b.row_id LEFT JOIN
    sku_soh_final c
        ON a.row_id = c.row_id LEFT JOIN
    -- receipt_po_final d
    --     ON a.row_id = d.row_id LEFT JOIN
    -- receipt_ds_final e
    --     ON a.row_id = e.row_id LEFT JOIN
    -- receipt_pah_final f
    --     ON a.row_id = f.row_id LEFT JOIN
    -- receipt_rs_final g
    --     ON a.row_id = g.row_id LEFT JOIN
    demand_base_final h
        ON a.row_id = h.row_id LEFT JOIN
    demand_cancel_final i
        ON a.row_id = i.row_id LEFT JOIN
    demand_dropship_final j
        ON a.row_id = j.row_id LEFT JOIN
    order_ship_final k
        ON a.row_id = k.row_id LEFT JOIN
    -- receipt_tot_final l
    --     ON a.row_id = l.row_id LEFT JOIN
    store_fulfill_final m
        ON a.row_id = m.row_id LEFT JOIN
    sku_soh_final n
        ON a.row_id = n.row_id_boh
        AND a.row_id IS NOT NULL
    LEFT JOIN prd_nap_usr_vws.store_dim st
		ON CAST(a.loc_idnt AS INTEGER) = st.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
)
WITH DATA
UNIQUE PRIMARY INDEX (row_id)
ON COMMIT PRESERVE ROWS;

collect stats
	primary index (row_id)
	,column (row_id)
		on final_baseline;

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

	------ DROP FINAL TABLES -----
	DROP TABLE TRANS_BASE_FINAL;
	DROP TABLE DEMAND_BASE_FINAL;
	DROP TABLE DEMAND_DROPSHIP_FINAL;
	DROP TABLE DEMAND_CANCEL_FINAL;
	DROP TABLE store_fulfill_final;
	DROP TABLE ORDER_SHIP_FINAL;
	DROP TABLE sku_soh_final;
	DROP TABLE final_baseline_union;
	DROP TABLE final_baseline_01;
	DROP TABLE final_baseline_02;
	DROP TABLE final_baseline;
	DROP TABLE sku_01;
	DROP TABLE STORE_01;
	DROP TABLE price_01;
	------------------------------

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
	where eff_end_dt >= {start_date}
)
with data
primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
on commit preserve rows;


collect stats
	primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
	,COLUMN (EFF_BEGIN_DT)
	,COLUMN (SKU_NUM ,LOCATION_NUM ,EFF_BEGIN_DT ,EFF_END_DT)
	,COLUMN (WEIGHTED_AVERAGE_COST)
		on wac_01;

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

create multiset volatile table sku_loc_final
as (
select sku_idnt
	,day_dt
	,loc_idnt
	,row_id
from sku_loc_01
group by 1,2,3,4
)
with data
primary index ( sku_idnt, day_dt, loc_idnt )
on commit preserve rows;

collect stats
	primary index ( sku_idnt, day_dt, loc_idnt )
	,column ( sku_idnt, day_dt, loc_idnt )
	,COLUMN (SKU_IDNT ,DAY_DT ,LOC_IDNT ,ROW_ID)
	,COLUMN (DAY_DT)
		on sku_loc_final;

create multiset volatile table wac_final
as (
	select row_id
		,sku_idnt
		,day_dt
		,loc_idnt
		,weighted_average_cost
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
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '2019-01-01' AND DATE '2025-01-01' EACH INTERVAL '1' DAY , NO RANGE)
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
	,column ( day_dt )
		on wac_final;

UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY
FROM wac_final b
	SET
		weighted_average_cost = b.weighted_average_cost_new
		,update_timestamp = current_timestamp
WHERE SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date}
	and SKU_LOC_PRICETYPE_DAY.ROW_ID = b.ROW_ID;

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

create multiset volatile table wac_ending_final
as (
	select row_id
		,sku_idnt
		,day_dt
		,loc_idnt
		,weighted_average_cost
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
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '2019-01-01' AND DATE '2025-01-01' EACH INTERVAL '1' DAY , NO RANGE)
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
	,column ( day_dt )
		on wac_ending_final;

UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY
FROM WAC_ENDING_FINAL b
	SET
		weighted_average_cost = b.weighted_average_cost
		,update_timestamp = current_timestamp
WHERE SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date}
	and SKU_LOC_PRICETYPE_DAY.ROW_ID = b.ROW_ID;

update {environment_schema}.sku_loc_pricetype_day
	set gross_margin = sku_loc_pricetype_day.sales_dollars - sku_loc_pricetype_day.cost_of_goods_sold
where sku_loc_pricetype_day.day_dt between {start_date} and {end_date}
	and sku_loc_pricetype_day.sales_dollars is not null
	and sku_loc_pricetype_day.cost_of_goods_sold is not null;

	------ DROP TABLES -----
	DROP TABLE WAC_01;
	DROP TABLE sku_loc_01;
	DROP TABLE sku_loc_final;
	DROP TABLE wac_final;
	DROP TABLE WAC_ENDING;
	DROP TABLE wac_ending_final;
	------------------------------

