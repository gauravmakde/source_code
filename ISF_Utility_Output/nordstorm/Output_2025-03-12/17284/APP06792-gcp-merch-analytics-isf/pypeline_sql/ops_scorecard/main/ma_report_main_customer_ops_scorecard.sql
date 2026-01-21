/*
Customer Ops Scorecard Main
Author: Shaila Karole
Date Created: 22/1/24
Date Last Updated: 18/4/24 --By Shaila Karole
*/

---------------------------------------------------------------------------------------------------------------------------
-- code begins here --
---------------------------------------------------------------------------------------------------------------------------

CREATE MULTISET VOLATILE TABLE WEEKLY_OPS_STANDUP,
 FALLBACK,
 NO BEFORE JOURNAL,
 NO AFTER JOURNAL,
 CHECKSUM = DEFAULT,
 DEFAULT MERGEBLOCKRATIO,
 MAP = TD_MAP1
(
  OPS_NAME VARCHAR(50),
  BANNER VARCHAR(50),
  CHANNEL VARCHAR(50),
  METRIC_NAME VARCHAR(50),
  FISCAL_NUM INTEGER,
  FISCAL_DESC VARCHAR(50),
  LABEL VARCHAR(50), -- Chart Plot Axis (x-axis)
  ROLLING_FISCAL_IND INTEGER,	-- Identifier for Rolling & Forward weeks
  PLAN_OP FLOAT,
  PLAN_CP FLOAT,
  TY FLOAT,
  LY FLOAT,
  DAY_DATE DATE
) PRIMARY INDEX (OPS_NAME,BANNER,METRIC_NAME,FISCAL_NUM,LABEL) ON COMMIT PRESERVE rows;



-- WK Identifier for automating weekly plots
CREATE MULTISET VOLATILE TABLE CURR_WK AS (
SELECT 
      WEEK_IDNT,
      WEEK_START_DAY_DATE,
      WEEK_END_DAY_DATE,
      MONTH_IDNT,
      MONTH_START_DAY_DATE,
      MONTH_END_DAY_DATE
FROM PRD_NAP_BASE_VWS.DAY_CAL_454_DIM
WHERE day_date = {start_date} - 7   
) WITH data PRIMARY INDEX (WEEK_IDNT) ON COMMIT PRESERVE rows ;
 

CREATE MULTISET VOLATILE TABLE CAL_LKUP AS (
SELECT DISTINCT
      a.WEEK_NUM,
      a.WEEK_454_NUM,
      a.MONTH_SHORT_DESC,
      b.WEEK_START_DAY_DATE,
      b.WEEK_END_DAY_DATE,
      a.MONTH_NUM,
      b.MONTH_START_DAY_DATE,
      b.MONTH_END_DAY_DATE,
      a.YEAR_NUM
FROM PRD_NAP_BASE_VWS.DAY_CAL a
LEFT JOIN PRD_NAP_BASE_VWS.DAY_CAL_454_DIM b
on a.WEEK_NUM = b.WEEK_IDNT and a.MONTH_NUM = b.MONTH_IDNT
WHERE b.day_date BETWEEN ({start_date}-30) AND ({start_date}+30)		
) WITH data PRIMARY INDEX (WEEK_NUM) ON COMMIT PRESERVE rows ;


CREATE VOLATILE MULTISET TABLE RETAIL_TRAN_DETAIL_FACT_VW_TEMP AS (
SELECT
  DTL.global_tran_id
, DTL.business_day_date
, DTL.tran_type_code
, DTL.line_item_seq_num
, DTL.line_item_activity_type_code
, DTL.line_item_activity_type_desc
, DTL.unique_source_id
, HDR.acp_id
, HDR.marketing_profile_type_ind
, HDR.deterministic_acp_id
, DTL.ringing_store_num
, HDR.fulfilling_store_num
, DTL.intent_store_num
, DTL.claims_destination_store_num
, DTL.register_num
, DTL.tran_num
, DTL.tran_version_num
, HDR.transaction_identifier
, HDR.transaction_identifier_source
, HDR.sa_tran_status_code
, HDR.original_global_tran_id
, HDR.reversal_flag
, HDR.data_source_code
, HDR.pre_sale_type_code
, HDR.order_num
, HDR.order_date
, HDR.followup_slsprsn_num
, HDR.pbfollowup_slsprsn_num
, DTL.tran_date
, DTL.tran_time
, DTL.sku_num
, DTL.upc_num
, DTL.line_item_merch_nonmerch_ind
, DTL.merch_dept_num
, DTL.nonmerch_fee_code
, DTL.line_item_promo_id
, DTL.line_net_usd_amt
, DTL.line_net_amt
, DTL.line_item_net_amt_currency_code
, DTL.line_item_quantity
, DTL.line_item_fulfillment_type
, DTL.line_item_order_type
, DTL.commission_slsprsn_num
, DTL.employee_discount_flag
, DTL.employee_discount_num
, DTL.employee_discount_usd_amt
, DTL.employee_discount_amt
, DTL.employee_discount_currency_code
, DTL.line_item_promo_usd_amt
, DTL.line_item_promo_amt
, DTL.line_item_promo_amt_currency_code
, DTL.merch_unique_item_id
, DTL.merch_price_adjust_reason
, DTL.line_item_capture_system
, DTL.original_business_date
, DTL.original_ringing_store_num
, DTL.original_register_num
, DTL.original_tran_num
, DTL.original_transaction_identifier
, DTL.original_line_item_usd_amt
, DTL.original_line_item_amt
, DTL.original_line_item_amt_currency_code
, DTL.line_item_regular_price
, DTL.line_item_regular_price_currency_code
, DTL.line_item_split_tax_seq_num
, DTL.line_item_split_tax_pct
, DTL.line_item_split_tax_type
, DTL.line_item_split_tax_amt
, DTL.dw_batch_date
, DTL.dw_sys_load_tmstp
, DTL.dw_sys_updt_tmstp
, DTL.error_flag
, DTL.tran_latest_version_ind
, DTL.item_source
, DTL.line_item_tax_amt
, DTL.line_item_tax_currency_code
, DTL.line_item_tax_usd_amt
, DTL.line_item_tax_exempt_flag
, DTL.line_item_tax_pct
, DTL.price_adj_code
, DTL.tran_line_id
, HDR.deterministic_profile_id
, DTL.financial_retail_tran_record_id
, DTL.financial_retail_tran_line_id
, DTL.original_financial_retail_tran_record_id
, DTL.ownership_model 
, DTL.partner_relationship_num
, DTL.partner_relationship_type_code
,(case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB') then 'NORDSTROM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'RACK'
          else null end as banner)
,(case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'N_STORE'
          when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'N_COM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA') then 'R_STORE'
          when st.business_unit_desc in ('OFFPRICE ONLINE') then 'R_COM'
          else null end as channel)
,(case when dtl.line_net_usd_amt <  0 then dtl.line_item_quantity *(-1)
         when dtl.line_net_usd_amt = 0 then 0
         else dtl.line_item_quantity end as net_items)
,(case when coalesce(dtl.nonmerch_fee_code, '-999') = '6666' then 1 else 0 end as gc_flag)
,(case
        when DTL.line_item_order_type = 'CustInitWebOrder'
        and DTL.line_item_fulfillment_type = 'StorePickUp'
        and DTL.line_item_net_amt_currency_code = 'USD'
        then 808
        when DTL.line_item_order_type = 'CustInitWebOrder'
        and DTL.line_item_fulfillment_type = 'StorePickUp'
        and DTL.line_item_net_amt_currency_code = 'CAD'
        then 867
        else DTL.intent_store_num end as store_num)
,(case when DTL.line_net_usd_amt > 0 then coalesce(HDR.order_date, DTL.tran_date) else DTL.tran_date end as customer_shopped_date)
,(case when DTL.line_net_usd_amt > 0   then acp_id||store_num||customer_shopped_date
        else null end as trip_id)
FROM PRD_NAP_BASE_VWS.RETAIL_TRAN_DETAIL_FACT_VW DTL
JOIN PRD_NAP_BASE_VWS.RETAIL_TRAN_HDR_FACT HDR
ON DTL.global_tran_id = HDR.global_tran_id AND DTL.business_day_date = HDR.business_day_date
join PRD_NAP_BASE_VWS.store_dim as st
    on (case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
                                 and dtl.line_item_net_amt_currency_code = 'CAD' then 867
             when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
                                 and dtl.line_item_net_amt_currency_code = 'USD' then 808
             else dtl.intent_store_num
         end) = st.store_num
WHERE DTL.business_day_date BETWEEN {start_date} - 90 AND {start_date} 
) WITH DATA PRIMARY INDEX (acp_id,business_day_date) ON COMMIT PRESERVE ROWS;


-- TRANSACTIONS (ERTM)
CREATE VOLATILE MULTISET TABLE TRANSACTIONS AS (
SELECT 
WEEK_IDNT,
WEEK_START_DAY_DATE,
WEEK_END_DAY_DATE,
CASE WHEN US_DMA_CODE = -1 THEN 'UNKNOWN'
	 WHEN US_DMA_CODE IS NOT NULL THEN 'US'
     WHEN CA_DMA_CODE IS NOT NULL THEN 'CA'
     ELSE 'UNKNOWN' END AS COUNTRY,
CASE WHEN STR.BUSINESS_UNIT_DESC IN ('FULL LINE','N.COM','FULL LINE CANADA','N.CA','TRUNK CLUB') THEN 'NORDSTROM'
	 WHEN STR.BUSINESS_UNIT_DESC IN ('RACK','OFFPRICE ONLINE', 'RACK CANADA') THEN 'NORDSTROM RACK'
	 END AS BANNER,
COUNT(DISTINCT DTL.ACP_ID) AS CUST_COUNT,
SUM(LINE_NET_USD_AMT) AS NET_SPEND,
SUM(CASE WHEN DTL.LINE_NET_USD_AMT > 0 THEN LINE_NET_USD_AMT ELSE 0 END) AS GROSS_SPEND,
COUNT(DISTINCT CASE WHEN DTL.LINE_NET_USD_AMT > 0 THEN DTL.ACP_ID||STR.STORE_NUM||COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) END) AS TRIPS
FROM RETAIL_TRAN_DETAIL_FACT_VW_TEMP AS DTL
JOIN PRD_NAP_BASE_VWS.STORE_DIM AS STR
ON CASE WHEN LINE_ITEM_ORDER_TYPE LIKE 'CustInit%' AND LINE_ITEM_FULFILLMENT_TYPE = 'StorePickUp' AND DATA_SOURCE_CODE = 'COM'
		THEN RINGING_STORE_NUM ELSE INTENT_STORE_NUM END = STR.STORE_NUM
JOIN PRD_NAP_BASE_VWS.DAY_CAL_454_DIM B ON CASE WHEN LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END = B.DAY_DATE
LEFT JOIN PRD_NAP_BASE_VWS.ANALYTICAL_CUSTOMER AC
ON DTL.ACP_ID = AC.ACP_ID
WHERE CASE WHEN LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END BETWEEN ({start_date}-28) AND (SELECT WEEK_END_DAY_DATE FROM CURR_WK)
AND BUSINESS_UNIT_DESC IN ('FULL LINE', 'FULL LINE CANADA', 'N.CA', 'N.COM', 'OFFPRICE ONLINE', 'RACK', 'RACK CANADA', 'TRUNK CLUB')
AND NOT DTL.ACP_ID IS NULL
GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX (WEEK_IDNT,BANNER) ON COMMIT PRESERVE ROWS;



-- NET SPEND CLARITY 
CREATE VOLATILE MULTISET TABLE CLARITY_NET_SALES AS (
SELECT
WEEK_IDNT,
CASE WHEN business_unit_desc IN ('FULL LINE','N.COM') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE','RACK') THEN 'NORDSTROM RACK' END as banner,
SUM(op_gmv_usd_amt) as net_op_sales_amt_act
FROM 
PRD_NAP_DSA_AI_BASE_VWS.FINANCE_SALES_DEMAND_MARGIN_TRAFFIC_FACT a
JOIN PRD_NAP_BASE_VWS.DAY_CAL_454_DIM b ON a.tran_date = b.day_date
WHERE tran_date BETWEEN ({start_date}-30) AND (SELECT WEEK_END_DAY_DATE FROM CURR_WK)
AND business_unit_desc in ('FULL LINE','N.COM','RACK','OFFPRICE ONLINE')
GROUP BY 1,2
) WITH DATA PRIMARY INDEX (WEEK_IDNT,BANNER) ON COMMIT PRESERVE ROWS;



-- TENDER SPEND
CREATE VOLATILE MULTISET TABLE TENDER_SPEND AS (
select week_num,
CASE WHEN BUSINESS_UNIT_DESC IN ('FULL LINE','N.COM') THEN 'NORDSTROM'
	 WHEN BUSINESS_UNIT_DESC IN ('RACK','OFFPRICE ONLINE') THEN 'NORDSTROM RACK'
	 END AS BANNER,
case when rttf.tender_type_code= 'CASH' then 'Cash'
	 when rttf.tender_type_code= 'AFFIRM' then 'Affirm'
	 when rttf.tender_type_code in ('PAYPAL','PP') then 'Paypal'
	 when rttf.tender_type_code= 'CHECK' then 'Check'
	 when rttf.card_type_code= 'NC' and rttf.card_subtype_code in ('MD','ND') then 'NordDebit'
	 when rttf.card_type_code= 'NV' and rttf.card_subtype_code in ('TP','TV','RV','ND') then 'NordVisa'
	 when rttf.card_type_code= 'NC' and rttf.card_subtype_code in ('RT','TR','RR') then 'NordRetail'
	 when rttf.card_type_code= 'NB' then 'Corp'
	 when rttf.tender_type_code= 'NORDSTROM_NOTE' or rttf.card_subtype_code in ('NN') then 'NordNote'
	 when rttf.tender_type_code= 'GIFT CARD' or rttf.card_subtype_code in ('GC') then 'GiftCard'
	 when rttf.tender_type_code= 'DEBIT_CARD' then '3rdPartyDebit'
	 when rttf.card_type_code='VC' or rttf.card_subtype_code in ('VC','NB') then '3rdPartyVisa'
	 when rttf.card_type_code='MC' then 'MasterCard'
	 when rttf.card_type_code='DS' then 'Discover'
	 when rttf.card_type_code='AE' then 'AMEX'
	 when rttf.card_type_code='JC' then 'JCB'
	 when rttf.card_type_code='DC' then 'Diners'
	 else 'Other' end as tender,
sum(case when rttf.tender_item_usd_amt>0 then rttf.tender_item_usd_amt*gross_item_percent else 0 end) as tender_gross_sales,
sum(case when rttf.tender_item_usd_amt<0 then rttf.tender_item_usd_amt*returned_items_percent else 0 end) as tender_returned
from PRD_NAP_BASE_VWS.RETAIL_TRAN_TENDER_FACT rttf
left join
(select a.*, 
case when gross_item_sales > 0 then (gross_item_sales/gross_item_sales_ttl) else 0 end as gross_item_percent,
case when returned_items < 0 then (returned_items/returned_items_ttl) else 0 end as returned_items_percent
from 
(select distinct a.global_tran_id,
case when line_item_order_type like ('CustInit%')
	  	and line_item_fulfillment_type = 'StorePickUp'
	  	and data_source_code='COM'
	  	then ringing_store_num
	  when intent_store_num is not null then intent_store_num
	  when intent_store_num is null and ringing_store_num is not null then ringing_store_num
	  when intent_store_num is null and ringing_store_num is null and fulfilling_store_num is not null then fulfilling_store_num else intent_store_num end as intent_store_num,
a.tran_type_code, gross_item_sales_ttl, returned_items_ttl,
sum(case when line_net_usd_amt>0 then line_net_usd_amt else 0 end) as gross_item_sales,
sum(case when line_net_usd_amt<0 then line_net_usd_amt else 0 end) as returned_items
from RETAIL_TRAN_DETAIL_FACT_VW_TEMP as a
left join 
(select distinct global_tran_id, tran_type_code,
sum(case when line_net_usd_amt>0 then line_net_usd_amt else 0 end) as gross_item_sales_ttl,
sum(case when line_net_usd_amt<0 then line_net_usd_amt else 0 end) as returned_items_ttl
from RETAIL_TRAN_DETAIL_FACT_VW_TEMP
where business_day_date BETWEEN ({start_date}-30) AND (SELECT WEEK_END_DAY_DATE FROM CURR_WK)
group by 1,2) b
on a.global_tran_id = b.global_tran_id
and a.tran_type_code = b.tran_type_code
where business_day_date BETWEEN ({start_date}-30) AND (SELECT WEEK_END_DAY_DATE FROM CURR_WK)
group by 1,2,3,4,5 ) as a) as rtdf
on rttf.global_tran_id = rtdf.global_tran_id
and rttf.tran_type_code = rtdf.tran_type_code
left join PRD_NAP_BASE_VWS.STORE_DIM sd
on rtdf.intent_store_num = sd.store_num
left join PRD_NAP_BASE_VWS.DAY_CAL dc
on rttf.business_day_date = dc.day_date
where rttf.business_day_date BETWEEN ({start_date}-30) AND (SELECT WEEK_END_DAY_DATE FROM CURR_WK)
AND BUSINESS_UNIT_DESC IN ( 'FULL LINE', 'N.COM', 'OFFPRICE ONLINE', 'RACK')
and rttf.tran_latest_version_ind = 'Y'
group by 1,2,3
) WITH DATA PRIMARY INDEX (WEEK_NUM,BANNER) ON COMMIT PRESERVE ROWS;


--------------------------------------------------------------------------------------------------------------------------------------

-- CUSTOMER COUNT
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'CUSTOMER' AS OPS_NAME,
BANNER,
NULL AS CHANNEL,
'CUSTOMER COUNT' AS METRIC_NAME,
A.WEEK_NUM AS FISCAL_NUM,
'WEEK' AS FISCAL_DESC,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
CASE WHEN WEEK_START_DAY_DATE <= ({start_date}-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
CUSTOMER_COUNT_PLAN AS PLAN_OP,
NULL AS PLAN_CP,
CUST_COUNT AS TY,
NULL AS LY,
NULL AS DAY_DATE
FROM 
(SELECT DISTINCT
COALESCE(A.BANNER, B.BANNER) AS BANNER,
COALESCE(A.WEEK_IDNT, B.WEEK_NUM) AS WEEK_NUM,
CUSTOMER_COUNT_PLAN,
CUST_COUNT
FROM 
(SELECT WEEK_IDNT, WEEK_START_DAY_DATE, WEEK_END_DAY_DATE, BANNER, SUM(CUST_COUNT) AS CUST_COUNT
FROM TRANSACTIONS 
WHERE COUNTRY IN  ('US','UNKNOWN')
GROUP BY 1,2,3,4) A
FULL JOIN t2dl_das_osu.weekly_ops_customer_plan B 	--PLAN DATA CREATED FROM EXCEL FILES
ON A.WEEK_IDNT = B.WEEK_NUM
AND A.BANNER = B.BANNER) A
JOIN CAL_LKUP B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE B.WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK);




-- TRIPS
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'CUSTOMER' AS OPS_NAME,
BANNER,
NULL AS CHANNEL,
'TRIPS' AS METRIC_NAME,
A.WEEK_NUM AS FISCAL_NUM,
'WEEK' AS FISCAL_DESC,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
CASE WHEN WEEK_START_DAY_DATE <= ({start_date}-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
TRIPS_PLAN AS PLAN_OP,
NULL AS PLAN_CP,
TRIPS AS TY,
NULL AS LY,
NULL AS DAY_DATE
FROM 
(SELECT DISTINCT
COALESCE(A.BANNER, B.BANNER) AS BANNER,
COALESCE(A.WEEK_IDNT, B.WEEK_NUM) AS WEEK_NUM,
TRIPS_PLAN,
TRIPS
FROM 
(SELECT WEEK_IDNT, WEEK_START_DAY_DATE, WEEK_END_DAY_DATE, BANNER, SUM(TRIPS) AS TRIPS
FROM TRANSACTIONS 
WHERE COUNTRY IN  ('US','UNKNOWN')
GROUP BY 1,2,3,4) A
FULL JOIN t2dl_das_osu.weekly_ops_customer_plan B 	--PLAN DATA CREATED FROM EXCEL FILES
ON A.WEEK_IDNT = B.WEEK_NUM
AND A.BANNER = B.BANNER) A
JOIN CAL_LKUP B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE B.WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK);



-- SPEND PER TRIP
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'CUSTOMER' AS OPS_NAME,
BANNER,
NULL AS CHANNEL,
'SPEND PER TRIP' AS METRIC_NAME,
A.WEEK_NUM AS FISCAL_NUM,
'WEEK' AS FISCAL_DESC,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
CASE WHEN WEEK_START_DAY_DATE <= ({start_date}-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
SPEND_PER_TRIPS_PLAN AS PLAN_OP,
NULL AS PLAN_CP,
SPEND_PER_TRIP AS TY,
NULL AS LY,
NULL AS DAY_DATE
FROM 
(SELECT DISTINCT
COALESCE(A.BANNER, B.BANNER) AS BANNER,
COALESCE(A.WEEK_IDNT, B.WEEK_NUM) AS WEEK_NUM,
SPEND_PER_TRIPS_PLAN,
SPEND_PER_TRIP
FROM 
(SELECT 
A.WEEK_IDNT,
A.BANNER,
CASE WHEN TRIPS = 0 THEN 0 ELSE net_op_sales_amt_act/TRIPS END AS SPEND_PER_TRIP
FROM 
(SELECT WEEK_IDNT, WEEK_START_DAY_DATE, WEEK_END_DAY_DATE, BANNER, SUM(TRIPS) AS TRIPS
FROM TRANSACTIONS 
WHERE COUNTRY IN  ('US','UNKNOWN')
GROUP BY 1,2,3,4) A
JOIN CLARITY_NET_SALES B ON A.WEEK_IDNT = B.WEEK_IDNT AND A.BANNER = B.BANNER) A
FULL JOIN t2dl_das_osu.weekly_ops_customer_plan B 	--PLAN DATA CREATED FROM EXCEL FILES
ON A.WEEK_IDNT = B.WEEK_NUM
AND A.BANNER = B.BANNER) A
JOIN CAL_LKUP B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE B.WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK);



-- CARD PENETRATION
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'CUSTOMER' AS OPS_NAME,
A.BANNER,
NULL AS CHANNEL,
'CARD PENETRATION' AS METRIC_NAME,
A.WEEK_NUM AS FISCAL_NUM,
'WEEK' AS FISCAL_DESC,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
CASE WHEN WEEK_START_DAY_DATE <= ({start_date}-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
CARD_PENETRATION_PLAN AS PLAN_OP,
NULL AS PLAN_CP,
(CARD_SPEND/JWN_SPEND) AS TY,
NULL AS LY,
NULL AS DAY_DATE
FROM
(SELECT DISTINCT
COALESCE(A.BANNER, B.BANNER) AS BANNER,
COALESCE(A.WEEK_NUM, B.WEEK_NUM) AS WEEK_NUM,
CARD_SPEND,
JWN_SPEND,
CARD_PENETRATION_PLAN
FROM
(SELECT 
WEEK_NUM,
BANNER,
SUM(CASE WHEN TENDER IN ('NordDebit','NordRetail','NordVisa') THEN (tender_gross_sales + TENDER_RETURNED) ELSE 0 END) AS CARD_SPEND,
SUM(tender_gross_sales + TENDER_RETURNED) AS JWN_SPEND
FROM TENDER_SPEND 
GROUP BY 1,2) A
FULL JOIN t2dl_das_osu.weekly_ops_customer_plan B 	
ON A.WEEK_NUM = B.WEEK_NUM
AND A.BANNER = B.BANNER) A
JOIN CAL_LKUP B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE B.WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK);


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Final table insertion

DELETE FROM {environment_schema}.WEEKLY_OPS_STANDUP_AGG
WHERE OPS_NAME = 'CUSTOMER' -- Adding an extra filter since all Ops Scorecard will be using the same table
AND UPDATED_WEEK = (SELECT WEEK_IDNT FROM CURR_WK);

INSERT INTO {environment_schema}.WEEKLY_OPS_STANDUP_AGG
SELECT 
	OPS_NAME,
	BANNER,
	CHANNEL,
	METRIC_NAME,
	FISCAL_NUM,
	FISCAL_DESC,
	LABEL,
	ROLLING_FISCAL_IND,
	PLAN_OP,
	PLAN_CP,
	TY,
	LY,
	WEEK_IDNT AS UPDATED_WEEK, 
	CURRENT_TIMESTAMP as update_timestamp,
	DAY_DATE
FROM WEEKLY_OPS_STANDUP AS A, CURR_WK;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
