
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08176;
DAG_ID=mothership_finance_sales_demand_fact_11521_ACE_ENG;
---     Task_Name=finance_sales_demand_fact;'*/

CREATE TEMPORARY TABLE IF NOT EXISTS loy AS
SELECT DISTINCT acp_id,
 rewards_level,
 start_day_date,
 end_day_date,
 LEAD(DATE_SUB(start_day_date, INTERVAL 1 DAY), 1) OVER (PARTITION BY acp_id ORDER BY start_day_date) AS end_day_date_2
 ,
 COALESCE(LEAD(DATE_SUB(start_day_date, INTERVAL 1 DAY), 1) OVER (PARTITION BY acp_id ORDER BY start_day_date),
  end_day_date) AS end_day_date_lead
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw
WHERE end_day_date >= {{params.start_date}} 
 AND start_day_date <= {{params.end_date}}
 AND start_day_date < end_day_date;

--COLLECT STATISTICS COLUMN (acp_id) ON loy;
--COLLECT STATISTICS COLUMN (acp_id,start_day_date, end_day_date) ON loy;
--COLLECT STATISTICS COLUMN (start_day_date) ON loy;
--COLLECT STATISTICS COLUMN (end_day_date_lead) ON loy;
--COLLECT STATISTICS COLUMN (end_day_date) ON loy;

CREATE TEMPORARY TABLE IF NOT EXISTS rtdf
AS
SELECT business_day_date,
 order_num,
 tran_line_id,
 upc_num,
 acp_id,
 global_tran_id,
 line_item_seq_num,
 line_item_activity_type_desc,
 line_net_amt,
 line_item_quantity,
 tran_type_code,
 sku_num,
 order_date,
 line_item_net_amt_currency_code,
 original_line_item_amt,
 original_line_item_amt_currency_code,
 intent_store_num,
 line_item_order_type,
 line_item_fulfillment_type,
 merch_dept_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw
WHERE line_net_usd_amt <> 0
 AND business_day_date BETWEEN {{params.start_date}} AND ({{params.end_date}}) 
 AND (intent_store_num = 828 AND business_day_date BETWEEN DATE '2020-08-30' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY
       )) OR COALESCE(intent_store_num, 0) <> 828 AND business_day_date BETWEEN DATE '2019-02-03' AND (DATE_SUB(CURRENT_DATE('PST8PDT')
       ,INTERVAL 1 DAY)));

--COLLECT STATISTICS COLUMN(acp_id,order_num, tran_line_id, business_day_date) ON rtdf;
--COLLECT STATISTICS COLUMN (order_num, tran_line_id) ON rtdf;
--COLLECT STATISTICS COLUMN (intent_store_num) ON rtdf;

CREATE TEMPORARY TABLE IF NOT EXISTS pre_oldf
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 fraud_cancel_ind,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 shopper_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 order_tmstp_pacific,
  CASE
  WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
  THEN 'N.COM'
  WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
  THEN 'N.CA'
  WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount_usd, 0) - COALESCE(order_line_employee_discount_amount_usd, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
  CASE
  WHEN LOWER(source_platform_code) = LOWER('POS')
  THEN source_store_num
  WHEN LOWER(CASE
     WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
     THEN 'N.COM'
     WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
     THEN 'N.CA'
     WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
     THEN 'OFFPRICE ONLINE'
     ELSE NULL
     END) = LOWER('N.COM')
  THEN 808
  WHEN LOWER(CASE
     WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
     THEN 'N.COM'
     WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
     THEN 'N.CA'
     WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
     THEN 'OFFPRICE ONLINE'
     ELSE NULL
     END) = LOWER('N.CA')
  THEN 867
  WHEN LOWER(CASE
     WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
     THEN 'N.COM'
     WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
     THEN 'N.CA'
     WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
     THEN 'OFFPRICE ONLINE'
     ELSE NULL
     END) = LOWER('OFFPRICE ONLINE')
  THEN 828
  ELSE NULL
  END AS source_store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf
WHERE order_date_pacific BETWEEN DATE_SUB({{params.start_date}}, INTERVAL 182 DAY) AND ({{params.end_date}}) 
 AND (LOWER(CASE
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.COM'
       WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.CA'
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
       THEN 'OFFPRICE ONLINE'
       ELSE NULL
       END) IN (LOWER('N.COM'), LOWER('N.CA')) AND LOWER(COALESCE(fraud_cancel_ind, 'N')) = LOWER('N') OR LOWER(CASE
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.COM'
       WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.CA'
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
       THEN 'OFFPRICE ONLINE'
       ELSE NULL
       END) = LOWER('OFFPRICE ONLINE') AND order_date_pacific >= DATE '2020-11-21')
 AND LOWER(COALESCE(beauty_sample_ind, 'N')) = LOWER('N')
 AND LOWER(COALESCE(gift_with_purchase_ind, 'N')) = LOWER('N')
 AND order_line_amount > 0;

--COLLECT STATISTICS COLUMN (ent_cust_id) ON pre_oldf;
--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON pre_oldf;
--COLLECT STATISTICS COLUMN (source_platform_code) ON pre_oldf;
--COLLECT STATISTICS COLUMN (order_date_pacific) ON pre_oldf;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_not_null
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag
FROM pre_oldf
WHERE ent_cust_id IS NOT NULL;

--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_not_null;
--COLLECT STATISTICS COLUMN (ent_cust_id) ON oldf_not_null;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_null
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 shopper_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag
FROM pre_oldf
WHERE ent_cust_id IS NULL;

--COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_null;
--COLLECT STATISTICS COLUMN (shopper_id) ON oldf_null;
--COLLECT STATISTICS COLUMN (shopper_id,order_num, order_line_num) ON oldf_null;

DROP TABLE IF EXISTS pre_oldf;


CREATE TEMPORARY TABLE IF NOT EXISTS aacx
AS
SELECT acp_id,
 cust_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_cust_xref
WHERE LOWER(cust_source) = LOWER('ICON')
 AND cust_id IN (SELECT DISTINCT ent_cust_id
   FROM oldf_not_null)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY dw_batch_date DESC)) = 1;

--COLLECT STATISTICS COLUMN (cust_id) ON aacx;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_not_null_acp
AS
SELECT oldf_not_null.order_num,
 oldf_not_null.order_line_num,
 oldf_not_null.source_platform_code,
 oldf_not_null.destination_node_num,
 oldf_not_null.last_released_node_type_code,
 oldf_not_null.shipped_node_type_code,
 oldf_not_null.delivery_method_code,
 oldf_not_null.cancel_reason_code,
 oldf_not_null.order_line_currency_code,
 oldf_not_null.order_line_amount,
 oldf_not_null.order_line_quantity,
 oldf_not_null.order_line_id,
 oldf_not_null.ent_cust_id,
 oldf_not_null.rms_sku_num,
 oldf_not_null.source_channel_country_code,
 oldf_not_null.order_date_pacific,
 oldf_not_null.source_store_num,
 oldf_not_null.order_tmstp_pacific,
 oldf_not_null.business_unit_desc,
 oldf_not_null.promo_flag,
 aacx.acp_id AS aacx_acp_id
FROM oldf_not_null
 LEFT JOIN aacx ON LOWER(oldf_not_null.ent_cust_id) = LOWER(aacx.cust_id);


CREATE TEMPORARY TABLE IF NOT EXISTS aapx_acp
AS
SELECT DISTINCT program_index_id,
 acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_program_xref AS aapx
WHERE LOWER(program_name) IN (LOWER('WEB'))
 AND acp_id IS NOT NULL
 AND program_index_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY acp_id ORDER BY program_index_id DESC)) = 1 AND (ROW_NUMBER() OVER (PARTITION BY
       program_index_id ORDER BY acp_id DESC)) = 1;

--COLLECT STATISTICS COLUMN (program_index_id) ON aapx_acp;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_null_acp
AS
SELECT oldf_null.order_num,
 oldf_null.order_line_num,
 oldf_null.source_platform_code,
 oldf_null.destination_node_num,
 oldf_null.last_released_node_type_code,
 oldf_null.shipped_node_type_code,
 oldf_null.delivery_method_code,
 oldf_null.cancel_reason_code,
 oldf_null.order_line_currency_code,
 oldf_null.order_line_amount,
 oldf_null.order_line_quantity,
 oldf_null.order_line_id,
 oldf_null.ent_cust_id,
 oldf_null.rms_sku_num,
 oldf_null.source_channel_country_code,
 oldf_null.order_date_pacific,
 oldf_null.source_store_num,
 oldf_null.order_tmstp_pacific,
 oldf_null.business_unit_desc,
 oldf_null.promo_flag,
 aapx_acp.acp_id AS shopper_acp_id
FROM oldf_null
 LEFT JOIN aapx_acp ON LOWER(oldf_null.shopper_id) = LOWER(aapx_acp.program_index_id) AND oldf_null.order_date_pacific <
   DATE '2020-02-12';

--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_null_acp;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_acp
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag,
 shopper_acp_id AS oldf_acp_id
FROM oldf_null_acp
UNION ALL
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag,
 aacx_acp_id AS oldf_acp_id
FROM oldf_not_null_acp;

--COLLECT STATISTICS COLUMN (oldf_acp_id) ON oldf_acp;
--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_acp;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_acp_not_null
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag,
 oldf_acp_id AS acp_id
FROM oldf_acp
WHERE oldf_acp_id IS NOT NULL;

--COLLECT STATISTICS COLUMN (acp_id) ON oldf_acp_not_null;
--COLLECT STATISTICS COLUMN (ORDER_DATE_PACIFIC) ON oldf_acp_not_null;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_acp_null
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag
FROM oldf_acp
WHERE oldf_acp_id IS NULL;

--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_acp_null;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_cust
AS
SELECT oldf.order_num,
 oldf.order_line_num,
 oldf.source_platform_code,
 oldf.destination_node_num,
 oldf.last_released_node_type_code,
 oldf.shipped_node_type_code,
 oldf.delivery_method_code,
 oldf.cancel_reason_code,
 oldf.order_line_currency_code,
 oldf.order_line_amount,
 oldf.order_line_quantity,
 oldf.order_line_id,
 oldf.ent_cust_id,
 oldf.rms_sku_num,
 oldf.source_channel_country_code,
 oldf.order_date_pacific,
 oldf.source_store_num,
 oldf.order_tmstp_pacific,
 oldf.business_unit_desc,
 oldf.promo_flag,
 loy.rewards_level
FROM oldf_acp_not_null AS oldf
 LEFT JOIN loy ON LOWER(loy.acp_id) = LOWER(oldf.acp_id) AND oldf.order_date_pacific <= loy.end_day_date_lead AND oldf.order_date_pacific
    >= loy.start_day_date;

--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_cust;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_cust_all
AS
SELECT *
FROM oldf_cust
UNION ALL
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag,
 SUBSTR(NULL, 1, 40) AS rewards_level
FROM oldf_acp_null;

--COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_cust_all;
--COLLECT STATISTICS COLUMN (rms_sku_num) ON oldf_cust_all;
--COLLECT STATISTICS COLUMN (source_channel_country_code) ON oldf_cust_all;
--COLLECT STATISTICS COLUMN (cancel_reason_code) ON oldf_cust_all;
--COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_cust_all;
--COLLECT STATISTICS COLUMN (source_platform_code) ON oldf_cust_all;

DROP TABLE IF EXISTS oldf_cust;


DROP TABLE IF EXISTS oldf_acp_null;


CREATE TEMPORARY TABLE IF NOT EXISTS price_type AS (
SELECT
rms_sku_num
, (eff_begin_tmstp + INTERVAL '1' SECOND) AS eff_begin_tmstp --add 1 second to prevent duplication of items that were ordered exactly when the price type changed. yes, it can happen
, eff_end_tmstp
, case when LOWER(selling_retail_price_type_code) = LOWER('REGULAR') then 'R'
    when LOWER(selling_retail_price_type_code) = LOWER('CLEARANCE') then 'C'
    when LOWER(selling_retail_price_type_code) = LOWER('PROMOTION') then 'P'
    else null end as current_price_type
, case when LOWER(ownership_retail_price_type_code) = LOWER('REGULAR') then 'R'
    when LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE') then 'C'
    when LOWER(ownership_retail_price_type_code) = LOWER('PROMOTION') then 'P'
    else null end as ownership_price_type
, store_num
, CASE
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM_RACK') AND LOWER(ppd.selling_channel) = LOWER('STORE') AND LOWER(ppd.channel_country) = LOWER('CA') THEN 'RACK CANADA'
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM_RACK') AND LOWER(ppd.selling_channel) = LOWER('STORE') AND LOWER(ppd.channel_country) = LOWER('US') THEN 'RACK'
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM_RACK') AND LOWER(ppd.selling_channel) = LOWER('ONLINE') AND LOWER(ppd.channel_country) = LOWER('US') THEN 'OFFPRICE ONLINE'
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM') AND LOWER(ppd.selling_channel) = LOWER('STORE') AND LOWER(ppd.channel_country) = LOWER('CA') THEN 'FULL LINE CANADA'
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM') AND LOWER(ppd.selling_channel) = LOWER('STORE') AND LOWER(ppd.channel_country) = LOWER('US') THEN 'FULL LINE'
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM') AND LOWER(ppd.selling_channel) = LOWER('ONLINE') AND LOWER(ppd.channel_country) = LOWER('CA') THEN 'N.CA'
WHEN LOWER(ppd.channel_brand)= LOWER('NORDSTROM') AND LOWER(ppd.selling_channel) = LOWER('ONLINE') AND LOWER(ppd.channel_country) = LOWER('US') THEN 'N.COM'
ELSE NULL END AS business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim ppd
WHERE eff_end_tmstp >= {{params.start_date}} and eff_begin_tmstp <= {{params.end_date}} 
);

--COLLECT STATISTICS COLUMN(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON price_type;

CREATE TEMPORARY TABLE IF NOT EXISTS pt_join
AS
SELECT order_num,
 order_line_id,
 business_day_date,
 global_tran_id,
 line_item_seq_num
FROM  `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact_base AS sarf
WHERE order_date BETWEEN {{params.start_date}} AND ({{params.end_date}})
 AND (intent_store_num = 828 AND business_day_date BETWEEN DATE '2020-08-30' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY
       )) OR COALESCE(intent_store_num, 0) <> 828 AND business_day_date BETWEEN DATE '2019-02-03' AND (DATE_SUB(CURRENT_DATE('PST8PDT')
       ,INTERVAL 1 DAY)));

--COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_join;

CREATE TEMPORARY TABLE IF NOT EXISTS pt_price_type
AS
SELECT business_day_date,
 global_tran_id,
 line_item_seq_num,
 price_type
FROM  `{{params.gcp_project_id}}`.t2dl_das_sales_returns.retail_tran_price_type_fact AS rtp
WHERE business_day_date BETWEEN {{params.start_date}} AND (DATE_ADD({{params.end_date}}, INTERVAL 365 DAY));

--COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_price_type;

CREATE TEMPORARY TABLE IF NOT EXISTS pt_join_price_type
AS
SELECT pt.order_num,
 pt.order_line_id,
 pt.business_day_date,
 pt.global_tran_id,
 pt.line_item_seq_num,
 ppt.price_type
FROM pt_join AS pt
 LEFT JOIN pt_price_type AS ppt ON pt.business_day_date = ppt.business_day_date AND pt.global_tran_id = ppt.global_tran_id
     AND pt.line_item_seq_num = ppt.line_item_seq_num
QUALIFY (ROW_NUMBER() OVER (PARTITION BY pt.order_num, pt.order_line_id ORDER BY pt.business_day_date DESC)) = 1;

--COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_join_price_type;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_excl_cancels
AS
SELECT *
FROM oldf_cust_all
WHERE cancel_reason_code IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS oldf_cancels
AS
SELECT *
FROM oldf_cust_all
WHERE cancel_reason_code IS NOT NULL;



CREATE TEMPORARY TABLE IF NOT EXISTS oldf_cancels_pt AS (
SELECT
oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, oldf.order_line_id, ent_cust_id, oldf.rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, oldf.business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
, SUBSTR(CASE WHEN promo_flag = 1 AND LOWER(ptc.current_price_type) = LOWER('R') then 'P'
    WHEN LOWER(ptc.ownership_price_type) = LOWER('C') then ptc.ownership_price_type
    ELSE ptc.current_price_type END,1,20) AS merch_price_type
FROM oldf_cancels AS oldf
LEFT JOIN price_type AS ptc 
ON LOWER(oldf.rms_sku_num) = LOWER(ptc.rms_sku_num) 
AND LOWER(oldf.business_unit_desc) = LOWER(ptc.business_unit_desc) 
AND order_tmstp_pacific BETWEEN ptc.eff_begin_tmstp AND ptc.eff_end_tmstp
);



CREATE TEMPORARY TABLE IF NOT EXISTS oldf_excl_cancels_pt_ss
AS
SELECT oldf.order_num,
 oldf.order_line_num,
 oldf.source_platform_code,
 oldf.destination_node_num,
 oldf.last_released_node_type_code,
 oldf.shipped_node_type_code,
 oldf.delivery_method_code,
 oldf.cancel_reason_code,
 oldf.order_line_currency_code,
 oldf.order_line_amount,
 oldf.order_line_quantity,
 oldf.order_line_id,
 oldf.ent_cust_id,
 oldf.rms_sku_num,
 oldf.source_channel_country_code,
 oldf.order_date_pacific,
 oldf.source_store_num,
 oldf.order_tmstp_pacific,
 oldf.business_unit_desc,
 oldf.promo_flag,
 oldf.rewards_level,
 SUBSTR(pt.price_type, 1, 20) AS merch_price_type
FROM oldf_excl_cancels AS oldf
 LEFT JOIN pt_join_price_type AS pt 
 ON LOWER(pt.order_num) = LOWER(oldf.order_num) 
 AND LOWER(pt.order_line_id) = LOWER(oldf.order_line_id);


CREATE TEMPORARY TABLE IF NOT EXISTS oldf_excl_cancels_pt_ss_null
AS
SELECT order_num,
 order_line_num,
 source_platform_code,
 destination_node_num,
 last_released_node_type_code,
 shipped_node_type_code,
 delivery_method_code,
 cancel_reason_code,
 order_line_currency_code,
 order_line_amount,
 order_line_quantity,
 order_line_id,
 ent_cust_id,
 rms_sku_num,
 source_channel_country_code,
 order_date_pacific,
 source_store_num,
 order_tmstp_pacific,
 business_unit_desc,
 promo_flag,
 rewards_level
FROM oldf_excl_cancels_pt_ss
WHERE merch_price_type IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS oldf_excl_cancels_pt_8 AS (
SELECT
oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, oldf.order_line_id, ent_cust_id, oldf.rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, oldf.business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
, SUBSTR(CASE WHEN promo_flag = 1 AND LOWER(pt8.current_price_type) = LOWER('R') then 'P'
    WHEN LOWER(pt8.ownership_price_type) = LOWER('C') then pt8.ownership_price_type
    ELSE pt8.current_price_type END,1,20) AS merch_price_type
FROM oldf_excl_cancels_pt_ss_null AS oldf
LEFT JOIN price_type AS pt8 
ON  LOWER(oldf.rms_sku_num) = LOWER(pt8.rms_sku_num) 
AND LOWER(oldf.business_unit_desc) = LOWER(pt8.business_unit_desc) 
AND order_tmstp_pacific BETWEEN pt8.eff_begin_tmstp AND pt8.eff_end_tmstp 
AND oldf.source_store_num = CAST(TRUNC(CAST(pt8.store_num AS FLOAT64)) AS INT64)
);


CREATE TEMPORARY TABLE IF NOT EXISTS oldf_pt AS (
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level, merch_price_type
FROM oldf_cancels_pt
	UNION ALL
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level, merch_price_type
FROM oldf_excl_cancels_pt_8
	UNION ALL
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level, merch_price_type
FROM oldf_excl_cancels_pt_ss
WHERE merch_price_type is NOT NULL
);

CREATE TEMPORARY TABLE IF NOT EXISTS DEMAND AS (
 SELECT
        order_date_pacific AS tran_date
        --, CAST(EXTRACT(HOUR FROM ORDER_TMSTP_PACIFIC) AS varchar(15)) AS hour_var
        , oldf.business_unit_desc
        , SUBSTR('NAP_ERTM',1,20) AS data_source
        , order_line_currency_code AS currency_code
        , CASE WHEN LOWER(CAST(CASE WHEN COALESCE(source_platform_code, '0') = 'POS' THEN 'DirectToConsumer'
        		WHEN LOWER(delivery_method_code) = LOWER('PICK') AND destination_node_num <> CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) AND destination_node_num > 0 THEN 'BOPUS'
        		    WHEN COALESCE(destination_node_num,-1) > 0 AND destination_node_num <> CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) THEN CASE
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToStore_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToStore_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToStore_from_StoreFulfill'  
        				ELSE 'ShipToStore_OtherUnknown' END
        	        WHEN COALESCE(destination_node_num,-1) <= 0 THEN CASE 
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToHome_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToHome_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToHome_from_StoreFulfill'
        				ELSE 'ShipToHome_OtherUnknown' END
                ELSE 'OtherUnknown'
                END AS STRING)) = LOWER('BOPUS') THEN destination_node_num
        	WHEN LOWER(CAST(CASE WHEN COALESCE(source_platform_code, '0') = 'POS' THEN 'DirectToConsumer'
        		WHEN LOWER(delivery_method_code) = LOWER('PICK') AND destination_node_num <>CAST(TRUNC(CAST( '808' AS FLOAT64)) AS INT64) AND (destination_node_num > 0) THEN 'BOPUS'
        		    WHEN COALESCE(destination_node_num,-1) > 0 AND destination_node_num <>CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) THEN CASE
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToStore_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToStore_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToStore_from_StoreFulfill'  
        				ELSE 'ShipToStore_OtherUnknown' END
        	        WHEN COALESCE(destination_node_num,-1) <= 0 THEN CASE 
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToHome_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToHome_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToHome_from_StoreFulfill'
        				ELSE 'ShipToHome_OtherUnknown' END
                ELSE 'OtherUnknown'
                END AS STRING)) = LOWER('DirectToConsumer') THEN source_store_num
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('N.COM') THEN 808
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('N.CA') THEN 867
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('OFFPRICE ONLINE') THEN 828
        	ELSE NULL END AS store_num_bopus_in_store_demand
        , CASE WHEN LOWER(CAST(CASE WHEN COALESCE(source_platform_code, '0') = 'POS' THEN 'DirectToConsumer'
        		WHEN LOWER(delivery_method_code) = LOWER('PICK') AND destination_node_num <>CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) AND (destination_node_num > 0) THEN 'BOPUS'
        		    WHEN COALESCE(destination_node_num,-1) > 0 AND destination_node_num <>CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) THEN CASE
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToStore_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToStore_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToStore_from_StoreFulfill'  
        				ELSE 'ShipToStore_OtherUnknown' END
        	        WHEN COALESCE(destination_node_num,-1) <= 0 THEN CASE 
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToHome_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToHome_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToHome_from_StoreFulfill'
        				ELSE 'ShipToHome_OtherUnknown' END
                ELSE 'OtherUnknown'
                END AS STRING)) = LOWER('DirectToConsumer') THEN source_store_num
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('N.COM') THEN 808
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('N.CA') THEN 867
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('OFFPRICE ONLINE') THEN 828
        	ELSE NULL END AS store_num_bopus_in_digital_demand
        , CASE WHEN LOWER(psd.div_desc) LIKE LOWER('INACT%') THEN 'INACT' ELSE psd.div_desc END AS merch_division
        	, CASE WHEN dpt.subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785) THEN dpt.subdivision_name ELSE NULL END AS merch_subdivision
        --break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
        	, merch_price_type	
        	, CASE WHEN LOWER(oldf.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA')) THEN th.rack_role_desc ELSE th.nord_role_desc END AS merch_role
	        --, cts_segment
	        --, rfm_segment
        	, rewards_level	
        , CASE WHEN LOWER(oldf.business_unit_desc) IN (LOWER('N.COM'),LOWER('N.CA')) THEN source_platform_code
        	WHEN LOWER(oldf.business_unit_desc) = LOWER('OFFPRICE ONLINE') AND order_date_pacific >= '2021-03-06' THEN source_platform_code 
        	ELSE 'total_nrhl' END AS platform --before 2021-03-06 OLDF isn't accurate at device level for r.com
        , CASE WHEN COALESCE(source_platform_code, '0') = 'POS' THEN 'DirectToConsumer'
        		WHEN LOWER(delivery_method_code) = LOWER('PICK') AND destination_node_num <> CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) AND (destination_node_num > 0) THEN 'BOPUS'
        		    WHEN COALESCE(destination_node_num,-1) > 0 AND destination_node_num <> CAST(TRUNC(CAST('808' AS FLOAT64)) AS INT64) THEN CASE
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToStore_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToStore_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToStore_from_StoreFulfill'  
        				ELSE 'ShipToStore_OtherUnknown' END
        	        WHEN COALESCE(destination_node_num,-1) <= 0 THEN CASE 
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FC') THEN 'ShipToHome_from_FulfillmentCenter'
        				WHEN LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('DS') THEN 'ShipToHome_from_DropShip'
        				WHEN (LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('FL') OR LOWER(COALESCE(last_released_node_type_code, shipped_node_type_code)) = LOWER('RK')) THEN 'ShipToHome_from_StoreFulfill'
        				ELSE 'ShipToHome_OtherUnknown' END
                ELSE 'OtherUnknown'
                END AS customer_journey
        , SUM(CAST(order_line_amount AS NUMERIC)) AS demand
        , SUM(CASE WHEN cancel_reason_code IS NOT NULL THEN order_line_amount ELSE NULL END) AS canceled_demand
        , SUM(order_line_quantity) AS units
        , SUM(CASE WHEN cancel_reason_code IS NOT NULL THEN order_line_quantity ELSE NULL END) AS canceled_units
        , 0 AS gross_merch_sales_amt
        , 0 AS merch_returns_amt
        , 0 AS net_merch_sales_amt
        , 0 AS gross_merch_sales_units
        , 0 AS merch_returns_units
        , 0 AS net_merch_sales_units
        , 0 AS gross_operational_sales_amt
        , 0 AS operational_returns_amt
        , 0 AS net_operational_sales_amt
        , 0 AS gross_operational_sales_units
        , 0 AS operational_returns_units
        , 0 AS net_operational_sales_units
        FROM oldf_pt AS oldf
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS psd 
        ON  LOWER(psd.rms_sku_num) = LOWER(oldf.rms_sku_num) 
        AND LOWER(psd.channel_country) = LOWER(oldf.source_channel_country_code)
        LEFT JOIN (SELECT epm_style_num, class_num, dept_num FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim QUALIFY Row_Number() OVER (PARTITION BY epm_style_num ORDER BY channel_country DESC, dw_batch_date DESC) = 1) AS pdc 
        ON pdc.epm_style_num = psd.epm_style_num
        LEFT JOIN  `{{params.gcp_project_id}}`.t2dl_das_po_visibility.ccs_merch_themes th  
        ON psd.dept_num = CAST(TRUNC(CAST(th.dept_idnt AS FLOAT64)) AS INT64) 
        AND psd.class_num = CAST(TRUNC(CAST(th.class_idnt AS FLOAT64)) AS INT64)
        AND psd.sbclass_num = CAST(TRUNC(CAST(th.sbclass_idnt AS FLOAT64)) AS INT64) -- contact Ivie Okieimen for access
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt 
        ON dpt.dept_num = pdc.dept_num --INNER JOIN excludes non-merch items bc they are missing UPC_num   
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
 );

--COLLECT STATISTICS COLUMN (tran_date, business_unit_desc, store_num_bopus_in_store_demand, merch_division) ON DEMAND;

CREATE TEMPORARY TABLE IF NOT EXISTS return_journey
AS
SELECT return_date,
 return_global_tran_id,
 return_line_item_seq_num,
 line_item_fulfillment_type AS line_item_fulfillment_type2,
 order_num,
 order_line_id
FROM  `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact_base AS sarf
WHERE intent_store_num IN (808, 828, 867)
 AND return_date BETWEEN {{params.start_date}} AND ({{params.end_date}}) 
 AND (intent_store_num = 828 AND return_date BETWEEN DATE '2020-08-30' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR
     COALESCE(intent_store_num, 0) <> 828 AND return_date BETWEEN DATE '2019-02-03' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
       1 DAY)))
 AND line_item_fulfillment_type IS NOT NULL
 AND return_line_item_seq_num IS NOT NULL;


--COLLECT STATISTICS COLUMN (return_date, return_global_tran_id, return_line_item_seq_num) ON return_journey;


CREATE TEMPORARY TABLE IF NOT EXISTS return_journey_oldf
AS
SELECT rj.return_date,
 rj.return_global_tran_id,
 rj.return_line_item_seq_num,
 rj.line_item_fulfillment_type2,
 rj.order_num,
 rj.order_line_id,
 oldf.source_platform_code,
 oldf.destination_node_num,
 oldf.last_released_node_type_code,
 oldf.shipped_node_type_code,
 oldf.delivery_method_code
FROM return_journey AS rj
 LEFT JOIN oldf_acp AS oldf ON LOWER(rj.order_num) = LOWER(oldf.order_num) AND LOWER(rj.order_line_id) = LOWER(oldf.order_line_id
    );

--COLLECT STATISTICS COLUMN (return_date, return_global_tran_id, return_line_item_seq_num) ON return_journey_oldf;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_rtdf
AS
SELECT rtdf.business_day_date,
 rtdf.order_num,
 rtdf.tran_line_id,
 rtdf.upc_num,
 rtdf.acp_id,
 rtdf.global_tran_id,
 rtdf.line_item_seq_num,
 rtdf.line_item_activity_type_desc,
 rtdf.line_net_amt,
 rtdf.line_item_quantity,
 rtdf.tran_type_code,
 rtdf.sku_num,
 rtdf.order_date,
 rtdf.line_item_net_amt_currency_code,
 rtdf.original_line_item_amt,
 rtdf.original_line_item_amt_currency_code,
 rtdf.intent_store_num,
 rtdf.line_item_order_type,
 rtdf.line_item_fulfillment_type,
 rtdf.merch_dept_num,
 oldf.source_platform_code,
 oldf.destination_node_num,
 oldf.last_released_node_type_code,
 oldf.shipped_node_type_code,
 oldf.delivery_method_code
FROM rtdf
 LEFT JOIN oldf_acp AS oldf ON LOWER(rtdf.order_num) = LOWER(oldf.order_num) AND rtdf.tran_line_id = oldf.order_line_num
   
WHERE rtdf.intent_store_num NOT IN (141, 173);

--COLLECT STATISTICS COLUMN (acp_id) ON sales_rtdf;
--COLLECT STATISTICS COLUMN (business_day_date) ON sales_rtdf;
--COLLECT STATISTICS COLUMN (acp_id,order_num,tran_line_id,business_day_date) ON sales_rtdf;

DROP TABLE IF EXISTS rtdf;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_rtdf_segment_loy
AS
SELECT rtdf_seg.business_day_date,
 rtdf_seg.order_num,
 rtdf_seg.tran_line_id,
 rtdf_seg.upc_num,
 rtdf_seg.acp_id,
 rtdf_seg.global_tran_id,
 rtdf_seg.line_item_seq_num,
 rtdf_seg.line_item_activity_type_desc,
 rtdf_seg.line_net_amt,
 rtdf_seg.line_item_quantity,
 rtdf_seg.tran_type_code,
 rtdf_seg.sku_num,
 rtdf_seg.order_date,
 rtdf_seg.line_item_net_amt_currency_code,
 rtdf_seg.original_line_item_amt,
 rtdf_seg.original_line_item_amt_currency_code,
 rtdf_seg.intent_store_num,
 rtdf_seg.line_item_order_type,
 rtdf_seg.line_item_fulfillment_type,
 rtdf_seg.merch_dept_num,
 rtdf_seg.source_platform_code,
 rtdf_seg.destination_node_num,
 rtdf_seg.last_released_node_type_code,
 rtdf_seg.shipped_node_type_code,
 rtdf_seg.delivery_method_code,
 loy.rewards_level
FROM sales_rtdf AS rtdf_seg
 LEFT JOIN loy ON LOWER(loy.acp_id) = LOWER(rtdf_seg.acp_id) AND rtdf_seg.business_day_date <= loy.end_day_date_lead AND
   rtdf_seg.business_day_date >= loy.start_day_date;

--COLLECT STATISTICS COLUMN (business_day_date) ON  sales_rtdf_segment_loy;
--COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON sales_rtdf_segment_loy;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_rtdf_segment_loy_pt
AS
SELECT rtdf_seg_loy.business_day_date,
 rtdf_seg_loy.order_num,
 rtdf_seg_loy.tran_line_id,
 rtdf_seg_loy.upc_num,
 rtdf_seg_loy.acp_id,
 rtdf_seg_loy.global_tran_id,
 rtdf_seg_loy.line_item_seq_num,
 rtdf_seg_loy.line_item_activity_type_desc,
 rtdf_seg_loy.line_net_amt,
 rtdf_seg_loy.line_item_quantity,
 rtdf_seg_loy.tran_type_code,
 rtdf_seg_loy.sku_num,
 rtdf_seg_loy.order_date,
 rtdf_seg_loy.line_item_net_amt_currency_code,
 rtdf_seg_loy.original_line_item_amt,
 rtdf_seg_loy.original_line_item_amt_currency_code,
 rtdf_seg_loy.intent_store_num,
 rtdf_seg_loy.line_item_order_type,
 rtdf_seg_loy.line_item_fulfillment_type,
 rtdf_seg_loy.merch_dept_num,
 rtdf_seg_loy.source_platform_code,
 rtdf_seg_loy.destination_node_num,
 rtdf_seg_loy.last_released_node_type_code,
 rtdf_seg_loy.shipped_node_type_code,
 rtdf_seg_loy.delivery_method_code,
 rtdf_seg_loy.rewards_level,
 SUBSTR(pt.price_type, 1, 20) AS merch_price_type
FROM sales_rtdf_segment_loy AS rtdf_seg_loy
 LEFT JOIN pt_price_type AS pt ON rtdf_seg_loy.business_day_date = pt.business_day_date AND rtdf_seg_loy.global_tran_id
    = pt.global_tran_id AND rtdf_seg_loy.line_item_seq_num = pt.line_item_seq_num;

--COLLECT STATISTICS COLUMN (business_day_date, source_platform_code, rewards_level, merch_price_type) ON sales_rtdf_segment_loy_pt;
--COLLECT STATISTICS COLUMN (upc_num) ON sales_rtdf_segment_loy_pt;
--COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON sales_rtdf_segment_loy_pt;
--COLLECT STATISTICS COLUMN (intent_store_num) ON  sales_rtdf_segment_loy_pt;

CREATE TEMPORARY TABLE IF NOT EXISTS sales
AS
SELECT rtdf.business_day_date AS tran_date,
 st.business_unit_desc,
 SUBSTR('NAP_ERTM', 1, 20) AS data_source,
  CASE
  WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
   IS NOT NULL
  THEN rtdf.original_line_item_amt_currency_code
  ELSE rtdf.line_item_net_amt_currency_code
  END AS currency_code,
 rtdf.intent_store_num AS store_num,
  CASE
  WHEN LOWER(psd.div_desc) LIKE LOWER('INACT%')
  THEN 'INACT'
  ELSE psd.div_desc
  END AS merch_division,
  CASE
  WHEN dpt.subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785)
  THEN dpt.subdivision_name
  ELSE NULL
  END AS merch_subdivision,
 rtdf.merch_price_type,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'))
  THEN th.rack_role_desc
  ELSE th.nord_role_desc
  END AS merch_role,
 rtdf.rewards_level,
 COALESCE(rtdf.source_platform_code, rj.source_platform_code) AS platform,
  CASE
  WHEN LOWER(rtdf.line_item_order_type) = LOWER('StoreInitStoreTake')
  THEN 'StoreTake'
  WHEN LOWER(rtdf.line_item_order_type) = LOWER('StoreInitSameStrSend')
  THEN 'ChargeSend'
  WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('StorePickUp')
  THEN 'BOPUS'
  WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('StoreInitDTCAuto'), LOWER('StoreInitDTCManual'))
  THEN 'DirectToConsumer'
  WHEN LOWER(COALESCE(rtdf.source_platform_code, rj.source_platform_code, FORMAT('%4d', 0))) = LOWER('POS')
  THEN 'DirectToConsumer'
  WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CustInitPhoneOrder'), LOWER('CustInitWebOrder'), LOWER('StoreInitPhoneOrder'
     ), LOWER('StoreInitWebOrder'))
  THEN CASE
   WHEN COALESCE(rtdf.destination_node_num, rj.destination_node_num, - 1) > 0 AND COALESCE(rtdf.destination_node_num, rj
      .destination_node_num) <> 808
   THEN CASE
    WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rj.line_item_fulfillment_type2)) = LOWER('FulfillmentCenter')
    THEN 'ShipToStore_from_FulfillmentCenter'
    WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rj.line_item_fulfillment_type2)) = LOWER('StoreShipSend')
    THEN 'ShipToStore_from_StoreFulfill'
    WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rj.line_item_fulfillment_type2)) = LOWER('VendorDropShip')
    THEN 'ShipToStore_from_DropShip'
    WHEN LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
       )) = LOWER('FC')
    THEN 'ShipToStore_from_FulfillmentCenter'
    WHEN LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
       )) = LOWER('DS')
    THEN 'ShipToStore_from_DropShip'
    WHEN LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
        )) = LOWER('FL') OR LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
        )) = LOWER('RK')
    THEN 'ShipToStore_from_StoreFulfill'
    ELSE 'ShipToStore_OtherUnknown'
    END
   WHEN COALESCE(rtdf.destination_node_num, rj.destination_node_num, - 1) <= 0
   THEN CASE
    WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rj.line_item_fulfillment_type2)) = LOWER('FulfillmentCenter')
    THEN 'ShipToHome_from_FulfillmentCenter'
    WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rj.line_item_fulfillment_type2)) = LOWER('StoreShipSend')
    THEN 'ShipToHome_from_StoreFulfill'
    WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rj.line_item_fulfillment_type2)) = LOWER('VendorDropShip')
    THEN 'ShipToHome_from_DropShip'
    WHEN LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
       )) = LOWER('FC')
    THEN 'ShipToHome_from_FulfillmentCenter'
    WHEN LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
       )) = LOWER('DS')
    THEN 'ShipToHome_from_DropShip'
    WHEN LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
        )) = LOWER('FL') OR LOWER(COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code
        )) = LOWER('RK')
    THEN 'ShipToHome_from_StoreFulfill'
    ELSE 'ShipToHome_OtherUnknown'
    END
   ELSE NULL
   END
  WHEN LOWER(COALESCE(rtdf.delivery_method_code, rj.delivery_method_code)) = LOWER('PICK') AND COALESCE(rtdf.destination_node_num
      , rj.destination_node_num) <> 808 AND COALESCE(rtdf.destination_node_num, rj.destination_node_num) > 0
  THEN 'BOPUS'
  ELSE 'OtherUnknown'
  END AS customer_journey,
 SUM(CASE
   WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
   THEN CASE
    WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
     IS NOT NULL
    THEN rtdf.original_line_item_amt
    ELSE rtdf.line_net_amt
    END
   ELSE 0
   END) AS gross_merch_sales_amt,
 SUM(CASE
   WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
   THEN CASE
     WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
      IS NOT NULL
     THEN - 1 * rtdf.original_line_item_amt
     ELSE rtdf.line_net_amt
     END * - 1
   ELSE 0
   END) AS merch_returns_amt,
  SUM(CASE
    WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
    THEN CASE
     WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
      IS NOT NULL
     THEN rtdf.original_line_item_amt
     ELSE rtdf.line_net_amt
     END
    ELSE 0
    END) - SUM(CASE
    WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
    THEN CASE
      WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
       IS NOT NULL
      THEN - 1 * rtdf.original_line_item_amt
      ELSE rtdf.line_net_amt
      END * - 1
    ELSE 0
    END) AS net_merch_sales_amt,
 SUM(CASE
   WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
   THEN CASE
    WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
     IS NOT NULL
    THEN rtdf.original_line_item_amt
    ELSE rtdf.line_net_amt
    END
   ELSE 0
   END) AS gross_operational_sales_amt,
 SUM(CASE
   WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
   THEN CASE
     WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
      IS NOT NULL
     THEN - 1 * rtdf.original_line_item_amt
     ELSE rtdf.line_net_amt
     END * - 1
   ELSE 0
   END) AS operational_returns_amt,
  SUM(CASE
    WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
    THEN CASE
     WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
      IS NOT NULL
     THEN rtdf.original_line_item_amt
     ELSE rtdf.line_net_amt
     END
    ELSE 0
    END) - SUM(CASE
    WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
    THEN CASE
      WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code) AND rtdf.original_line_item_amt_currency_code
       IS NOT NULL
      THEN - 1 * rtdf.original_line_item_amt
      ELSE rtdf.line_net_amt
      END * - 1
    ELSE 0
    END) AS net_operational_sales_amt,
 SUM(CASE
   WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
   THEN rtdf.line_item_quantity
   ELSE 0
   END) AS gross_merch_sales_units,
 SUM(CASE
   WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
   THEN rtdf.line_item_quantity
   ELSE 0
   END) AS merch_returns_units,
  SUM(CASE
    WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
    THEN rtdf.line_item_quantity
    ELSE 0
    END) - SUM(CASE
    WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
    THEN rtdf.line_item_quantity
    ELSE 0
    END) AS net_merch_sales_units,
 SUM(CASE
   WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
   THEN rtdf.line_item_quantity
   ELSE 0
   END) AS gross_operational_sales_units,
 SUM(CASE
   WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
   THEN rtdf.line_item_quantity
   ELSE 0
   END) AS operational_returns_units,
  SUM(CASE
    WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('SALE')
    THEN rtdf.line_item_quantity
    ELSE 0
    END) - SUM(CASE
    WHEN LOWER(rtdf.line_item_activity_type_desc) = LOWER('RETURN')
    THEN rtdf.line_item_quantity
    ELSE 0
    END) AS net_operational_sales_units
FROM sales_rtdf_segment_loy_pt AS rtdf
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON rtdf.intent_store_num = st.store_num
 LEFT JOIN (SELECT upc_num,
   rms_sku_num,
   epm_sku_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY upc_num ORDER BY dw_batch_date DESC, epm_sku_num DESC)) = 1) AS pud ON LOWER(pud
   .upc_num) = LOWER(rtdf.upc_num)
 LEFT JOIN (SELECT epm_sku_num,
   epm_style_num,
   rms_style_num,
   div_desc,
   sbclass_num,
   class_num,
   dept_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num ORDER BY channel_country DESC, dw_batch_date DESC)) = 1) AS psd
 ON pud.epm_sku_num = psd.epm_sku_num
 LEFT JOIN (SELECT epm_style_num,
   class_num,
   dept_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_style_num ORDER BY channel_country DESC, dw_batch_date DESC)) = 1) AS pdc
 ON psd.epm_style_num = pdc.epm_style_num
 LEFT JOIN  `{{params.gcp_project_id}}`.t2dl_das_po_visibility.ccs_merch_themes AS th ON psd.dept_num = CAST(th.dept_idnt AS FLOAT64) AND psd.class_num
     = CAST(th.class_idnt AS FLOAT64) AND psd.sbclass_num = CAST(th.sbclass_idnt AS FLOAT64)
 LEFT JOIN return_journey_oldf AS rj ON rtdf.business_day_date = rj.return_date AND rtdf.global_tran_id = rj.return_global_tran_id
     AND rtdf.line_item_seq_num = rj.return_line_item_seq_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt ON dpt.dept_num = CAST(COALESCE(FORMAT('%11d', pdc.dept_num), rtdf.merch_dept_num) AS FLOAT64)
  
WHERE LOWER(st.business_unit_desc) <> LOWER('TRUNK CLUB')
 AND dpt.division_num NOT IN (800, 900, 100)
GROUP BY tran_date,
 st.business_unit_desc,
 data_source,
 currency_code,
 store_num,
 merch_division,
 merch_subdivision,
 rtdf.merch_price_type,
 merch_role,
 rtdf.rewards_level,
 platform,
 customer_journey
HAVING gross_merch_sales_amt + ABS(merch_returns_amt) + ABS(gross_merch_sales_amt - merch_returns_amt) +
          gross_operational_sales_amt + ABS(operational_returns_amt) + ABS(gross_operational_sales_amt -
          operational_returns_amt) + gross_merch_sales_units + ABS(merch_returns_units) + ABS(gross_merch_sales_units -
       merch_returns_units) + gross_operational_sales_units + ABS(operational_returns_units) + ABS(gross_operational_sales_units
     - operational_returns_units) <> 0
UNION ALL
SELECT bdd.day_date AS tran_date,
 'OFFPRICE ONLINE' AS business_unit_desc,
 'NRHL_redshift' AS data_source,
 'USD' AS currency_code,
 828 AS store_num,
 'total_nrhl' AS merch_division,
 'total_nrhl' AS merch_subdivision,
 'total_nrhl' AS merch_price_type,
 'total_nrhl' AS merch_role,
 'total_nrhl' AS rewards_level,
 'total_nrhl' AS platform,
 'total_nrhl' AS customer_journey,
 bdd.revenueshipped AS gross_merch_sales_amt,
 r.merch_returns_amt,
 bdd.revenuenetsales AS net_merch_sales_amt,
 bdd.revenueshipped AS gross_operational_sales_amt,
 r.merch_returns_amt AS operational_returns_amt,
 bdd.revenuenetsales AS net_operational_sales_amt,
 u.units_shipped AS gross_merch_sales_units,
 r.merch_returns_units,
  u.units_shipped - r.merch_returns_units AS net_merch_sales_units,
 u.units_shipped AS gross_operational_sales_units,
 r.merch_returns_units AS operational_returns_units,
  u.units_shipped - r.merch_returns_units AS net_operational_sales_units
FROM  `{{params.gcp_project_id}}`.t2dl_das_mothership.nrhl_history_redshift_merch_revenue AS bdd
 LEFT JOIN (SELECT day_date,
   units_shipped
  FROM  `{{params.gcp_project_id}}`.t2dl_das_mothership.nrhl_history_redshift_merch_units) AS u ON bdd.day_date = u.day_date
 LEFT JOIN (SELECT date_id,
   SUM(merch_returns_amt) AS merch_returns_amt,
   SUM(merch_returns_units) AS merch_returns_units
  FROM  `{{params.gcp_project_id}}`.t2dl_das_mothership.nrhl_history_redshift_cancels_todt_returns
  GROUP BY date_id) AS r ON bdd.day_date = CAST(r.date_id AS DATE)
WHERE bdd.day_date BETWEEN DATE '2019-02-03' AND DATE '2020-08-29'
 AND bdd.day_date BETWEEN {{params.start_date}} AND ({{params.end_date}}); 

--COLLECT STATISTICS COLUMN (tran_date,business_unit_desc,store_num) ON sales;

DELETE FROM  `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.finance_sales_demand_fact
WHERE tran_date BETWEEN {{params.start_date}} AND ({{params.end_date}});



INSERT INTO  `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.FINANCE_SALES_DEMAND_FACT
WITH can as
(
SELECT 
date_id
--, CAST(hour_id AS varchar(15)) AS hour_var 
, SUM(units_cancelled) AS canceled_units
, SUM(revenue_cancelled) AS canceled_demand
FROM  `{{params.gcp_project_id}}`.t2dl_DAS_MOTHERSHIP.nrhl_history_redshift_cancels_todt_returns --originally T3DL_ACE_OP.birdseye_cancels_todt_returns_historical
WHERE date_id BETWEEN '2019-02-03' AND '2020-11-20'
GROUP BY 1
)
, bd AS 
(
SELECT 
day_date
--, CAST(hour_id AS varchar(15)) AS hour_var 
, SUM(demand) AS demand --incl cancels AND fraud
, SUM(units) AS units --incl cancels AND fraud
FROM  `{{params.gcp_project_id}}`.t2dl_DAS_MOTHERSHIP.nrhl_history_redshift_birdseye_hourly --originally T3DL_ACE_OP.birdseye_hourly
WHERE day_date BETWEEN '2019-02-03' AND '2020-11-20'
GROUP BY 1
)
, combo AS 
( SELECT
        tran_date
        --, CAST(EXTRACT(HOUR FROM ORDER_TMSTP_PACIFIC) AS varchar(15)) AS hour_var
        , business_unit_desc
        , data_source
        , currency_code
        , store_num_bopus_in_store_demand
        , store_num_bopus_in_digital_demand
        , merch_division
      	, merch_subdivision
      --break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
      	, merch_price_type	
      	, merch_role
        , CAST(null AS STRING) as cts_segment
        , CAST(null AS STRING) as rfm_segment
      	, rewards_level	
        , platform --before 2021-03-06 OLDF isn't accurate at device level for r.com
        , customer_journey
        , cast(demand.demand as numeric) as demand
        , canceled_demand
        , units
        , canceled_units
        , gross_merch_sales_amt
        , merch_returns_amt
        , net_merch_sales_amt
        , gross_merch_sales_units
        , merch_returns_units
        , net_merch_sales_units
        , gross_operational_sales_amt
        , operational_returns_amt
        , net_operational_sales_amt
        , gross_operational_sales_units
        , operational_returns_units
        , net_operational_sales_units
        FROM DEMAND
        	UNION ALL
        --add r.com historical demand data (pre project rocket)- before 2021-03-06 OLDF isn't accurate at device level for r.com
        --before 2020-11-21 OLDF isn't accurate at total demand/units level for r.com
        SELECT 
        CAST(bd.day_date AS date) AS tran_date
        --, bd.hour_var 
        , 'OFFPRICE ONLINE' AS business_unit_desc
        , 'NRHL_redshift' AS data_source
        , 'USD' AS currency_code
        , 828 AS store_num_bopus_in_store_demand
        , 828 AS store_num_bopus_in_digital_demand
        , 'total_nrhl' AS merch_division
        , 'total_nrhl' AS merch_subdivision
        , 'total_nrhl' AS merch_price_type	
        , 'total_nrhl' AS merch_role
        , CAST(null AS STRING) AS cts_segment
        , CAST(null AS STRING) AS rfm_segment
        , 'total_nrhl' AS rewards_level	
        , 'total_nrhl' AS platform
        , 'total_nrhl' AS customer_journey
        , CAST(demand AS NUMERIC) AS demand
        , CAST(canceled_demand AS NUMERIC) AS canceled_demand
        , CAST(units AS NUMERIC) AS units
        , CAST(canceled_units AS NUMERIC) AS canceled_units
        --, CAST(orders AS decimal(38,0)) AS orders 
        , 0 AS gross_merch_sales_amt
        , 0 AS merch_returns_amt
        , 0 AS net_merch_sales_amt
        , 0 AS gross_merch_sales_units
        , 0 AS merch_returns_units
        , 0 AS net_merch_sales_units
        , 0 AS gross_operational_sales_amt
        , 0 AS operational_returns_amt
        , 0 AS net_operational_sales_amt
        , 0 AS gross_operational_sales_units
        , 0 AS operational_returns_units
        , 0 AS net_operational_sales_units
        FROM bd 
        INNER JOIN can 
        on bd.day_date = can.date_id --and bd.hour_var = can.hour_var 
        WHERE CAST(bd.day_date AS DATE) BETWEEN '2019-02-03' AND '2020-11-20' 
        --and bd.day_date IN ('2019-02-04', '2020-02-04') --for testing
        AND CAST(bd.day_date as DATE) BETWEEN {{params.start_date}} AND {{params.end_date}} --Need to ADD
        	UNION ALL
        --sales data
        SELECT
        tran_date
        --, hour_var
        , business_unit_desc
        , data_source
        , currency_code
        , store_num AS store_num_bopus_in_store_demand
        , store_num AS store_num_bopus_in_digital_demand
        , merch_division
        , merch_subdivision
        , merch_price_type	
        , merch_role
        , CAST(null AS STRING) as cts_segment
        , CAST(null AS STRING) as rfm_segment
        , rewards_level	
        , platform 
        , customer_journey
        , cast(NULL as numeric) AS demand
        , NULL AS canceled_demand
        , NULL AS units
        , NULL AS canceled_units
        , SUM(gross_merch_sales_amt) AS gross_merch_sales_amt
        , SUM(merch_returns_amt) AS merch_returns_amt
        , SUM(net_merch_sales_amt) AS net_merch_sales_amt
        , SUM(gross_merch_sales_units) AS gross_merch_sales_units
        , SUM(merch_returns_units) AS merch_returns_units
        , SUM(net_merch_sales_units) AS net_merch_sales_units
        , SUM(gross_operational_sales_amt) AS gross_operational_sales_amt
        , SUM(operational_returns_amt) AS operational_returns_amt
        , SUM(net_operational_sales_amt) AS net_operational_sales_amt
        , SUM(gross_operational_sales_units) AS gross_operational_sales_units
        , SUM(operational_returns_units) AS operational_returns_units
        , SUM(net_operational_sales_units) AS net_operational_sales_units
        FROM sales AS fsf
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

SELECT 
-- Time dimensions:
tran_date
--, hour_var
-- Other dimensions:
, sd.business_unit_desc AS business_unit_desc_bopus_in_store_demand
, sd2.business_unit_desc AS business_unit_desc_bopus_in_digital_demand
, data_source
, currency_code
, combo.store_num_bopus_in_store_demand
, combo.store_num_bopus_in_digital_demand
, merch_division
, merch_subdivision
, merch_price_type	
, merch_role
, cts_segment
, rfm_segment
, rewards_level	
, platform
, customer_journey
-- demand
, CAST(CASE WHEN LOWER(sd.business_unit_desc) IN (LOWER('N.CA'),LOWER('N.COM'), LOWER('OFFPRICE ONLINE')) then SUM(demand)
	ELSE (SUM(CASE WHEN LOWER(customer_journey) IN (LOWER('BOPUS'),LOWER('DirectToConsumer')) then COALESCE(demand,0) ELSE gross_merch_sales_amt END)) 
	END AS NUMERIC) AS demand_amt_bopus_in_store --shift bopus AND DTC demand FROM digital to store. 	
, CAST(CASE WHEN LOWER(sd2.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE')) then SUM(demand)
	ELSE (SUM(CASE WHEN LOWER(customer_journey) IN (LOWER('BOPUS'), LOWER('DirectToConsumer')) then COALESCE(demand,0) ELSE gross_merch_sales_amt END)) 
	END AS NUMERIC) AS demand_amt_bopus_in_digital --shift DTC demand FROM digital to store, keep bopus IN digital.
, SUM(canceled_demand) AS demand_canceled_amt
, CAST(CASE WHEN LOWER(sd.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE')) then SUM(units)
	ELSE (SUM(CASE WHEN LOWER(customer_journey) IN (LOWER('BOPUS'), 'DirectToConsumer') then COALESCE(units,0) ELSE gross_merch_sales_units END))
	END AS NUMERIC) AS demand_units_bopus_in_store --shift bopus AND DTC units FROM digital to store. 	
, CAST(CASE WHEN LOWER(sd2.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE')) then SUM(units)
	ELSE (SUM(CASE WHEN LOWER(customer_journey) IN (LOWER('BOPUS'),LOWER('DirectToConsumer')) then COALESCE(units,0) ELSE gross_merch_sales_units END))
	END AS NUMERIC) AS demand_units_bopus_in_digital --shift DTC units FROM digital to store, keep bopus IN digital.	
, SUM(canceled_units) AS demand_canceled_units
-- sales
, CAST(SUM(gross_merch_sales_amt) AS NUMERIC) AS gross_merch_sales_amt
, CAST(SUM(merch_returns_amt) AS NUMERIC) AS merch_returns_amt
, CAST(SUM(net_merch_sales_amt) AS NUMERIC) AS net_merch_sales_amt
, CAST(SUM(gross_operational_sales_amt) AS NUMERIC) AS gross_operational_sales_amt
, CAST(SUM(operational_returns_amt) AS NUMERIC) AS operational_returns_amt
, CAST(SUM(net_operational_sales_amt) AS NUMERIC) AS net_operational_sales_amt
, CAST(SUM(gross_merch_sales_units) AS NUMERIC) AS gross_merch_sales_units
, CAST(SUM(merch_returns_units) AS NUMERIC) AS merch_returns_units
, CAST(SUM(net_merch_sales_units) AS NUMERIC) AS net_merch_sales_units
, CAST(SUM(gross_operational_sales_units) AS NUMERIC) AS gross_operational_sales_units
, CAST(SUM(operational_returns_units) AS NUMERIC) AS operational_returns_units
, CAST(SUM(net_operational_sales_units) AS NUMERIC) AS net_operational_sales_units
, current_datetime('PST8PDT') as dw_sys_load_tmstp
FROM combo
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim sd ON sd.store_num = combo.store_num_bopus_in_store_demand
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim sd2 ON sd2.store_num = combo.store_num_bopus_in_digital_demand
--WHERE tran_date IN ('2021-07-01', '2021-07-20', '2021-08-08', '2021-08-31') --for testing
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}} 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;
--COLLECT STATISTICS COLUMN(tran_date), COLUMN(customer_journey), COLUMN(merch_division) , COLUMN(business_unit_desc_bopus_in_store_demand), COLUMN(business_unit_desc_bopus_in_digital_demand) , COLUMN(store_num_bopus_in_store_demand), COLUMN(store_num_bopus_in_digital_demand)  ON t2dl_das_mothership.FINANCE_SALES_DEMAND_FACT;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
