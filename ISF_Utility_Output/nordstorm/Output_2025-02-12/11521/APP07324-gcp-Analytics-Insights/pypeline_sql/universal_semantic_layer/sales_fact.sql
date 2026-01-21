/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=sales_fact_11521_ACE_ENG;
     Task_Name=sales_fact;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_usl.sales_fact
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: Nov 15th 2023

Note:
-- Purpose of the table: Table to directly get transaction related metrics and attributes
-- Update Cadence: Daily

*/

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/

create multiset volatile table sf_start_date as 
(select
case
when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
then date'2021-01-31'
else current_date() - {incremental_look_back}
end as start_date,
case
when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
then 'backfill'
else 'incremental'
end as delete_range
) with data primary index(start_date) 
on commit preserve rows;

/*create the sales fact temp table based on the above time period*/

CREATE multiset volatile TABLE sarf AS (
select global_tran_id, line_item_seq_num, order_num, has_employee_discount, source_platform_code, promise_type_code, price_type, transaction_type, return_qty, return_usd_amt
		 from T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT
		 where business_day_date between (select start_date from sf_start_date) and current_date() -1
  ) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

create multiset volatile table vt2 as (
SELECT
 base.business_day_date
,base.global_tran_id
,base.line_item_seq_num
,(case
      when line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and intent_store_num = 828 then 828
      when line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and line_item_net_amt_currency_code = 'CAD' then 867
      when line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') then 808
      when intent_store_num=5405 then 808
      else intent_store_num end) store_number
,base.acp_id
,case when base.line_net_usd_amt >= 0 then coalesce(base.order_date,base.tran_date) else base.tran_date end as sale_date
,CASE WHEN base.line_net_usd_amt >0
        THEN cast(trim(base.acp_id)||'_'||trim(cast(store_number as integer))||'_'||trim(sale_date) AS varchar(60))
        ELSE null END trip_id
,base.employee_discount_flag
,base.line_item_activity_type_desc
,base.tran_type_code
,base.reversal_flag
,base.line_item_quantity
,base.line_item_net_amt_currency_code
,base.original_line_item_amt_currency_code
,base.original_line_item_amt
,base.line_net_amt
,base.line_net_usd_amt
,case when base.nonmerch_fee_code = '6666' then 0 else base.line_net_usd_amt end as non_gc_line_net_usd_amt
,base.line_item_regular_price
,base.line_item_merch_nonmerch_ind
,case when coalesce(base.nonmerch_fee_code, '-999') = '6666' then 1 else 0 end giftcard_flag
,base.sku_num
,base.upc_num
,st.store_country_code
,case when intent_store_num=5405 then 1 else 0 end marketplace_flag
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw base
JOIN prd_nap_usr_vws.store_dim st
on (case
      when line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and intent_store_num = 828 then 828
      when line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and line_item_net_amt_currency_code = 'CAD' then 867
      when line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') then 808
      when intent_store_num=5405 then 808
      else intent_store_num end) = st.store_num
WHERE base.business_day_date between (select max(start_date) from sf_start_date) and current_date() -1
--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) with data
no primary index
on commit preserve rows;

	COLLECT STATISTICS COLUMN (STORE_COUNTRY_CODE) ON     vt2;
     COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM)     ON vt2;
     COLLECT STATISTICS COLUMN (SKU_NUM ,STORE_COUNTRY_CODE) ON     vt2;
     COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE) ON     vt2;
     COLLECT STATISTICS COLUMN (SKU_NUM) ON vt2;
	 
     COLLECT STATISTICS COLUMN (TRANSACTION_TYPE) ON sarf;
     COLLECT STATISTICS COLUMN (SOURCE_PLATFORM_CODE) ON sarf;
     COLLECT STATISTICS COLUMN (PROMISE_TYPE_CODE) ON sarf;
     COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM)     ON sarf;
    
-- COLLECT STATISTICS COLUMN (PRICE_TYPE) ON {usl_t2_schema}.price_type_dim;
-- COLLECT STATISTICS COLUMN (PROMISE_TYPE_CODE) ON {usl_t2_schema}.ship_method_dim;     
-- COLLECT STATISTICS COLUMN (TRANSACTION_TYPE) ON {usl_t2_schema}.transaction_type_dim;
-- COLLECT STATISTICS COLUMN (SOURCE_PLATFORM_CODE) ON {usl_t2_schema}.device_dim;

--modifed code


--load 1
create multiset volatile table usl_sales_fact as (
select * from (
SELECT
       base.business_day_date date_id
      ,base.global_tran_id
      ,base.line_item_seq_num
    ,base.store_number
    ,base.acp_id
    ,psd.rms_sku_num sku_id
    ,base.upc_num
      ,base.sale_date
      ,base.trip_id
    ,base.employee_discount_flag
    ,coalesce(tt.transaction_type_id,1) transaction_type_id
    ,coalesce(devices.device_id,1) device_id
    ,coalesce(sm.ship_method_id,1) ship_method_id
    ,coalesce(pt.price_type_id,1) price_type_id


,base.line_item_activity_type_desc
      ,base.tran_type_code
      ,base.reversal_flag
      ,base.line_item_quantity


,base.line_item_net_amt_currency_code
      ,base.original_line_item_amt_currency_code
      ,base.original_line_item_amt
      ,base.line_net_amt
      ,base.line_net_usd_amt


,dpt.merch_dept_ind
      ,base.non_gc_line_net_usd_amt


,base.line_item_regular_price


,coalesce(return_qty,0) units_returned


,coalesce(return_usd_amt,0) sales_returned


,base.giftcard_flag
      ,case when (dpt.merch_dept_ind = 'Y' AND base.line_item_merch_nonmerch_ind = 'MERCH' AND dpt.DIVISION_NUM NOT IN ('800','900','100')) then 1 else 0 end as merch_flag
,base.marketplace_flag
From vt2 base
LEFT JOIN (SELECT channel_country, rms_sku_num, epm_sku_num, epm_style_num, rms_style_num, dept_num, div_desc FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) psd
--left join vt_psd_dpt as psd
ON base.sku_num = psd.rms_sku_num 
--on CASE WHEN  base.sku_num IS NULL THEN RANDOM(1, 200000000) ELSE base.sku_num END = psd.rms_sku_num 
and base.store_country_code = psd.channel_country

--LEFT JOIN vt_dpt dpt ON psd.dept_num = dpt.dept_num
left join PRD_NAP_USR_VWS.DEPARTMENT_DIM dpt ON psd.dept_num = dpt.dept_num 

LEFT JOIN sarf as sarf on sarf.global_tran_id = base.global_tran_id AND sarf.line_item_seq_num = base.line_item_seq_num
LEFT JOIN t2dl_das_usl.transaction_type_dim tt on sarf.transaction_type = tt.transaction_type
LEFT JOIN t2dl_das_usl.price_type_dim pt on sarf.price_type = pt.price_type
LEFT JOIN t2dl_das_usl.ship_method_dim sm on sarf.promise_type_code = sm.promise_type_code
LEFT JOIN t2dl_das_usl.device_dim devices on sarf.source_platform_code = devices.source_platform_code

WHERE base.business_day_date between (select start_date from sf_start_date) and current_date() -1
and base.sku_num is not null
and base.sku_num <> '151'


union all


SELECT
       base.business_day_date date_id
      ,base.global_tran_id
      ,base.line_item_seq_num
    ,base.store_number
    ,base.acp_id
    ,psd.rms_sku_num sku_id
    ,base.upc_num
      ,base.sale_date
      ,base.trip_id
    ,base.employee_discount_flag
    ,coalesce(tt.transaction_type_id,1) transaction_type_id
    ,coalesce(devices.device_id,1) device_id
    ,coalesce(sm.ship_method_id,1) ship_method_id
    ,coalesce(pt.price_type_id,1) price_type_id


,base.line_item_activity_type_desc
      ,base.tran_type_code
      ,base.reversal_flag
      ,base.line_item_quantity


,base.line_item_net_amt_currency_code
      ,base.original_line_item_amt_currency_code
      ,base.original_line_item_amt
      ,base.line_net_amt
      ,base.line_net_usd_amt


,dpt.merch_dept_ind
      ,base.non_gc_line_net_usd_amt


,base.line_item_regular_price


,coalesce(return_qty,0) units_returned


,coalesce(return_usd_amt,0) sales_returned


,base.giftcard_flag
      ,case when (dpt.merch_dept_ind = 'Y' AND base.line_item_merch_nonmerch_ind = 'MERCH' AND dpt.DIVISION_NUM NOT IN ('800','900','100')) then 1 else 0 end as merch_flag
,base.marketplace_flag
From vt2 base
LEFT JOIN (SELECT channel_country, rms_sku_num, epm_sku_num, epm_style_num, rms_style_num, dept_num, div_desc FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) psd
--left join vt_psd_dpt as psd
ON base.sku_num = psd.rms_sku_num 
--on CASE WHEN  base.sku_num IS NULL THEN RANDOM(1, 200000000) ELSE base.sku_num END = psd.rms_sku_num 
and base.store_country_code = psd.channel_country

--LEFT JOIN vt_dpt dpt ON psd.dept_num = dpt.dept_num
left join PRD_NAP_USR_VWS.DEPARTMENT_DIM dpt ON psd.dept_num = dpt.dept_num 

LEFT JOIN sarf as sarf on sarf.global_tran_id = base.global_tran_id AND sarf.line_item_seq_num = base.line_item_seq_num
LEFT JOIN t2dl_das_usl.transaction_type_dim tt on sarf.transaction_type = tt.transaction_type
LEFT JOIN t2dl_das_usl.price_type_dim pt on sarf.price_type = pt.price_type
LEFT JOIN t2dl_das_usl.ship_method_dim sm on sarf.promise_type_code = sm.promise_type_code
LEFT JOIN t2dl_das_usl.device_dim devices on sarf.source_platform_code = devices.source_platform_code

WHERE base.business_day_date between (select start_date from sf_start_date) and current_date() -1
and base.sku_num is null

union all


SELECT
       base.business_day_date date_id
      ,base.global_tran_id
      ,base.line_item_seq_num
    ,base.store_number
    ,base.acp_id
    ,psd.rms_sku_num sku_id
    ,base.upc_num
      ,base.sale_date
      ,base.trip_id
    ,base.employee_discount_flag
    ,coalesce(tt.transaction_type_id,1) transaction_type_id
    ,coalesce(devices.device_id,1) device_id
    ,coalesce(sm.ship_method_id,1) ship_method_id
    ,coalesce(pt.price_type_id,1) price_type_id


,base.line_item_activity_type_desc
      ,base.tran_type_code
      ,base.reversal_flag
      ,base.line_item_quantity


,base.line_item_net_amt_currency_code
      ,base.original_line_item_amt_currency_code
      ,base.original_line_item_amt
      ,base.line_net_amt
      ,base.line_net_usd_amt


,dpt.merch_dept_ind
      ,base.non_gc_line_net_usd_amt


,base.line_item_regular_price


,coalesce(return_qty,0) units_returned


,coalesce(return_usd_amt,0) sales_returned


,base.giftcard_flag
      ,case when (dpt.merch_dept_ind = 'Y' AND base.line_item_merch_nonmerch_ind = 'MERCH' AND dpt.DIVISION_NUM NOT IN ('800','900','100')) then 1 else 0 end as merch_flag
,base.marketplace_flag
From vt2 base
LEFT JOIN (SELECT channel_country, rms_sku_num, epm_sku_num, epm_style_num, rms_style_num, dept_num, div_desc FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) psd
--left join vt_psd_dpt as psd
ON base.sku_num = psd.rms_sku_num 
--on CASE WHEN  base.sku_num IS NULL THEN RANDOM(1, 200000000) ELSE base.sku_num END = psd.rms_sku_num 
and base.store_country_code = psd.channel_country

--LEFT JOIN vt_dpt dpt ON psd.dept_num = dpt.dept_num
left join PRD_NAP_USR_VWS.DEPARTMENT_DIM dpt ON psd.dept_num = dpt.dept_num 

LEFT JOIN sarf as sarf on sarf.global_tran_id = base.global_tran_id AND sarf.line_item_seq_num = base.line_item_seq_num
LEFT JOIN t2dl_das_usl.transaction_type_dim tt on sarf.transaction_type = tt.transaction_type
LEFT JOIN t2dl_das_usl.price_type_dim pt on sarf.price_type = pt.price_type
LEFT JOIN t2dl_das_usl.ship_method_dim sm on sarf.promise_type_code = sm.promise_type_code
LEFT JOIN t2dl_das_usl.device_dim devices on sarf.source_platform_code = devices.source_platform_code

WHERE base.business_day_date between (select start_date from sf_start_date) and current_date() -1
and base.sku_num = '151'
)sub
 ) with data 
 primary index(global_tran_id,line_item_seq_num) 
 PARTITION BY RANGE_N(date_id  BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY ,NO RANGE)
 on commit preserve rows;
 
--query 6 
 
collect stats on usl_sales_fact column (date_id);
collect stats on usl_sales_fact column (partition);


/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------

*/

DELETE 
FROM {usl_t2_schema}.sales_fact
where 1=1 
and date_id >= (select start_date from sf_start_date)
and exists
(select 1 from sf_start_date where delete_range = 'incremental'); 

DELETE 
FROM {usl_t2_schema}.sales_fact
where exists
(select 1 from sf_start_date where delete_range = 'backfill');

INSERT INTO {usl_t2_schema}.sales_fact
select date_id
      ,global_tran_id
      ,line_item_seq_num
      ,store_number
      ,acp_id
      ,sku_id
      ,upc_num
      ,sale_date
      ,trip_id
      ,employee_discount_flag
      ,transaction_type_id
	    ,device_id
 	    ,ship_method_id
	    ,price_type_id
      ,line_item_activity_type_desc
      ,tran_type_code
      ,reversal_flag
      ,line_item_quantity
      ,line_item_net_amt_currency_code
      ,original_line_item_amt_currency_code
      ,original_line_item_amt
      -- ,line_net_amt
      ,line_net_usd_amt
      ,merch_dept_ind
      ,non_gc_line_net_usd_amt
      ,line_item_regular_price
      ,units_returned
      ,sales_returned
      ,giftcard_flag
      ,merch_flag
      ,marketplace_flag
      ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from usl_sales_fact
;

COLLECT STATISTICS  COLUMN (global_tran_id),
                    COLUMN (line_item_seq_num),
                    COLUMN (acp_id),
                    COLUMN (date_id),
                    COLUMN (sale_date),
                    COLUMN (store_number),
                    COLUMN (sku_id),
                    COLUMN (upc_num),
                    COLUMN (transaction_type_id),
                    COLUMN (device_id),
                    COLUMN (ship_method_id),
                    COLUMN (price_type_id)
on {usl_t2_schema}.sales_fact;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;