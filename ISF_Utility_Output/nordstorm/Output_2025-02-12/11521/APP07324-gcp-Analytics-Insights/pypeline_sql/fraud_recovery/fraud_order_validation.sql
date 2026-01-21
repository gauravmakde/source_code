SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=fraud_order_validation_11521_ACE_ENG;
     Task_Name=fraud_order_validation;'
     FOR SESSION VOLATILE;

/*Created Nov 2022*/

/* Purpose: The purpose of this query is to identify orders approved before or after 
 our third part fraud solution (Forter) has declined under the same shopper ID; 
the query runs as a second strategy in attempt to capture any inconsistent decisions by our third part fraud solution (Forter) 
which could indicate suspicious activity being approved in error. */
 
/*step 1: build list of shoppper_ids identified with fraud*/
create multiset volatile table fraud_shopper_ids
as
(select  shopper_id, max(order_date_pacific) as order_date_pacific 
from  prd_nap_usr_vws.order_line_detail_fact oldf 
where cancel_reason_code = 'fraud_check_failed' 
and order_date_pacific >= current_date -30
group by shopper_id 
)
with data primary index(shopper_id, order_date_pacific)
on commit preserve rows;
 
/*step 2: insert into table any orders for fraud shopper_ids
from step 1 that were not processed as fraud. Use
a 14 day window (7 days on each side of fraud order as actionable
time frame*/
 
insert into {fraud_recovery_t2_schema}.fraud_order_validation
select
 case when oldf.source_channel_country_code = 'ca' then 'n.ca' 
when oldf.source_channel_code = 'rack' then 'nr.com'
when oldf.source_channel_code = 'full_line' and oldf.source_channel_country_code = 'us' then 'n.com' 
end  as  brand_name,
oldf.shopper_id,
oldf.order_num, 
oldf.order_date_pacific,
sum(oldf.order_line_amount + oldf.order_line_tax_amount) as amount,
date as report_date,
now() as report_tmstp,
CURRENT_TIMESTAMP as dw_sys_load_tmstp 
FROM PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT oldf 
JOIN 
fraud_shopper_ids  FSIDS
on oldf.shopper_id = FSIDS.shopper_id
and oldf.order_date_pacific between FSIDS.order_date_pacific -7 and FSIDS.order_date_pacific +7
where oldf.cancel_reason_code is null
AND OLDF.SHOPPER_ID NOT IN ('0B6B20F2EC254D45A39EF84D74C192A0')
AND NOT EXISTS
(SELECT 1
FROM
{fraud_recovery_t2_schema}.fraud_order_validation  RFD
WHERE OLDF.ORDER_NUM = RFD.ORDER_NUM)
group by
brand_name,
oldf.SHOPPER_ID,
oldf.ORDER_NUM,
oldf.ORDER_DATE_PACIFIC;

collect statistics
column ( partition),
column (shopper_id)
on
{fraud_recovery_t2_schema}.fraud_order_validation;


SET QUERY_BAND = NONE FOR SESSION;