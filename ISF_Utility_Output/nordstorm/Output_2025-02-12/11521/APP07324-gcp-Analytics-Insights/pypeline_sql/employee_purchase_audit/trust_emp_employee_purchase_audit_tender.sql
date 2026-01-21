/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=trust_emp_employee_purchase_audit_tender_11521_ACE_ENG;
     Task_Name=trust_emp_employee_purchase_audit_tender;'
     FOR SESSION VOLATILE;


/*
T2/Table Name:T2DL_DAS_TRUST_EMP.employee_purchase_audit_tender
Team/Owner: Indentity and Trust analytics - Deboleena Ganguly
Date Created/Modified: 08/27/2024

Note:
-- What is the the purpose of the table
-- Stores nonmerch and merch transactions for select employees
-- What is the update cadence/lookback window
-- 3 months

*/



/*
Temp table notes here if applicable
*/
CREATE MULTISET VOLATILE TABLE employee_purchase_audit_tender_order AS (
select
distinct
hr.first_name, -- employee first name
hr.last_name, -- employee last name
hr.discount_percent, -- employee eligible discount %
rtdf.business_day_date as "business_date",
rtdf.intent_store_num as "intent_store",
rtdf.ringing_store_num as "ringing_store",
rtdf.tran_date as "ringing_date",
rtdf.followup_slsprsn_num as "sales_person",
rtdf.original_register_num as "original_register_num",
rtdf.global_tran_id as "original_transaction_id",
rtdf.sa_tran_status_code as "status_code",
rtdf.merch_dept_num as "dept",
--dtl_pos_cls_id,
rtdf.employee_discount_flag as "emp_discount_flag",
ABS(rtdf.employee_discount_usd_amt) as "emp_discount_amount",
rtdf.employee_discount_num as "emp_discount_number",
rtdf.upc_num as "upc_no",
rtdf.nonmerch_fee_code as "fee_code",
rtdf.line_item_seq_num as "line_item_num",
rtdf.line_net_amt as "line_net_amount",
rthd.total_amt as "tran_total_amount",
rtdf.line_item_tax_amt as "item_tax_amt",
rthd.total_manual_tax_usd_amt as "tran_total_tax_amt", -- Manually entered/adjusted total sales tax amount associated with all the lines of a RETAIL TRANSACTION (derived data)
rtdf.line_item_tax_exempt_flag as "tax_exempt_flag",
--tax exempt date,
--tax exempt reason code,
rtdf.original_business_date AS "original_bus_date",
--rtdf.original_tran_date AS "Original Tran Date",
rtdf.original_ringing_store_num AS "original_store",
rtdf.tran_type_code AS "tran_type",
rtdf.employee_discount_usd_amt AS "tran_total_emp_disc_amt",
rtdf.original_ringing_store_num as "tax_store",
rthd.total_manual_tax_usd_amt as "tran_total_manual_tax_amt",
sku.sku_desc as "sku_desc",
sku.rms_sku_num as "sku_num",
rtdf.line_item_fulfillment_type as "fulfillment_type",
rthd.total_amt_currency_code as "tran_currency",
rtdf.original_line_item_amt_currency_code as "original_currency",
--DT.DTL_RCPT_RCLL_RSP_CD AS "RR response code",
rtdf.line_item_order_type as "order_type",
-- HD.SI1_ADDR   AS "Addr",
-- HD.SI1_APT_NUM    AS "Apt",
oldf.DESTINATION_CITY as "city",
oldf.DESTINATION_STATE as "state",
oldf.DESTINATION_ZIP_CODE as "zip",
substr(oldf.DESTINATION_ZIP_CODE,1,3) as "zip_3",
rtdf.original_tran_num,
oldf.ORIGINAL_DESTINATION_CITY as "original_destination_city",
oldf.ORIGINAL_DESTINATION_STATE as "original_destination_state",
oldf.ORIGINAL_DESTINATION_ZIP_CODE as "original_destination_zip",
rtdf.line_item_activity_type_code,
rtdf.ITEM_SOURCE as "item_source",
rtdf.banner,
st.business_unit_desc,
rttf.tender_item_account_number_v2,
CASE
         WHEN tender_type_code IN ('CASH', 'CA')
          THEN 'Cash'
         WHEN tender_type_code IN ('PAYPAL','PP')
          THEN 'PayPal'
         WHEN tender_type_code = 'CHECK'
          THEN 'Check'
         WHEN card_type_code = 'NC'
          AND card_subtype_code IN ('MD','ND')
          THEN 'Nordstrom Debit'
         WHEN card_type_code = 'NV'
          AND card_subtype_code IN ('TP','TV','RV')
          OR card_type_code = 'NV'
          THEN 'Nordstrom Visa'
         WHEN card_type_code = 'NC'
          AND card_subtype_code IN ('RT','TR','RR')
          THEN 'Nordstrom Retail'
         WHEN card_type_code = 'NB'
          THEN 'Nordstrom Corporate'
         WHEN tender_type_code = 'NORDSTROM_NOTE'
          OR card_subtype_code = 'NN'
          THEN 'Nordstrom Note'
         WHEN tender_type_code = 'GIFT CARD'
          OR card_subtype_code = 'GC'
          THEN 'Gift Card'
         WHEN card_type_code = 'VC'
          THEN 'Third-party Visa'
         WHEN card_type_code = 'MC'
          THEN 'Master Card'
         WHEN card_type_code = 'DS'
          THEN 'Discover'
         WHEN card_type_code = 'AE'
          THEN 'American Express'
         WHEN card_type_code = 'JC'
          THEN 'JCB'
         WHEN card_type_code ='DC'
          THEN 'Diners'
         WHEN card_type_code ='IA'
          THEN 'Interact Debit'
        ELSE 'Other'
    END AS tender,
case
	when epat.tender_item_account_number_v2 is not null then 'verified'
	else 'not_verified'
end as tender_verification_flag,
CURRENT_TIMESTAMP as dw_sys_load_tmstp
-- Main source of data RETAIL_TRAN_DETAIL_FACT_VW
from PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW rtdf
-- Joining with RETAIL_TRAN_HDR_FACT for Transaction amount and Tax fields
left join PRD_NAP_USR_VWS.RETAIL_TRAN_HDR_FACT rthd
on rtdf.global_tran_id = rthd.global_tran_id
and rtdf.business_day_date = rthd.business_day_date
-- OLDF table for CITY, STATE, ZIP
left join PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT oldf
ON rtdf.order_num = oldf.ORDER_NUM
AND rtdf.business_day_date = oldf.ORDER_DATE_PACIFIC
AND rtdf.tran_line_id = oldf.order_line_num
AND rtdf.sku_num = oldf.sku_num
-- Store info business unit description
left JOIN PRD_NAP_USR_VWS.STORE_DIM AS st
--ON rtdf.fulfilling_store_num = st.store_num
ON rtdf.ringing_store_num = st.store_num
-- Sku description
left JOIN
(SELECT rms_sku_num, sku_desc
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW
QUALIFY Row_Number() OVER (PARTITION BY rms_sku_num ORDER BY channel_country DESC, dw_batch_date DESC) = 1) as sku
ON rtdf.sku_num = sku.rms_sku_num
-- Join HR table
left Join PRD_NAP_HR_USR_VWS.HR_WORKER_V2_DIM hr
on rtdf.employee_discount_num = hr.worker_number
-- Join Tender table
left join PRD_NAP_USR_VWS.RETAIL_TRAN_TENDER_FACT rttf
on rtdf.global_tran_id = rttf.global_tran_id
--join look up table
left join T2DL_DAS_TRUST_EMP.employee_purchase_audit_look_up epat
on rttf.tender_item_account_number_v2 = epat.tender_item_account_number_v2
where rtdf.business_day_date >= CURRENT_DATE - 90 --look back window 90 days
and employee_discount_flag = 1
--and employee_discount_num = '636480'
and rtdf.employee_discount_num in (
'10381911',
'11405024',
'30176067',
'30176088',
'10555621',
'9688193',
'10874634',
'30326208',
'30326211',
'30658407',
'30797349',
'30824733',
'30875907',
'1004100',
'7319320',
'2629285',
'8827750',
'10771715',
'3202165',
'8256075',
'4027678',
'8955379',
'7008436',
'1001742',
'4027793',
'1002153',
'1404334',
'1003268',
'1002906',
'1510296',
'1510494',
'8309668',
'1398320',
'8309650',
'10348696',
'7350846',
'8365041',
'4027769',
'1398353',
'1510577',
'1398338',
'1398346',
'20073',
'8859829'
)
)
WITH DATA
PRIMARY INDEX(original_transaction_id,line_item_num, sku_num,tender_item_account_number_v2)
ON COMMIT PRESERVE ROWS
;



/*
--------------------------------------------
DELETE any overlapping records from destination
table prior to INSERT of new data
--------------------------------------------
*/

DELETE
FROM    {trust_emp_t2_schema}.employee_purchase_audit_tender
WHERE   business_date >= {start_date}
AND     business_date <= {end_date}
;


INSERT INTO {trust_emp_t2_schema}.employee_purchase_audit_tender
SELECT
        first_name,
        last_name,
        discount_percent,
        business_date,
        intent_store,
        ringing_store,
        ringing_date,
        sales_person,
        original_register_num,
        original_transaction_id,
        status_code,
        dept,
        emp_discount_flag,
        emp_discount_amount,
        emp_discount_number,
        upc_no,
        fee_code,
        line_item_num,
        line_net_amount,
        tran_total_amount,
        item_tax_amt,
        tran_total_tax_amt,
        tax_exempt_flag,
        original_bus_date,
        original_store,
        tran_type,
        tran_total_emp_disc_amt,
        tax_store,
        tran_total_manual_tax_amt,
        sku_desc,
        sku_num,
        fulfillment_type,
        tran_currency,
        original_currency,
        order_type,
        city,
        state,
        zip,
        zip_3,
        original_tran_num,
        original_destination_city,
        original_destination_state,
        original_destination_zip,
        line_item_activity_type_code,
        item_source,
        banner,
        business_unit_desc,
        tender_item_account_number_v2,
        tender,
        tender_verification_flag,
        dw_sys_load_tmstp
FROM	employee_purchase_audit_tender_order
WHERE   business_date >= {start_date}
AND     business_date <= {end_date}
;


/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
