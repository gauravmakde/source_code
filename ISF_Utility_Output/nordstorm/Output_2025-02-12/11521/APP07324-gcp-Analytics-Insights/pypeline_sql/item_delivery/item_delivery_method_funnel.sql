SET QUERY_BAND = 'App_ID=APP08153;
     DAG_ID=item_delivery_method_funnel_11521_ACE_ENG;
     Task_Name=item_delivery_method_funnel;'
     FOR SESSION VOLATILE;

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/
CREATE MULTISET VOLATILE TABLE idmf_start_date
AS (
        select case when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
                    then date'2019-01-27'
                    else current_date() -{incremental_look_back}
                end as start_date,
                case when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
                     then 'backfill'
                     else 'incremental'
                end as delete_range
) with data primary index(start_date) 
on commit preserve rows;



-- Daily Refresh ETL for item delivery method type table
/*
---------------------------------------------------
Step 1: Gather OMS (order management) line 
        item data. FC BOPUS is new. Filled
        by FC sent overnight via Nordstrom 
        courrier
---------------------------------------------------
*/


CREATE  MULTISET VOLATILE TABLE olf_order_items 
AS (

SELECT a.order_date_pacific,
       a.order_tmstp_pacific,
       a.shopper_id,
       a.source_channel_code,
       a.source_channel_country_code,
       a.source_platform_code,
       --a.order_type_code,
       a.order_num,
       a.order_line_num,
       a.order_line_id,
       a.delivery_method_code,
       a.padded_upc,
       a.rms_sku_num,
       --a.seq_num,
       a.item_delivery_method,
       CASE WHEN a.item_delivery_method IN ('FC_BOPUS','NEXT_DAY_BOPUS','SAME_DAY_BOPUS','NEXT_DAY_SHIP_TO_STORE','FREE_2DAY_SHIP TO STORE'
                                            ,'SHIP_TO_STORE','PAID_EXPEDITED_SHIP_TO_STORE') THEN 1 ELSE NULL
       END AS order_pickup_ind, --includes picks and ship to store
       CASE WHEN a.item_delivery_method IN ('NEXT_DAY_SHIP_TO_STORE','FREE_2DAY_SHIP_TO_STORE','SHIP_TO_STORE','PAID_EXPEDITED_SHIP_TO_STORE') THEN 1 ELSE NULL
       END AS ship_to_store_ind,
       CASE WHEN a.item_delivery_method IN ('NEXT_DAY_BOPUS','SAME_DAY_BOPUS') THEN 1 ELSE NULL 
       END AS bopus_filled_from_stores_ind,
       CASE WHEN a.item_delivery_method IN ('FC_BOPUS','NEXT_DAY_BOPUS','NEXT_DAY_SHIP_TO_STORE') THEN 1 ELSE NULL 
       END AS customer_promised_next_day_pickup_ind,                                
       a.curbside_ind,
       a.fulfilled_node_num,
       a.shipped_node_num,
       a.destination_node_num,
       a.picked_up_by_customer_node_num,
       a.destination_zip_code,
       a.bill_zip_code,
       a.canceled_tmstp_pacific, 
       a.canceled_date_pacific,
       a.cancel_reason_code,
       a.fraud_cancel_ind,
       a.pre_release_cancel_ind,
       a.order_line_currency_code,
       a.order_line_amount,
       a.order_line_amount_usd,
       a.order_line_quantity,
       a.order_line_employee_discount_percentage,
       a.order_line_employee_discount_amount,
       a.order_line_employee_discount_amount_usd,
       a.requested_max_promise_tmstp_pacific,
       a.gift_with_purchase_ind,
       a.beauty_sample_ind,
       a.backorder_ind,
       a.shipped_tmstp_pacific,
       a.arrived_at_order_pickup_tmstp_pacific,
       a.fulfilled_tmstp_pacific,
       a.picked_up_by_customer_tmstp_pacific
       
FROM 
    (
    SELECT  f.source_channel_code,
            f.source_channel_country_code,
            f.source_platform_code,
            f.order_num,
            f.order_line_num,
            f.order_line_id,
            f.order_date_pacific,
            f.order_tmstp_pacific,
            f.shopper_id,
            f.delivery_method_code, 
            f.promise_type_code,
            --f.order_type_code,
            LPAD(TRIM(f.upc_code),15,'0') padded_upc,
            f.rms_sku_num,
            --ROW_NUMBER() OVER (PARTITION BY f.order_num, LPAD(TRIM(f.upc_code),15,'0') ORDER BY LPAD(TRIM(f.upc_code),15,'0')) AS seq_num,
            CASE WHEN TRIM(UPPER(f.promise_type_code)) = 'SAME_DAY_COURIER' THEN 'SAME_DAY_DELIVERY'
                    WHEN f.requested_level_of_service_code = '07' THEN 'NEXT_DAY_BOPUS' --Part of "Next-Day Pickup"
                    WHEN UPPER(COALESCE(F.delivery_method_code,'-1')) = 'PICK' THEN 'SAME_DAY_BOPUS'
                    WHEN olrd.order_line_id IS NOT NULL AND f.requested_level_of_service_code = '11' THEN 'FC_BOPUS' -- see note at top
                    WHEN f.requested_level_of_service_code = '11' THEN 'NEXT_DAY_SHIP_TO_STORE' --Part of "Next-Day Pickup"
                    WHEN COALESCE(f.destination_node_num,-1) > 0 AND UPPER(COALESCE(F.delivery_method_code,'-1')) <> 'PICK' AND F.requested_level_of_service_code = '42' THEN 'FREE_2DAY_SHIP_TO_STORE' --Part of "Next-Day Pickup"
                    WHEN f.requested_level_of_service_code = '42' THEN 'FREE_2DAY_DELIVERY'
                    WHEN (substring(UPPER(f.promise_type_code),1,3) IN ('ONE','TWO','EXP') OR UPPER(f.promise_type_code) LIKE '%BUSINESSDAY%') AND COALESCE(f.destination_node_num,-1) > 0 --may need to move up in case statement
                    THEN 'PAID_EXPEDITED_SHIP_TO_STORE'
                    WHEN (substring(UPPER(f.promise_type_code),1,3) IN ('ONE','TWO','EXP') OR UPPER(f.promise_type_code) LIKE '%BUSINESSDAY%') AND COALESCE(f.destination_node_num,-1) < 0 --may need to move up in case statement
                    THEN 'PAID_EXPEDITED_SHIP_TO_HOME'
                    WHEN COALESCE(f.destination_node_num,-1) > 0 AND UPPER(COALESCE(F.delivery_method_code,'-1')) <> 'PICK' THEN 'SHIP_TO_STORE'                 
            ELSE 'SHIP_TO_HOME' END AS item_delivery_method, --change to customer_requested_delivery_method
            CASE WHEN UPPER(TRIM(f.CURBSIDE_PICKUP_IND))='Y' THEN 1 ELSE NULL END AS curbside_ind,
            f.fulfilled_node_num,
            f.shipped_node_num,
            f.destination_node_num, -- the financial view of pickup location. This will include Virtual FLS store numbers
            f.picked_up_by_customer_node_num, -- actual Rack location customer picked up purchases from 
            f.destination_zip_code,
            f.bill_zip_code,
            f.canceled_tmstp_pacific, 
            f.canceled_date_pacific,
            f.cancel_reason_code,
            f.fraud_cancel_ind,
            f.pre_release_cancel_ind,
            f.order_line_currency_code,
            f.order_line_amount,
            f.order_line_amount_usd,
            f.order_line_quantity,
            f.order_line_employee_discount_percentage,
            f.order_line_employee_discount_amount,
            f.order_line_employee_discount_amount_usd,
            f.requested_max_promise_tmstp_pacific,
            f.gift_with_purchase_ind,
            f.beauty_sample_ind,
            f.backorder_ind,
            f.shipped_tmstp_pacific,
            f.arrived_at_order_pickup_tmstp_pacific,
            f.fulfilled_tmstp_pacific,
            f.picked_up_by_customer_tmstp_pacific

    FROM prd_nap_usr_vws.order_line_detail_fact f

    LEFT JOIN (
               SELECT olr.order_num, 
                      olr.order_line_id
    
               FROM prd_nap_usr_vws.order_line_release_detail_fact olr

               WHERE olr.carrier_code in ('NORD', NULL)
               AND olr.order_date_pacific BETWEEN (select start_date from idmf_start_date) and current_date() -1
               ) olrd

    ON f.order_num = olrd.order_num
    AND f.order_line_id = olrd.order_line_id

    WHERE f.order_date_pacific BETWEEN (select start_date from idmf_start_date) and current_date() -1
    AND f.order_date_pacific <> '4444-04-04' -- excluding orders WHERE NAP received partial data only. eg: only canceled event AND not other events
    AND f.order_num IS NOT NULL
    AND f.order_line_num IS NOT NULL
    AND f.order_line_amount_usd > 0
    AND COALESCE(TRIM(f.upc_code),'-1') <> '-1'
    ) a 
    ) 
WITH DATA UNIQUE PRIMARY INDEX (order_num, order_line_id) ON COMMIT PRESERVE ROWS;
/*
---------------------------------------------------
Step 2: Pull in sales AND return transactions
---------------------------------------------------
*/
--drop table order_line_sales_returns;
CREATE  MULTISET VOLATILE TABLE order_line_sales_returns 
AS (
SELECT  distinct sr.business_day_date,
        sr.order_num AS order_number,
        sr.order_line_id,
        --sr.transaction_identifier,
        sr.global_tran_id,
        sr.order_date,
        lpad(sr.upc_num, 15,'0') AS primary_upc,
        --ROW_NUMBER() OVER (PARTITION BY sr.order_num, lpad(sr.upc_num, 15,'0') ORDER BY lpad(sr.upc_num, 15,'0')) AS seq_num,
        sr.acp_id,
        sr.shipped_sales,
        sr.shipped_usd_sales,
        sr.has_employee_discount,
        sr.employee_discount_amt,
        sr.employee_discount_usd_amt,
        sr.shipped_qty,
        sr.intent_store_num,
        sr.return_ringing_store_num,
        sr.return_date,
        sr.return_qty,
        sr.return_amt,
        sr.return_usd_amt,
        sr.return_employee_disc_amt,
        sr.return_employee_disc_usd_amt,
        --sr.line_item_order_type
        sr.days_to_return,
        sr.line_item_seq_num,
        line_item_fulfillment_type

FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact sr
WHERE sr.business_day_date >= '2019-02-03' --beginning of fiscal 2019. needs to stay hard-coded
AND sr.order_num IS NOT NULL -- only want digital sales and returns, cannot tie back unknown returns to digital orders
AND sr.order_line_id IS NOT NULL
AND transaction_type = 'retail'
qualify row_number() over (partition by order_number, order_line_id order by order_date desc) = 1
) 
WITH DATA UNIQUE PRIMARY INDEX(order_number, order_line_id) ON COMMIT PRESERVE ROWS;

/*
---------------------------------------------------
Step 3: Combine OMS with sales and returns
---------------------------------------------------
*/

DELETE 
FROM    {item_delivery_t2_schema}.item_delivery_method_funnel_daily
WHERE   order_date_pacific BETWEEN (select start_date from idmf_start_date) and current_date() -1;

INSERT INTO {item_delivery_t2_schema}.item_delivery_method_funnel_daily
(
         order_date_pacific,
        order_tmstp_pacific,
        shopper_id,
        source_channel_code,
        source_channel_country_code,
        source_platform_code,
        order_num,
        order_line_num,
        order_line_id,
        delivery_method_code,
        padded_upc,
        rms_sku_num,
        item_delivery_method,
        order_pickup_ind,  
        ship_to_store_ind,
        bopus_filled_from_stores_ind,
        customer_promised_next_day_pickup_ind,                           
        curbside_ind,
        fulfilled_node_num, 
        shipped_node_num, 
        destination_node_num, 
        picked_up_by_customer_node_num, 
        destination_zip_code,
        bill_zip_code,
        canceled_tmstp_pacific,
        canceled_date_pacific,
        cancel_reason_code,
        fraud_cancel_ind,
        pre_release_cancel_ind,
        order_line_currency_code,
        order_line_amount,
        order_line_amount_usd,
        order_line_quantity,
        order_line_employee_discount_percentage,
        order_line_employee_discount_amount,
        order_line_employee_discount_amount_usd,
        requested_max_promise_tmstp_pacific,
        gift_with_purchase_ind,
        beauty_sample_ind,
        backorder_ind,
        shipped_tmstp_pacific,
        arrived_at_order_pickup_tmstp_pacific,
        fulfilled_tmstp_pacific,
        picked_up_by_customer_tmstp_pacific,
        business_day_date,
        global_tran_id, 
        acp_id,
        shipped_sales,
        shipped_usd_sales,
        has_employee_discount,
        employee_discount_amt,
        employee_discount_usd_amt,
        shipped_qty,
        intent_store_num,
        return_ringing_store_num,
        return_date,
        return_qty,
        return_amt,
        return_usd_amt,
        return_employee_disc_amt,
        return_employee_disc_usd_amt,
        days_to_return,
        line_item_fulfillment_type,
        line_item_seq_num,
        dw_sys_load_tmstp
)

SELECT  olf.order_date_pacific,
        olf.order_tmstp_pacific,
        olf.shopper_id,
        olf.source_channel_code,
        olf.source_channel_country_code,
        olf.source_platform_code,
        olf.order_num,
        olf.order_line_num,
        olf.order_line_id,
        olf.delivery_method_code,
        olf.padded_upc,
        olf.rms_sku_num,
        olf.item_delivery_method,
        olf.order_pickup_ind, --includes picks and ship to store
        olf.ship_to_store_ind,
        olf.bopus_filled_from_stores_ind,
        olf.customer_promised_next_day_pickup_ind,                                
        olf.curbside_ind,
        olf.fulfilled_node_num,
        olf.shipped_node_num,
        olf.destination_node_num,
        olf.picked_up_by_customer_node_num,
        olf.destination_zip_code,
        olf.bill_zip_code,
        olf.canceled_tmstp_pacific, 
        olf.canceled_date_pacific,
        olf.cancel_reason_code,
        olf.fraud_cancel_ind,
        olf.pre_release_cancel_ind,
        olf.order_line_currency_code,
        olf.order_line_amount,
        olf.order_line_amount_usd,
        olf.order_line_quantity,
        olf.order_line_employee_discount_percentage,
        olf.order_line_employee_discount_amount,
        olf.order_line_employee_discount_amount_usd,
        olf.requested_max_promise_tmstp_pacific,
        olf.gift_with_purchase_ind,
        olf.beauty_sample_ind,
        olf.backorder_ind,
        olf.shipped_tmstp_pacific,
        olf.arrived_at_order_pickup_tmstp_pacific,
        olf.fulfilled_tmstp_pacific,
        olf.picked_up_by_customer_tmstp_pacific,
        sr2.business_day_date,
        --NULL AS transaction_identifier,
        sr2.global_tran_id,
        sr2.acp_id,
        sr2.shipped_sales,
        sr2.shipped_usd_sales,
        sr2.has_employee_discount,
        sr2.employee_discount_amt,
        sr2.employee_discount_usd_amt,
        sr2.shipped_qty,
        sr2.intent_store_num,
        sr2.return_ringing_store_num,
        sr2.return_date,
        sr2.return_qty,
        sr2.return_amt,
        sr2.return_usd_amt,
        sr2.return_employee_disc_amt,
        sr2.return_employee_disc_usd_amt,
        sr2.days_to_return,
        sr2.line_item_fulfillment_type,
        sr2.line_item_seq_num,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM olf_order_items olf 
LEFT JOIN order_line_sales_returns sr2
ON olf.order_num = sr2.order_number
AND olf.order_line_id = sr2.order_line_id;

COLLECT STATISTICS COLUMN(order_num), COLUMN(order_line_id) ,COLUMN(order_date_pacific) ON {item_delivery_t2_schema}.item_delivery_method_funnel_daily;
COLLECT STATISTICS COLUMN (PARTITION , order_date_pacific) ON {item_delivery_t2_schema}.item_delivery_method_funnel_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {item_delivery_t2_schema}.item_delivery_method_funnel_daily;


SET QUERY_BAND = NONE FOR SESSION;