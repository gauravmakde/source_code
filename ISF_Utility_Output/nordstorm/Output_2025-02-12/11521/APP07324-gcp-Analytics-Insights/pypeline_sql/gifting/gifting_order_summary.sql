SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=gifting_order_summary_11521_ACE_ENG;
     Task_Name=gifting_order_summary;'
     FOR SESSION VOLATILE;

/*
This creates T2DL_DAS_GIFTING_DIGITAL_EXP.gifting_order_summary
Creation of an order level table with breakdown of ship type and gift option type
to calculate take rate and other gifting KPIs.

Returns will be limited to within 30 days to match the ETL window, so data will
remain consistent when running backfills. When using this table to calculate return rate
the user should not use the most recent 30 days, as this will fluctuate as more returns
are added. 80% of items are returned within 30 days based on the sales and returns table.

Because of the requested dimensions (ship type and gift type) are at the item level
and KPIs are at the order level, this table is required as the base for the gifting
summary dashboard view
*/

/*
First create the order level summary to get ship type and gift option type
at the item level per order. 

This also creates the order sequence number to join back to the subsequent
return of that item.

Confirmed with stakeholder to remove orders canceled due to fraud.
*/
--drop table order_detail;
CREATE MULTISET VOLATILE TABLE order_detail AS (
SELECT
    --Dimensions
      order_num 
    , order_date_pacific
    , upc_code
    , source_platform_code
    , source_channel_country_code
    , source_channel_code
    , promise_type_code 
    , delivery_method_code
    , CASE WHEN UPPER(promise_type_code) = 'SAME_DAY_COURIER' THEN 'SHIP_TO_HOME'
           WHEN UPPER(delivery_method_code) = 'PICK'          THEN 'BOPUS'
           WHEN destination_node_num > 0                      THEN 'SHIP_TO_STORE'
           WHEN destination_node_num IS NULL                  THEN 'SHIP_TO_HOME'
           ELSE 'SHIP_TO_HOME' END AS ship_type
    , destination_node_num
    , CASE WHEN upper(gift_option_type) = 'UNKNOWN' THEN NULL ELSE UPPER(gift_option_type) END AS gift_option_type --sets all "unknown" to NULL
    --Measures
    , order_line_quantity 
    , order_line_amount_usd
    , gift_option_amount_usd
    , order_line_amount
    , gift_option_amount
    , ROW_NUMBER() OVER (PARTITION BY order_num, upc_code ORDER BY upc_code) AS order_seq_num --used for joining to transactions for returns

FROM prd_nap_usr_vws.order_line_detail_fact

WHERE order_date_pacific >= {start_date}
AND   order_date_pacific <= {end_date}
AND   COALESCE(fraud_cancel_ind, '9999') <> 'Y'
)
WITH DATA
PRIMARY INDEX(order_num, upc_code, order_seq_num)
ON COMMIT PRESERVE ROWS
;

/*
Taking the orders table from above and joining to sales and returns to
get the shipped sales and returns data for each order
*/
--drop table gifting_detail;
CREATE MULTISET VOLATILE TABLE gifting_detail AS (
SELECT
    --Dimensions
      od.order_date_pacific
    , od.order_num
    , od.source_platform_code
    , od.source_channel_country_code
    , od.source_channel_code
    , od.ship_type
    , od.destination_node_num
    , od.gift_option_type
    --Measures
    , SUM(od.order_line_quantity)    AS order_items
    , SUM(tr.shipped_qty)            AS shipped_items
    , SUM(tr.return_qty_30_day)      AS return_items_30_day
    --converted to USD
    , SUM(od.order_line_amount_usd)  AS order_amount_usd
    , SUM(tr.shipped_usd_sales)      AS shipped_usd_sales
    , SUM(tr.return_usd_amt_30_day)  AS return_usd_amount_30_day
    , SUM(od.gift_option_amount_usd) AS gift_option_amount_usd
    --not converted to USD
    , SUM(od.order_line_amount)      AS order_amount
    , SUM(tr.shipped_sales)          AS shipped_sales
    , SUM(tr.return_amt_30_day)      AS return_amount_30_day
    , SUM(od.gift_option_amount)     AS gift_option_amount
    
    FROM order_detail od
    
    LEFT JOIN (
        SELECT
          order_num
        , line_item_seq_num
        , upc_num
        , shipped_qty
        , shipped_usd_sales
        , shipped_sales
        , case when days_to_return between 0 and 30 then return_qty else 0 end as return_qty_30_day
        , case when days_to_return between 0 and 30 then return_usd_amt else 0 end as return_usd_amt_30_day
        , case when days_to_return between 0 and 30 then return_amt else 0 end as return_amt_30_day
        , ROW_NUMBER() OVER (PARTITION BY order_num, upc_num ORDER BY upc_num, line_item_seq_num) AS order_seq_num

        FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact ---changed to new fact table

        WHERE order_date >= {start_date}
        AND order_date <= {end_date}
        ) tr
    ON od.order_num = tr.order_num
    AND LPAD(od.upc_code,15,'0') = LPAD(tr.upc_num,15,'0')
    AND od.order_seq_num = tr.order_seq_num
    
    GROUP BY 1,2,3,4,5,6,7,8
    )
WITH DATA
PRIMARY INDEX(order_date_pacific)
ON COMMIT PRESERVE ROWS
;

--should delete any records in final table for data from delete>append overlap
DELETE FROM {gifting_digital_exp_t2_schema}.gifting_order_summary
WHERE order_date_pacific >= {start_date}
AND order_date_pacific <= {end_date}
;

INSERT INTO {gifting_digital_exp_t2_schema}.gifting_order_summary
SELECT 
    --Dimensions  
      order_date_pacific
    , order_num
    , source_platform_code
    , source_channel_country_code
    , source_channel_code
    , ship_type
    , destination_node_num
    , gift_option_type
    --Measures
    , order_items
    , shipped_items
    , return_items_30_day
    --Converted to USD
    , order_amount_usd
    , shipped_usd_sales
    , return_usd_amount_30_day
    , gift_option_amount_usd
    --Not Converted to USD
    , order_amount
    , shipped_sales
    , return_amount_30_day
    , gift_option_amount
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM gifting_detail
;

COLLECT STATISTICS  COLUMN (order_num), 
                    COLUMN (order_date_pacific),
                    COLUMN (source_platform_code),
                    COLUMN (source_channel_country_code),
                    COLUMN (source_channel_code),
                    COLUMN (ship_type),
                    COLUMN (destination_node_num),
                    COLUMN (gift_option_type)
ON {gifting_digital_exp_t2_schema}.gifting_order_summary;

SET QUERY_BAND = NONE FOR SESSION;