SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=convenient_services_vw_11521_ACE_ENG;
     Task_Name=convenient_services_vw;'
     FOR SESSION VOLATILE;

/*------------------------------------------------------
 Item Delivery Method Funnel Daily View
 Purpose: Create a day-store level view based on T2DL_DAS_ITEM_DELIVERY.item_delivery_method_funnel_daily to support convinient service reporting
 Last Update: 2/14/22 Niharika Srivastava - creating intial MR
 Contact for logic/code: Rebecca Liao (Strategic Analytics)
 Contact for ETL: Analytics Engineering
 */


REPLACE VIEW {item_delivery_t2_schema}.convenient_services_vw as
 SELECT com.order_date_pacific AS ACTIVITY_DATE,
       dt.year_num AS FISCAL_YEAR,
       dt.quarter_454_num AS FISCAL_QUARTER,
       RIGHT(CAST(dt.halfyear_num AS varchar(10)), 1) AS FISCAL_HALF,
       dt.month_454_num AS FISCAL_MONTH,
       dt.week_454_num AS FISCAL_WEEK,
       dt.day_454_num AS FISCAL_DAY,
       dt.day_date,
       dt.last_year_day_date,
       dt.last_year_day_date_realigned,
       trim(leading '0' from com.destination_node_num) AS DESTINATION_STORE_NUM,
       st.store_name,
       st.store_type_desc,
       st.region_desc,
       coalesce(com.source_platform_code, 'unknown') AS PLATFORM,
       coalesce(com.source_channel_code, 'unknown') AS CHANNEL,
       prd.div_desc,
       prd.grp_desc,
       prd.dept_desc,
       com.item_delivery_method,
       com.shipped_node_num AS SHIPNODE, -- use this field as shipnode / other conditions?
       s.STORE_NAME AS FULFILLING_STORE_NAME, -- not sure whether the join below is correct way to get fulfillment store info
       SUM(CASE WHEN coalesce(com.fraud_cancel_ind, 'N') <> 'Y' then com.order_line_quantity ELSE null END) AS UNITS, 
       SUM(CASE WHEN coalesce(com.fraud_cancel_ind, 'N') <> 'Y' then com.order_line_amount ELSE null END) AS DEMAND,
       COUNT(DISTINCT CASE WHEN coalesce(com.fraud_cancel_ind, 'N') <> 'Y' then com.order_num ELSE null END) AS ORDERS,
       SUM(com.return_amt) AS RETURN_AMOUNT,
       COUNT(DISTINCT com.acp_id) AS CUSTOMERS,
       SUM(CASE WHEN com.cancel_reason_code like '%fraud%' THEN com.order_line_quantity ELSE 0 END) AS FRAUD_UNITS,
       SUM(CASE WHEN com.cancel_reason_code like '%fraud%' THEN com.order_line_amount ELSE 0 END) AS FRAUD_DEMAND,
       COUNT(DISTINCT CASE WHEN com.cancel_reason_code like '%fraud%' THEN com.order_num ELSE NULL END) AS FRAUD_ORDERS,
       COUNT(DISTINCT CASE WHEN com.cancel_reason_code like '%fraud%' THEN com.acp_id ELSE NULL END) AS FRAUD_CUSTOMERS,
       SUM(CASE WHEN com.cancel_reason_code IS NULL THEN com.order_line_quantity ELSE 0 END) AS COMPLETED_UNITS,
       SUM(CASE WHEN com.cancel_reason_code IS NULL THEN com.order_line_amount ELSE 0 END) AS COMPLETED_DEMAND,
       COUNT(DISTINCT CASE WHEN com.cancel_reason_code IS NULL THEN com.order_num ELSE NULL END) AS COMPLETED_ORDERS,
       COUNT(DISTINCT CASE WHEN com.cancel_reason_code IS NULL THEN com.acp_id ELSE NULL END) AS COMPLETED_CUSTOMERS,
       SUM(com.return_qty) AS RETURNED_UNITS,
       COUNT(DISTINCT CASE WHEN com.return_qty IS NOT NULL THEN com.order_num ELSE NULL END) AS RETURNED_ORDERS,
       COUNT(DISTINCT CASE WHEN com.return_qty IS NOT NULL THEN com.acp_id ELSE NULL END) AS RETURNED_CUSTOMERS
FROM T2DL_DAS_ITEM_DELIVERY.item_delivery_method_funnel_daily com
LEFT JOIN prd_nap_usr_vws.store_dim st
ON trim(leading '0' from com.destination_node_num) = st.store_num
LEFT JOIN prd_nap_usr_vws.store_dim s
ON com.shipped_node_num = s.store_num  -- correct way to get the fulfillment store name?
LEFT JOIN prd_nap_usr_vws.day_cal dt
ON com.order_date_pacific = dt.day_date
LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_vw prd
ON com.source_channel_country_code=prd.channel_country and prd.rms_sku_num = com.rms_sku_num
where order_date_pacific >= date'2021-01-31'
and source_channel_code IN ('FULL_LINE','RACK')
and item_delivery_method not in ('SHIP_TO_HOME')
and COALESCE(source_platform_code,'-1') <> 'POS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22;


SET QUERY_BAND = NONE FOR SESSION;