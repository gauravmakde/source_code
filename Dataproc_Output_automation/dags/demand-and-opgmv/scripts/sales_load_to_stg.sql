TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk (line_item_seq_num, record_source,
 global_tran_id, demand_date, demand_tmstp_pacific,demand_tmstp_pacific_tz,business_day_date, tran_type_code, line_item_activity_type_code,
 channel, channel_country, channel_brand, selling_channel, business_unit_num, ringing_store_num, intent_store_num,
 ship_zip_code, bill_zip_code, order_num, rms_sku_num, acp_id, ent_cust_id, loyalty_status, register_num, tran_num,
 order_platform_type, platform_code, first_routed_location_type, first_routed_method, first_routed_location,
 fulfillment_status, fulfillment_tmstp_pacific, fulfilled_from_location_type, fulfilled_from_method,
 fulfilled_from_location, line_item_fulfillment_type, line_item_order_type, delivery_method, delivery_method_subtype,
 curbside_ind, jwn_first_routed_demand_ind, jwn_fulfilled_demand_ind, gift_with_purchase_ind, smart_sample_ind,
 line_item_quantity, demand_units, line_net_usd_amt, line_net_amt, line_item_currency_code, jwn_gross_demand_ind,
 jwn_reported_demand_ind, employee_discount_ind, employee_discount_usd_amt, employee_discount_amt, compare_price_amt,
 price_type, price_adj_code, same_day_price_adjust_ind, price_adjustment_ind, wac_eligible_ind, clarity_metric,	
 clarity_metric_line_item, variable_cost_usd_amt, variable_cost_amt, inventory_business_model, upc_num, rms_style_num,
 style_desc, style_group_num, style_group_desc, sbclass_num, sbclass_name, class_num, class_name, dept_num, dept_name,
 subdivision_num, subdivision_name, division_num, division_name, merch_dept_num, nonmerch_fee_code, fee_code_desc,
 vpn_num, vendor_brand_name, supplier_num, supplier_name, payto_vendor_num, payto_vendor_name, npg_ind,
 same_day_store_return_ind, product_return_ind, non_merch_ind, operational_gmv_usd_amt, operational_gmv_amt,
 jwn_operational_gmv_ind, jwn_reported_gmv_ind, jwn_merch_net_sales_ind, net_sales_usd_amt, net_sales_amt,
 jwn_reported_net_sales_ind, jwn_reported_cost_of_sales_ind, jwn_reported_gross_margin_ind, jwn_variable_expenses_ind,
 original_business_date, original_ringing_store_num, original_register_num, original_tran_num, original_global_tran_id,
 original_transaction_identifier, original_line_item_usd_amt, original_line_item_amt,
 original_line_item_amt_currency_code, return_date, sale_date, remote_selling_type, ertm_tran_date, tran_line_id,
 service_type, zero_value_unit_ind, dw_batch_date, dw_sys_load_tmstp)
(SELECT rtdf.line_item_seq_num,
  RPAD('S', 1, ' ') AS record_source,
  rtdf.global_tran_id,
   CASE
   WHEN rtdf.order_date IS NOT NULL
   THEN rtdf.order_date
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R') 
   AND rtdf.original_business_date IS NOT NULL
   THEN rtdf.original_business_date
   ELSE rtdf.business_day_date
   END AS demand_date,
  (COALESCE(timestamp(FORMAT_TIMESTAMP('%F %H:%M:%E6S', rtdf.original_business_date)), rtdf.tran_time_utc))
  AS demand_tmstp_pacific,
COALESCE(`{{params.bq_project_id}}.jwn_udf.udf_time_zone`(cast(FORMAT_TIMESTAMP('%F %H:%M:%E6S', rtdf.original_business_date)as string)), rtdf.tran_time_tz) AS demand_tmstp_pacific_tz,
  rtdf.business_day_date,
  rtdf.tran_type_code,
  rtdf.line_item_activity_type_code,
   CASE
   WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
   THEN SUBSTR('DIGITAL', 1, 20)
   WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder'), LOWER('DESKTOP_WEB'
        ), LOWER('MOBILE_WEB'), LOWER('IOS_APP'), LOWER('ANDROID_APP'), LOWER('CUSTOMER_CARE_PHONE')) 
  AND LOWER(rtdf.line_item_activity_type_code) = LOWER('S') 
  THEN 'DIGITAL'
  WHEN sd.business_unit_num IN (5000, 6000, 6500, 5800)
   THEN 'DIGITAL'
   WHEN LOWER(rtdf.order_num) <> LOWER('') 
   AND LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StorePickup'), LOWER('SHIP_TO_STORE')) 
  AND LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
   THEN 'DIGITAL'
   ELSE 'STORE'
   END AS channel,
  sd.store_country_code AS channel_country2,
   CASE
   WHEN sd.business_unit_num IN (1000, 6000, 6500, 9000, 5800)
   THEN SUBSTR('NORDSTROM', 1, 20)
   WHEN sd.business_unit_num IN (2000, 5000, 9500)
   THEN 'NORDSTROM_RACK'
   WHEN LOWER(sd.store_type_code) IN (LOWER('RR'))
   THEN 'NORDSTROM_RACK'
   WHEN LOWER(sd.store_type_code) IN (LOWER('RS'))
   THEN 'NORDSTROM'
   WHEN sd.business_unit_num IN (3500)
   THEN 'TRUNK_CLUB'
   WHEN sd.business_unit_num IN (8000, 8500, 5500, 3000, 7000, 8500)
   THEN 'JWN'
   ELSE 'UNKNOWN_VALUE'
   END AS channel_brand,
   CASE
   WHEN LOWER(CASE
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN SUBSTR('DIGITAL', 1, 20)
      WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder'), LOWER('DESKTOP_WEB'
           ), LOWER('MOBILE_WEB'), LOWER('IOS_APP'), LOWER('ANDROID_APP'), LOWER('CUSTOMER_CARE_PHONE')) AND LOWER(rtdf
          .line_item_activity_type_code) = LOWER('S') OR sd.business_unit_num IN (5000, 6000, 6500, 5800)
      THEN 'DIGITAL'
      WHEN LOWER(rtdf.order_num) <> LOWER('') AND LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StorePickup'), LOWER('SHIP_TO_STORE'
           )) AND LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
      THEN 'DIGITAL'
      ELSE 'STORE'
      END) = LOWER('DIGITAL')
   THEN SUBSTR('ONLINE', 1, 20)
   ELSE 'STORE'
   END AS selling_channel,
  sd.business_unit_num,
  rtdf.ringing_store_num,
  rtdf.intent_store_num,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) IN (LOWER('S'), LOWER('R')) AND LOWER(rtdf.order_num) <> LOWER('')
   THEN SUBSTR('UNKNOWN_VALUE', 1, 15)
   ELSE 'NOT_APPLICABLE'
   END AS ship_zip_code,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) IN (LOWER('S'), LOWER('R')) AND LOWER(rtdf.order_num) <> LOWER('')
   THEN SUBSTR('UNKNOWN_VALUE', 1, 15)
   ELSE 'NOT_APPLICABLE'
   END AS bill_zip_code,
  rtdf.order_num,
   CASE
   WHEN LOWER(rtdf.rms_sku_num) <> LOWER('')
   THEN rtdf.rms_sku_num
   WHEN LOWER(SUBSTR(rtdf.sales_sku_num, 1, 1)) = LOWER('M')
   THEN rtdf.sales_sku_num
   END AS rms_sku_num,
  rtdf.acp_id,
  SUBSTR(NULL, 1, 50) AS ent_cust_id,
  SUBSTR('UNKNOWN_VALUE', 1, 20) AS loyalty_status,
  rtdf.register_num,
  rtdf.tran_num,
   CASE
   WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
   THEN SUBSTR('Trunk Club', 1, 30)
   WHEN LOWER(rtdf.line_item_order_type) LIKE LOWER('StoreInitDTC%')
   THEN 'Direct to Customer (DTC)'
   WHEN LOWER(rtdf.line_item_order_type) = LOWER('StoreInitSameStrSend') 
   OR LOWER(rtdf.line_item_order_type) LIKE LOWER('StoreInit%') 
   OR LOWER(rtdf.data_source_code) = LOWER('RPOS') 
   OR LOWER(rtdf.line_item_order_type) = LOWER('RPOS')
   THEN 'Store POS'
   WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CustInitPhoneOrder'), LOWER('CUSTOMER_CARE_PHONE'))
   THEN 'Phone (Customer Care)'
   WHEN LOWER(rtdf.price_adj_code) = LOWER('B')
   THEN 'NOT_APPLICABLE'
   WHEN LOWER(CASE
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
      THEN RPAD('N', 1, ' ')
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
             WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
             THEN RPAD('N', 1, ' ')
             WHEN COALESCE(sku.division_num, dept.division_num) = 900
             THEN 'N'
             WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
             THEN 'N'
             WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                 WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                    .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                 THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                 WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                    ) NOT IN (721, 799)
                 THEN 'Leased Boutique'
                 WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                       ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                 THEN 'Restaurant'
                 WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                  AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                 THEN 'Last Chance'
                 WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                 THEN 'Spa Services'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                     ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                 THEN 'Gift Wrap'
                 WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                 THEN 'Alterations'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                     ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                 THEN 'Shipping Revenue'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                 THEN 'Gift Card'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                 THEN 'Shoe Shine'
                 WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                     ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                 THEN 'Other Non-Merchandise'
                 ELSE 'Retail'
                 END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
             THEN 'Y'
             ELSE 'N'
             END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
          LOWER('B'))
      THEN 'Y'
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Last Chance')) AND LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) = LOWER('N')
      THEN 'Y'
      ELSE 'N'
      END) = LOWER('N')
   THEN 'NOT_APPLICABLE'
   ELSE 'UNKNOWN_VALUE'
   END AS order_platform_type,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) IN (LOWER('S'), LOWER('R')) AND LOWER(CASE
       WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
              WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
              THEN RPAD('N', 1, ' ')
              WHEN COALESCE(sku.division_num, dept.division_num) = 900
              THEN 'N'
              WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
              THEN 'N'
              WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                  WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND
                    COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                  THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                  WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                     ) NOT IN (721, 799)
                  THEN 'Leased Boutique'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                        ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Restaurant'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                   AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Last Chance'
                  WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                  THEN 'Spa Services'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                      ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                  THEN 'Gift Wrap'
                  WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                  THEN 'Alterations'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                      ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                  THEN 'Shipping Revenue'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                  THEN 'Gift Card'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                  THEN 'Shoe Shine'
                  WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                      ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                  THEN 'Other Non-Merchandise'
                  ELSE 'Retail'
                  END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
              THEN 'Y'
              ELSE 'N'
              END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
           LOWER('B'))
       THEN 'Y'
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Last Chance')) AND LOWER(CASE
           WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN RPAD('Y', 1, ' ')
           ELSE 'N'
           END) = LOWER('N')
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y')
   THEN SUBSTR('UNKNOWN_VALUE', 1, 20)
   ELSE 'NOT_APPLICABLE'
   END AS platform_code,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN SUBSTR('NOT_APPLICABLE', 1, 20)
   WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 'Marketplace'
   WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
   THEN 'Trunk Club'
   WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 'Vendor (Drop Ship)'
   WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
   THEN 'Store'
   WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
   THEN 'FC'
   WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
   THEN 'DC'
   WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
   THEN 'Store'
   WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
   THEN 'FC'
   WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
   THEN 'DC'
   ELSE 'UNKNOWN_VALUE'
   END AS first_routed_location_type,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN SUBSTR('NOT_APPLICABLE', 1, 20)
   WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 'Marketplace'
   WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
   THEN 'Trunk Club'
   WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 'Vendor (Drop Ship)'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StoreTake'))
   THEN 'Store Take'
   WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rtdf.data_source_code)) = LOWER('RPOS')
   THEN 'Store Fulfilled'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN SUBSTR('NOT_APPLICABLE', 1, 20)
      WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN 'Marketplace'
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN 'Trunk Club'
      WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
      THEN 'Vendor (Drop Ship)'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC')
        )
      THEN 'Store'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
      THEN 'Store'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      ELSE 'UNKNOWN_VALUE'
      END) = LOWER('Store')
   THEN 'Store Fulfilled'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN SUBSTR('NOT_APPLICABLE', 1, 20)
      WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN 'Marketplace'
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN 'Trunk Club'
      WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
      THEN 'Vendor (Drop Ship)'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC')
        )
      THEN 'Store'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
      THEN 'Store'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      ELSE 'UNKNOWN_VALUE'
      END) = LOWER('FC')
   THEN 'FC Filled'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN SUBSTR('NOT_APPLICABLE', 1, 20)
      WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN 'Marketplace'
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN 'Trunk Club'
      WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
      THEN 'Vendor (Drop Ship)'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC')
        )
      THEN 'Store'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
      THEN 'Store'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      ELSE 'UNKNOWN_VALUE'
      END) = LOWER('DC')
   THEN 'DC Filled'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('FulfillmentCenter'), LOWER('StoreShipSend'))
   THEN 'FC Filled'
   ELSE 'UNKNOWN_VALUE'
   END AS first_routed_method,
  CAST(FLOOR(CAST(CASE
    WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
    THEN COALESCE(rtdf.partner_relationship_num, FORMAT('%11d', rtdf.fulfilling_store_num), FORMAT('%11d', rtdf.intent_store_num))
    ELSE NULL
    END AS FLOAT64)) AS INTEGER) AS first_routed_location,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) IN (LOWER('S'), LOWER('R')) AND LOWER(CASE
       WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
              WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
              THEN RPAD('N', 1, ' ')
              WHEN COALESCE(sku.division_num, dept.division_num) = 900
              THEN 'N'
              WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
              THEN 'N'
              WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                  WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND
                    COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                  THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                  WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                     ) NOT IN (721, 799)
                  THEN 'Leased Boutique'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                        ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Restaurant'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                   AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Last Chance'
                  WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                  THEN 'Spa Services'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                      ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                  THEN 'Gift Wrap'
                  WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                  THEN 'Alterations'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                      ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                  THEN 'Shipping Revenue'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                  THEN 'Gift Card'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                  THEN 'Shoe Shine'
                  WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                      ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                  THEN 'Other Non-Merchandise'
                  ELSE 'Retail'
                  END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
              THEN 'Y'
              ELSE 'N'
              END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
           LOWER('B'))
       THEN 'Y'
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Last Chance')) AND LOWER(CASE
           WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN RPAD('Y', 1, ' ')
           ELSE 'N'
           END) = LOWER('N')
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y')
   THEN SUBSTR('Fulfilled', 1, 20)
   ELSE 'NOT_APPLICABLE'
   END AS fulfillment_status,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') 
   AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) 
   AND (rtdf.order_num IS NULL 
   OR rtdf.intent_store_num = 141 
   OR LOWER(rtdf.line_item_order_type) = LOWER('TC'))
   THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf.tran_time AS DATETIME)) AS DATETIME)
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'
       ))
   THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf.business_day_date AS DATETIME)) AS DATETIME)
   ELSE NULL
   END AS fulfillment_tmstp_pacific,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN SUBSTR('NOT_APPLICABLE', 1, 20)
   WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 'Marketplace'
   WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
   THEN 'Trunk Club'
   WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 'Vendor (Drop Ship)'
   WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
   THEN 'Store'
   WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
   THEN 'FC'
   WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
   THEN 'DC'
   WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
   THEN 'Store'
   WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
   THEN 'FC'
   WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
   THEN 'DC'
   ELSE 'UNKNOWN_VALUE'
   END AS fulfilled_from_location_type,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN SUBSTR('NOT_APPLICABLE', 1, 20)
   WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 'Marketplace'
   WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
   THEN 'Trunk Club'
   WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 'Vendor (Drop Ship)'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StoreTake'))
   THEN 'Store Take'
   WHEN LOWER(COALESCE(rtdf.line_item_fulfillment_type, rtdf.data_source_code)) = LOWER('RPOS')
   THEN 'Store Fulfilled'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN SUBSTR('NOT_APPLICABLE', 1, 20)
      WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN 'Marketplace'
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN 'Trunk Club'
      WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
      THEN 'Vendor (Drop Ship)'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC')
        )
      THEN 'Store'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
      THEN 'Store'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      ELSE 'UNKNOWN_VALUE'
      END) = LOWER('Store')
   THEN 'Store Fulfilled'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN SUBSTR('NOT_APPLICABLE', 1, 20)
      WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN 'Marketplace'
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN 'Trunk Club'
      WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
      THEN 'Vendor (Drop Ship)'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC')
        )
      THEN 'Store'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
      THEN 'Store'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      ELSE 'UNKNOWN_VALUE'
      END) = LOWER('FC')
   THEN 'FC Filled'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN SUBSTR('NOT_APPLICABLE', 1, 20)
      WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN 'Marketplace'
      WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
      THEN 'Trunk Club'
      WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
      THEN 'Vendor (Drop Ship)'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC')
        )
      THEN 'Store'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(fstr.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'), LOWER('NL'), LOWER('VS'), LOWER('CC'))
      THEN 'Store'
      WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('LH'), LOWER('OC'))
      THEN 'FC'
      WHEN LOWER(sd.store_type_code) IN (LOWER('RR'), LOWER('RS'), LOWER('DC'))
      THEN 'DC'
      ELSE 'UNKNOWN_VALUE'
      END) = LOWER('DC')
   THEN 'DC Filled'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('FulfillmentCenter'), LOWER('StoreShipSend'))
   THEN 'FC Filled'
   ELSE 'UNKNOWN_VALUE'
   END AS fulfilled_from_method,
  CAST(FLOOR(CAST(CASE
    WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
    THEN COALESCE(rtdf.partner_relationship_num, FORMAT('%11d', rtdf.fulfilling_store_num), FORMAT('%11d', rtdf.intent_store_num))
    ELSE NULL
    END AS FLOAT64)) AS INTEGER) AS fulfilled_from_location,
   CASE
   WHEN LOWER(rtdf.line_item_fulfillment_type) NOT IN (LOWER('RPOS'), LOWER(''))
   THEN rtdf.line_item_fulfillment_type
   WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('RPOS') OR LOWER(rtdf.data_source_code) = LOWER('RPOS')
   THEN 'RPOS'
   ELSE 'UNKNOWN_VALUE'
   END AS line_item_fulfillment_type,
   CASE
   WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('DESKTOP_WEB'), LOWER('MOBILE_WEB'), LOWER('IOS_APP'), LOWER('ANDROID_APP'
      ), LOWER('CustInitWebOrder'))
   THEN SUBSTR('CustInitWebOrder', 1, 32)
   WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CUSTOMER_CARE_PHONE'), LOWER('CustInitPhoneOrder'))
   THEN 'CustInitPhoneOrder'
   WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('RESTAURANT_POS'), LOWER('RPOS'))
   THEN 'RPOS'
   WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('STORE_ORDER'), LOWER('StoreInitWebOrder'))
   THEN 'StoreInitWebOrder'
   WHEN LOWER(rtdf.line_item_order_type) = LOWER('GLOBAL_STORE') AND rtdf.intent_store_num = 5405
   THEN 'StoreInitDTCAuto'
   ELSE rtdf.line_item_order_type
   END AS line_item_order_type_new,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN SUBSTR('NOT_APPLICABLE', 1, 30)
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StoreTake'))
   THEN 'Store Take'
   WHEN LOWER(rtdf.line_item_order_type) = LOWER('StoreInitSameStrSend')
   THEN 'Charge send'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StorePickup')) AND LOWER(CASE
       WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('DESKTOP_WEB'), LOWER('MOBILE_WEB'), LOWER('IOS_APP'), LOWER('ANDROID_APP'
          ), LOWER('CustInitWebOrder'))
       THEN SUBSTR('CustInitWebOrder', 1, 32)
       WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CUSTOMER_CARE_PHONE'), LOWER('CustInitPhoneOrder'))
       THEN 'CustInitPhoneOrder'
       WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('RESTAURANT_POS'), LOWER('RPOS'))
       THEN 'RPOS'
       WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('STORE_ORDER'), LOWER('StoreInitWebOrder'))
       THEN 'StoreInitWebOrder'
       WHEN LOWER(rtdf.line_item_order_type) = LOWER('GLOBAL_STORE') AND rtdf.intent_store_num = 5405
       THEN 'StoreInitDTCAuto'
       ELSE rtdf.line_item_order_type
       END) LIKE LOWER('StoreInitDTC%')
   THEN 'StorePickup'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StorePickup')) AND LOWER(rtdf.order_num) <> LOWER('')
   THEN 'BOPUS'
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StoreShipSend'), LOWER('SHIP_TO_STORE')) AND LOWER(rtdf.order_num
      ) <> LOWER('')
   THEN 'Ship to Store'
   ELSE 'Ship to Customer/Other'
   END AS delivery_method,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN SUBSTR('NOT_APPLICABLE', 1, 40)
   WHEN LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StoreTake'))
   THEN 'Store Take'
   WHEN LOWER(rtdf.line_item_order_type) = LOWER('StoreInitSameStrSend')
   THEN 'Charge send'
   ELSE 'UNKNOWN_VALUE'
   END AS delivery_method_subtype,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') 
   AND (LOWER(CASE
         WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
         THEN SUBSTR('DIGITAL', 1, 20)
         WHEN LOWER(rtdf.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder'), LOWER('DESKTOP_WEB'
              ), LOWER('MOBILE_WEB'), LOWER('IOS_APP'), LOWER('ANDROID_APP'), LOWER('CUSTOMER_CARE_PHONE')) AND LOWER(rtdf
             .line_item_activity_type_code) = LOWER('S') OR sd.business_unit_num IN (5000, 6000, 6500, 5800)
         THEN 'DIGITAL'
         WHEN LOWER(rtdf.order_num) <> LOWER('') AND LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StorePickup'),
             LOWER('SHIP_TO_STORE')) AND LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
         THEN 'DIGITAL'
         ELSE 'STORE'
         END) = LOWER('STORE') OR rtdf.intent_store_num = 141)
   THEN SUBSTR('N', 1, 10)
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(rtdf.line_item_fulfillment_type) IN (LOWER('StorePickup'
       ))
   THEN 'UNKNOWN_VALUE'
   ELSE 'NOT_APPLICABLE'
   END AS curbside_ind,
   CASE
   WHEN LOWER(CASE
       WHEN LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
          THEN RPAD('N', 1, ' ')
          WHEN COALESCE(sku.division_num, dept.division_num) = 900
          THEN 'N'
          WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
          THEN 'N'
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
              WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                 .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
              THEN SUBSTR('Pass-Thru Revenue', 1, 20)
              WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                 ) NOT IN (721, 799)
              THEN 'Leased Boutique'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS')
                 OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Restaurant'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
                LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Last Chance'
              WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
              THEN 'Spa Services'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                  ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
              THEN 'Gift Wrap'
              WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
              THEN 'Alterations'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                  ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
              THEN 'Shipping Revenue'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
              THEN 'Gift Card'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
              THEN 'Shoe Shine'
              WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                  ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
              THEN 'Other Non-Merchandise'
              ELSE 'Retail'
              END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
          THEN 'Y'
          ELSE 'N'
          END) = LOWER('N')
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
          WHEN rtdf.order_date IS NULL AND (MIN(CASE
                 WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
                 THEN rtdf.tran_date
                 ELSE NULL
                 END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier),
                  COALESCE(rtdf.original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
                  , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = (MIN(CASE
                 WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
                 THEN COALESCE(rtdf.order_date, rtdf.tran_date)
                 ELSE NULL
                 END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier),
                  COALESCE(rtdf.original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
                  , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AND LOWER(rtdf
              .line_item_activity_type_code) = LOWER('S') AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'
              ))
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) = LOWER('Y')
       THEN 'N'
       WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) = LOWER('B')
       THEN 'N'
       ELSE 'Y'
       END) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_code) IN (LOWER('S'), LOWER('R'))
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_first_routed_demand_ind,
   CASE
   WHEN LOWER(CASE
       WHEN LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
          THEN RPAD('N', 1, ' ')
          WHEN COALESCE(sku.division_num, dept.division_num) = 900
          THEN 'N'
          WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
          THEN 'N'
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
              WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                 .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
              THEN SUBSTR('Pass-Thru Revenue', 1, 20)
              WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                 ) NOT IN (721, 799)
              THEN 'Leased Boutique'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS')
                 OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Restaurant'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
                LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Last Chance'
              WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
              THEN 'Spa Services'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                  ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
              THEN 'Gift Wrap'
              WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
              THEN 'Alterations'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                  ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
              THEN 'Shipping Revenue'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
              THEN 'Gift Card'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
              THEN 'Shoe Shine'
              WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                  ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
              THEN 'Other Non-Merchandise'
              ELSE 'Retail'
              END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
          THEN 'Y'
          ELSE 'N'
          END) = LOWER('N')
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
          WHEN rtdf.order_date IS NULL AND (MIN(CASE
                 WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
                 THEN rtdf.tran_date
                 ELSE NULL
                 END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier),
                  COALESCE(rtdf.original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
                  , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = (MIN(CASE
                 WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
                 THEN COALESCE(rtdf.order_date, rtdf.tran_date)
                 ELSE NULL
                 END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier),
                  COALESCE(rtdf.original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
                  , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AND LOWER(rtdf
              .line_item_activity_type_code) = LOWER('S') AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'
              ))
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) = LOWER('Y')
       THEN 'N'
       WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) = LOWER('B')
       THEN 'N'
       ELSE 'Y'
       END) = LOWER('Y') AND LOWER(rtdf.line_item_activity_type_code) IN (LOWER('S'), LOWER('R'))
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_fulfilled_demand_ind,
   CASE
   WHEN LOWER(sku.gwp_ind) <> LOWER('')
   THEN sku.gwp_ind
   WHEN LOWER(sku.class_name) LIKE LOWER('%GWP%') AND rtdf.line_net_amt = 0
   THEN 'Y'
   ELSE 'N'
   END AS gift_with_purchase_ind,
   CASE
   WHEN LOWER(sku.smart_sample_ind) <> LOWER('')
   THEN sku.smart_sample_ind
   WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (584, 585, 18) AND rtdf.line_net_amt = 0
   THEN 'Y'
   ELSE 'N'
   END AS smart_sample_ind,
  CAST(FLOOR(CASE
     WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
     THEN - 1
     WHEN LOWER(rtdf.price_adj_code) IN (LOWER('A'), LOWER('B'))
     THEN 0
     ELSE 1
     END * COALESCE(rtdf.line_item_quantity, 0)) AS INTEGER) AS line_item_quantity2,
  CAST(FLOOR(CASE
     WHEN rtdf.intent_store_num = 141 OR LOWER(rtdf.line_item_order_type) = LOWER('TC')
     THEN rtdf.line_item_quantity
     WHEN LOWER(rtdf.price_adj_code) IN (LOWER('A'), LOWER('B'))
     THEN 0
     WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
     THEN rtdf.line_item_quantity
     WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) NOT IN (LOWER('Retail'), LOWER('Leased Boutique')) AND rtdf.line_item_quantity IS NOT NULL
     THEN rtdf.line_item_quantity
     ELSE 0
     END * CASE
     WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
     THEN - 1
     ELSE 1
     END) AS INTEGER) AS demand_units,
  CAST(CASE
    WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
    THEN 0
    ELSE COALESCE(rtdf.line_net_usd_amt, 0)
    END AS NUMERIC) AS line_net_usd_amt,
  CAST(CASE
    WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
    THEN 0
    ELSE COALESCE(rtdf.line_net_amt, 0)
    END AS NUMERIC) AS line_net_amt,
  rtdf.line_item_net_amt_currency_code AS line_item_currency_code,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
   THEN RPAD('N', 1, ' ')
   WHEN COALESCE(sku.division_num, dept.division_num) = 900
   THEN 'N'
   WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
   THEN 'N'
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
   THEN 'Y'
   ELSE 'N'
   END AS jwn_gross_demand_ind,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
      THEN RPAD('N', 1, ' ')
      WHEN COALESCE(sku.division_num, dept.division_num) = 900
      THEN 'N'
      WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
      THEN 'N'
      WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
      THEN 'Y'
      ELSE 'N'
      END) = LOWER('N')
   THEN RPAD('N', 1, ' ')
   WHEN LOWER(CASE
      WHEN rtdf.order_date IS NULL AND (MIN(CASE
             WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
             THEN rtdf.tran_date
             ELSE NULL
             END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier),
              COALESCE(rtdf.original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
              , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = (MIN(CASE
             WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
             THEN COALESCE(rtdf.order_date, rtdf.tran_date)
             ELSE NULL
             END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier),
              COALESCE(rtdf.original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
              , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AND LOWER(rtdf
          .line_item_activity_type_code) = LOWER('S') AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
      THEN RPAD('Y', 1, ' ')
      ELSE 'N'
      END) = LOWER('Y')
   THEN 'N'
   WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) = LOWER('B')
   THEN 'N'
   ELSE 'Y'
   END AS jwn_reported_demand_ind,
   CASE
   WHEN rtdf.employee_discount_amt <> 0
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS employee_discount_ind,
  CAST(CASE
    WHEN rtdf.employee_discount_usd_amt <> 0
    THEN - 1 * rtdf.employee_discount_usd_amt
    ELSE 0
    END AS NUMERIC) AS employee_discount_usd_amt,
  COALESCE(- 1 * rtdf.employee_discount_amt, 0) AS employee_discount_amt,
   CASE
   WHEN rtdf.merch_price_adjust_reason IS NOT NULL AND rtdf.line_item_promo_amt = 0
   THEN ABS(rtdf.line_item_regular_price)
   WHEN ABS(ABS(rtdf.line_net_amt) + ABS(COALESCE(rtdf.employee_discount_amt, 0)) + ABS(COALESCE(rtdf.line_item_promo_amt
         , 0)) - ABS(COALESCE(rtdf.line_item_regular_price, 0))) = 0.01
   THEN ABS(rtdf.line_net_amt) + ABS(COALESCE(rtdf.employee_discount_amt, 0)) - 0.01
   WHEN LOWER(rtdf.channel_brand) = LOWER('NORDSTROM_RACK')
   THEN ABS(rtdf.line_net_amt) + ABS(COALESCE(rtdf.employee_discount_amt, 0)) + ABS(COALESCE(rtdf.line_item_promo_amt, 0
      ))
   ELSE ABS(rtdf.line_net_amt) + ABS(COALESCE(rtdf.employee_discount_amt, 0))
   END AS compare_price_amt,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
      THEN RPAD('N', 1, ' ')
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
             WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
             THEN RPAD('N', 1, ' ')
             WHEN COALESCE(sku.division_num, dept.division_num) = 900
             THEN 'N'
             WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
             THEN 'N'
             WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                 WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                    .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                 THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                 WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                    ) NOT IN (721, 799)
                 THEN 'Leased Boutique'
                 WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                       ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                 THEN 'Restaurant'
                 WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                  AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                 THEN 'Last Chance'
                 WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                 THEN 'Spa Services'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                     ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                 THEN 'Gift Wrap'
                 WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                 THEN 'Alterations'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                     ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                 THEN 'Shipping Revenue'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                 THEN 'Gift Card'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                 THEN 'Shoe Shine'
                 WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                     ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                 THEN 'Other Non-Merchandise'
                 ELSE 'Retail'
                 END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
             THEN 'Y'
             ELSE 'N'
             END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
          LOWER('B'))
      THEN 'Y'
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Last Chance')) AND LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) = LOWER('N')
      THEN 'Y'
      ELSE 'N'
      END) = LOWER('N')
   THEN SUBSTR('NOT_APPLICABLE', 1, 20)
   WHEN rtdf.line_item_promo_amt <> 0 AND rtdf.line_item_promo_id IS NOT NULL
   THEN 'Promotion'
   ELSE 'UNKNOWN_VALUE'
   END AS price_type,
  rtdf.price_adj_code,
   CASE
   WHEN LOWER(rtdf.price_adj_code) = LOWER('B') AND rtdf.business_day_date = CASE
      WHEN rtdf.order_date IS NOT NULL
      THEN rtdf.order_date
      WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R') AND rtdf.original_business_date IS NOT NULL
      THEN rtdf.original_business_date
      ELSE rtdf.business_day_date
      END
   THEN RPAD('Y', 1, ' ')
   WHEN LOWER(rtdf.price_adj_code) = LOWER('B')
   THEN 'X'
   ELSE 'N'
   END AS same_day_price_adjust_ind,
   CASE
   WHEN LOWER(rtdf.price_adj_code) = LOWER('B')
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS price_adjustment_ind,
   CASE
   WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 'N'
   WHEN LOWER(rtdf.line_item_merch_nonmerch_ind) = LOWER('MERCH') AND LOWER(rtdf.sa_tran_status_code) <> LOWER('D') AND
      LOWER(rtdf.data_source_code) IN (LOWER('POS'), LOWER('COM'), LOWER('TRUNK'), LOWER('HAUTE'), LOWER('SA')) AND
     COALESCE(sku.division_num, dept.division_num) NOT IN (600, 800)
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS wac_eligible_ind,
   CASE
   WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
   THEN SUBSTR('Customer Care Expense', 1, 60)
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
      THEN SUBSTR('Goodwill/Net No Product Returns', 1, 60)
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Restaurant')
      THEN 'Restaurant Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Last Chance')
      THEN 'Last Chance Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Spa Services')
      THEN 'Spa Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Gift Wrap')
      THEN 'Giftwrap'
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) = LOWER('Other Non-Merchandise') AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT))
        <> 475
      THEN 'Other Non-Merchandise Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) IN (LOWER('Alterations'), LOWER('Shoe Shine'), LOWER('Shipping Revenue'))
      THEN CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END
      ELSE NULL
      END) = LOWER('Last Chance Sales')
   THEN 'Operational GMV'
   WHEN LOWER(CASE
      WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
      THEN SUBSTR('Goodwill/Net No Product Returns', 1, 60)
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Restaurant')
      THEN 'Restaurant Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Last Chance')
      THEN 'Last Chance Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Spa Services')
      THEN 'Spa Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) = LOWER('Gift Wrap')
      THEN 'Giftwrap'
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) = LOWER('Other Non-Merchandise') AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT))
        <> 475
      THEN 'Other Non-Merchandise Sales'
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) IN (LOWER('Alterations'), LOWER('Shoe Shine'), LOWER('Shipping Revenue'))
      THEN CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END
      ELSE NULL
      END) <> LOWER('')
   THEN 'Net Sales'
   ELSE NULL
   END AS clarity_metric,
   CASE
   WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
   THEN SUBSTR('Goodwill/Net No Product Returns', 1, 60)
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) = LOWER('Restaurant')
   THEN 'Restaurant Sales'
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) = LOWER('Last Chance')
   THEN 'Last Chance Sales'
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) = LOWER('Spa Services')
   THEN 'Spa Sales'
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) = LOWER('Gift Wrap')
   THEN 'Giftwrap'
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) = LOWER('Other Non-Merchandise') AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) <>
     475
   THEN 'Other Non-Merchandise Sales'
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) IN (LOWER('Alterations'), LOWER('Shoe Shine'), LOWER('Shipping Revenue'))
   THEN CASE
    WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
       , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
    THEN SUBSTR('Pass-Thru Revenue', 1, 20)
    WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
       ) NOT IN (721, 799)
    THEN 'Leased Boutique'
    WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
         = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
    THEN 'Restaurant'
    WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
       .line_item_activity_type_code) <> LOWER('N')
    THEN 'Last Chance'
    WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
    THEN 'Spa Services'
    WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
      COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
    THEN 'Gift Wrap'
    WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
    THEN 'Alterations'
    WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR LOWER(rtdf
       .fee_type_code) = LOWER('SH')
    THEN 'Shipping Revenue'
    WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
    THEN 'Gift Card'
    WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
    THEN 'Shoe Shine'
    WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num) =
       900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
    THEN 'Other Non-Merchandise'
    ELSE 'Retail'
    END
   ELSE NULL
   END AS clarity_metric_line_item,
  CAST(CASE
    WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
    THEN rtdf.line_net_usd_amt
    ELSE 0
    END AS NUMERIC) AS variable_cost_usd_amt,
  CAST(CASE
    WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
    THEN rtdf.line_net_amt
    ELSE 0
    END AS NUMERIC) AS variable_cost_amt,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
   THEN SUBSTR('Non-Merch', 1, 30)
   WHEN LOWER(rtdf.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 'eConcession'
   WHEN COALESCE(sku.division_num, dept.division_num) = 800
   THEN 'Concession'
   WHEN COALESCE(sku.division_num, dept.division_num) = 600 AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
      ) NOT IN (935, 936)
   THEN 'Wholesession'
   WHEN COALESCE(sku.division_num, dept.division_num) = 600 AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
      ) IN (935, 936)
   THEN 'Resale'
   WHEN LOWER(rtdf.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 'Drop ship'
   ELSE 'Wholesale'
   END AS inventory_business_model,
  rtdf.upc_num,
  sku.rms_style_num,
  SUBSTR(sku.style_desc, 1, 100) AS style_desc,
  sku.style_group_num,
  sku.style_group_desc,
  sku.sbclass_num,
  sku.sbclass_name,
  sku.class_num,
  sku.class_name,
  COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) AS dept_num2,
  COALESCE(sku.dept_name, dept.dept_name) AS dept_name2,
  COALESCE(sku.subdivision_num, dept.subdivision_num) AS subdivision_num2,
  COALESCE(sku.subdivision_name, dept.subdivision_name) AS subdivision_name2,
  COALESCE(sku.division_num, dept.division_num) AS division_num2,
  COALESCE(sku.division_name, dept.division_name) AS division_name2,
  CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT) AS merch_dept_num,
  rtdf.nonmerch_fee_code,
  rtdf.fee_code_desc,
  sku.vpn_num,
  sku.vendor_brand_name,
  sku.supplier_num,
  sku.supplier_name,
  sku.payto_vendor_num,
  sku.payto_vendor_name,
  COALESCE(sku.npg_ind, 'N') AS npg_ind,
   CASE
   WHEN rtdf.order_date IS NULL AND (MIN(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
          THEN rtdf.tran_date
          ELSE NULL
          END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier), COALESCE(rtdf
            .original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
           , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = (MIN(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
          THEN COALESCE(rtdf.order_date, rtdf.tran_date)
          ELSE NULL
          END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier), COALESCE(rtdf
            .original_ringing_store_num, rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING)
           , rtdf.upc_num, rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AND LOWER(rtdf.line_item_activity_type_code
       ) = LOWER('S') AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS same_day_store_return_ind,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS product_return_ind,
   CASE
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS non_merch_ind,
  CAST(CASE
    WHEN LOWER(CASE
       WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
              WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
              THEN RPAD('N', 1, ' ')
              WHEN COALESCE(sku.division_num, dept.division_num) = 900
              THEN 'N'
              WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
              THEN 'N'
              WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                  WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND
                    COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                  THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                  WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                     ) NOT IN (721, 799)
                  THEN 'Leased Boutique'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                        ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Restaurant'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                   AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Last Chance'
                  WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                  THEN 'Spa Services'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                      ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                  THEN 'Gift Wrap'
                  WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                  THEN 'Alterations'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                      ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                  THEN 'Shipping Revenue'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                  THEN 'Gift Card'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                  THEN 'Shoe Shine'
                  WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                      ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                  THEN 'Other Non-Merchandise'
                  ELSE 'Retail'
                  END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
              THEN 'Y'
              ELSE 'N'
              END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
           LOWER('B'))
       THEN 'Y'
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Last Chance')) AND LOWER(CASE
           WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN RPAD('Y', 1, ' ')
           ELSE 'N'
           END) = LOWER('N')
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y')
    THEN rtdf.line_net_usd_amt
    ELSE 0
    END AS NUMERIC) AS operational_gmv_usd_amt,
  CAST(CASE
    WHEN LOWER(CASE
       WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
              WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
              THEN RPAD('N', 1, ' ')
              WHEN COALESCE(sku.division_num, dept.division_num) = 900
              THEN 'N'
              WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
              THEN 'N'
              WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                  WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND
                    COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                  THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                  WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                     ) NOT IN (721, 799)
                  THEN 'Leased Boutique'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                        ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Restaurant'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                   AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Last Chance'
                  WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                  THEN 'Spa Services'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                      ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                  THEN 'Gift Wrap'
                  WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                  THEN 'Alterations'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                      ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                  THEN 'Shipping Revenue'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                  THEN 'Gift Card'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                  THEN 'Shoe Shine'
                  WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                      ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                  THEN 'Other Non-Merchandise'
                  ELSE 'Retail'
                  END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
              THEN 'Y'
              ELSE 'N'
              END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
           LOWER('B'))
       THEN 'Y'
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Last Chance')) AND LOWER(CASE
           WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN RPAD('Y', 1, ' ')
           ELSE 'N'
           END) = LOWER('N')
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y')
    THEN rtdf.line_net_amt
    ELSE 0
    END AS NUMERIC) AS operational_gmv_amt,
   CASE
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
   THEN RPAD('N', 1, ' ')
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
          THEN RPAD('N', 1, ' ')
          WHEN COALESCE(sku.division_num, dept.division_num) = 900
          THEN 'N'
          WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
          THEN 'N'
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
              WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                 .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
              THEN SUBSTR('Pass-Thru Revenue', 1, 20)
              WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                 ) NOT IN (721, 799)
              THEN 'Leased Boutique'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS')
                 OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Restaurant'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
                LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Last Chance'
              WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
              THEN 'Spa Services'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                  ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
              THEN 'Gift Wrap'
              WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
              THEN 'Alterations'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                  ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
              THEN 'Shipping Revenue'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
              THEN 'Gift Card'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
              THEN 'Shoe Shine'
              WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                  ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
              THEN 'Other Non-Merchandise'
              ELSE 'Retail'
              END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
          THEN 'Y'
          ELSE 'N'
          END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
       LOWER('B'))
   THEN 'Y'
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Last Chance')) AND LOWER(CASE
       WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN RPAD('Y', 1, ' ')
       ELSE 'N'
       END) = LOWER('N')
   THEN 'Y'
   ELSE 'N'
   END AS jwn_operational_gmv_ind,
   CASE
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
   THEN RPAD('N', 1, ' ')
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
          THEN RPAD('N', 1, ' ')
          WHEN COALESCE(sku.division_num, dept.division_num) = 900
          THEN 'N'
          WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
          THEN 'N'
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
              WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                 .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
              THEN SUBSTR('Pass-Thru Revenue', 1, 20)
              WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                 ) NOT IN (721, 799)
              THEN 'Leased Boutique'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS')
                 OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Restaurant'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
                LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Last Chance'
              WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
              THEN 'Spa Services'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                  ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
              THEN 'Gift Wrap'
              WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
              THEN 'Alterations'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                  ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
              THEN 'Shipping Revenue'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
              THEN 'Gift Card'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
              THEN 'Shoe Shine'
              WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                  ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
              THEN 'Other Non-Merchandise'
              ELSE 'Retail'
              END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
          THEN 'Y'
          ELSE 'N'
          END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
       LOWER('B'))
   THEN 'Y'
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Last Chance')) AND LOWER(CASE
       WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN RPAD('Y', 1, ' ')
       ELSE 'N'
       END) = LOWER('N')
   THEN 'Y'
   ELSE 'N'
   END AS jwn_reported_gmv_ind,
   CASE
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
   THEN RPAD('N', 1, ' ')
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
          THEN RPAD('N', 1, ' ')
          WHEN COALESCE(sku.division_num, dept.division_num) = 900
          THEN 'N'
          WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
          THEN 'N'
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
              WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                 .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
              THEN SUBSTR('Pass-Thru Revenue', 1, 20)
              WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                 ) NOT IN (721, 799)
              THEN 'Leased Boutique'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS')
                 OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Restaurant'
              WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
                LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
              THEN 'Last Chance'
              WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
              THEN 'Spa Services'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                  ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
              THEN 'Gift Wrap'
              WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
              THEN 'Alterations'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                  ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
              THEN 'Shipping Revenue'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
              THEN 'Gift Card'
              WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
              THEN 'Shoe Shine'
              WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                  ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
              THEN 'Other Non-Merchandise'
              ELSE 'Retail'
              END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
          THEN 'Y'
          ELSE 'N'
          END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
       LOWER('B'))
   THEN 'Y'
   WHEN LOWER(CASE
       WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
          , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
       THEN SUBSTR('Pass-Thru Revenue', 1, 20)
       WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
          ) NOT IN (721, 799)
       THEN 'Leased Boutique'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
           .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
       THEN 'Restaurant'
       WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
          .line_item_activity_type_code) <> LOWER('N')
       THEN 'Last Chance'
       WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
       THEN 'Spa Services'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
         COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
       THEN 'Gift Wrap'
       WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
       THEN 'Alterations'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
         LOWER(rtdf.fee_type_code) = LOWER('SH')
       THEN 'Shipping Revenue'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
       THEN 'Gift Card'
       WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
       THEN 'Shoe Shine'
       WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
           ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN 'Other Non-Merchandise'
       ELSE 'Retail'
       END) IN (LOWER('Last Chance')) AND LOWER(CASE
       WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
       THEN RPAD('Y', 1, ' ')
       ELSE 'N'
       END) = LOWER('N')
   THEN 'Y'
   ELSE 'N'
   END AS jwn_merch_net_sales_ind,
  CAST(CASE
    WHEN LOWER(COALESCE(CASE
        WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
           ) NOT IN (780, 695)
        THEN SUBSTR('Pass-Thru Revenue', 1, 20)
        WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
           ) NOT IN (721, 799)
        THEN 'Leased Boutique'
        WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
            .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
        THEN 'Restaurant'
        WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
           .line_item_activity_type_code) <> LOWER('N')
        THEN 'Last Chance'
        WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
        THEN 'Spa Services'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
         OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
        THEN 'Gift Wrap'
        WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
        THEN 'Alterations'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
          LOWER(rtdf.fee_type_code) = LOWER('SH')
        THEN 'Shipping Revenue'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
        THEN 'Gift Card'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
        THEN 'Shoe Shine'
        WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
            ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
        THEN 'Other Non-Merchandise'
        ELSE 'Retail'
        END, '')) NOT IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
    THEN CASE
     WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code)
     THEN - 1 * rtdf.original_line_item_usd_amt
     ELSE rtdf.line_net_usd_amt
     END
    ELSE 0
    END AS NUMERIC) AS net_sales_usd_amt,
  CAST(CASE
    WHEN LOWER(COALESCE(CASE
        WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
           ) NOT IN (780, 695)
        THEN SUBSTR('Pass-Thru Revenue', 1, 20)
        WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
           ) NOT IN (721, 799)
        THEN 'Leased Boutique'
        WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept
            .division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
        THEN 'Restaurant'
        WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
           .line_item_activity_type_code) <> LOWER('N')
        THEN 'Last Chance'
        WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
        THEN 'Spa Services'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
         OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
        THEN 'Gift Wrap'
        WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
        THEN 'Alterations'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
          LOWER(rtdf.fee_type_code) = LOWER('SH')
        THEN 'Shipping Revenue'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
        THEN 'Gift Card'
        WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
        THEN 'Shoe Shine'
        WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
            ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
        THEN 'Other Non-Merchandise'
        ELSE 'Retail'
        END, '')) NOT IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
    THEN CASE
     WHEN LOWER(rtdf.line_item_net_amt_currency_code) <> LOWER(rtdf.original_line_item_amt_currency_code)
     THEN - 1 * rtdf.original_line_item_amt
     ELSE rtdf.line_net_amt
     END
    ELSE 0
    END AS NUMERIC) AS net_sales_amt,
   CASE
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
   THEN 'N'
   WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
   THEN 'N'
   WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) = 475
   THEN 'N'
   ELSE 'Y'
   END AS jwn_reported_net_sales_ind,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(CASE
         WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
            , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
         THEN SUBSTR('Pass-Thru Revenue', 1, 20)
         WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
            ) NOT IN (721, 799)
         THEN 'Leased Boutique'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
             dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
         THEN 'Restaurant'
         WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
            .line_item_activity_type_code) <> LOWER('N')
         THEN 'Last Chance'
         WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
         THEN 'Spa Services'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%')
          OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
         THEN 'Gift Wrap'
         WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
         THEN 'Alterations'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
           LOWER(rtdf.fee_type_code) = LOWER('SH')
         THEN 'Shipping Revenue'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
         THEN 'Gift Card'
         WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
         THEN 'Shoe Shine'
         WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
             ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
         THEN 'Other Non-Merchandise'
         ELSE 'Retail'
         END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
      THEN RPAD('N', 1, ' ')
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
             WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
             THEN RPAD('N', 1, ' ')
             WHEN COALESCE(sku.division_num, dept.division_num) = 900
             THEN 'N'
             WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
             THEN 'N'
             WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                 WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku
                    .dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                 THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                 WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                    ) NOT IN (721, 799)
                 THEN 'Leased Boutique'
                 WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                       ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                 THEN 'Restaurant'
                 WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                  AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                 THEN 'Last Chance'
                 WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                 THEN 'Spa Services'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                     ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                 THEN 'Gift Wrap'
                 WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                 THEN 'Alterations'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                     ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                 THEN 'Shipping Revenue'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                 THEN 'Gift Card'
                 WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                 THEN 'Shoe Shine'
                 WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                     ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                 THEN 'Other Non-Merchandise'
                 ELSE 'Retail'
                 END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
             THEN 'Y'
             ELSE 'N'
             END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
          LOWER('B'))
      THEN 'Y'
      WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Last Chance')) AND LOWER(CASE
          WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) = LOWER('N')
      THEN 'Y'
      ELSE 'N'
      END) = LOWER('Y')
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_reported_cost_of_sales_ind,
   CASE
   WHEN LOWER(CASE
      WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
         , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
      THEN SUBSTR('Pass-Thru Revenue', 1, 20)
      WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
         ) NOT IN (721, 799)
      THEN 'Leased Boutique'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
           = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
      THEN 'Restaurant'
      WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
         .line_item_activity_type_code) <> LOWER('N')
      THEN 'Last Chance'
      WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
      THEN 'Spa Services'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
        COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
      THEN 'Gift Wrap'
      WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
      THEN 'Alterations'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
        LOWER(rtdf.fee_type_code) = LOWER('SH')
      THEN 'Shipping Revenue'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
      THEN 'Gift Card'
      WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
      THEN 'Shoe Shine'
      WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num)
         = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
      THEN 'Other Non-Merchandise'
      ELSE 'Retail'
      END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
   THEN 'N'
   WHEN LOWER(rtdf.nonmerch_fee_code) = LOWER('7008')
   THEN 'N'
   WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) = 475
   THEN 'N'
   ELSE 'Y'
   END AS jwn_reported_gross_margin_ind,
   CASE
   WHEN LOWER(rtdf.nonmerch_fee_code) IN (LOWER('7008'))
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_variable_expenses_ind,
  rtdf.original_business_date,
  rtdf.original_ringing_store_num,
  rtdf.original_register_num,
  rtdf.original_tran_num,
  rtdf.original_global_tran_id,
  rtdf.original_transaction_identifier,
  rtdf.original_line_item_usd_amt,
  rtdf.original_line_item_amt,
  rtdf.original_line_item_amt_currency_code,
  MIN(CASE
    WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R')
    THEN rtdf.tran_date
    ELSE NULL
    END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier), COALESCE(rtdf.original_ringing_store_num
      , rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING), rtdf.upc_num,
     rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS return_date,
  MIN(CASE
    WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S')
    THEN COALESCE(rtdf.order_date, rtdf.tran_date)
    ELSE NULL
    END) OVER (PARTITION BY COALESCE(rtdf.original_transaction_identifier, rtdf.transaction_identifier), COALESCE(rtdf.original_ringing_store_num
      , rtdf.ringing_store_num), CAST(COALESCE(rtdf.original_register_num, rtdf.register_num) AS STRING), rtdf.upc_num,
     rtdf.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sale_date,
   CASE
   WHEN LOWER(rtdf.item_source) IN (LOWER('SB_SALESEMPINIT'), LOWER('STYLE_BOARDS'))
   THEN SUBSTR('STYLE_BOARD', 1, 30)
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
       WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
              WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
              THEN RPAD('N', 1, ' ')
              WHEN COALESCE(sku.division_num, dept.division_num) = 900
              THEN 'N'
              WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
              THEN 'N'
              WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                  WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND
                    COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                  THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                  WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                     ) NOT IN (721, 799)
                  THEN 'Leased Boutique'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                        ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Restaurant'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                   AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Last Chance'
                  WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                  THEN 'Spa Services'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                      ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                  THEN 'Gift Wrap'
                  WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                  THEN 'Alterations'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                      ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                  THEN 'Shipping Revenue'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                  THEN 'Gift Card'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                  THEN 'Shoe Shine'
                  WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                      ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                  THEN 'Other Non-Merchandise'
                  ELSE 'Retail'
                  END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
              THEN 'Y'
              ELSE 'N'
              END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
           LOWER('B'))
       THEN 'Y'
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Last Chance')) AND LOWER(CASE
           WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN RPAD('Y', 1, ' ')
           ELSE 'N'
           END) = LOWER('N')
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y')
   THEN 'Self-Service'
   WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('R') AND LOWER(CASE
       WHEN LOWER(CASE
          WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
             , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
          THEN SUBSTR('Pass-Thru Revenue', 1, 20)
          WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
             ) NOT IN (721, 799)
          THEN 'Leased Boutique'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
              dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
          THEN 'Restaurant'
          WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf
             .line_item_activity_type_code) <> LOWER('N')
          THEN 'Last Chance'
          WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
          THEN 'Spa Services'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
              ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
          THEN 'Gift Wrap'
          WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
          THEN 'Alterations'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR
            LOWER(rtdf.fee_type_code) = LOWER('SH')
          THEN 'Shipping Revenue'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
          THEN 'Gift Card'
          WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
          THEN 'Shoe Shine'
          WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
              ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
          THEN 'Other Non-Merchandise'
          ELSE 'Retail'
          END) IN (LOWER('Gift Card'), LOWER('Pass-Thru Revenue'))
       THEN RPAD('N', 1, ' ')
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Retail'), LOWER('Leased Boutique')) AND (LOWER(CASE
              WHEN LOWER(rtdf.line_item_activity_type_code) <> LOWER('S')
              THEN RPAD('N', 1, ' ')
              WHEN COALESCE(sku.division_num, dept.division_num) = 900
              THEN 'N'
              WHEN LOWER(dept.dept_subtype_code) <> LOWER('MO') AND dept.division_num NOT IN (100, 800)
              THEN 'N'
              WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('S') AND LOWER(CASE
                  WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND
                    COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
                  THEN SUBSTR('Pass-Thru Revenue', 1, 20)
                  WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
                     ) NOT IN (721, 799)
                  THEN 'Leased Boutique'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS'
                        ) OR dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Restaurant'
                  WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC'))
                   AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
                  THEN 'Last Chance'
                  WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
                  THEN 'Spa Services'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
                      ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
                  THEN 'Gift Wrap'
                  WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
                  THEN 'Alterations'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %'
                      ) OR LOWER(rtdf.fee_type_code) = LOWER('SH')
                  THEN 'Shipping Revenue'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
                  THEN 'Gift Card'
                  WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
                  THEN 'Shoe Shine'
                  WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
                      ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
                  THEN 'Other Non-Merchandise'
                  ELSE 'Retail'
                  END) IN (LOWER('Retail'), LOWER('Leased Boutique'))
              THEN 'Y'
              ELSE 'N'
              END) = LOWER('Y') OR LOWER(rtdf.line_item_activity_type_code) = LOWER('R') OR LOWER(rtdf.price_adj_code) =
           LOWER('B'))
       THEN 'Y'
       WHEN LOWER(CASE
           WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
              , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
           THEN SUBSTR('Pass-Thru Revenue', 1, 20)
           WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
              ) NOT IN (721, 799)
           THEN 'Leased Boutique'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR
               dept.division_num = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Restaurant'
           WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND
             LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
           THEN 'Last Chance'
           WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
           THEN 'Spa Services'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%'
               ) OR COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
           THEN 'Gift Wrap'
           WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
           THEN 'Alterations'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %')
            OR LOWER(rtdf.fee_type_code) = LOWER('SH')
           THEN 'Shipping Revenue'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
           THEN 'Gift Card'
           WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
           THEN 'Shoe Shine'
           WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num
               ) = 900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN 'Other Non-Merchandise'
           ELSE 'Retail'
           END) IN (LOWER('Last Chance')) AND LOWER(CASE
           WHEN LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
           THEN RPAD('Y', 1, ' ')
           ELSE 'N'
           END) = LOWER('N')
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y')
   THEN 'UNKNOWN_VALUE'
   ELSE 'NOT_APPLICABLE'
   END AS remote_selling_type,
  rtdf.tran_date AS ertm_tran_date,
  rtdf.tran_line_id,
   CASE
   WHEN COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
      , CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
   THEN SUBSTR('Pass-Thru Revenue', 1, 20)
   WHEN COALESCE(sku.division_num, dept.division_num) IN (100, 800) AND COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)
      ) NOT IN (721, 799)
   THEN 'Leased Boutique'
   WHEN (COALESCE(sku.division_num, dept.division_num) = 70 OR LOWER(rtdf.data_source_code) = LOWER('RPOS') OR dept.division_num
        = 70) AND LOWER(rtdf.line_item_activity_type_code) <> LOWER('N')
   THEN 'Restaurant'
   WHEN (COALESCE(sku.division_num, dept.division_num) = 96 OR LOWER(sd.store_type_code) = LOWER('CC')) AND LOWER(rtdf.line_item_activity_type_code
      ) <> LOWER('N')
   THEN 'Last Chance'
   WHEN COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (102, 497)
   THEN 'Spa Services'
   WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT WRAP%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT BOX%') OR
     COALESCE(sku.dept_num, CAST(TRIM(rtdf.merch_dept_num) AS SMALLINT)) IN (482, 765)
   THEN 'Gift Wrap'
   WHEN LOWER(rtdf.fee_type_code) = LOWER('AL')
   THEN 'Alterations'
   WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHIPPING%') OR LOWER(rtdf.fee_code_desc) LIKE LOWER('% SHIP %') OR LOWER(rtdf
      .fee_type_code) = LOWER('SH')
   THEN 'Shipping Revenue'
   WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%GIFT CARD%')
   THEN 'Gift Card'
   WHEN LOWER(rtdf.fee_code_desc) LIKE LOWER('%SHOE SHINE%')
   THEN 'Shoe Shine'
   WHEN LOWER(dept.dept_subtype_code) IN (LOWER('SO'), LOWER('SL')) OR COALESCE(sku.division_num, dept.division_num) =
      900 OR LOWER(rtdf.line_item_activity_type_code) = LOWER('N')
   THEN 'Other Non-Merchandise'
   ELSE 'Retail'
   END AS service_type,
   CASE
   WHEN LOWER(CASE
       WHEN LOWER(sku.gwp_ind) <> LOWER('')
       THEN sku.gwp_ind
       WHEN LOWER(sku.class_name) LIKE LOWER('%GWP%') AND rtdf.line_net_amt = 0
       THEN 'Y'
       ELSE 'N'
       END) = LOWER('Y') AND rtdf.line_net_amt = 0
   THEN RPAD('Y', 1, ' ')
   WHEN LOWER(sku.smart_sample_ind) = LOWER('Y') AND rtdf.line_net_amt = 0
   THEN 'Y'
   ELSE 'N'
   END AS zero_value_unit_ind,
  k.dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.jwn_retail_tran_hdr_detail_fact_vw AS rtdf
  INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS k ON rtdf.global_tran_id = k.global_tran_id AND
      rtdf.business_day_date = k.business_day_date AND rtdf.line_item_seq_num = k.line_item_seq_num AND LOWER(k.status_code
     ) = LOWER('+')
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim AS sd ON rtdf.intent_store_num = sd.store_num
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim AS fstr ON rtdf.fulfilling_store_num = fstr.store_num
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.department_dim_hist AS dept 
  ON dept.dept_num = CAST(rtdf.merch_dept_num AS FLOAT64) 
  AND dept.eff_begin_tmstp_utc<= COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf.original_business_date AS DATETIME)) AS TIMESTAMP),rtdf.tran_time_utc) 
  AND dept.eff_end_tmstp_utc  > COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf.original_business_date AS DATETIME)) AS TIMESTAMP), rtdf.tran_time_utc)
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.demand_product_detail_dim_vw AS sku 
  ON LOWER(sku.rms_sku_num) = LOWER(CASE
        WHEN LOWER(rtdf.rms_sku_num) <> LOWER('')
        THEN rtdf.rms_sku_num
        ELSE '@' || TRIM(FORMAT('%20d', rtdf.global_tran_id))
        END) AND (LOWER(sku.channel_country) = LOWER(sd.store_country_code) 
        OR sku.country_count = 1) 
        AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf.business_day_date AS DATETIME)) AS TIMESTAMP) < sku.eff_end_tmstp_utc 
       AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf.business_day_date AS DATETIME)) AS TIMESTAMP) >= sku.eff_begin_tmstp_utc );


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk AS tgt SET
 rms_sku_num = NULL,
 gift_with_purchase_ind = 'N',
 smart_sample_ind = 'N',
 rms_style_num = NULL,
 style_desc = NULL,
 style_group_num = NULL,
 style_group_desc = NULL,
 sbclass_num = NULL,
 sbclass_name = NULL,
 class_num = NULL,
 class_name = NULL,
 dept_num = dept.dept_num,
 dept_name = dept.dept_name,
 subdivision_num = dept.subdivision_num,
 subdivision_name = dept.subdivision_name,
 division_num = dept.division_num,
 division_name = dept.division_name,
 vpn_num = NULL,
 vendor_brand_name = NULL,
 supplier_num = NULL,
 supplier_name = NULL,
 payto_vendor_num = NULL,
 payto_vendor_name = NULL,
 npg_ind = 'N' FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.department_dim_hist AS dept
WHERE dept.dept_num = tgt.merch_dept_num 
AND dept.eff_begin_tmstp_utc <= tgt.demand_tmstp_pacific 
AND dept.eff_end_tmstp_utc > tgt.demand_tmstp_pacific 
AND LOWER(tgt.service_type) = LOWER('Restaurant') 
AND tgt.division_num <> 70;


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk AS tgt SET
 wac_available_ind = SRC.wac_available_ind,
 weighted_average_cost = SRC.weighted_average_cost2,
 cost_of_sales_usd_amt = SRC.cost_of_sales_usd_amt2,
 cost_of_sales_amt = SRC.cost_of_sales_amt2,
 return_probability = SRC.return_probability,
 jwn_srr_retail_usd_amt = SRC.jwn_srr_retail_usd_amt,
 jwn_srr_retail_amt = SRC.jwn_srr_retail_amt,
 jwn_srr_units = SRC.jwn_srr_units,
 jwn_srr_cost_usd_amt = SRC.jwn_srr_cost_usd_amt,
 jwn_srr_cost_amt = SRC.jwn_srr_cost_amt,
 rp_on_sale_date = SRC.rp_on_sale_date,
 rp_off_sale_date = SRC.rp_off_sale_date FROM (SELECT rtdf.record_source,
   rtdf.line_item_seq_num,
   rtdf.global_tran_id,
   rtdf.business_day_date,
    CASE
    WHEN LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND (LOWER(wac.sku_num) <> LOWER('') OR LOWER(wacc.sku_num) <> LOWER(''
         ))
    THEN RPAD('Y', 1, ' ')
    ELSE 'N'
    END AS wac_available_ind,
    CASE
    WHEN rtdf.line_net_amt <> 0
    THEN COALESCE(CASE
       WHEN CASE
         WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
          rtdf.original_line_item_amt_currency_code IS NOT NULL
         THEN - 1 * rtdf.original_line_item_amt
         ELSE rtdf.line_net_amt
         END > 0
       THEN CASE
        WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
          LOWER('Y')
        THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
        ELSE 0
        END
       ELSE CASE
        WHEN CASE
           WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
            rtdf.original_line_item_amt_currency_code IS NOT NULL
           THEN - 1 * rtdf.original_line_item_amt
           ELSE rtdf.line_net_amt
           END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
        THEN CASE
         WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind)
           = LOWER('Y')
         THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
         ELSE 0
         END
        ELSE NULL
        END
       END, 0) - COALESCE(CASE
       WHEN CASE
         WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
          rtdf.original_line_item_amt_currency_code IS NOT NULL
         THEN - 1 * rtdf.original_line_item_amt
         ELSE rtdf.line_net_amt
         END < 0
       THEN CASE
        WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
          LOWER('Y')
        THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
        ELSE 0
        END
       WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
          WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
           rtdf.original_line_item_amt_currency_code IS NOT NULL
          THEN - 1 * rtdf.original_line_item_amt
          ELSE rtdf.line_net_amt
          END = 0
       THEN CASE
        WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
          LOWER('Y')
        THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
        ELSE 0
        END
       ELSE NULL
       END, 0)
    ELSE 0
    END AS weighted_average_cost2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * IFNULL(CASE
       WHEN rtdf.line_net_amt <> 0
       THEN COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END > 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE CASE
           WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
           THEN CASE
            WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
               ) = LOWER('Y')
            THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
            ELSE 0
            END
           ELSE NULL
           END
          END, 0) - COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END < 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
           ELSE 0
           END
          WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
             WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
              AND rtdf.original_line_item_amt_currency_code IS NOT NULL
             THEN - 1 * rtdf.original_line_item_amt
             ELSE rtdf.line_net_amt
             END = 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE NULL
          END, 0)
       ELSE 0
       END, 0)
    ELSE 0
    END AS cost_of_sales_usd_amt2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * IFNULL(CASE
       WHEN rtdf.line_net_amt <> 0
       THEN COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END > 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE CASE
           WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
           THEN CASE
            WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
               ) = LOWER('Y')
            THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
            ELSE 0
            END
           ELSE NULL
           END
          END, 0) - COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END < 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
           ELSE 0
           END
          WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
             WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
              AND rtdf.original_line_item_amt_currency_code IS NOT NULL
             THEN - 1 * rtdf.original_line_item_amt
             ELSE rtdf.line_net_amt
             END = 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE NULL
          END, 0)
       ELSE 0
       END, 0)
    ELSE 0
    END AS cost_of_sales_amt2,
   rtrn.return_probability_pct AS return_probability,
   IFNULL(- 1 * rtrn.return_probability_pct * rtdf.operational_gmv_usd_amt, 0) AS jwn_srr_retail_usd_amt,
   IFNULL(- 1 * rtrn.return_probability_pct * rtdf.operational_gmv_amt, 0) AS jwn_srr_retail_amt,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN IFNULL(- 1 * rtdf.line_item_quantity * rtrn.return_probability_pct, 0)
    ELSE 0
    END AS jwn_srr_units,
   IFNULL(- 1 * rtrn.return_probability_pct * CASE
      WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
      THEN - 1 * IFNULL(CASE
         WHEN rtdf.line_net_amt <> 0
         THEN COALESCE(CASE
            WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END > 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
             ELSE 0
             END
            ELSE CASE
             WHEN CASE
                WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                    ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                THEN - 1 * rtdf.original_line_item_amt
                ELSE rtdf.line_net_amt
                END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
             THEN CASE
              WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                 ) = LOWER('Y')
              THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
              ELSE 0
              END
             ELSE NULL
             END
            END, 0) - COALESCE(CASE
            WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END < 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
             ELSE 0
             END
            WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
               WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                   ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
               THEN - 1 * rtdf.original_line_item_amt
               ELSE rtdf.line_net_amt
               END = 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
             ELSE 0
             END
            ELSE NULL
            END, 0)
         ELSE 0
         END, 0)
      ELSE 0
      END, 0) AS jwn_srr_cost_usd_amt,
   IFNULL(- 1 * rtrn.return_probability_pct * CASE
      WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
      THEN - 1 * IFNULL(CASE
         WHEN rtdf.line_net_amt <> 0
         THEN COALESCE(CASE
            WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END > 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
             ELSE 0
             END
            ELSE CASE
             WHEN CASE
                WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                    ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                THEN - 1 * rtdf.original_line_item_amt
                ELSE rtdf.line_net_amt
                END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
             THEN CASE
              WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                 ) = LOWER('Y')
              THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
              ELSE 0
              END
             ELSE NULL
             END
            END, 0) - COALESCE(CASE
            WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END < 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
             ELSE 0
             END
            WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
               WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                   ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
               THEN - 1 * rtdf.original_line_item_amt
               ELSE rtdf.line_net_amt
               END = 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, wacc.weighted_average_cost, 0)
             ELSE 0
             END
            ELSE NULL
            END, 0)
         ELSE 0
         END, 0)
      ELSE 0
      END, 0) AS jwn_srr_cost_amt,
   RANGE_START(rp.sale_period) AS rp_on_sale_date,
   RANGE_END(rp.sale_period) AS rp_off_sale_date
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk AS rtdf
   LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.weighted_average_cost_date_dim AS wac 
ON LOWER(wac.sku_num) = LOWER(rtdf.rms_sku_num) 
AND LOWER(wac.location_num) = LOWER(TRIM(FORMAT('%11d', rtdf.intent_store_num))) 
AND rtdf.business_day_date >= wac.eff_begin_dt 
AND rtdf.business_day_date < wac.eff_end_dt 
AND LOWER(wac.weighted_average_cost_currency_code) =  LOWER(rtdf.line_item_currency_code) AND LOWER(wac.sku_num_type) = LOWER('RMS')
   LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim AS s01 ON rtdf.intent_store_num = s01.store_num
   LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.weighted_average_cost_channel_dim AS wacc 
 ON LOWER(wacc.sku_num) = LOWER(rtdf.rms_sku_num) 
 AND s01.channel_num = wacc.channel_num 
 AND LOWER(wacc.sku_num_type) = LOWER('RMS') 
 AND rtdf.business_day_date >= wacc.eff_begin_dt 
 AND rtdf.business_day_date < wacc.eff_end_dt
   LEFT JOIN (SELECT DISTINCT rms_sku_num,
     location_num,
     range(on_sale_date ,
     off_sale_date) AS sale_period
    FROM (SELECT rms_sku_num,
        location_num,
        on_sale_date,
        off_sale_date
       FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.rp_sku_loc_dim_hist
       WHERE location_num NOT IN (SELECT DISTINCT store_num
          FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim
          WHERE channel_num IN (120, 250))
       QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC)) = 1
       UNION ALL
       SELECT a.rms_sku_num,
        b.store_num,
        a.on_sale_date,
        a.off_sale_date
       FROM (SELECT rms_sku_num,
          location_num,
          on_sale_date,
          off_sale_date
         FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.rp_sku_loc_dim_hist
         WHERE location_num = 808
         QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC)) =
          1) AS a
        INNER JOIN (SELECT DISTINCT store_num
         FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim
         WHERE channel_num = 120) AS b ON TRUE
       UNION ALL
       SELECT a.rms_sku_num,
        b.store_num,
        a.on_sale_date,
        a.off_sale_date
       FROM (SELECT rms_sku_num,
          location_num,
          on_sale_date,
          off_sale_date
         FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.rp_sku_loc_dim_hist
         WHERE location_num = 828
         QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC)) =
          1) AS a
        INNER JOIN (SELECT DISTINCT store_num
         FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim
         WHERE channel_num = 250) AS b ON TRUE) AS rp0) AS rp 
ON LOWER(rp.rms_sku_num) = LOWER(CASE
       WHEN LOWER(rtdf.rms_sku_num) <> LOWER('')
       THEN rtdf.rms_sku_num
       ELSE '@' || TRIM(FORMAT('%20d', rtdf.global_tran_id))
       END) AND rtdf.intent_store_num = rp.location_num
	AND range_contains(rp.SALE_PERIOD, rtdf.business_day_date)

   LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.product_sales_return_probability_fact AS rtrn 
ON rtdf.global_tran_id = rtrn.global_tran_id
AND rtdf.line_item_seq_num = rtrn.line_item_seq_num 
AND rtdf.business_day_date = rtrn.business_day_date
WHERE rtdf.global_tran_id IS NOT NULL) AS SRC
WHERE tgt.line_item_seq_num = SRC.line_item_seq_num 
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND tgt.global_tran_id = SRC.global_tran_id 
AND tgt.business_day_date = SRC.business_day_date;


TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_new_acp_wrk;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_new_acp_wrk (global_tran_id, business_day_date, acp_id)
(SELECT DISTINCT rthf.global_tran_id,
  rthf.business_day_date,
  rthf.acp_id
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact AS rthf
  INNER JOIN (SELECT DISTINCT global_tran_id,
    business_day_date,
    acp_id
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk
   WHERE LOWER(record_source) = LOWER('S')) AS jctf 
ON rthf.global_tran_id = jctf.global_tran_id 
AND rthf.business_day_date = jctf.business_day_date 
AND LOWER(COALESCE(jctf.acp_id, '')) <> LOWER(COALESCE(rthf.acp_id, ''))
WHERE rthf.business_day_date BETWEEN DATE '2021-01-01' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)));


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk AS tgt SET
 loyalty_status = SRC.loyalty_status,
 acp_id = SRC.acp_id 
FROM (SELECT fct.line_item_seq_num,
   fct.record_source,
   wrk.global_tran_id,
   wrk.acp_id,
   lvl.rewards_level AS loyalty_status
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_new_acp_wrk AS wrk
   INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk AS fct 
ON wrk.global_tran_id = fct.global_tran_id
AND wrk.business_day_date = fct.business_day_date 
AND LOWER(fct.record_source) = LOWER('S')
LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.analytical_customer AS acust 
ON LOWER(acust.acp_id) = LOWER(COALESCE(wrk.acp_id, '@' || TRIM(FORMAT('%20d', wrk.global_tran_id)))) 
AND LOWER(acust.acp_loyalty_id) <> LOWER('')
   LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.loyalty_level_lifecycle_fact AS lvl 
ON LOWER(lvl.loyalty_id) = LOWER(COALESCE(acust.acp_loyalty_id
         , '@' || TRIM(FORMAT('%20d', wrk.global_tran_id)))) 
AND CAST(fct.demand_tmstp_pacific AS DATE) >= lvl.start_day_date
AND CAST(fct.demand_tmstp_pacific AS DATE) < lvl.end_day_date 
AND lvl.end_day_date >= DATE '2019-01-01'
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY fct.line_item_seq_num, fct.record_source, fct.global_tran_id ORDER BY CASE
        WHEN LOWER(lvl.rewards_level) IN (LOWER('MEMBER'))
        THEN 1
        WHEN LOWER(lvl.rewards_level) IN (LOWER('INSIDER'), LOWER('L1'))
        THEN 2
        WHEN LOWER(lvl.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2'))
        THEN 3
        WHEN LOWER(lvl.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3'))
        THEN 4
        WHEN LOWER(lvl.rewards_level) IN (LOWER('ICON'), LOWER('L4'))
        THEN 5
        ELSE 0
        END, lvl.start_day_date, lvl.end_day_date, lvl.dw_sys_updt_tmstp DESC, lvl.loyalty_id)) = 1) AS SRC
WHERE tgt.line_item_seq_num = SRC.line_item_seq_num 
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND COALESCE(tgt.global_tran_id, - 1) = COALESCE(SRC.global_tran_id, - 1);
