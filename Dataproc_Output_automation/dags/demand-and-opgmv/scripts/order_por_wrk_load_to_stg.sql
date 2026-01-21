
TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_por_wrk;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_por_wrk (order_num, order_line_num, business_day_date, order_line_id,
 demand_date_pacific, demand_tmstp_pacific, demand_tmstp_pacific_tz,channel, channel_country_code, channel_brand_code, selling_channel_code,
 business_unit_num, ringing_store_num, intent_store_num, ship_zip_code, bill_zip_code, acp_id, ent_cust_id,
 order_platform_type, platform_subtype, platform_code, first_routed_tmstp_pacific, first_routed_location_type_code,
 first_routed_method_code, first_routed_location, first_released_date_pacific, fulfillment_status,
 fulfillment_tmstp_pacific, fulfilled_from_location_type_code, fulfilled_from_method_code, fulfilled_from_location,
 line_item_fulfillment_type_code, line_item_order_type_code, pickup_store_num, delivery_method_code,
 delivery_method_subtype_code, curbside_pickup_ind, cancel_reason_code, cancel_reason_subtype_code,
 order_line_canceled_date_pacific, order_line_canceled_tmstp_pacific, same_day_fraud_cancel_ind,
 pre_route_fraud_cancel_ind, cancel_timing, customer_cancel_ind, same_day_cust_cancel_ind, pre_route_cust_cancel_ind,
 post_route_cancel_ind, first_route_pending_ind, backorder_hold_ind, curr_backorder_hold_ind, unrouted_demand_ind,
 gift_with_purchase_ind, smart_sample_ind, line_item_quantity, demand_units, line_net_amt_usd, line_net_amt,
 line_item_currency_code, employee_discount_ind, employee_discount_amt_usd, employee_discount_amt, compare_price_amt,
 upc_num, product_return_ind, return_date, sale_date, bu_type_code, rms_sku_num, return_probability_pct,
 predicted_net_sales_amt, order_date_utc, inventory_business_model, rms_style_num, style_desc, style_group_num,
 style_group_desc, subclass_num, subclass_name, class_num, class_name, dept_num, dept_name, subdivision_num,
 subdivision_name, division_num, division_name, vpn_num, vendor_brand_name, supplier_num, supplier_name,
 payto_vendor_num, payto_vendor_name, npg_ind, service_type, remote_selling_type, 
   rp_on_sale_date,
   rp_off_sale_date,
 jwn_first_routed_demand_ind,
 jwn_fulfilled_demand_ind, jwn_pre_route_cancel_ind, jwn_gross_demand_ind, jwn_reported_demand_ind, record_source,
 dw_batch_date, dw_sys_load_tmstp, zero_value_unit_ind, jwn_operational_gmv_ind)
(SELECT oldf.order_num,
  oldf.order_line_num,
  oldf.business_day_date,
  oldf.order_line_id,
  oldf.order_date_pacific AS demand_date_pacific,
  cast(oldf.order_tmstp_pacific as timestamp) AS demand_tmstp_pacific,
  jwn_udf.udf_time_zone(cast(cast(oldf.order_tmstp_pacific as timestamp) as string)) as demand_tmstp_pacific_tz,
  oldf.channel,
  oldf.channel_country_code,
  oldf.channel_brand_code,
  oldf.selling_channel_code,
  oldf.business_unit_num,
  oldf.ringing_store_num,
  oldf.intent_store_num,
  oldf.ship_zip_code,
  oldf.bill_zip_code,
  oldf.acp_id,
  oldf.ent_cust_id,
  oldf.order_platform_type,
  oldf.platform_subtype,
  oldf.platform_code,
  oldf.first_routed_tmstp_pacific,
  oldf.first_routed_location_type_code,
  oldf.first_routed_method_code,
  oldf.first_routed_location,
  oldf.first_released_date_pacific,
  oldf.fulfillment_status,
  oldf.fulfillment_tmstp_pacific,
  oldf.fulfilled_from_location_type_code,
  oldf.fulfilled_from_method_code,
  oldf.fulfilled_from_location,
  oldf.line_item_fulfillment_type_code,
  oldf.line_item_order_type_code,
  oldf.pickup_store_num,
  oldf.delivery_method_code,
  oldf.delivery_method_subtype_code,
  oldf.curbside_pickup_ind,
  oldf.cancel_reason_code,
  oldf.cancel_reason_subtype_code,
  oldf.order_line_canceled_date_pacific,
  oldf.order_line_canceled_tmstp_pacific,
  oldf.same_day_fraud_cancel_ind,
  oldf.pre_route_fraud_cancel_ind,
  oldf.cancel_timing,
  oldf.customer_cancel_ind,
  oldf.same_day_cust_cancel_ind,
  oldf.pre_route_cust_cancel_ind,
  oldf.post_route_cancel_ind,
  oldf.first_route_pending_ind,
  oldf.backorder_hold_ind,
  oldf.curr_backorder_hold_ind,
  oldf.unrouted_demand_ind,
   CASE
   WHEN LOWER(oldf.gift_with_purchase_ind) <> LOWER('')
   THEN oldf.gift_with_purchase_ind
   WHEN LOWER(sku.gwp_ind) <> LOWER('')
   THEN sku.gwp_ind
   WHEN LOWER(sku.class_name) LIKE LOWER('%GWP%') AND oldf.line_net_amt = 0
   THEN 'Y'
   ELSE 'N'
   END AS gift_with_purchase_ind,
   CASE
   WHEN LOWER(sku.smart_sample_ind) <> LOWER('')
   THEN sku.smart_sample_ind
   WHEN LOWER(oldf.beauty_sample_ind) <> LOWER('')
   THEN oldf.beauty_sample_ind
   WHEN sku.dept_num IN (584, 585, 18) AND oldf.order_line_current_amount = 0
   THEN 'Y'
   ELSE 'N'
   END AS smart_sample_ind,
  oldf.line_item_quantity,
  oldf.line_item_quantity AS demand_units,
  CAST(oldf.line_net_amt_usd AS NUMERIC) AS line_net_amt_usd,
  CAST(oldf.line_net_amt AS NUMERIC) AS line_net_amt,
  oldf.line_item_currency_code,
  oldf.employee_discount_ind,
  CAST(oldf.employee_discount_amt_usd AS NUMERIC) AS employee_discount_amt_usd,
  ROUND(CAST(oldf.employee_discount_amt AS NUMERIC), 2) AS employee_discount_amt,
  ROUND(CAST(oldf.compare_price_amt AS NUMERIC), 2) AS compare_price_amt,
  oldf.upc_num,
  oldf.product_return_ind,
  oldf.return_date,
  oldf.sale_date,
  rtrn.bu_type_code,
  oldf.rms_sku_num,
  rtrn.return_probability_pct,
  rtrn.predicted_net_sales_amt,
  rtrn.order_date_utc,
   CASE
   WHEN LOWER(oldf.partner_relationship_type_code) IN (LOWER('ECONCESSION'))
   THEN SUBSTR('eConcession', 1, 30)
   WHEN sku.division_num = 800
   THEN 'Concession'
   WHEN sku.division_num = 600 AND sku.dept_num NOT IN (935, 936)
   THEN 'Wholesession'
   WHEN sku.division_num = 600 AND sku.dept_num IN (935, 936)
   THEN 'Resale'
   WHEN LOWER(oldf.fulfilled_node_type_code) = LOWER('DS')
   THEN 'Drop ship'
   WHEN LOWER(CASE
      WHEN LOWER(oldf.same_day_fraud_cancel_ind) = LOWER('Y')
      THEN RPAD('N', 1, ' ')
      WHEN LOWER(oldf.same_day_cust_cancel_ind) = LOWER('Y')
      THEN 'N'
      ELSE 'Y'
      END) = LOWER('Y')
   THEN 'Wholesale'
   ELSE 'UNKNOWN_VALUE'
   END AS inventory_business_model,
  sku.rms_style_num,
  SUBSTR(sku.style_desc, 1, 100) AS style_desc,
  sku.style_group_num,
  sku.style_group_desc,
  sku.sbclass_num AS subclass_num,
  sku.sbclass_name AS subclass_name,
  sku.class_num,
  sku.class_name,
  sku.dept_num,
  sku.dept_name,
  sku.subdivision_num,
  sku.subdivision_name,
  sku.division_num,
  sku.division_name,
  sku.vpn_num,
  sku.vendor_brand_name,
  sku.supplier_num,
  sku.supplier_name,
  sku.payto_vendor_num,
  sku.payto_vendor_name,
  COALESCE(sku.npg_ind, 'N') AS npg_ind,
   CASE
   WHEN sku.division_num IN (100, 800)
   THEN 'Leased Boutique'
   WHEN sku.division_num = 70
   THEN 'Restaurant'
   ELSE 'Retail'
   END AS service_type,
  COALESCE(sbol.board_type, SUBSTR('Self-Service', 1, 30)) AS remote_selling_type,
  rp.sale_period_start AS rp_on_sale_date,
  rp.sale_period_end AS rp_off_sale_date,
   CASE
   WHEN COALESCE(oldf.first_released_date_pacific, CAST(oldf.fulfillment_tmstp_pacific AS DATE)) <= CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
       AS DATE) AND LOWER(oldf.same_day_fraud_cancel_ind) = LOWER('N') AND LOWER(oldf.same_day_cust_cancel_ind) = LOWER('N'
      )
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_first_routed_demand_ind,
   CASE
   WHEN oldf.fulfillment_tmstp_pacific IS NOT NULL AND oldf.canceled_date_pacific IS NULL
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_fulfilled_demand_ind,
   CASE
   WHEN LOWER(oldf.pre_release_cancel_ind) = LOWER('Y') AND (oldf.order_line_canceled_tmstp_pacific <= oldf.first_routed_tmstp_pacific
        OR oldf.first_routed_tmstp_pacific IS NULL)
   THEN RPAD('Y', 1, ' ')
   ELSE 'N'
   END AS jwn_pre_route_cancel_ind,
  RPAD('Y', 1, ' ') AS jwn_gross_demand_ind,
   CASE
   WHEN LOWER(oldf.same_day_fraud_cancel_ind) = LOWER('Y')
   THEN RPAD('N', 1, ' ')
   WHEN LOWER(oldf.same_day_cust_cancel_ind) = LOWER('Y')
   THEN 'N'
   ELSE 'Y'
   END AS jwn_reported_demand_ind,
  RPAD('O', 1, ' ') AS record_source,
  k.dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
   CASE
   WHEN LOWER(oldf.gift_with_purchase_ind) = LOWER('Y') AND oldf.order_line_current_amount = 0
   THEN RPAD('Y', 1, ' ')
   WHEN LOWER(sku.smart_sample_ind) = LOWER('Y') AND oldf.order_line_current_amount = 0
   THEN 'Y'
   ELSE 'N'
   END AS zero_value_unit_ind,
   CASE
   WHEN LOWER(CASE
      WHEN oldf.fulfillment_tmstp_pacific IS NOT NULL AND oldf.canceled_date_pacific IS NULL
      THEN RPAD('Y', 1, ' ')
      ELSE 'N'
      END) = LOWER('N')
   THEN RPAD('N', 1, ' ')
   ELSE 'Y'
   END AS jwn_operational_gmv_ind
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_line_detail_fact_vw AS oldf
  LEFT JOIN (SELECT colf.order_line_id,
    COALESCE(cf.board_type, 'STYLE_BOARD') AS board_type
   FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.curation_order_line_fact AS colf
    INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.curation_fact AS cf ON LOWER(cf.board_id) = LOWER(colf.board_id)
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY colf.order_line_id ORDER BY colf.activity_tmstp_pst, cf.activity_tmstp_pst,
        cf.board_sent_pst, cf.board_type)) = 1) AS sbol ON LOWER(oldf.order_line_id) = LOWER(sbol.order_line_id)
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.demand_product_detail_dim_vw AS sku ON LOWER(sku.rms_sku_num) = LOWER(oldf.rms_sku_num) AND
      (LOWER(sku.channel_country) = LOWER(oldf.channel_country_code) OR sku.country_count = 1) AND oldf.order_tmstp_pacific
      < CAST(sku.eff_end_tmstp AS DATETIME) AND oldf.order_tmstp_pacific >= CAST(sku.eff_begin_tmstp AS DATETIME)
  LEFT JOIN (SELECT rms_sku_num,
    location_num,
    sale_period_start,
    sale_period_end
   FROM (SELECT rms_sku_num,
      location_num,
      GroupID,
      MIN(sale_period_start) AS sale_period_start,
      MAX(sale_period_end) AS sale_period_end
     FROM (SELECT rms_sku_num,
        location_num,
        sale_period_start,
        sale_period_end,
        SUM(GroupStartFlag) OVER (PARTITION BY rms_sku_num, location_num ORDER BY sale_period_start ROWS BETWEEN
         UNBOUNDED PRECEDING AND CURRENT ROW) AS GroupID
       FROM (SELECT rms_sku_num,
          location_num,
          on_sale_date AS sale_period_start,
          off_sale_date AS sale_period_end,
           CASE
           WHEN on_sale_date <= (LAG(off_sale_date) OVER (PARTITION BY rms_sku_num, location_num ORDER BY on_sale_date,
                off_sale_date))
           THEN 0
           ELSE 1
           END AS GroupStartFlag
         FROM (SELECT rms_sku_num,
             location_num,
             on_sale_date,
             off_sale_date
            FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.rp_sku_loc_dim_hist
            WHERE location_num NOT IN (SELECT DISTINCT store_num
               FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim
               WHERE channel_num IN (120, 250))
            QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC)
              ) = 1
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
              QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC
                   )) = 1) AS a
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
              QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC
                   )) = 1) AS a
             INNER JOIN (SELECT DISTINCT store_num
              FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim
              WHERE channel_num = 250) AS b ON TRUE) AS rp0) AS t29) AS t30
     GROUP BY rms_sku_num,
      location_num,
      GroupID
     ORDER BY rms_sku_num,
      location_num,
      GroupID) AS t32) AS rp ON LOWER(rp.rms_sku_num) = LOWER(oldf.rms_sku_num) AND oldf.intent_store_num = rp.location_num AND 
      rp.sale_period_start <= oldf.order_date_pacific 
AND rp.sale_period_end >= oldf.order_date_pacific
    
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.product_orders_return_probability_fact AS rtrn ON LOWER(oldf.order_num) = LOWER(rtrn.order_num
       ) AND LOWER(oldf.order_line_id) = LOWER(rtrn.order_line_id) AND oldf.order_date_pacific = rtrn.order_date_pacific
      AND LOWER(oldf.rms_sku_num) = LOWER(rtrn.rms_sku_num)
  INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_keys_wrk AS k ON LOWER(k.order_num) = LOWER(oldf.order_num) AND oldf
    .order_date_pacific = k.order_date_pacific);

--COLLECT STATISTICS COLUMN(order_num), COLUMN(order_line_num), COLUMN(order_line_id), COLUMN(business_day_date) ON prd_NAP_JWN_METRICS_STG.JWN_ORDER_POR_WRK;

