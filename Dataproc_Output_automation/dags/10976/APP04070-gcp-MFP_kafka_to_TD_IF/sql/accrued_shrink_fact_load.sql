DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_accrued_shrink_subclass_store_week_fact
WHERE week_num =  (SELECT CAST(trunc(cast(CASE WHEN config_value = '' THEN '0' ELSE config_value END as float64))  AS INTEGER) AS config_value 
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK'));





INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_accrued_shrink_subclass_store_week_fact (week_num, store_num, dept_num, classs_num,
 subclass_num, total_stock_take_adjustment_cost, total_shrinkage_cost, accrued_shrink_cost, accrued_shrink_units)
(SELECT COALESCE(rw01.week_num, s03.week_num) AS week_num,
  COALESCE(p01.store_num, s03.store_num) AS store_num,
  COALESCE(p01.dept_num, s03.dept_num) AS dept_num,
  COALESCE(p01.classs_num, s03.classs_num) AS classs_num,
  COALESCE(p01.subclass_num, s03.subclass_num) AS subclass_num,
  COALESCE(s03.total_stock_take_adjustment_cost, 0) AS total_stock_take_adjustment_cost,
  COALESCE(s03.total_shrinkage_cost, 0) AS total_shrinkage_cost,
  CAST(CASE
    WHEN (MAX(s03.pi_week_ind) OVER (PARTITION BY COALESCE(rw01.week_num, s03.week_num), COALESCE(p01.store_num, s03.store_num) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 1
    THEN CASE
     WHEN s03.sls_avlbl_ind = 1
     THEN COALESCE(s03.accrued_shrink_cost, 0)
     ELSE 0
     END
    ELSE CASE
      WHEN s03.sls_avlbl_ind = 1
      THEN COALESCE(s03.accrued_shrink_cost, 0)
      ELSE 0
      END + COALESCE(p01.accrued_shrink_cost, 0)
    END AS NUMERIC) AS accrued_shrink_cost,
   CASE
   WHEN (MAX(s03.pi_week_ind) OVER (PARTITION BY COALESCE(rw01.week_num, s03.week_num), COALESCE(p01.store_num, s03.store_num
         ) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 1
   THEN COALESCE(s03.accrued_shrink_units, 0)
   ELSE COALESCE(s03.accrued_shrink_units, 0) + COALESCE(p01.accrued_shrink_units, 0)
   END AS accrued_shrink_units
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_accrued_shrink_subclass_store_week_fact AS p01
  INNER JOIN (SELECT *
   FROM (SELECT week_idnt AS week_num,
      MIN(week_idnt) OVER (PARTITION BY NULL ORDER BY week_idnt ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS
      prev_week_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw) AS pw
   WHERE week_num = (SELECT CAST(trunc(cast(CASE
         WHEN config_value = ''
         THEN '0'
         ELSE config_value
         END as float64)) AS INTEGER) AS config_value
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
      WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK')
       AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK'))) AS rw01 ON p01.week_num = rw01.prev_week_num
  FULL JOIN (SELECT t22.week_num,
    t22.store_num0 AS store_num,
    t22.dept_num,
    t22.classs_num,
    t22.subclass_num,
    t22.pi_week_ind,
    t22.sls_avlbl_ind,
    t22.total_stock_take_adjustment_cost,
    t22.total_shrinkage_cost,
     t22.total_shrinkage_cost - t22.total_stock_take_adjustment_cost AS accrued_shrink_cost,
     CASE
     WHEN t22.A29823320 = 0
     THEN 0
     ELSE t22.A1230787222 / t22.A29823320
     END AS average_net_sales_cost,
     CASE
     WHEN CASE
       WHEN t22.A29823320 = 0
       THEN 0
       ELSE t22.A1230787222 / t22.A29823320
       END = 0
     THEN 0
     ELSE CAST(trunc(cast(ROUND((t22.total_shrinkage_cost - t22.total_stock_take_adjustment_cost) / CASE
         WHEN t22.A29823320 = 0
         THEN 0
         ELSE t22.A1230787222 / t22.A29823320
         END) as float64)) AS INTEGER)
     END AS accrued_shrink_units
   FROM (SELECT s01.week_idnt AS week_num,
      s01.product_hierarchy_dept_num AS dept_num,
      s01.product_hierarchy_class_num AS classs_num,
      s01.product_hierarchy_subclass_num AS subclass_num,
      s01.location_num AS store_num0,
      MAX(s01.pi_week_ind) AS pi_week_ind,
      MAX(CASE
        WHEN sal.store_num IS NOT NULL
        THEN 1
        ELSE 0
        END) AS sls_avlbl_ind,
      SUM(COALESCE(s01.total_stock_take_adjustment_cost, 0)) AS total_stock_take_adjustment_cost,
      SUM(COALESCE(s01.total_shrinkage_cost, 0)) AS total_shrinkage_cost,
      SUM(COALESCE(s01.total_shrinkage_cost, 0)) AS A2014116571,
      SUM(COALESCE(s01.total_stock_take_adjustment_cost, 0)) AS A1702450881,
      SUM(COALESCE(sal.net_sales_quantity, 0)) AS A29823320,
      SUM(COALESCE(sal.net_sales_cost, 0)) AS A1230787222
     FROM (SELECT s.eowsl_num,
        s.correlation_num,
        s.event_time,
        s.location_num,
        s.last_updated_time_in_millis,
        s.last_updated_time,
        s.ledger_book_name,
        s.end_of_week_date,
        s.product_hierarchy_dept_num,
        s.product_hierarchy_class_num,
        s.product_hierarchy_subclass_num,
        s.fiscal_year,
        s.calendar_half,
        s.month_name,
        s.week,
        s.closing_stock_retail_currency_code,
        s.total_closing_stock_retail,
        s.closing_stock_cost_currency_code,
        s.total_closing_stock_cost,
        s.open_stock_retail_currency_code,
        s.total_open_stock_retail,
        s.open_stock_cost_currency_code,
        s.total_open_stock_cost,
        s.stock_adjustment_retail_currency_code,
        s.stock_adjustment_cost_currency_code,
        s.stock_take_adjustment_retail_currency_code,
        s.stock_take_adjustment_cost_currency_code,
        s.stock_adjustment_cogs_retail_currency_code,
        s.stock_adjustment_cogs_cost_currency_code,
        s.purchase_retail_currency_code,
        s.purchase_cost_currency_code,
        s.deal_income_purchases_currency_code,
        s.freight_claim_retail_currency_code,
        s.freight_claim_cost_currency_code,
        s.freight_cost_currency_code,
        s.profit_up_charges_currency_code,
        s.expense_up_charges_currency_code,
        s.rtv_retail_currency_code,
        s.rtv_cost_currency_code,
        s.transfer_in_retail_currency_code,
        s.transfer_in_cost_currency_code,
        s.transfer_in_book_retail_currency_code,
        s.transfer_in_book_cost_currency_code,
        s.transfer_out_retail_currency_code,
        s.transfer_out_cost_currency_code,
        s.transfer_out_book_retail_currency_code,
        s.transfer_out_book_cost_currency_code,
        s.intercompany_in_retail_currency_code,
        s.intercompany_in_cost_currency_code,
        s.intercompany_out_retail_currency_code,
        s.intercompany_out_cost_currency_code,
        s.intercompany_mark_up_currency_code,
        s.intercompany_mark_down_currency_code,
        s.intercompany_margin_currency_code,
        s.sales_quantities,
        s.net_sale_retail_currency_code,
        s.net_sales_cost_currency_code,
        s.net_sales_retail_excluding_vat_currency_code,
        s.returns_retail_currency_code,
        s.returns_cost_currency_code,
        s.net_sales_non_inventory_retail_currency_code,
        s.net_sales_non_inventory_cost_currency_code,
        s.net_sales_non_inventory_retail_excluding_vat_currency_code,
        s.markup_retail_currency_code,
        s.markup_cancelation_retail_currency_code,
        s.clear_markdown_retail_currency_code,
        s.permanent_markdown_retail_currency_code,
        s.promotion_markdown_retail_currency_code,
        s.markdown_cancelation_retail_currency_code,
        s.franchise_returns_retail_currency_code,
        s.franchise_returns_cost_currency_code,
        s.franchise_sales_retail_currency_code,
        s.franchise_sales_cost_currency_code,
        s.franchise_markup_retail_currency_code,
        s.franchise_markdown_retail_currency_code,
        s.employee_discount_retail_currency_code,
        s.deal_income_sales_currency_code,
        s.workroom_amt_currency_code,
        s.cash_discount_amt_currency_code,
        s.cumulative_markon_percent,
        s.shrinkage_retail_currency_code,
        s.shrinkage_cost_currency_code,
        s.reclass_in_retail_currency_code,
        s.reclass_in_cost_currency_code,
        s.reclass_out_retail_currency_code,
        s.reclass_out_cost_currency_code,
        s.gross_margin_amt_currency_code,
        s.margin_cost_variance_currency_code,
        s.retail_cost_variance_currency_code,
        s.cost_variance_amt_currency_code,
        s.half_to_date_doods_available_for_sale_retail_currency_code,
        s.half_to_date_goods_available_for_sale_cost_currency_code,
        s.receiver_cost_adjustment_variance_currency_code,
        s.workorder_actvity_update_inventory_amt_currency_code,
        s.workorder_actvity_post_to_finance_amt_currency_code,
        s.restocking_fee_currency_code,
        s.franchise_restocking_fee_currency_code,
        s.input_vat_amt_currency_code,
        s.output_vat_amt_currency_code,
        s.weight_variance_retail_currency_code,
        s.recoverable_tax_amt_currency_code,
        s.total_stock_adjustment_retail,
        s.total_stock_adjustment_cost,
        s.total_stock_take_adjustment_retail,
        s.total_stock_take_adjustment_cost,
        s.total_stock_adjustment_cogs_retail,
        s.total_stock_adjustment_cogs_cost,
        s.total_purchase_retail,
        s.total_purchase_cost,
        s.total_deal_income_purchases_cost,
        s.total_freight_claim_retail,
        s.total_freight_claim_cost,
        s.total_freight_cost,
        s.total_profit_up_charges_cost,
        s.total_expense_up_charges_cost,
        s.total_rtv_retail,
        s.total_rtv_cost,
        s.total_transfer_in_retail,
        s.total_transfer_in_cost,
        s.total_transfer_in_book_retail,
        s.total_transfer_in_book_cost_cost,
        s.total_transfer_out_retail,
        s.total_transfer_out_cost,
        s.total_transfer_out_book_retail,
        s.total_transfer_out_book_cost,
        s.total_intercompany_in_retail,
        s.total_intercompany_in_cost,
        s.total_intercompany_out_retail,
        s.total_intercompany_out_cost,
        s.total_intercompany_mark_up_cost,
        s.total_intercompany_mark_down_cost,
        s.total_intercompany_margin_cost,
        s.total_net_sale_retail,
        s.total_net_sales_cost_cost,
        s.total_net_sales_retail_excluding_vat_cost,
        s.total_returns_retail,
        s.total_returns_cost,
        s.total_net_sales_non_inventory_retail,
        s.total_net_sales_non_inventory_cost,
        s.total_net_sales_non_inventory_retail_excluding_vat_cost,
        s.total_markup_retail,
        s.total_markup_cancelation_retail,
        s.total_clear_markdown_retail,
        s.total_permanent_markdown_retail,
        s.total_promotion_markdown_retail,
        s.total_markdown_cancelation_retail,
        s.total_franchise_returns_retail,
        s.total_franchise_returns_cost,
        s.total_franchise_sales_retail,
        s.total_franchise_sales_cost,
        s.total_franchise_markup_retail,
        s.total_franchise_markdown_retail,
        s.total_employee_discount_retail,
        s.total_deal_income_sales_cost,
        s.total_workroom_amt_cost,
        s.total_cash_discount_amt_cost,
        s.total_shrinkage_retail,
        s.total_shrinkage_cost,
        s.total_reclass_in_retail,
        s.total_reclass_in_cost,
        s.total_reclass_out_retail,
        s.total_reclass_out_cost,
        s.total_gross_margin_amt_cost,
        s.total_margin_cost_variance_cost,
        s.total_retail_cost_variance_cost,
        s.total_cost_variance_amt_cost,
        s.total_half_to_date_doods_available_for_sale_retail,
        s.total_half_to_date_goods_available_for_sale_cost,
        s.total_receiver_cost_adjustment_variance_cost,
        s.total_workorder_actvity_update_inventory_amt_cost,
        s.total_workorder_actvity_post_to_finance_amt_cost,
        s.total_restocking_fee_cost,
        s.total_franchise_restocking_fee_cost,
        s.total_input_vat_amt_cost,
        s.total_output_vat_amt_cost,
        s.total_weight_variance_retail,
        s.total_recoverable_tax_amt_cost,
        s.dw_sys_load_tmstp,
        s.dw_sys_updt_tmstp,
        w02.week_idnt,
         CASE
         WHEN s.total_stock_take_adjustment_cost <> 0
         THEN 1
         ELSE 0
         END AS pi_avlbl_ind,
        MAX(CASE
          WHEN s.total_stock_take_adjustment_cost <> 0
          THEN 1
          ELSE 0
          END) OVER (PARTITION BY s.location_num, w02.week_idnt ORDER BY NULL RANGE BETWEEN UNBOUNDED PRECEDING AND
         UNBOUNDED FOLLOWING) AS pi_week_ind
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_stockledger_week_fact AS s
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS w02 ON s.end_of_week_date = w02.week_end_day_date
       WHERE w02.week_idnt = (SELECT CAST(trunc(cast(CASE
             WHEN config_value = ''
             THEN '0'
             ELSE config_value
             END as float64)) AS INTEGER) AS config_value
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
          WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK')
           AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK'))) AS s01
      LEFT JOIN (SELECT week_num,
        dept_num,
        class_num,
        subclass_num,
        store_num,
        SUM(COALESCE(CASE
            WHEN line_net_amt > 0
            THEN CASE
             WHEN LOWER(tran_type_code) IN (LOWER('VOID')) OR LOWER(reversal_flag) = LOWER('Y')
             THEN - 1 * weighted_average_cost
             ELSE weighted_average_cost
             END
            ELSE CASE
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
             THEN weighted_average_cost
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('VOID')) AND original_line_item_amt_currency_code
              IS NULL
             THEN - 1 * weighted_average_cost
             ELSE NULL
             END
            END, 0) - COALESCE(CASE
            WHEN line_net_amt < 0
            THEN CASE
             WHEN LOWER(tran_type_code) IN (LOWER('VOID')) OR LOWER(reversal_flag) = LOWER('Y')
             THEN - 1 * weighted_average_cost
             ELSE weighted_average_cost
             END
            ELSE CASE
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('RETN'))
             THEN weighted_average_cost
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('VOID')) AND original_line_item_amt_currency_code
              IS NOT NULL
             THEN - 1 * weighted_average_cost
             ELSE NULL
             END
            END, 0)) AS net_sales_cost,
        SUM(COALESCE(CASE
            WHEN line_net_amt > 0
            THEN CASE
             WHEN LOWER(tran_type_code) IN (LOWER('VOID')) OR LOWER(reversal_flag) = LOWER('Y')
             THEN - 1 * line_item_quantity
             ELSE line_item_quantity
             END
            ELSE CASE
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
             THEN line_item_quantity
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('VOID')) AND original_line_item_amt_currency_code
              IS NULL
             THEN - 1 * line_item_quantity
             ELSE NULL
             END
            END, 0) - COALESCE(CASE
            WHEN line_net_amt < 0
            THEN CASE
             WHEN LOWER(tran_type_code) IN (LOWER('VOID')) OR LOWER(reversal_flag) = LOWER('Y')
             THEN - 1 * line_item_quantity
             ELSE line_item_quantity
             END
            ELSE CASE
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('RETN'))
             THEN line_item_quantity
             WHEN line_net_amt = 0 AND LOWER(tran_type_code) IN (LOWER('VOID')) AND original_line_item_amt_currency_code
              IS NOT NULL
             THEN - 1 * line_item_quantity
             ELSE NULL
             END
            END, 0)) AS net_sales_quantity
       FROM (SELECT cal.week_num,
          sku.dept_num,
          sku.class_num,
          sku.sbclass_num AS subclass_num,
          dtl.intent_store_num AS store_num,
          dtl.business_day_date,
          dtl.tran_type_code,
          dtl.global_tran_id,
          dtl.sku_num,
          dtl.reversal_flag,
           CASE
           WHEN LOWER(dtl.price_adj_code) IN (LOWER('A'), LOWER('B'))
           THEN 0
           ELSE dtl.line_item_quantity
           END AS line_item_quantity,
          dtl.line_net_amt AS line_net_amt_orig,
          dtl.line_item_net_amt_currency_code,
          dtl.original_line_item_amt,
          dtl.original_line_item_amt_currency_code,
           CASE
           WHEN dtl.original_line_item_amt_currency_code IS NOT NULL AND - 1 * dtl.original_line_item_amt <> dtl.line_net_amt
             
           THEN - 1 * dtl.original_line_item_amt
           ELSE dtl.line_net_amt
           END AS line_net_amt,
           CASE
           WHEN LOWER(dtl.price_adj_code) IN (LOWER('A'), LOWER('B'))
           THEN 0
           ELSE COALESCE(wac.weighted_average_cost, 0)
           END AS weighted_average_cost
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_dtl_fct AS dtl
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS cal ON dtl.business_day_date = cal.day_date
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 ON dtl.intent_store_num = s01.store_num
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_upc_dim_hist AS upc ON LOWER(dtl.upc_num) = LOWER(upc.upc_num) AND LOWER(upc
               .channel_country) = LOWER(s01.store_country_code) AND dtl.business_day_date >= CAST(upc.eff_begin_tmstp AS DATE)
              AND dtl.business_day_date < CAST(upc.eff_end_tmstp AS DATE)
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS sku ON LOWER(upc.rms_sku_num) = LOWER(sku.rms_sku_num) AND
              LOWER(sku.channel_country) = LOWER(s01.store_country_code) AND dtl.business_day_date >= CAST(sku.eff_begin_tmstp AS DATE)
              AND dtl.business_day_date < CAST(sku.eff_end_tmstp AS DATE)
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS mrch_dept ON sku.dept_num = mrch_dept.dept_num AND dtl.business_day_date
              >= CAST(mrch_dept.eff_begin_tmstp AS DATE) AND dtl.business_day_date < CAST(mrch_dept.eff_end_tmstp AS DATE)
            
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_wac_fct AS wac ON LOWER(wac.sku_num) = LOWER(sku.rms_sku_num) AND dtl.intent_store_num
              = wac.location_num AND RANGE_CONTAINS(RANGE(wac.eff_beg_dt_drvd, wac.eff_end_dt_drvd), dtl.dw_batch_date)
         WHERE LOWER(mrch_dept.dept_subtype_code) = LOWER('MO')
          AND dtl.tran_type_code IN (SELECT config_value
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup AS chnl
            WHERE LOWER(interface_code) = LOWER('MFP_COST_SALES')
             AND LOWER(config_key) = LOWER('TRAN_TYPE_CODE')
             AND cal.week_num = (SELECT CAST(trunc(cast(CASE
                  WHEN config_value = ''
                  THEN '0'
                  ELSE config_value
                  END as float64)) AS INTEGER) AS A1202843830
               FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
               WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK')
                AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK')))
          AND sku.dept_num IS NOT NULL) AS sub
       GROUP BY week_num,
        dept_num,
        class_num,
        subclass_num,
        store_num) AS sal ON s01.week_idnt = sal.week_num AND sal.store_num = CAST(s01.location_num AS FLOAT64) AND s01
          .product_hierarchy_dept_num = sal.dept_num AND s01.product_hierarchy_class_num = sal.class_num AND s01.product_hierarchy_subclass_num
         = sal.subclass_num
     WHERE s01.week_idnt = (SELECT CAST(trunc(cast(CASE
           WHEN config_value = ''
           THEN '0'
           ELSE config_value
           END as float64)) AS INTEGER) AS config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK')
         AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK'))
     GROUP BY week_num,
      dept_num,
      classs_num,
      subclass_num,
      store_num0
     HAVING total_shrinkage_cost <> 0 OR total_stock_take_adjustment_cost <> 0) AS t22) AS s03 ON rw01.week_num = s03.week_num
        AND LOWER(p01.store_num) = LOWER(s03.store_num) AND p01.dept_num = s03.dept_num AND p01.classs_num = s03.classs_num
      AND p01.subclass_num = s03.subclass_num);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup SET
    config_value = CAST(CASE WHEN (SELECT next_week_num
                    FROM (SELECT week_idnt AS week_num, MIN(week_idnt) OVER (PARTITION BY NULL ORDER BY week_idnt ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS next_week_num
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw) AS pw
                    WHERE week_num = (SELECT CAST(trunc(cast(CASE WHEN config_value = '' THEN '0' ELSE config_value END as float64))  AS INTEGER) AS config_value 
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
                                WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK'))) > (SELECT end_rebuild_week_num
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
                    WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK')) THEN 0 ELSE (SELECT next_week_num
                FROM (SELECT week_idnt AS week_num, MIN(week_idnt) OVER (PARTITION BY NULL ORDER BY week_idnt ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS next_week_num
                        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw) AS pw
                WHERE week_num =  (SELECT CAST(trunc(cast(CASE WHEN config_value = '' THEN '0' ELSE config_value END as float64))  AS INTEGER) AS config_value 
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
                            WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK'))) END AS STRING)
WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND LOWER(config_key) = LOWER('CURR_PROCESS_WEEK');