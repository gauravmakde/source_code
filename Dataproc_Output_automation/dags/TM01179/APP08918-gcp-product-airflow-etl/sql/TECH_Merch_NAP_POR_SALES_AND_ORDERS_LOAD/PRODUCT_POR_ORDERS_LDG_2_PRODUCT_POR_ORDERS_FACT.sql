MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.product_orders_return_probability_fact AS tgt
USING (SELECT order_num,
  acp_id,
  PARSE_DATE('%F', order_date_pacific) AS order_date_pacific,
  bu_type_code,
  order_line_id,
  rms_sku_num,
  return_probability_pct,
  predicted_net_sales_amt,
  PARSE_DATE('%F', process_date) AS process_date,
  PARSE_DATE('%F', order_date_utc) AS order_date_utc
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_orders_por_ldg
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_num, PARSE_DATE('%F', CAST(order_date_pacific AS STRING)), order_line_id ORDER BY
      PARSE_DATE('%F', CAST(process_date AS STRING)) DESC)) = 1) AS t0
ON tgt.order_date_pacific = t0.order_date_pacific AND LOWER(tgt.order_num) = LOWER(t0.order_num) AND LOWER(tgt.order_line_id
   ) = LOWER(t0.order_line_id)
WHEN MATCHED THEN UPDATE SET
 acp_id = t0.acp_id,
 bu_type_code = t0.bu_type_code,
 rms_sku_num = t0.rms_sku_num,
 return_probability_pct = CAST(t0.return_probability_pct AS NUMERIC),
 predicted_net_sales_amt = CAST(t0.predicted_net_sales_amt AS NUMERIC),
 order_date_utc = t0.order_date_utc,
 dw_batch_date = CASE
  WHEN tgt.return_probability_pct <> t0.return_probability_pct OR tgt.predicted_net_sales_amt <> t0.predicted_net_sales_amt
    
  THEN CURRENT_DATE('PST8PDT')
  ELSE tgt.dw_batch_date
  END,
 dw_sys_load_tmstp = CASE
  WHEN tgt.return_probability_pct <> t0.return_probability_pct OR tgt.predicted_net_sales_amt <> t0.predicted_net_sales_amt
    
  THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
  ELSE tgt.dw_sys_load_tmstp
  END,
 dw_sys_updt_tmstp = CASE
  WHEN tgt.return_probability_pct <> t0.return_probability_pct OR tgt.predicted_net_sales_amt <> t0.predicted_net_sales_amt
    
  THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
  ELSE tgt.dw_sys_updt_tmstp
  END
WHEN NOT MATCHED THEN INSERT VALUES(CAST(t0.order_date_pacific AS STRING), t0.order_num, CAST(t0.rms_sku_num AS DATE),
 t0.order_line_id, t0.acp_id, t0.bu_type_code, CAST(t0.return_probability_pct AS NUMERIC), CAST(t0.predicted_net_sales_amt AS NUMERIC), t0.order_date_utc
 , CURRENT_DATE('PST8PDT'), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 );

