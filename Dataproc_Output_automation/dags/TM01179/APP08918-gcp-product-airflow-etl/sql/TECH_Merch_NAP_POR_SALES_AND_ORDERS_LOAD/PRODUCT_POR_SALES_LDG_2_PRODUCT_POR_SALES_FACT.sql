--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=PRODUCT_POR_SALES;' UPDATE FOR SESSION;

-- APPLY DAILY CHANGES FROM POR SALES


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.product_sales_return_probability_fact AS tgt
USING (SELECT CAST(SUBSTR(LTRIM(RTRIM(global_tran_id)), 1, 20) AS BIGINT) AS global_tran_id,
  acp_id,
  tran_date,
  bu_type_code,
  CAST(line_item_seq_num AS SMALLINT) AS line_item_seq_num,
  rms_sku_num,
  return_probability_pct,
  predicted_net_sales_amt,
  PARSE_DATE('%F', process_date) AS process_date,
  PARSE_DATE('%F', business_day_date) AS business_day_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sales_por_ldg
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY CAST(SUBSTR(LTRIM(RTRIM(CAST(global_tran_id AS STRING))), 1, 20) AS BIGINT), PARSE_DATE('%F',
       CAST(business_day_date AS STRING)), CAST(line_item_seq_num AS SMALLINT) ORDER BY PARSE_DATE('%F', CAST(process_date AS STRING)) DESC)) = 1) AS t0

ON tgt.global_tran_id = t0.global_tran_id AND tgt.business_day_date = t0.business_day_date AND tgt.line_item_seq_num =
  t0.line_item_seq_num
WHEN MATCHED THEN UPDATE SET
 acp_id = t0.acp_id,
 tran_date = CAST(t0.tran_date AS DATE),
 rms_sku_num = t0.rms_sku_num,
 bu_type_code = t0.bu_type_code,
 return_probability_pct = CAST(t0.return_probability_pct AS NUMERIC),
 predicted_net_sales_amt = CAST(t0.predicted_net_sales_amt AS NUMERIC),
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
WHEN NOT MATCHED THEN INSERT VALUES(t0.global_tran_id,CAST(t0.acp_id AS STRING), CAST(t0.tran_date AS DATE),CAST(t0.bu_type_code AS STRING), CAST(TRUNC(t0.line_item_seq_num) AS INT64),CAST(t0.rms_sku_num AS STRING) ,    CAST(t0.return_probability_pct AS NUMERIC), CAST(t0.predicted_net_sales_amt AS NUMERIC), CAST(t0.business_day_date AS DATE), CURRENT_DATE('PST8PDT'), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME)
 , CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));


--COLLECT STATISTICS              COLUMN ( BUSINESS_DAY_DATE ) ,              COLUMN ( GLOBAL_TRAN_ID ) ,              COLUMN ( ACP_ID ) ,              COLUMN ( TRAN_DATE ) ,              COLUMN ( BU_TYPE_CODE ) ,              COLUMN ( LINE_ITEM_SEQ_NUM ) ,              COLUMN ( RMS_SKU_NUM ) ,              COLUMN ( GLOBAL_TRAN_ID, LINE_ITEM_SEQ_NUM )          ON PRD_NAP_FCT.PRODUCT_SALES_RETURN_PROBABILITY_FACT