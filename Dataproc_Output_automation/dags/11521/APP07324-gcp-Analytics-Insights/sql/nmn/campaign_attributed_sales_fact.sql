BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=campaign_attributed_sales_fact_11521_ACE_ENG;
---     Task_Name=campaign_attributed_sales_fact;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nmn_sales_fact_temp AS
SELECT r.business_day_date,
 store.channel_country,
 store.channel_brand AS channel_banner,
 store.channel_num,
 COALESCE(CASE
   WHEN LOWER(lmt.mktg_type) = LOWER('SEO_SEARCH')
   THEN 'UNPAID'
   ELSE mch.marketing_type
   END, 'BASE') AS mktg_type,
 COALESCE(CASE
   WHEN LOWER(lmt.mktg_type) = LOWER('SEO_SEARCH')
   THEN 'SEO'
   ELSE COALESCE(mch.finance_rollup, 'BASE')
   END, 'BASE') AS finance_rollup,
 COALESCE(CASE
   WHEN LOWER(lmt.mktg_type) = LOWER('SEO_SEARCH')
   THEN 'SEO_SEARCH'
   ELSE mch.finance_detail
   END, 'BASE') AS finance_detail,
 lmt.utm_source,
 TRIM(SUBSTR(lmt.utm_channel, 0, 70)) AS utm_channel,
 TRIM(SUBSTR(lmt.utm_campaign, 0, 90)) AS utm_campaign,
 TRIM(SUBSTR(lmt.utm_term, 0, 90)) AS utm_term,
 TRIM(SUBSTR(lmt.utm_content, 0, 300)) AS utm_content,
 v.vendor_name AS supplier_name,
 sku.brand_name,
 r.sku_num,
 SUM(r.net_sales_usd) AS net_sales_amt_usd,
 SUM(r.gross_sales_usd) AS gross_sales_amt_usd,
 SUM(r.returns_usd) AS returns_amt_usd,
 SUM(r.net_sales_units) AS net_sales_units,
 SUM(r.gross_sales_units) AS gross_sales_units,
 SUM(r.return_units) AS return_units
FROM (SELECT TRIM(COALESCE(sr.order_num, FORMAT('%20d', sr.global_tran_id))) AS order_idnt,
   sr.business_day_date,
    CASE
    WHEN LOWER(sr.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(sr.line_item_fulfillment_type) = LOWER('StorePickUp')
    THEN CASE
     WHEN LOWER(sr.business_unit_desc) LIKE LOWER('%CA%')
     THEN 867
     ELSE 808
     END
    ELSE sr.intent_store_num
    END AS store_num,
   sr.sku_num,
   SUM(sr.shipped_usd_sales) AS gross_sales_usd,
   SUM(sr.shipped_qty) AS gross_sales_units,
   SUM(sr.return_usd_amt) AS returns_usd,
   SUM(sr.return_qty) AS return_units,
    SUM(sr.shipped_usd_sales) - SUM(IFNULL(sr.return_usd_amt, 0)) AS net_sales_usd,
    SUM(sr.shipped_qty) - SUM(IFNULL(sr.return_qty, 0)) AS net_sales_units
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS sr
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd 
   ON sr.business_day_date = dd.day_date  
   AND dd.day_date BETWEEN Cast({{params.start_date}} AS DATE) AND Cast({{params.end_date}} AS DATE)
  WHERE LOWER(sr.transaction_type) = LOWER('retail')
   AND sr.nonmerch_fee_code IS NULL
  GROUP BY order_idnt,
   sr.business_day_date,
   store_num,
   sr.sku_num) AS r
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mta.mta_last_touch AS lmt 
 ON LOWER(lmt.order_number) = LOWER(r.order_idnt)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mta.utm_channel_lookup AS ucl 
 ON LOWER(ucl.utm_mkt_chnl) = LOWER(lmt.utm_channel)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mta.marketing_channel_hierarchy AS mch 
 ON LOWER(mch.join_channel) = LOWER(ucl.bi_channel)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS store 
 ON r.store_num = store.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku 
 ON LOWER(r.sku_num) = LOWER(sku.rms_sku_num) 
 AND LOWER(sku.channel_country) = LOWER(store.store_country_code) 
 AND LOWER(sku.gwp_ind) <> LOWER('Y') 
 AND LOWER(sku.smart_sample_ind) <> LOWER('Y' )
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v 
 ON LOWER(sku.prmy_supp_num) = LOWER(v.vendor_num)
GROUP BY r.business_day_date,
 store.channel_country,
 channel_banner,
 store.channel_num,
 mktg_type,
 finance_rollup,
 finance_detail,
 lmt.utm_source,
 utm_channel,
 utm_campaign,
 utm_term,
 utm_content,
 supplier_name,
 sku.brand_name,
 r.sku_num;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.campaign_attributed_sales_fact
WHERE business_day_date >= {{params.start_date}} AND business_day_date <= {{params.end_date}};


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN

SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.campaign_attributed_sales_fact
(SELECT business_day_date,
  SUBSTR(channel_country, 1, 2) AS channel_country,
  channel_banner,
  channel_num,
  mktg_type,
  finance_rollup,
  finance_detail,
  utm_source,
  utm_channel,
  utm_campaign,
  utm_term,
  utm_content,
  supplier_name,
  brand_name,
  sku_num,
  net_sales_amt_usd,
  gross_sales_amt_usd,
  returns_amt_usd,
  CAST(net_sales_units AS NUMERIC ) AS net_sales_units,
  CAST(gross_sales_units AS NUMERIC ) AS gross_sales_units,
  CAST(return_units AS NUMERIC ) AS return_units,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM nmn_sales_fact_temp
 WHERE business_day_date >= {{params.start_date}}
  AND business_day_date <= {{params.end_date}});


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATISTICS COLUMN (business_day_date, sku_num), COLUMN (business_day_date) ON {{params.nmn_t2_schema}}.CAMPAIGN_ATTRIBUTED_SALES_FACT;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
