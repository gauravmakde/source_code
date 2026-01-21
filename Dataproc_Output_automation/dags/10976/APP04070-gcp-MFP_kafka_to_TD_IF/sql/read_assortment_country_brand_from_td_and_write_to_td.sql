DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_assortment_category_country_plan_fact AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_assortment_category_country_plan_ldg AS stg
 WHERE LOWER(selling_country) = LOWER(tgt.selling_country)
  AND LOWER(tgt.selling_brand) = LOWER(selling_brand)
  AND tgt.dept_num = CAST(TRUNC(CAST(CASE
     WHEN dept_num = ''
     THEN '0'
     ELSE dept_num
     END AS FLOAT64)) AS INTEGER)
  AND LOWER(tgt.price_band) = LOWER(price_band)
  AND LOWER(tgt.category) = LOWER(category)
  and tgt.month_num = (SELECT MAX(cal.MONTH_IDNT) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw cal
                              WHERE LOWER(stg.MONTH_LABEL) =LOWER(cal.MONTH_LABEL))
  AND LOWER(tgt.alternate_inventory_model) = LOWER(alternate_inventory_model)
  AND LOWER(last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS')
  AND tgt.last_updated_time_in_millis < CAST(TRUNC(CAST(CASE
     WHEN last_updated_time_in_millis = ''
     THEN '0'
     ELSE last_updated_time_in_millis
     END AS FLOAT64)) AS BIGINT));

DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_assortment_category_country_plan_ldg AS stg
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_assortment_category_country_plan_fact AS tgt
 inner join  (select MAX(MONTH_IDNT) as MONTH_IDNT,any_value(MONTH_LABEL) as MONTH_LABEL from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw) cal
 on LOWER(stg.MONTH_LABEL) = LOWER(cal.MONTH_LABEL)
 WHERE LOWER(stg.selling_country) = LOWER(selling_country)
  AND LOWER(selling_brand) = LOWER(stg.selling_brand)
  AND dept_num = CAST(TRUNC(CAST(CASE
     WHEN stg.dept_num = ''
     THEN '0'
     ELSE stg.dept_num
     END AS FLOAT64)) AS INTEGER)
  AND LOWER(price_band) = LOWER(stg.price_band)
  AND LOWER(category) = LOWER(stg.category)
  and tgt.MONTH_NUM = cal.MONTH_IDNT
  AND LOWER(alternate_inventory_model) = LOWER(stg.alternate_inventory_model)
  AND LOWER(stg.last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS')
  AND CAST(TRUNC(CAST(CASE
     WHEN stg.last_updated_time_in_millis = ''
     THEN '0'
     ELSE stg.last_updated_time_in_millis
     END AS FLOAT64)) AS BIGINT) <= last_updated_time_in_millis);


INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_assortment_category_country_plan_fact
(
    EVENT_TIME,
    event_time_tz,
    PLAN_PUBLISHED_DATE,
    SELLING_COUNTRY,
    SELLING_BRAND,
    CATEGORY,
    PRICE_BAND,
    DEPT_NUM,
    LAST_UPDATED_TIME_IN_MILLIS,
    LAST_UPDATED_TIME,
    LAST_UPDATED_TIME_TZ,
    MONTH_NUM,
    MONTH_LABEL,
    ALTERNATE_INVENTORY_MODEL,
    AVERAGE_INVENTORY_UNITS,
    AVERAGE_INVENTORY_RETAIL_CURRCYCD,
    AVERAGE_INVENTORY_RETAIL_AMT,
    AVERAGE_INVENTORY_COST_CURRCYCD,
    AVERAGE_INVENTORY_COST_AMT,
    BEGINNING_OF_PERIOD_INVENTORY_UNITS,
    BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD,
    BEGINNING_OF_PERIOD_INVENTORY_RETAIL_AMT,
    BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD,
    BEGINNING_OF_PERIOD_INVENTORY_COST_AMT,
    RETURN_TO_VENDOR_UNITS,
    RETURN_TO_VENDOR_RETAIL_CURRCYCD,
    RETURN_TO_VENDOR_RETAIL_AMT,
    RETURN_TO_VENDOR_COST_CURRCYCD,
    RETURN_TO_VENDOR_COST_AMT,
    RACK_TRANSFER_UNITS,
    RACK_TRANSFER_RETAIL_CURRCYCD,
    RACK_TRANSFER_RETAIL_AMT,
    RACK_TRANSFER_COST_CURRCYCD,
    RACK_TRANSFER_COST_AMT,
    ACTIVE_INVENTORY_IN_UNITS,
    ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD,
    ACTIVE_INVENTORY_IN_RETAIL_AMT,
    ACTIVE_INVENTORY_IN_COST_CURRCYCD,
    ACTIVE_INVENTORY_IN_COST_AMT,
    ACTIVE_INVENTORY_OUT_UNITS,
    ACTIVE_INVENTORY_OUT_RETAIL_CURRCYCD,
    ACTIVE_INVENTORY_OUT_RETAIL_AMT,
    ACTIVE_INVENTORY_OUT_COST_CURRCYCD,
    ACTIVE_INVENTORY_OUT_COST_AMT,
    RECEIPTS_UNITS,
    RECEIPTS_RETAIL_CURRCYCD,
    RECEIPTS_RETAIL_AMT,
    RECEIPTS_COST_CURRCYCD,
    RECEIPTS_COST_AMT,
    RECEIPT_LESS_RESERVE_UNITS,
    RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD,
    RECEIPT_LESS_RESERVE_RETAIL_AMT,
    RECEIPT_LESS_RESERVE_COST_CURRCYCD,
    RECEIPT_LESS_RESERVE_COST_AMT,
    PAH_TRANSFER_IN_UNITS,
    PAH_TRANSFER_IN_RETAIL_CURRCYCD,
    PAH_TRANSFER_IN_RETAIL_AMT,
    PAH_TRANSFER_IN_COST_CURRCYCD,
    PAH_TRANSFER_IN_COST_AMT,
    SHRINK_UNITS,
    SHRINK_RETAIL_CURRCYCD,
    SHRINK_RETAIL_AMT,
    SHRINK_COST_CURRCYCD,
    SHRINK_COST_AMT,
    DW_SYS_LOAD_TMSTP
)

SELECT 

CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(stg.event_time) AS TIMESTAMP) AS event_time,
`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(stg.event_time) AS TIMESTAMP) AS STRING)) AS event_time_tz,
 CAST(stg.plan_published_date AS DATE) AS plan_published_date,
 stg.selling_country,
 stg.selling_brand,
 stg.category,
 stg.price_band,
 CAST(TRUNC(CAST(CASE
   WHEN stg.dept_num = ''
   THEN '0'
   ELSE stg.dept_num
   END AS FLOAT64)) AS INTEGER) AS dept_num,
 CAST(TRUNC(CAST(CASE
   WHEN stg.last_updated_time_in_millis = ''
   THEN '0'
   ELSE stg.last_updated_time_in_millis
   END AS FLOAT64)) AS BIGINT) AS last_updated_time_in_millis,
  `{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(CASE
    WHEN stg.last_updated_time_in_millis = ''
    THEN '0'
    ELSE stg.last_updated_time_in_millis
    END AS FLOAT64)) AS BIGINT)) AS last_updated_time,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(trunc(cast(CASE
    WHEN stg.last_updated_time_in_millis = ''
    THEN '0'
    ELSE stg.last_updated_time_in_millis
    END as float64)) AS BIGINT)) AS STRING)) AS last_updated_time_tz,
 COALESCE(cal.month_idnt, - 1) AS month_num,
 stg.month_label,
 stg.alternate_inventory_model,
 ROUND(CAST(CASE
    WHEN stg.average_inventory_units = ''
    THEN '0'
    ELSE stg.average_inventory_units
    END AS NUMERIC), 4) AS average_inventory_units,
 stg.average_inventory_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.average_inventory_retail_units = ''
      THEN '0'
      ELSE stg.average_inventory_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.average_inventory_retail_nanos = ''
       THEN '0'
       ELSE stg.average_inventory_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS average_inventory_retail_amt,
 stg.average_inventory_cost_currcycd,
 ROUND(CAST(CAST(trunc(cast(CASE
      WHEN stg.average_inventory_cost_units = ''
      THEN '0'
      ELSE stg.average_inventory_cost_units
      END as float64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.average_inventory_cost_nanos = ''
       THEN '0'
       ELSE stg.average_inventory_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS average_inventory_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.beginning_of_period_inventory_units = ''
    THEN '0'
    ELSE stg.beginning_of_period_inventory_units
    END AS NUMERIC), 4) AS beginning_of_period_inventory_units,
 stg.beginning_of_period_inventory_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.beginning_of_period_inventory_retail_units = ''
      THEN '0'
      ELSE stg.beginning_of_period_inventory_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.beginning_of_period_inventory_retail_nanos = ''
       THEN '0'
       ELSE stg.beginning_of_period_inventory_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS beginning_of_period_inventory_retail_amt,
 stg.beginning_of_period_inventory_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.beginning_of_period_inventory_cost_units = ''
      THEN '0'
      ELSE stg.beginning_of_period_inventory_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.beginning_of_period_inventory_cost_nanos = ''
       THEN '0'
       ELSE stg.beginning_of_period_inventory_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS beginning_of_period_inventory_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.return_to_vendor_units = ''
    THEN '0'
    ELSE stg.return_to_vendor_units
    END AS NUMERIC), 4) AS return_to_vendor_units,
 stg.return_to_vendor_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.return_to_vendor_retail_units = ''
      THEN '0'
      ELSE stg.return_to_vendor_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.return_to_vendor_retail_nanos = ''
       THEN '0'
       ELSE stg.return_to_vendor_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS return_to_vendor_retail_amt,
 stg.return_to_vendor_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.return_to_vendor_cost_units = ''
      THEN '0'
      ELSE stg.return_to_vendor_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.return_to_vendor_cost_nanos = ''
       THEN '0'
       ELSE stg.return_to_vendor_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS return_to_vendor_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.rack_transfer_units = ''
    THEN '0'
    ELSE stg.rack_transfer_units
    END AS NUMERIC), 4) AS rack_transfer_units,
 stg.rack_transfer_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.rack_transfer_retail_units = ''
      THEN '0'
      ELSE stg.rack_transfer_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.rack_transfer_retail_nanos = ''
       THEN '0'
       ELSE stg.rack_transfer_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS rack_transfer_retail_amt,
 stg.rack_transfer_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.rack_transfer_cost_units = ''
      THEN '0'
      ELSE stg.rack_transfer_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.rack_transfer_cost_nanos = ''
       THEN '0'
       ELSE stg.rack_transfer_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS rack_transfer_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.active_inventory_in_units = ''
    THEN '0'
    ELSE stg.active_inventory_in_units
    END AS NUMERIC), 4) AS active_inventory_in_units,
 stg.active_inventory_in_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.active_inventory_in_retail_units = ''
      THEN '0'
      ELSE stg.active_inventory_in_retail_units
      END  AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.active_inventory_in_retail_nanos = ''
       THEN '0'
       ELSE stg.active_inventory_in_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_inventory_in_retail_amt,
 stg.active_inventory_in_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.active_inventory_in_cost_units = ''
      THEN '0'
      ELSE stg.active_inventory_in_cost_units
      END  AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.active_inventory_in_cost_nanos = ''
       THEN '0'
       ELSE stg.active_inventory_in_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_inventory_in_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.active_inventory_out_units = ''
    THEN '0'
    ELSE stg.active_inventory_out_units
    END AS NUMERIC), 4) AS active_inventory_out_units,
 stg.active_inventory_out_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.active_inventory_out_retail_units = ''
      THEN '0'
      ELSE stg.active_inventory_out_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.active_inventory_out_retail_nanos = ''
       THEN '0'
       ELSE stg.active_inventory_out_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_inventory_out_retail_amt,
 stg.active_inventory_out_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.active_inventory_out_cost_units = ''
      THEN '0'
      ELSE stg.active_inventory_out_cost_units
      END  AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.active_inventory_out_cost_nanos = ''
       THEN '0'
       ELSE stg.active_inventory_out_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS active_inventory_out_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.receipts_units = ''
    THEN '0'
    ELSE stg.receipts_units
    END AS NUMERIC), 4) AS receipts_units,
 stg.receipts_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.receipts_retail_units = ''
      THEN '0'
      ELSE stg.receipts_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.receipts_retail_nanos = ''
       THEN '0'
       ELSE stg.receipts_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS receipts_retail_amt,
 stg.receipts_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.receipts_cost_units = ''
      THEN '0'
      ELSE stg.receipts_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.receipts_cost_nanos = ''
       THEN '0'
       ELSE stg.receipts_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS receipts_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.receipt_less_reserve_units = ''
    THEN '0'
    ELSE stg.receipt_less_reserve_units
    END AS NUMERIC), 4) AS receipt_less_reserve_units,
 stg.receipt_less_reserve_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.receipt_less_reserve_retail_units = ''
      THEN '0'
      ELSE stg.receipt_less_reserve_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.receipt_less_reserve_retail_nanos = ''
       THEN '0'
       ELSE stg.receipt_less_reserve_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS receipt_less_reserve_retail_amt,
 stg.receipt_less_reserve_cost_currcycd AS receipt_less_reserve_cost_currency_code,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.receipt_less_reserve_cost_units = ''
      THEN '0'
      ELSE stg.receipt_less_reserve_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.receipt_less_reserve_cost_nanos = ''
       THEN '0'
       ELSE stg.receipt_less_reserve_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS receipt_less_reserve_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.pah_transfer_in_units = ''
    THEN '0'
    ELSE stg.pah_transfer_in_units
    END AS NUMERIC), 4) AS pah_transfer_in_units,
 stg.pah_transfer_in_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.pah_transfer_in_retail_units = ''
      THEN '0'
      ELSE stg.pah_transfer_in_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.pah_transfer_in_retail_nanos = ''
       THEN '0'
       ELSE stg.pah_transfer_in_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS pah_transfer_in_retail_amt,
 stg.pah_transfer_in_cost_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.pah_transfer_in_cost_units = ''
      THEN '0'
      ELSE stg.pah_transfer_in_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.pah_transfer_in_cost_nanos = ''
       THEN '0'
       ELSE stg.pah_transfer_in_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS pah_transfer_in_cost_amt,
 ROUND(CAST(CASE
    WHEN stg.shrink_units = ''
    THEN '0'
    ELSE stg.shrink_units
    END AS NUMERIC), 4) AS shrink_units,
 stg.shrink_retail_currcycd,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.shrink_retail_units = ''
      THEN '0'
      ELSE stg.shrink_retail_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.shrink_retail_nanos = ''
       THEN '0'
       ELSE stg.shrink_retail_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS shrink_retail_amt,
 stg.shrink_cost_currcycd AS shrink_cost_currency_code,
 ROUND(CAST(CAST(TRUNC(CAST(CASE
      WHEN stg.shrink_cost_units = ''
      THEN '0'
      ELSE stg.shrink_cost_units
      END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
       WHEN stg.shrink_cost_nanos = ''
       THEN '0'
       ELSE stg.shrink_cost_nanos
       END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS shrink_cost_amt,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_assortment_category_country_plan_ldg AS stg
 LEFT JOIN (SELECT DISTINCT month_idnt,
   month_label
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw) AS cal ON LOWER(stg.month_label) = LOWER(cal.month_label)
WHERE LOWER(stg.dept_num) <> LOWER('DEPT_NUM');