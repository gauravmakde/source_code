
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_nap_sales_fact_load;
---Task_Name=t3_dw_batch_update;'*/
--ET;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_dlta_fact;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_dlta_fact
(
rms_sku_num,
intent_store_num,
day_num,
week_num,
month_num,
quarter_num,
halfyear_num,
year_num,
business_day_date,
tran_type_code,
fulfill_type_code,
wac_avlbl_ind,
flash_event_ind,
merch_ownership_dept_ind,
line_item_promo_id,
line_item_net_amt_currency_code,
weighted_average_cost_currency_code,
reversal_flag,
line_item_quantity,
line_net_amt_orig,
original_line_item_amt,
original_line_item_amt_currency_code,
line_net_amt,
weighted_average_cost,
tran_time,
tran_time_tz,
original_business_date,
price_type_timestamp,
price_type_timestamp_tz,
retl_currency_code,
dw_batch_date,
dw_sys_load_tmstp
)
WITH
dt AS (SELECT START_REBUILD_DATE , END_REBUILD_DATE
              FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw dt
             WHERE LOWER(dt.INTERFACE_CODE) = LOWER('MERCH_NAP_SALE_DLY')),
dtl AS (SELECT 
     			 d.global_tran_id
     			,h.business_day_date
     			,d.tran_type_code
     			,d.intent_store_num
     			,d.upc_num
     			,d.sku_num
     			,d.line_net_amt
     			,d.line_item_quantity
     			,d.line_item_fulfillment_type
     			,d.line_item_net_amt_currency_code
     			,d.original_line_item_amt
     			,d.original_line_item_amt_currency_code
     			,d.dw_batch_date
     			,h.reversal_flag
				,d.PRICE_ADJ_CODE
				,d.ORIGINAL_BUSINESS_DATE
				,d.ORIGINAL_REGISTER_NUM
				,d.ORIGINAL_TRAN_NUM
				,d.ORIGINAL_RINGING_STORE_NUM
				,d.MERCH_UNIQUE_ITEM_ID
				,d.TRAN_TIME_UTC
                ,d.TRAN_TIME_TZ
				,d.LINE_ITEM_PROMO_ID
				,d.LINE_ITEM_SEQ_NUM
				,d.TRAN_VERSION_NUM
     		FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_detail_fact d
     		INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_hdr_fact h 
			   ON d.global_tran_id = h.global_tran_id 
				AND d.business_day_date = h.business_day_date
     		INNER JOIN dt 
				ON d.business_day_date BETWEEN dt.START_REBUILD_DATE AND dt.END_REBUILD_DATE
                AND h.business_day_date BETWEEN dt.START_REBUILD_DATE AND dt.END_REBUILD_DATE
     		WHERE LOWER(h.error_flag) = LOWER('N')
            AND LOWER(d.error_flag) = LOWER('N')
				AND LOWER(COALESCE(h.sa_tran_status_code, 'NA')) <> LOWER('D')
                AND LOWER(d.line_item_merch_nonmerch_ind) = LOWER('MERCH')
				AND h.DATA_SOURCE_CODE IN (SELECT  CONFIG_VALUE  
				FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup  
				WHERE LOWER(interface_code) = LOWER('MFP_COST_SALES')
                AND LOWER(config_key) = LOWER('DATA_SOURCE_CODE'))       
				AND d.TRAN_TYPE_CODE IN (SELECT  CONFIG_VALUE  
				FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup  
				WHERE LOWER(interface_code) = LOWER('MFP_COST_SALES')
    AND LOWER(config_key) = LOWER('TRAN_TYPE_CODE')))

SELECT COALESCE(UPC.RMS_SKU_NUM ,CAST( -1 AS STRING)) AS RMS_SKU_NUM
       ,DTL.INTENT_STORE_NUM
       ,CAL.DAY_IDNT AS DAY_NUM
       ,CAL.WEEK_IDNT AS WEEK_NUM
       ,CAL.MONTH_IDNT AS MONTH_NUM
       ,CAL.QUARTER_IDNT AS QUARTER_NUM
       ,CAL.FISCAL_HALFYEAR_NUM AS HALFYEAR_NUM
       ,CAL.FISCAL_YEAR_NUM AS YEAR_NUM
       ,DTL.BUSINESS_DAY_DATE  
       ,DTL.TRAN_TYPE_CODE
       ,COALESCE(DTL.LINE_ITEM_FULFILLMENT_TYPE,'NA') AS FULFILLMENT_TYPE 
       ,CASE WHEN WACD.SKU_NUM IS NOT NULL OR WACC.SKU_NUM IS NOT NULL
                THEN 'Y'
                ELSE 'N'
            END AS  WAC_AVLBL_IND
       ,CASE WHEN evtsku.SKU_NUM IS NOT NULL
				THEN 'Y'
				ELSE 'N' 
			END AS FLASH_EVENT_IND 
       ,CASE WHEN  LOWER(mrch_dept.dept_subtype_code) = LOWER('MO')
                THEN 'Y'
                ELSE 'N'
                END MO_DEPT_IND
       ,DTL.LINE_ITEM_PROMO_ID
       ,DTL.LINE_ITEM_NET_AMT_CURRENCY_CODE
       ,COALESCE(WACD.WEIGHTED_AVERAGE_COST_CURRENCY_CODE ,WACC.WEIGHTED_AVERAGE_COST_CURRENCY_CODE ) WEIGHTED_AVERAGE_COST_CURRENCY_CODE
       ,DTL.REVERSAL_FLAG
       , CAST(TRUNC(CAST(CASE WHEN LOWER(DTL.PRICE_ADJ_CODE) IN (LOWER('A'),LOWER('B'))
            THEN 0
            ELSE DTL.LINE_ITEM_QUANTITY
            END AS FLOAT64))  AS INT64)AS LINE_ITEM_QUANTITY
       ,DTL.LINE_NET_AMT AS LINE_NET_AMT_ORIG
       ,DTL.ORIGINAL_LINE_ITEM_AMT
       ,DTL.ORIGINAL_LINE_ITEM_AMT_CURRENCY_CODE
       ,CASE WHEN DTL.ORIGINAL_LINE_ITEM_AMT_CURRENCY_CODE IS NOT NULL 
                        AND -1 * DTL.ORIGINAL_LINE_ITEM_AMT <>  DTL.LINE_NET_AMT 
                    THEN -1 * DTL.ORIGINAL_LINE_ITEM_AMT
                ELSE DTL.LINE_NET_AMT
                END LINE_NET_AMT
        ,CASE WHEN LOWER(dtl.price_adj_code) IN (LOWER('A'), LOWER('B'))
            THEN 0
            ELSE COALESCE(WACD.WEIGHTED_AVERAGE_COST ,WACC.WEIGHTED_AVERAGE_COST )
            END AS WEIGHTED_AVERAGE_COST
        ,DTL.TRAN_TIME_UTC
        ,DTL.TRAN_TIME_TZ
        ,DTL.ORIGINAL_BUSINESS_DATE
        ,COALESCE( RTN.TRAN_TIME_UTC, CAST(DTL.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),DTL.TRAN_TIME_UTC) AS PRICE_TYPE_TIMESTAMP -- GET TRAN TIME OF ORIGINAL SALE IF PRESENT
        ,`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(COALESCE( RTN.TRAN_TIME_UTC, CAST(DTL.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),DTL.TRAN_TIME_UTC) AS STRING)) AS PRICE_TYPE_TIMESTAMP_TZ
        ,COALESCE(DTL.ORIGINAL_LINE_ITEM_AMT_CURRENCY_CODE, DTL.LINE_ITEM_NET_AMT_CURRENCY_CODE) RETL_CURRENCY_CODE
        ,DTL.DW_BATCH_DATE AS DW_BATCH_DATE
        ,current_datetime('PST8PDT') AS dw_sys_load_tmstp
  FROM dtl  
  LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_detail_fact RTN  -- TO FETCH DETAILS OF RETURN TRANSACTION
  	ON DTL.ORIGINAL_BUSINESS_DATE = RTN.BUSINESS_DAY_DATE
	AND RTN.REGISTER_NUM = DTL.ORIGINAL_REGISTER_NUM
	AND RTN.TRAN_NUM = CAST(DTL.ORIGINAL_TRAN_NUM AS NUMERIC)
	AND RTN.RINGING_STORE_NUM = DTL.ORIGINAL_RINGING_STORE_NUM
	AND DTL.MERCH_UNIQUE_ITEM_ID = RTN.MERCH_UNIQUE_ITEM_ID
	
    AND LOWER(RTN.line_item_activity_type_code) = LOWER('S')   

  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim CAL
            ON CAL.DAY_DATE = DTL.BUSINESS_DAY_DATE 

  INNER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim S01  
            ON DTL.INTENT_STORE_NUM = S01.STORE_NUM 

  LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_upc_dim_hist UPC
            ON LOWER(dtl.upc_num) = LOWER(upc.upc_num) 
            AND LOWER(upc.channel_country ) = LOWER(s01.store_country_code)
                AND DTL.BUSINESS_DAY_DATE >= UPC.EFF_BEGIN_TMSTP
                AND DTL.BUSINESS_DAY_DATE <  UPC.EFF_END_TMSTP    

  LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist sku
        ON LOWER(upc.rms_sku_num) = LOWER(sku.rms_sku_num) 
        AND LOWER(sku.channel_country) = LOWER(s01.store_country_code)
                AND dtl.business_day_date >= sku.eff_begin_tmstp
                AND dtl.business_day_date <  sku.eff_end_tmstp  

  LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist mrch_dept
        ON mrch_dept.dept_num = sku.dept_num
                AND dtl.business_day_date >= mrch_dept.eff_begin_tmstp
                AND dtl.business_day_date <  mrch_dept.eff_end_tmstp
                AND LOWER(mrch_dept.dept_subtype_code) = LOWER('MO')             
  LEFT OUTER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim  WACD
        ON LOWER(wacd.sku_num) = LOWER(upc.rms_sku_num)
            AND LOWER(WACD.LOCATION_NUM) = LOWER(CAST(DTL.INTENT_STORE_NUM AS STRING)) 
            AND RANGE_CONTAINS(RANGE(wacd.eff_begin_dt, wacd.eff_end_dt), dtl.dw_batch_date)
  LEFT OUTER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim  WACC 
        ON LOWER(wacc.sku_num) = LOWER(upc.rms_sku_num)
    AND WACC.CHANNEL_NUM =  s01.CHANNEL_NUM
    AND RANGE_CONTAINS(RANGE(wacc.eff_begin_dt, wacc.eff_end_dt), dtl.dw_batch_date)			
  LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw PSD
    ON DTL.INTENT_STORE_NUM = PSD.STORE_NUM
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_selling_event_flash_vw evtsku
     ON LOWER(evtsku.sku_num) = LOWER(upc.rms_sku_num)
      AND LOWER(evtsku.channel_brand) = LOWER(psd.channel_brand) 
      AND LOWER(evtsku.channel_country) = LOWER(psd.store_country_code)
	  AND LOWER(evtsku.selling_channel) = LOWER(psd.selling_channel)
      AND COALESCE( RTN.TRAN_TIME_UTC, CAST(DTL.ORIGINAL_BUSINESS_DATE AS TIMESTAMP),DTL.TRAN_TIME_UTC)  
      BETWEEN  timestamp(CAST(evtsku.item_start_tmstp_utc AS STRING),'America/Los_Angeles') AND timestamp(CAST(evtsku.item_end_tmstp_utc AS STRING),'America/Los_Angeles')
	  AND LOWER(evtsku.tag_name) LIKE LOWER('%FLASH%') 
      AND LOWER(evtsku.sku_type) = LOWER('RMS')
      WHERE RTN.BUSINESS_DAY_DATE BETWEEN (SELECT START_REBUILD_DATE FROM DT) - 365 AND (SELECT END_REBUILD_DATE FROM DT) 
QUALIFY ROW_NUMBER() OVER (PARTITION BY  DTL.GLOBAL_TRAN_ID, DTL.BUSINESS_DAY_DATE, DTL.LINE_ITEM_SEQ_NUM, DTL.TRAN_VERSION_NUM  ORDER BY NULL)	= 1
;
--ET;
--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.MERCH_SALE_RETURN_SKU_STORE_DLTA_FACT;
--ET;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_day_fact
WHERE week_num BETWEEN (SELECT start_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY')) AND (SELECT end_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SALE_DLY'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_day_fact (rms_sku_num, store_num, day_num, week_num, month_num,
 quarter_num, halfyear_num, year_num, fulfill_type_code, rp_ind, flash_event_ind, wac_avlbl_ind,
 merch_ownership_dept_ind, price_type, sales_retl_curr_code, sales_cost_curr_code, net_sales_retl, net_sales_cost,
 net_sales_units, returns_retl, returns_cost, returns_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT sub.rms_sku_num,
  sub.intent_store_num AS store_num,
  sub.day_num,
  sub.week_num,
  sub.month_num,
  sub.quarter_num,
  sub.halfyear_num,
  sub.year_num,
  sub.fulfill_type_code,
  CAST(NULL AS STRING) AS rp_ind,
  sub.flash_event_ind,
  MAX(sub.wac_avlbl_ind),
  MAX(sub.merch_ownership_dept_ind) AS merch_ownership_dept_ind,
   CASE
   WHEN LOWER(COALESCE(pd.ownership_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE')
   THEN 'C'
   WHEN sub.line_item_promo_id IS NOT NULL AND LOWER(COALESCE(pd.ownership_retail_price_type_code, 'Z')) <> LOWER('CLEARANCE'
      )
   THEN 'P'
   ELSE CASE
    WHEN LOWER(COALESCE(pd.selling_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(COALESCE(pd.selling_retail_price_type_code, 'REGULAR')) = LOWER('PROMOTION')
    THEN 'P'
    ELSE 'R'
    END
   END AS price_type_drvd,
  sub.retl_currency_code AS sales_retl_curr_code,
  COALESCE(sub.weighted_average_cost_currency_code, sub.retl_currency_code) AS sales_cost_curr_code,
  CAST(SUM(COALESCE(CASE
      WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
      THEN - 1 * sub.line_net_amt
      ELSE sub.line_net_amt
      END, 0)) AS NUMERIC) AS net_sales_retail,
  CAST(SUM(COALESCE(CASE
       WHEN sub.line_net_amt > 0
       THEN CASE
        WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
        THEN - 1 * sub.weighted_average_cost
        ELSE sub.weighted_average_cost
        END
       ELSE CASE
        WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
        THEN sub.weighted_average_cost
        WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('VOID')) AND sub.original_line_item_amt_currency_code IS NULL
        THEN - 1 * sub.weighted_average_cost
        ELSE NULL
        END
       END, 0) - COALESCE(CASE
       WHEN sub.line_net_amt < 0
       THEN CASE
        WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
        THEN - 1 * sub.weighted_average_cost
        ELSE sub.weighted_average_cost
        END
       ELSE CASE
        WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('RETN'))
        THEN sub.weighted_average_cost
        WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('VOID')) AND sub.original_line_item_amt_currency_code IS NOT NULL
        THEN - 1 * sub.weighted_average_cost
        ELSE NULL
        END
       END, 0)) AS NUMERIC) AS net_sales_cost,
  SUM(COALESCE(CASE
      WHEN sub.line_net_amt > 0
      THEN CASE
       WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
       THEN - 1 * sub.line_item_quantity
       ELSE sub.line_item_quantity
       END
      ELSE CASE
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
       THEN sub.line_item_quantity
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('VOID')) AND sub.original_line_item_amt_currency_code
        IS NULL
       THEN - 1 * sub.line_item_quantity
       ELSE NULL
       END
      END, 0) - COALESCE(CASE
      WHEN sub.line_net_amt < 0
      THEN CASE
       WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
       THEN - 1 * sub.line_item_quantity
       ELSE sub.line_item_quantity
       END
      ELSE CASE
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('RETN'))
       THEN sub.line_item_quantity
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('VOID')) AND sub.original_line_item_amt_currency_code
        IS NOT NULL
       THEN - 1 * sub.line_item_quantity
       ELSE NULL
       END
      END, 0)) AS net_sales_units,
  CAST(ABS(SUM(COALESCE(CASE
       WHEN sub.line_net_amt < 0 AND (LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y'))
       THEN - 1 * sub.line_net_amt
       WHEN sub.line_net_amt < 0
       THEN sub.line_net_amt
       ELSE NULL
       END, 0))) AS NUMERIC) AS returns_retail,
  CAST(SUM(COALESCE(CASE
      WHEN sub.line_net_amt < 0
      THEN CASE
       WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
       THEN - 1 * sub.weighted_average_cost
       ELSE sub.weighted_average_cost
       END
      ELSE CASE
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('RETN'))
       THEN sub.weighted_average_cost
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('VOID')) AND sub.original_line_item_amt_currency_code IS NOT NULL
       THEN - 1 * sub.weighted_average_cost
       ELSE NULL
       END
      END, 0)) AS NUMERIC) AS returns_cost,
  ABS(SUM(COALESCE(CASE
      WHEN sub.line_net_amt < 0
      THEN CASE
       WHEN LOWER(sub.tran_type_code) IN (LOWER('VOID')) OR LOWER(sub.reversal_flag) = LOWER('Y')
       THEN - 1 * sub.line_item_quantity
       ELSE sub.line_item_quantity
       END
      ELSE CASE
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('RETN'))
       THEN sub.line_item_quantity
       WHEN sub.line_net_amt = 0 AND LOWER(sub.tran_type_code) IN (LOWER('VOID')) AND sub.original_line_item_amt_currency_code
        IS NOT NULL
       THEN - 1 * sub.line_item_quantity
       ELSE NULL
       END
      END, 0))) AS returns_quantity,
  MAX(sub.dw_batch_date) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME(('PST8PDT'))) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_dlta_fact AS sub
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON sub.intent_store_num = psd.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pd 
  ON LOWER(psd.store_country_code) = LOWER(pd.channel_country) 
  AND LOWER(psd.selling_channel) = LOWER(pd.selling_channel) 
  AND LOWER(psd.channel_brand) = LOWER(pd.channel_brand) 
  AND LOWER(sub.rms_sku_num) = LOWER(pd.rms_sku_num)
  AND RANGE_CONTAINS(RANGE(PD.eff_begin_tmstp_utc ,PD.eff_end_tmstp_utc),
  TIMESTAMP(CAST(CONCAT(DATE_SUB(DATE_ADD(SUB.PRICE_TYPE_TIMESTAMP , interval '1' day) , interval CAST(TRUNC(CAST(0.001 AS FLOAT64)) AS INT64) second) ,'GMT') AS STRING))
  ) 
 GROUP BY sub.day_num,
  sub.week_num,
  sub.month_num,
  sub.quarter_num,
  sub.halfyear_num,
  sub.year_num,
  sub.rms_sku_num,
  sub.fulfill_type_code,
  store_num,
  price_type_drvd,
  sales_retl_curr_code,
  sub.weighted_average_cost_currency_code,
  sub.flash_event_ind);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sale_return_sku_store_day_fact  mtdf 
SET RP_IND = 'Y'
FROM
`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_rp_sku_loc_dim_hist  rp  
INNER JOIN 
`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
ON RANGE_CONTAINS(rp.RP_PERIOD,CAL.DAY_DATE)
WHERE LOWER(COALESCE(mtdf.RP_IND,'N')) = LOWER('N')
and mtdf.day_num =  CAL.day_idnt
and LOWER(rp.RMS_SKU_NUM) = LOWER(mtdf.RMS_SKU_NUM) and mtdf.STORE_NUM = rp.LOCATION_NUM
and CAL.DAY_DATE between
(SELECT DW_BATCH_DT - 100 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup WHERE
LOWER(INTERFACE_CODE) = LOWER('MERCH_NAP_SALE_DLY'))
and (SELECT DW_BATCH_DT FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup WHERE
LOWER(INTERFACE_CODE) = LOWER('MERCH_NAP_SALE_DLY'));

--ET;
/*SET QUERY_BAND = NONE FOR SESSION;*/
--ET;

