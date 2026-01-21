-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=merch_on_order_fact_snapshot_load_10976_tech_nap_merch;
-- Task_Name=merch_on_order_fact_snapshot_load;'
-- FOR SESSION VOLATILE;

-- ET;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_fact_snap_wrk ;



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_fact_snap_wrk 
    (  
		WEEK_IDNT,
		DAY_DATE,        
        DW_SYS_LOAD_DT,
        DW_SYS_LOAD_TMSTP ,
        DW_SYS_LOAD_TMSTP_TZ
    )
    SELECT 
	 WEEK_IDNT,
	 DAY_DATE ,
	 CURRENT_DATE('PST8PDT') AS DW_SYS_LOAD_DT,
    timestamp(current_datetime('PST8PDT')) AS DW_SYS_LOAD_TMSTP ,
     `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS DW_SYS_LOAD_TMSTP_TZ
	 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim 
WHERE DAY_DATE=
(
SELECT DAY_DATE - DAY_NUM_OF_FISCAL_WEEK AS BATCH_DATE
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
    WHERE DAY_DATE = (    
    SELECT MAX(DW_BATCH_DATE) FROM
prd_nap_fct.merch_on_order_fact)
);

-- ET;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_snapshot_fact AS TGT
WHERE SNAPSHOT_WEEK_NUM = 
(SELECT WEEK_IDNT FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_fact_snap_wrk);



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_snapshot_fact 
    (  
		SNAPSHOT_WEEK_NUM,
        SNAPSHOT_WEEK_DATE,
        PURCHASE_ORDER_NUMBER,
        EXTERNAL_DISTRIBUTION_ID,
        RMS_SKU_NUM,
        EPM_SKU_NUM,
        STORE_NUM,
        WEEK_NUM,
        SHIP_LOCATION_ID,
        RMS_CASEPACK_NUM,
        EPM_CASEPACK_NUM,
        ORDER_FROM_VENDOR_ID,
        ANTICIPATED_PRICE_TYPE,
        ORDER_CATEGORY_CODE,
        PO_TYPE,
        ORDER_TYPE,
        PURCHASE_TYPE,
        STATUS,
        CANCEL_REASON,
        START_SHIP_DATE,
        END_SHIP_DATE,
        OTB_EOW_DATE,
        FIRST_APPROVAL_DATE,
        LATEST_APPROVAL_DATE,
        FIRST_APPROVAL_EVENT_TMSTP_PACIFIC,
        FIRST_APPROVAL_EVENT_TMSTP_PACIFIC_TZ,
        LATEST_APPROVAL_EVENT_TMSTP_PACIFIC,
        LATEST_APPROVAL_EVENT_TMSTP_PACIFIC_TZ,
        ANTICIPATED_RETAIL_AMT,
        TOTAL_EXPENSES_PER_UNIT_CURRENCY,
        CROSS_REF_ID,
        WRITTEN_DATE,
        QUANTITY_ORDERED,
        QUANTITY_CANCELED,
        QUANTITY_RECEIVED,
        QUANTITY_OPEN,
        UNIT_COST_AMT,
        TOTAL_EXPENSES_PER_UNIT_AMT,
        TOTAL_DUTY_PER_UNIT_AMT,
        UNIT_ESTIMATED_LANDING_COST,
        TOTAL_ESTIMATED_LANDING_COST,
        TOTAL_ANTICIPATED_RETAIL_AMT,
        DW_BATCH_DATE,
        DW_SYS_LOAD_TMSTP,
        DW_SYS_LOAD_TMSTP_TZ
    )
    SELECT 
	(SELECT WEEK_IDNT FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_fact_snap_wrk) AS SNAPSHOT_WEEK_NUM,
	(SELECT DAY_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_fact_snap_wrk) AS SNAPSHOT_WEEK_DATE,
    oo.PURCHASE_ORDER_NUMBER AS PURCHASE_ORDER_NUMBER,
    oo.EXTERNAL_DISTRIBUTION_ID AS EXTERNAL_DISTRIBUTION_ID,
    oo.RMS_SKU_NUM AS RMS_SKU_NUM,
    oo.EPM_SKU_NUM AS EPM_SKU_NUM,
    oo.STORE_NUM AS STORE_NUM,
    oo.WEEK_NUM AS WEEK_NUM,
    oo.SHIP_LOCATION_ID AS SHIP_LOCATION_ID,
    oo.RMS_CASEPACK_NUM AS RMS_CASEPACK_NUM,
    oo.EPM_CASEPACK_NUM AS EPM_CASEPACK_NUM,
    oo.ORDER_FROM_VENDOR_ID AS ORDER_FROM_VENDOR_ID,
    oo.ANTICIPATED_PRICE_TYPE AS ANTICIPATED_PRICE_TYPE,
    oo.ORDER_CATEGORY_CODE AS ORDER_CATEGORY_CODE,
    oo.PO_TYPE AS PO_TYPE,
    oo.ORDER_TYPE AS ORDER_TYPE,
    oo.PURCHASE_TYPE AS PURCHASE_TYPE,
    oo.STATUS AS STATUS,
    oo.CANCEL_REASON AS CANCEL_REASON,
    oo.START_SHIP_DATE AS START_SHIP_DATE,
    oo.END_SHIP_DATE AS END_SHIP_DATE,
    oo.OTB_EOW_DATE AS OTB_EOW_DATE,
    oo.FIRST_APPROVAL_DATE AS FIRST_APPROVAL_DATE,
    oo.LATEST_APPROVAL_DATE AS LATEST_APPROVAL_DATE,
    oo.FIRST_APPROVAL_EVENT_TMSTP_PACIFIC_UTC AS FIRST_APPROVAL_EVENT_TMSTP_PACIFIC,
    oo.FIRST_APPROVAL_EVENT_TMSTP_PACIFIC_TZ,
    oo.LATEST_APPROVAL_EVENT_TMSTP_PACIFIC_UTC AS LATEST_APPROVAL_EVENT_TMSTP_PACIFIC,
    oo.LATEST_APPROVAL_EVENT_TMSTP_PACIFIC_tz,
    oo.ANTICIPATED_RETAIL_AMT AS ANTICIPATED_RETAIL_AMT,
    oo.TOTAL_EXPENSES_PER_UNIT_CURRENCY AS TOTAL_EXPENSES_PER_UNIT_CURRENCY,
    oo.CROSS_REF_ID AS CROSS_REF_ID,
    oo.WRITTEN_DATE AS WRITTEN_DATE,
    oo.QUANTITY_ORDERED AS QUANTITY_ORDERED,
    oo.QUANTITY_CANCELED AS QUANTITY_CANCELED,
    oo.QUANTITY_RECEIVED AS QUANTITY_RECEIVED,
    oo.QUANTITY_OPEN AS QUANTITY_OPEN,
    oo.UNIT_COST_AMT AS UNIT_COST_AMT,
    oo.TOTAL_EXPENSES_PER_UNIT_AMT AS TOTAL_EXPENSES_PER_UNIT_AMT,
    oo.TOTAL_DUTY_PER_UNIT_AMT AS TOTAL_DUTY_PER_UNIT_AMT,
    oo.UNIT_ESTIMATED_LANDING_COST AS UNIT_ESTIMATED_LANDING_COST,
    oo.TOTAL_ESTIMATED_LANDING_COST AS TOTAL_ESTIMATED_LANDING_COST,
    oo.TOTAL_ANTICIPATED_RETAIL_AMT AS TOTAL_ANTICIPATED_RETAIL_AMT,
    CURRENT_DATE('PST8PDT') AS DW_BATCH_DATE,
    timestamp(current_datetime('PST8PDT'))
 AS DW_SYS_LOAD_TMSTP,
    `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() as DW_SYS_LOAD_TMSTP_TZ
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_on_order_fact_vw oo
    LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim orgstore
    ON oo.STORE_NUM = orgstore.store_num 
    WHERE oo.QUANTITY_OPEN > 0
    AND oo.FIRST_APPROVAL_DATE IS NOT NULL
    AND orgstore.CHANNEL_NUM IN 
    (SELECT CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INT64) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup chnl 
    WHERE LOWER(chnl.interface_code) = LOWER('MFP_COST_MRCH_MARGIN') AND LOWER(chnl.config_key) = LOWER('CHANNEL_NUM'))
    AND ((oo.STATUS = 'CLOSED' AND oo.END_SHIP_DATE >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY)) OR LOWER(oo.STATUS) IN (LOWER('APPROVED'),LOWER('WORKSHEET')) );

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;
-- 
