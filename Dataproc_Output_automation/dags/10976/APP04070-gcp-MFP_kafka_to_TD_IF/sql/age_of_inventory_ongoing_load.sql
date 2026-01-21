-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=aoi_ongoing_load_10976_tech_nap_merch;
-- Task_Name=aoi_load_job_2;'
-- FOR SESSION VOLATILE;

-- ET;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inventory_last_receipt_sku_store_fact AS inv
WHERE
    month_num = (
        SELECT
            month_idnt
        FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd
        WHERE
            day_date = DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY)
    );

---Modified to have new WAC Fallback logic
INSERT INTO
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inventory_last_receipt_sku_store_fact 
    (
        RMS_SKU_NUM,
        STORE_NUM,
        INVENTORY_SNAPSHOT_DATE,
        MONTH_NUM,
        RP_IND,
        WAC_AVLBL_IND,
        INVENTORY_IN_LAST_DATE,
        INVENTORY_AGE_DAYS,
        OWNERSHIP_PRICE_TYPE,
        CURRENT_PRICE_TYPE,
        WEIGHTED_AVERAGE_COST,
        OWNERSHIP_RETAIL_AMT,
        CURRENT_RETAIL_AMT,
        END_OF_PERIOD_UNITS,
        DW_BATCH_DATE,
        DW_SYS_LOAD_TMSTP
    )
SELECT
    inv.rms_sku_id AS RMS_SKU_NUM,
    CAST(TRUNC(CAST(inv.location_id AS FLOAT64)) AS INT64) AS STORE_NUM,
    current_date('PST8PDT') -1 AS INVENTORY_SNAPSHOT_DATE,
    dcd.month_idnt AS MONTH_NUM,
    CASE
        WHEN LOWER(psd.FP_REPLENISHMENT_ELIGIBLE_IND) = LOWER('Y')
        OR LOWER(psd.OP_REPLENISHMENT_ELIGIBLE_IND) = LOWER('Y')
        OR LOWER(psd.FP_ITEM_PLANNING_ELIGIBLE_IND) = LOWER('Y')
        OR LOWER(psd.OP_ITEM_PLANNING_ELIGIBLE_IND) = LOWER('Y') THEN 'Y'
        ELSE 'N'
    END AS RP_IND,
    'N' AS WAC_AVLBL_IND,
    rds.INVENTORY_IN_LAST_DATE AS INVENTORY_IN_LAST_DATE,
    CASE
        WHEN rds.INVENTORY_IN_LAST_DATE IS NULL THEN -1
        ELSE DATE_DIFF((current_date('PST8PDT') -1) , rds.INVENTORY_IN_LAST_DATE, DAY)
    END AS INVENTORY_AGE_DAYS,
    case
        when LOWER(COALESCE(ppd.ownership_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE') then 'C'
        when LOWER(COALESCE(ppd.ownership_retail_price_type_code, 'REGULAR')) = LOWER('PROMOTION') then 'P'
        else 'R'
    end AS OWNERSHIP_PRICE_TYPE,
    case
        when LOWER(COALESCE(ppd.selling_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE') then 'C'
        when LOWER(COALESCE(ppd.selling_retail_price_type_code, 'REGULAR')) = LOWER('PROMOTION') then 'P'
        else 'R'
    end AS CURRENT_PRICE_TYPE,
    0 AS WEIGHTED_AVERAGE_COST,
    COALESCE(ppd.ownership_retail_price_amt, 0) AS OWNERSHIP_RETAIL_AMT,
    COALESCE(ppd.selling_retail_price_amt, 0) AS CURRENT_RETAIL_AMT,
    CASE
        WHEN lcc.CHANNEL_NUM IS NULL THEN COALESCE(inv.stock_on_hand_qty, 0)
        ELSE 0
    END + COALESCE(inv.in_transit_qty, 0) AS END_OF_PERIOD_UNITS,
    current_date('PST8PDT') - 1 AS DW_BATCH_DATE,
    current_datetime('PST8PDT') AS DW_SYS_LOAD_TMSTP
FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_logical_fact inv
    LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_receipt_date_sku_store_fact rds 
    ON LOWER(inv.rms_sku_id) = LOWER(rds.RMS_SKU_NUM)
    and CAST(TRUNC(CAST(inv.location_id AS FLOAT64)) AS INT64) = rds.STORE_NUM
    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim s01 
    ON CAST(TRUNC(CAST(inv.location_id AS FLOAT64)) AS INT64) = s01.store_num
    LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim psd 
    ON LOWER(psd.rms_sku_num) = LOWER(inv.rms_sku_id)
    AND LOWER(psd.CHANNEL_COUNTRY) = LOWER(s01.STORE_COUNTRY_CODE)
    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcd 
    ON dcd.day_date = current_date('PST8PDT') -1
    LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw psdv 
    ON CAST(TRUNC(CAST(inv.location_id AS FLOAT64)) AS INT64) = psdv.store_num -- location to be changed
    LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS ppd 
    ON LOWER(psdv.store_country_code) = LOWER(ppd.channel_country)
    AND LOWER(psdv.selling_channel) = LOWER(ppd.selling_channel)
    AND LOWER(psdv.channel_brand) = LOWER(ppd.channel_brand)
    AND LOWER(inv.rms_sku_id) = LOWER(ppd.rms_sku_num)
    AND  RANGE_CONTAINS(RANGE(ppd.eff_begin_tmstp_utc, ppd.eff_end_tmstp_utc), (
        cast(DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY) as TIMESTAMP) + INTERVAL 1 DAY - INTERVAL 1 MILLISECOND))
    AND ppd.eff_end_tmstp >= DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY)
    LEFT OUTER JOIN (
        SELECT
            CAST(trunc(CAST(CONFIG_VALUE AS FLOAT64)) AS INTEGER) CHANNEL_NUM
        FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE
            INTERFACE_CODE = 'MERCH_NAP_AOI'
            AND CONFIG_KEY = 'LAST_CHANCE_CHANNEL'
    ) lcc ON s01.CHANNEL_NUM = lcc.CHANNEL_NUM
WHERE
    CASE
        WHEN lcc.CHANNEL_NUM IS NULL THEN COALESCE(inv.stock_on_hand_qty, 0)
        ELSE 0
    END + COALESCE(inv.in_transit_qty, 0) > 0;

-- update wac details in a separate statement to increase query performance
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inventory_last_receipt_sku_store_fact AS tgt USING (
    SELECT
        w01.rms_sku_num,
        w01.store_num,
        w01.inventory_snapshot_date,
        w01.month_num,
        CASE
            WHEN wacd.sku_num IS NULL
            AND wacc.sku_num IS NULL THEN 'N'
            ELSE 'Y'
        END AS wac_avlbl_ind,
        COALESCE(
            wacd.weighted_average_cost,
            wacc.weighted_average_cost,
            0
        ) AS weighted_average_cost
    FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inventory_last_receipt_sku_store_fact AS w01
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 ON w01.store_num = s01.store_num
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wacd ON LOWER(RTRIM (wacd.sku_num)) = LOWER(RTRIM (w01.rms_sku_num))
        AND CAST(RTRIM (wacd.location_num) AS FLOAT64) = w01.store_num
        AND DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY) >= wacd.eff_begin_dt
        AND DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY) < wacd.eff_end_dt
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wacc ON LOWER(RTRIM (wacc.sku_num)) = LOWER(RTRIM (w01.rms_sku_num))
        AND s01.channel_num = wacc.channel_num
        AND DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY) >= wacc.eff_begin_dt
        AND DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY) < wacc.eff_end_dt
    WHERE
        w01.dw_batch_date = DATE_SUB (current_date('PST8PDT'), INTERVAL 1 DAY)
) AS SRC ON LOWER(RTRIM (SRC.rms_sku_num)) = LOWER(RTRIM (tgt.rms_sku_num))
AND SRC.store_num = tgt.store_num
AND SRC.inventory_snapshot_date = tgt.inventory_snapshot_date
AND SRC.month_num = tgt.month_num WHEN MATCHED THEN
UPDATE
SET
    weighted_average_cost = SRC.weighted_average_cost,
    wac_avlbl_ind = SRC.wac_avlbl_ind;
    
-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;
