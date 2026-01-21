/* SET QUERY_BAND = '

App_ID=APP04070;

DAG_ID=aoi_ongoing_load_10976_tech_nap_merch;

Task_Name=aoi_load_job_1;'

FOR SESSION VOLATILE;*/

--ET;
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_sku_store_fact AS tgt USING (
    SELECT
        rms_sku_num AS sku_num,
        store_num,
        MIN(tran_date) AS inventory_in_first_date,
        MAX(tran_date) AS inventory_in_last_date,
        MIN(
            CASE
                WHEN reservestock_transfer_in_units > 0 THEN tran_date
                ELSE NULL
            END
        ) AS reserve_stock_transfer_in_first_date,
        MAX(
            CASE
                WHEN reservestock_transfer_in_units > 0 THEN tran_date
                ELSE NULL
            END
        ) AS reserve_stock_transfer_in_last_date,
        MIN(
            CASE
                WHEN packandhold_transfer_in_units > 0 THEN tran_date
                ELSE NULL
            END
        ) AS packandhold_transfer_in_first_date,
        MAX(
            CASE
                WHEN packandhold_transfer_in_units > 0 THEN tran_date
                ELSE NULL
            END
        ) AS packandhold_transfer_in_last_date,
        MIN(
            CASE
                WHEN warehouse_transfer_in_units > 0 THEN tran_date
                ELSE NULL
            END
        ) AS warehouse_transfer_in_first_date,
        MAX(
            CASE
                WHEN warehouse_transfer_in_units > 0 THEN tran_date
                ELSE NULL
            END
        ) AS warehouse_transfer_in_last_date
    FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_transfer_sku_store_day_columnar_vw AS mtsc
    WHERE
        tran_date >= DATE_SUB (
            CURRENT_DATE('PST8PDT'),
            INTERVAL CAST(
                TRUNC(CAST(
                    (SELECT
                        config_value
                    FROM
                        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
                    WHERE
                        LOWER(RTRIM (interface_code)) = LOWER(RTRIM ('MERCH_NAP_AOI_DT'))
                        AND LOWER(RTRIM (config_key)) = LOWER(RTRIM ('REBUILD_DAYS'))
            ) AS float64)) AS INTEGER
            ) DAY
        )
        AND tran_date < CURRENT_DATE('PST8PDT')
        AND (
            reservestock_transfer_in_units > 0
            OR packandhold_transfer_in_units > 0
            OR warehouse_transfer_in_units > 0
        )
    GROUP BY
        sku_num,
        store_num
) AS src ON LOWER(RTRIM (src.sku_num)) = LOWER(RTRIM (tgt.rms_sku_num))
AND src.store_num = tgt.store_num WHEN MATCHED THEN
UPDATE
SET
    inventory_in_first_date = CASE
        WHEN COALESCE(tgt.inventory_in_first_date, DATE '4444-04-04') > COALESCE(src.inventory_in_first_date, DATE '4444-04-04') THEN src.inventory_in_first_date
        ELSE tgt.inventory_in_first_date
    END,
    inventory_in_last_date = CASE
        WHEN COALESCE(tgt.inventory_in_last_date, DATE '1900-01-01') < COALESCE(src.inventory_in_last_date, DATE '1900-01-01') THEN src.inventory_in_last_date
        ELSE tgt.inventory_in_last_date
    END,
    reserve_stock_transfer_in_first_date = CASE
        WHEN COALESCE(
            tgt.reserve_stock_transfer_in_first_date,
            DATE '4444-04-04'
        ) > COALESCE(
            src.reserve_stock_transfer_in_first_date,
            DATE '4444-04-04'
        ) THEN src.reserve_stock_transfer_in_first_date
        ELSE tgt.reserve_stock_transfer_in_first_date
    END,
    reserve_stock_transfer_in_last_date = CASE
        WHEN COALESCE(
            tgt.reserve_stock_transfer_in_last_date,
            DATE '1900-01-01'
        ) < COALESCE(
            src.reserve_stock_transfer_in_last_date,
            DATE '1900-01-01'
        ) THEN src.reserve_stock_transfer_in_last_date
        ELSE tgt.reserve_stock_transfer_in_last_date
    END,
    packandhold_transfer_in_first_date = CASE
        WHEN COALESCE(
            tgt.packandhold_transfer_in_first_date,
            DATE '4444-04-04'
        ) > COALESCE(
            src.packandhold_transfer_in_first_date,
            DATE '4444-04-04'
        ) THEN src.packandhold_transfer_in_first_date
        ELSE tgt.packandhold_transfer_in_first_date
    END,
    packandhold_transfer_in_last_date = CASE
        WHEN COALESCE(
            tgt.packandhold_transfer_in_last_date,
            DATE '1900-01-01'
        ) < COALESCE(
            src.packandhold_transfer_in_last_date,
            DATE '1900-01-01'
        ) THEN src.packandhold_transfer_in_last_date
        ELSE tgt.packandhold_transfer_in_last_date
    END,
    warehouse_transfer_in_first_date = CASE
        WHEN COALESCE(
            tgt.warehouse_transfer_in_first_date,
            DATE '4444-04-04'
        ) > COALESCE(
            src.warehouse_transfer_in_first_date,
            DATE '4444-04-04'
        ) THEN src.warehouse_transfer_in_first_date
        ELSE tgt.warehouse_transfer_in_first_date
    END,
    warehouse_transfer_in_last_date = CASE
        WHEN COALESCE(
            tgt.warehouse_transfer_in_last_date,
            DATE '1900-01-01'
        ) < COALESCE(
            src.warehouse_transfer_in_last_date,
            DATE '1900-01-01'
        ) THEN src.warehouse_transfer_in_last_date
        ELSE tgt.warehouse_transfer_in_last_date
    END,
    dw_sys_updt_tmstp = CAST(
        FORMAT_TIMESTAMP ('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME
    ),
    dw_batch_date = CURRENT_DATE('PST8PDT') WHEN NOT MATCHED THEN INSERT (
        rms_sku_num,
        store_num,
        inventory_in_first_date,
        inventory_in_last_date,
        reserve_stock_transfer_in_first_date,
        reserve_stock_transfer_in_last_date,
        packandhold_transfer_in_first_date,
        packandhold_transfer_in_last_date,
        warehouse_transfer_in_first_date,
        warehouse_transfer_in_last_date,
        dw_sys_load_tmstp,
        dw_batch_date
    )
VALUES
    (
        src.sku_num,
        src.store_num,
        src.inventory_in_first_date,
        src.inventory_in_last_date,
        src.reserve_stock_transfer_in_first_date,
        src.reserve_stock_transfer_in_last_date,
        src.packandhold_transfer_in_first_date,
        src.packandhold_transfer_in_last_date,
        src.warehouse_transfer_in_first_date,
        src.warehouse_transfer_in_last_date,
        CAST(FORMAT_TIMESTAMP ('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
        CURRENT_DATE('PST8PDT')
    );

/* SET QUERY_BAND = NONE FOR SESSION;*/