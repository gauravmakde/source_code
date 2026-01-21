DELETE FROM  DEV_nap_fct.ecf_salesreporting_batch_fact WHERE dw_sys_end_tmstp is null;
ET;

INSERT INTO DEV_nap_fct.ecf_salesreporting_batch_fact(business_day_start_date, business_day_end_date)
WITH start_ly as (
    SELECT day_date as start_date_ly
    FROM DEV_NAP_USR_VWS.DAY_CAL_SALES_DIM
    WHERE fiscal_day_num = 1 and fiscal_year_num = YEAR(CURRENT_DATE - 1) - 1
    ),
    updated_detail_records_start_date as (
    SELECT MIN(business_day_date) as dtl_start_date
    FROM DEV_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT, start_ly
    WHERE cast(dw_sys_updt_tmstp as date) >= CURRENT_DATE - 1
    AND business_day_date >= start_date_ly
    ),
    updated_hdr_records_start_date as (
    SELECT MIN(business_day_date) as hdr_start_date
    FROM DEV_NAP_USR_VWS.RETAIL_TRAN_HDR_FACT, start_ly
    WHERE cast(dw_sys_updt_tmstp as date) >= CURRENT_DATE - 1
    AND business_day_date >= start_date_ly
    ),
    max_batch_end_date as (
    SELECT MAX(business_day_end_date) + 1 as max_batch_end_date
    FROM DEV_NAP_FCT.ECF_SALESREPORTING_BATCH_FACT
    WHERE business_day_end_date < CURRENT_DATE - 1
    )
SELECT  CASE
            WHEN max_batch_end_date IS NULL THEN start_date_ly
            WHEN hdr_start_date < dtl_start_date AND hdr_start_date < max_batch_end_date THEN hdr_start_date
            WHEN dtl_start_date < hdr_start_date AND dtl_start_date < max_batch_end_date THEN dtl_start_date
            ELSE max_batch_end_date
            END as business_day_start_date,
        CURRENT_DATE - 1 as business_day_end_date
FROM start_ly, updated_detail_records_start_date, updated_hdr_records_start_date, max_batch_end_date
;

CREATE VOLATILE MULTISET TABLE  batch_date AS
( SELECT
       business_day_start_date,
       business_day_end_date
     FROM DEV_NAP_FCT.ECF_SALESREPORTING_BATCH_FACT
    WHERE dw_sys_end_tmstp is null
)
WITH DATA ON COMMIT PRESERVE ROWS;
ET;

-- CREATE VOLATILE MULTISET TABLE product_table AS
-- (
--     SELECT
--         LTRIM(upc.upc_num, '0') AS upc_num,
--         upc.prmy_upc_ind,
--         upc.rms_sku_num,
--         CAST(dpt.dept_num AS varchar(8)) dept_num,
--         CAST(dpt.division_num AS varchar(8)) division_num
--     FROM
--         (
--             SELECT
--                 upc_num,
--                 channel_country,
--                 prmy_upc_ind,
--                 rms_sku_num --, eff_begin_tmstp, eff_end_tmstp
--             FROM
--             DEV_NAP_USR_VWS.PRODUCT_UPC_DIM
--             WHERE
--                 channel_country = 'US' QUALIFY ROW_NUMBER() OVER (
--                     PARTITION BY trim(
--                         leading '0'
--                         from
--                             upc_num
--                     ),
--                     channel_country
--                     ORDER BY
--                         prmy_upc_ind DESC
--                 ) = 1
--         ) upc
--         JOIN (
--             SELECT
--                 rms_sku_num,
--                 dept_num
--             FROM
--                 DEV_NAP_USR_VWS.product_sku_dim_vw psdv
--             WHERE
--                 channel_country = 'US' QUALIFY ROW_NUMBER() OVER (
--                     PARTITION BY rms_sku_num
--                     ORDER BY
--                         channel_country DESC
--                 ) = 1
--         ) sku ON upc.rms_sku_num = sku.rms_sku_num
--         JOIN DEV_NAP_USR_VWS.DEPARTMENT_DIM dpt ON sku.dept_num = dpt.dept_num
-- )
-- WITH DATA
-- PRIMARY INDEX ( upc_num )
-- ON COMMIT PRESERVE ROWS;
--ET;

CREATE VOLATILE MULTISET TABLE tran_table AS
(
SELECT rthf.business_day_date,
    COALESCE(LPAD(rtdf.commission_slsprsn_num, 15, '0'), '0') AS commission_slsprsn_num,
    SUM(
            CASE
                WHEN COALESCE(rtdf.item_source, 'NOT') = 'SB_SALESEMPINIT' THEN (
                    CASE
                        WHEN (
                                 rtdf.line_item_activity_type_code = 'S'
                                     AND rthf.tran_type_code = 'VOID'
                                     AND rthf.reversal_flag = 'N'
                                 )
                            OR (
                                 rtdf.line_item_activity_type_code = 'S'
                                     AND rthf.tran_type_code <> 'VOID'
                                     AND rthf.reversal_flag = 'Y'
                                 ) THEN (
                            CASE
                                WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                    AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN -1 * rtdf.original_line_item_usd_amt
                                ELSE -1 * rtdf.line_net_usd_amt
                                END
                            )
                        WHEN rtdf.line_item_activity_type_code = 'R' THEN 0
                        ELSE CASE
                                 WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                     AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN rtdf.original_line_item_usd_amt
                                 ELSE rtdf.line_net_usd_amt
                            END
                        END
                    )
                END
    ) AS digital_gross_amt,
    SUM(
            CASE
                WHEN COALESCE(rtdf.item_source, 'NOT') = 'SB_SALESEMPINIT' THEN (
                    CASE
                        WHEN (
                                 rtdf.line_item_activity_type_code = 'R'
                                     AND rthf.tran_type_code = 'VOID'
                                     AND rthf.reversal_flag = 'N'
                                 )
                            OR (
                                 rtdf.line_item_activity_type_code = 'R'
                                     AND rthf.tran_type_code <> 'VOID'
                                     AND rthf.reversal_flag = 'Y'
                                 ) THEN (
       CASE
                 WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                    AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN rtdf.original_line_item_usd_amt
                                ELSE -1 * rtdf.line_net_usd_amt
                                END
                            ) --modified to remove -1 multiplication
                        WHEN rtdf.line_item_activity_type_code = 'S' THEN 0
                        ELSE CASE
                                 WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                     AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN -1 * rtdf.original_line_item_usd_amt
                                 ELSE rtdf.line_net_usd_amt
                            END
                        END
                    )
                END
    ) AS digital_return_amt,
    SUM(
            CASE
                WHEN coalesce(rtdf.item_source, 'NOT') <> 'SB_SALESEMPINIT' THEN (
             CASE
                 WHEN (
                                 rtdf.line_item_activity_type_code = 'S'
                                     AND rthf.tran_type_code = 'VOID'
                                     AND rthf.reversal_flag = 'N'
                                 )
                            OR (
                                 rtdf.line_item_activity_type_code = 'S'
                                     AND rthf.tran_type_code <> 'VOID'
                                     AND rthf.reversal_flag = 'Y'
                                 ) THEN (
                            CASE
                                WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                    AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN -1 * rtdf.original_line_item_usd_amt
                                ELSE -1 * rtdf.line_net_usd_amt
                                END
                            )
                        WHEN rtdf.line_item_activity_type_code = 'R' THEN 0
                        ELSE CASE
                                 WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                     AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN rtdf.original_line_item_usd_amt
                                 ELSE rtdf.line_net_usd_amt
                            END
                        END
                    )
                END
    ) AS store_gross_amt,
    SUM(
            CASE
                WHEN coalesce(rtdf.item_source, 'NOT') <> 'SB_SALESEMPINIT' THEN (
                    CASE
                        WHEN (
                                 rtdf.line_item_activity_type_code = 'R'
                                     AND rthf.tran_type_code = 'VOID'
                                     AND rthf.reversal_flag = 'N'
                                 )
                            OR (
                                 rtdf.line_item_activity_type_code = 'R'
                                     AND rthf.tran_type_code <> 'VOID'
                                     AND rthf.reversal_flag = 'Y'
                                 ) THEN (
                            CASE
                                WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                    AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN rtdf.original_line_item_usd_amt
                                ELSE -1 * rtdf.line_net_usd_amt
                                END
                            ) --modified to remove -1 multiplication
                        WHEN rtdf.line_item_activity_type_code = 'S' THEN 0
                        ELSE CASE
                                 WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                                     and rtdf.original_line_item_amt_currency_code IS NOT NULL THEN -1 * rtdf.original_line_item_usd_amt
                                 ELSE rtdf.line_net_usd_amt
                            END
                        END
                    )
                END
    ) AS store_return_amt,
    SUM(
            CASE
                WHEN (
                         rtdf.line_item_activity_type_code = 'S'
                             AND rthf.tran_type_code = 'VOID'
                             AND rthf.reversal_flag = 'N'
                         )
                    OR (
                         rtdf.line_item_activity_type_code = 'S'
                             AND rthf.tran_type_code <> 'VOID'
                             AND rthf.reversal_flag = 'Y'
                         ) THEN (
                    CASE
                        WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                            AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN -1 * rtdf.original_line_item_usd_amt
                        ELSE -1 * rtdf.line_net_usd_amt
                        END
                    )
                WHEN rtdf.line_item_activity_type_code = 'R' THEN 0
                ELSE CASE
                         WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                             AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN rtdf.original_line_item_usd_amt
                         ELSE rtdf.line_net_usd_amt
                    END
                END
    ) AS gross_amt,
    SUM(
            CASE
                WHEN (
                         rtdf.line_item_activity_type_code = 'R'
                             AND rthf.tran_type_code = 'VOID'
                             AND rthf.reversal_flag = 'N'
                          )
                     OR (
                         rtdf.line_item_activity_type_code = 'R'
                             AND rthf.tran_type_code <> 'VOID'
                             AND rthf.reversal_flag = 'Y'
                         ) THEN (
                    CASE
                        WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                            AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN rtdf.original_line_item_amt
                        ELSE -1 * rtdf.line_net_usd_amt
                        END
                    ) --modified to remove -1 multiplication
                WHEN rtdf.line_item_activity_type_code = 'S' THEN 0
                ELSE CASE
                         WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code
                             AND rtdf.original_line_item_amt_currency_code IS NOT NULL THEN -1 * rtdf.original_line_item_usd_amt
                         ELSE rtdf.line_net_usd_amt
                    END
                END
    ) AS return_amt
FROM
     DEV_NAP_USR_VWS.RETAIL_TRAN_HDR_FACT rthf
    JOIN  DEV_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT rtdf
      ON rthf.global_tran_id = rtdf.global_tran_id
    AND rthf.business_day_date = rtdf.business_day_date
    AND rthf.tran_type_code IN ('SALE', 'RETN', 'EXCH', 'VOID')
    AND rthf.data_source_code NOT IN ('TRUNK') -- Modified to include NRHL data
    AND rthf.sa_tran_status_code NOT IN ('D', 'O') -- get these codes confirmed
    AND (
       rtdf.line_item_order_type IN (
                                     'RPOS',
                                     'StoreInitDTCManual',
                                     'StoreInitStoreTake',
                                     'StoreInitDTCAuto',
                                     'StoreInitSameStrSend'
           )
                              OR rtdf.item_source = 'SB_SALESEMPINIT'
       )
    AND rtdf.line_item_merch_nonmerch_ind = 'MERCH'
    JOIN batch_date b
       ON rtdf.business_day_date BETWEEN b.business_day_start_date AND b.business_day_end_date
--     JOIN product_table p
--        ON LTRIM(rtdf.upc_num, '0') = p.upc_num
--        AND p.division_num NOT IN (900, 100) -- removed 800
GROUP BY
    1,
    2
)
WITH DATA
PRIMARY INDEX ( commission_slsprsn_num )
ON COMMIT PRESERVE ROWS;
ET;

DELETE FROM DEV_NAP_FCT.ECF_SALESREPORTING_FACT
WHERE business_day_date between (select business_day_start_date from batch_date) and (select business_day_end_date from batch_date)
;

INSERT INTO DEV_NAP_FCT.ECF_SALESREPORTING_FACT
        (
         commission_slsprsn_num,
         business_day_date,
         digital_gross_amt,
         digital_return_amt,
         digital_net_amt,
         store_gross_amt,
         store_return_amt,
         store_net_amt,
         gross_amt,
         return_amt,
         net_amt,
         dw_batch_id,
         dw_batch_date,
         dw_sys_load_tmstp,
         dw_sys_upd_tmstp
        )
    SELECT
        t.commission_slsprsn_num,
        t.business_day_date,
        NVL(t.digital_gross_amt, 0) as digital_gross_amt,
        NVL(t.digital_return_amt, 0) as digital_return_amt,
        NVL(t.digital_gross_amt, 0) + NVL(t.digital_return_amt, 0) as digital_net_amt,
        NVL(t.store_gross_amt, 0) as store_gross_amt,
        NVL(t.store_return_amt, 0) as store_return_amt,
        NVL(t.store_gross_amt, 0) + NVL(t.store_return_amt, 0) as store_net_amt,
        NVL(t.gross_amt, 0) as gross_amt,
        NVL(t.return_amt, 0) as return_amt,
        NVL(t.gross_amt, 0) + NVL(t.return_amt, 0) as net_amt,
        batch_id as dw_batch_id,
        batch_date as dw_batch_date,
        current_timestamp as dw_sys_load_tmstp,
        current_timestamp as dw_sys_upd_tmstp
    FROM tran_table t
    JOIN DEV_nap_fct.ecf_salesreporting_batch_fact b
        ON dw_sys_end_tmstp is null
;

UPDATE DEV_nap_fct.ecf_salesreporting_batch_fact
set  dw_sys_end_tmstp = CURRENT_TIMESTAMP
WHERE dw_sys_end_tmstp is null;