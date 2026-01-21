TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.po_inbound_tableau_extract_fact;

-- NULL,
-- NULL,
INSERT INTO
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.po_inbound_tableau_extract_fact (
        purchase_order_num,
        rms_sku_num,
        store_num,
        store_desc,
        channel_num,
        channel_desc,
        banner_country,
        location_type_code,
        wk_idnt,
        wk_desc,
        mth_idnt,
        mth_desc,
        wk_idnt_nb,
        wk_desc_nb,
        mth_idnt_nb,
        mth_desc_nb,
        wk_idnt_na,
        wk_desc_na,
        mth_idnt_na,
        mth_desc_na,
        wk_idnt_fr,
        wk_desc_fr,
        mth_idnt_fr,
        mth_desc_fr,
        wk_idnt_lr,
        wk_desc_lr,
        mth_idnt_lr,
        mth_desc_lr,
        div_num,
        div_desc,
        grp_num,
        grp_desc,
        dept_num,
        dept_desc,
        class_num,
        class_desc,
        rms_style_num,
        vpn,
        vpn_desc,
        nrf_color_code,
        supplier_color,
        size_desc,
        supplier_num,
        supplier_name,
        manufacturer_num,
        manufacturer_name,
        product_source_code,
        product_source_desc,
        selling_channel_eligibility,
        shipping_method,
        dir_to_store_ind,
        edi_ind,
        dropship_ind,
        import_flag,
        rp_flag,
        npg_flag,
        sample_ind,
        plan_season_id,
        commit_ind,
        clear_ind,
        po_type,
        order_type,
        purchase_type,
        order_status,
        not_before_dt,
        not_after_dt,
        orig_not_before_dt,
        orig_not_after_dt,
        otb_eow_dt,
        orig_approval_dt,
        first_received_date,
        last_received_date,
        written_date,
        cost_currency_cd,
        expected_ship_date,
        last_ship_date,
        ship_signal,
        partial_vasn_signal,
        received_signal,
        f_days_vasn_written,
        f_days_vasn_firstapprov,
        f_days_vasn_notbefore,
        f_days_vasn_notafter,
        f_days_vasn_otbeow,
        f_days_received_written,
        f_days_received_firstapprov,
        f_days_received_notbefore,
        f_days_received_notafter,
        f_days_received_otbeow,
        f_vasn_sku_qty,
        f_vasn_sku_retail_amt,
        f_vasn_sku_cost_amt,
        f_vasn_validation_qty,
        f_vasn_validation_retail_amt,
        f_vasn_validation_cost_amt,
        f_open_oo_qty,
        f_open_oo_retail_amt,
        f_open_oo_po_retail_amt,
        f_open_oo_cost_amt,
        f_ordered_qty,
        f_ordered_retail_amt,
        f_ordered_cost_amt,
        f_received_qty,
        f_received_retail_amt,
        f_received_cost_amt,
        f_arrived_qty,
        f_arrived_retail_amt,
        f_arrived_cost_amt,
        f_canceled_qty,
        f_canceled_retail_amt,
        f_canceled_cost_amt,
        retail_price_source,
        f_current_retail,
        f_anticipated_retail,
        f_po_unit_cost,
        f_po_unit_retail,
        rcd_load_dt
    ) (
        SELECT
            stg.purchase_order_num,
            stg.rms_sku_num,
            stg.store_num,
            stg.store_desc,
            stg.channel_num,
            stg.channel_desc,
            stg.banner_country,
            stg.location_type_code,
            stg.wk_idnt,
            stg.wk_desc,
            stg.mth_idnt,
            stg.mth_desc,
            stg.wk_idnt_nb,
            stg.wk_desc_nb,
            stg.mth_idnt_nb,
            stg.mth_desc_nb,
            stg.wk_idnt_na,
            stg.wk_desc_na,
            stg.mth_idnt_na,
            stg.mth_desc_na,
            stg.wk_idnt_fr,
            stg.wk_desc_fr,
            stg.mth_idnt_fr,
            stg.mth_desc_fr,
            stg.wk_idnt_lr,
            stg.wk_desc_lr,
            stg.mth_idnt_lr,
            stg.mth_desc_lr,
            stg.div_num,
            stg.div_desc,
            stg.grp_num,
            stg.grp_desc,
            stg.dept_num,
            stg.dept_desc,
            stg.class_num,
            stg.class_desc,
            stg.rms_style_num,
            stg.vpn,
            stg.vpn_desc,
            stg.nrf_color_code,
            stg.supplier_color,
            stg.size_desc,
            stg.supplier_num,
            stg.supplier_name,
            stg.manufacturer_num,
            stg.manufacturer_name,
            stg.product_source_code,
            stg.product_source_desc,
            stg.selling_channel_eligibility,
            stg.shipping_method,
            stg.dir_to_store_ind,
            stg.edi_ind,
            stg.dropship_ind,
            stg.import_flag,
            stg.rp_flag,
            stg.npg_flag,
            stg.sample_ind,
            CAST(NULL AS NUMERIC),
            CAST(NULL AS STRING),
            CASE
                WHEN LOWER(ppd.ownership_retail_price_type_code) = LOWER('CLEARANCE') THEN 'Y'
                WHEN LOWER(ppd.selling_retail_price_type_code) = LOWER('CLEARANCE') THEN 'Y'
                ELSE 'N'
            END AS clear_ind,
            stg.po_type,
            stg.order_type,
            stg.purchase_type,
            stg.order_status,
            stg.not_before_dt,
            stg.not_after_dt,
            stg.orig_not_before_dt,
            stg.orig_not_after_dt,
            stg.otb_eow_dt,
            stg.orig_approval_dt,
            stg.first_received_date,
            stg.last_received_date,
            stg.written_date,
            stg.cost_currency_cd,
            stg.expected_ship_date,
            stg.last_ship_date,
            CASE
                WHEN stg.f_vasn_sku_qty > 0 THEN 'Y'
                ELSE 'N'
            END AS ship_signal,
            CASE
                WHEN COALESCE(stg.f_ordered_qty - stg.f_received_qty, 0) > stg.f_vasn_sku_qty
                AND stg.f_vasn_sku_qty <> 0 THEN 'Y'
                ELSE 'N'
            END AS partial_vasn_signal,
            CASE
                WHEN stg.f_received_qty > 0 THEN 'Y'
                ELSE 'N'
            END AS received_signal,
            stg.f_days_vasn_written,
            stg.f_days_vasn_firstapprov,
            stg.f_days_vasn_notbefore,
            stg.f_days_vasn_notafter,
            stg.f_days_vasn_otbeow,
            stg.f_days_received_written,
            stg.f_days_received_firstapprov,
            stg.f_days_received_notbefore,
            stg.f_days_received_notafter,
            stg.f_days_received_otbeow,
            stg.f_vasn_sku_qty,
            ROUND(
                CAST(
                    COALESCE(
                        stg.f_vasn_sku_qty * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_vasn_sku_retail_amt,
            ROUND(
                CAST(
                    COALESCE(stg.f_vasn_sku_qty * stg.f_po_unit_cost, 0) AS NUMERIC
                ),
                4
            ) AS f_vasn_sku_cost_amt,
            COALESCE(
                stg.f_ordered_qty - stg.f_received_qty - stg.f_vasn_sku_qty,
                0
            ) AS f_vasn_validation_qty,
            ROUND(
                CAST(
                    COALESCE(
                        (
                            stg.f_ordered_qty - stg.f_received_qty - stg.f_vasn_sku_qty
                        ) * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_vasn_validation_retail_amt,
            ROUND(
                CAST(
                    COALESCE(
                        (
                            stg.f_ordered_qty - stg.f_received_qty - stg.f_vasn_sku_qty
                        ) * stg.f_po_unit_cost,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_vasn_validation_cost_amt,
            COALESCE(stg.f_ordered_qty - stg.f_received_qty, 0) AS f_open_oo_qty,
            ROUND(
                CAST(
                    COALESCE(
                        (stg.f_ordered_qty - stg.f_received_qty) * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_open_oo_retail_amt,
            ROUND(
                CAST(
                    COALESCE(
                        (stg.f_ordered_qty - stg.f_received_qty) * stg.f_po_unit_retail,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_open_oo_po_retail_amt,
            ROUND(
                CAST(
                    COALESCE(
                        (stg.f_ordered_qty - stg.f_received_qty) * stg.f_po_unit_cost,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_open_oo_cost_amt,
            stg.f_ordered_qty,
            ROUND(
                CAST(
                    COALESCE(
                        stg.f_ordered_qty * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_ordered_retail_amt,
            ROUND(
                CAST(
                    COALESCE(stg.f_ordered_qty * stg.f_po_unit_cost, 0) AS NUMERIC
                ),
                4
            ) AS f_ordered_cost_amt,
            stg.f_received_qty,
            ROUND(
                CAST(
                    COALESCE(
                        stg.f_received_qty * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_received_retail_amt,
            ROUND(
                CAST(
                    COALESCE(stg.f_received_qty * stg.f_po_unit_cost, 0) AS NUMERIC
                ),
                4
            ) AS f_received_cost_amt,
            stg.f_arrived_qty,
            ROUND(
                CAST(
                    COALESCE(
                        stg.f_arrived_qty * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_arrived_retail_amt,
            ROUND(
                CAST(
                    COALESCE(stg.f_arrived_qty * stg.f_po_unit_cost, 0) AS NUMERIC
                ),
                4
            ) AS f_arrived_cost_amt,
            stg.f_canceled_qty,
            ROUND(
                CAST(
                    COALESCE(
                        stg.f_canceled_qty * CASE
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                            WHEN ppd.ownership_retail_price_amt IS NULL
                            AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                            ELSE ppd.ownership_retail_price_amt
                        END,
                        0
                    ) AS NUMERIC
                ),
                4
            ) AS f_canceled_retail_amt,
            ROUND(
                CAST(
                    COALESCE(stg.f_canceled_qty * stg.f_po_unit_cost, 0) AS NUMERIC
                ),
                4
            ) AS f_canceled_cost_amt,
            CASE
                WHEN ppd.ownership_retail_price_amt IS NULL
                AND ppd.selling_retail_price_amt IS NULL THEN 'PO'
                WHEN ppd.ownership_retail_price_amt IS NULL
                AND ppd.selling_retail_price_amt IS NOT NULL THEN 'CUR'
                ELSE 'OWN'
            END AS retail_price_source,
            CASE
                WHEN ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                ELSE ppd.selling_retail_price_amt
            END AS f_current_retail,
            CASE
                WHEN ppd.ownership_retail_price_amt IS NULL
                AND ppd.selling_retail_price_amt IS NULL THEN stg.f_po_unit_retail
                WHEN ppd.ownership_retail_price_amt IS NULL
                AND ppd.selling_retail_price_amt IS NOT NULL THEN ppd.selling_retail_price_amt
                ELSE ppd.ownership_retail_price_amt
            END AS f_anticipated_retail,
            stg.f_po_unit_cost,
            stg.f_po_unit_retail,
            CURRENT_DATE('PST8PDT')
        FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.po_inbound_tableau_extract_stg AS stg
            LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON stg.dist_store_num = psdv.store_num
            LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS ppd ON CAST(ppd.store_num AS FLOAT64) = psdv.price_store_num
            AND LOWER(stg.rms_sku_num) = LOWER(ppd.rms_sku_num)
            AND stg.pricing_date >= CAST(ppd.eff_begin_tmstp AS DATE)
            AND stg.pricing_date < CAST(ppd.eff_end_tmstp AS DATE) QUALIFY (
                ROW_NUMBER() OVER (
                    PARTITION BY
                        stg.purchase_order_num,
                        stg.rms_sku_num,
                        CAST(stg.store_num AS STRING)
                    ORDER BY
                        ppd.dw_batch_id DESC
                )
            ) = 1
    );