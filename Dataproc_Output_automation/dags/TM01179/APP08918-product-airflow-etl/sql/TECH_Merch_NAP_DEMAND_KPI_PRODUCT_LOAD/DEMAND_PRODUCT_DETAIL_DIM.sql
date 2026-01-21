--BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND='AppName=NAP-Product-Demand-KPI;AppRelease=1;AppFreq=Daily;AppPhase=dim-lkup;AppSubArea=DEMAND_PRODUCT_DETAIL_DIM_Load;' UPDATE FOR SESSION;*/
BEGIN TRANSACTION;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 1') AS `A12180`;
-- END IF;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('DEMAND_PRODUCT_DETAIL_DIM',  'PRD_NAP_DIM',  'TECH_Merch_NAP_DEMAND_KPI_PRODUCT_LOAD',  'demand_product_detail_dim_load',  1,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'NAP_PRODUCT_DEMAND_DETAIL');

COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 2') AS `A12180`;
-- END IF;

BEGIN TRANSACTION;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 3') AS `A12180`;
-- END IF;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.demand_product_detail_dim;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 4') AS `A12180`;
-- END IF;

--END;





INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.demand_product_detail_dim (
    rms_sku_num,
    channel_country,
    sbclass_num,
    sbclass_name,
    class_num,
    class_name,
    dept_num,
    dept_name,
    merch_dept_ind,
    subdivision_num,
    subdivision_name,
    division_num,
    division_name,
    rms_style_num,
    style_desc,
    style_group_num,
    style_group_desc,
    vpn_num,
    supplier_num,
    supplier_name,
    payto_vendor_num,
    payto_vendor_name,
    vendor_label_code,
    vendor_label_name,
    vendor_label_status,
    vendor_brand_code,
    vendor_brand_name,
    sku_type_code,
    smart_sample_ind,
    gwp_ind,
    country_count,
    npg_ind,
    eff_begin_tmstp,
    eff_end_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz
)

WITH SRC AS (
    SELECT 
        epm_style_num,
        channel_country,
        style_desc,
        style_group_num,
        style_group_desc,
        vendor_label_name,
        sbclass_num,
        sbclass_name,
        class_num,
        class_name,
        dept_num,
        dept_name,
        merch_dept_ind,
        subdivision_num,
        subdivision_name,
        division_num,
        division_name,
        MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp, 
        MAX(eff_end_tmstp_utc) AS eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            *,
            SUM(discontinuity_flag) OVER (
                PARTITION BY epm_style_num,
                channel_country,
                style_desc,
                style_group_num,
                style_group_desc,
                vendor_label_name,
                sbclass_num,
                sbclass_name,
                class_num,
                class_name,
                dept_num,
                dept_name,
                merch_dept_ind,
                subdivision_num,
                subdivision_name,
                division_num,
                division_name 
                ORDER BY eff_begin_tmstp_utc 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS range_group
        FROM (
            SELECT 
                *,
                CASE 
                    WHEN LAG(eff_end_tmstp_utc) OVER (
                        PARTITION BY epm_style_num,
                        channel_country,
                        style_desc,
                        style_group_num,
                        style_group_desc,
                        vendor_label_name,
                        sbclass_num,
                        sbclass_name,
                        class_num,
                        class_name,
                        dept_num,
                        dept_name,
                        merch_dept_ind,
                        subdivision_num,
                        subdivision_name,
                        division_num,
                        division_name 
                        ORDER BY eff_begin_tmstp_utc
                    ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                    THEN 0
                    ELSE 1
                END AS discontinuity_flag
            FROM (
                SELECT DISTINCT 
                    product_style_dim_hist.epm_style_num,
                    product_style_dim_hist.channel_country,
                    product_style_dim_hist.style_desc,
                    product_style_dim_hist.style_group_num,
                    product_style_dim_hist.style_group_desc,
                    product_style_dim_hist.vendor_label_name,
                    product_style_dim_hist.sbclass_num,
                    COALESCE(hier.sbclass_desc, product_style_dim_hist.sbclass_desc) AS sbclass_name,
                    product_style_dim_hist.class_num,
                    COALESCE(hier.class_desc, product_style_dim_hist.class_desc) AS class_name,
                    product_style_dim_hist.dept_num,
                    COALESCE(department_dim_hist.dept_name, hier.dept_desc, product_style_dim_hist.dept_desc) AS dept_name,
                    COALESCE(department_dim_hist.merch_dept_ind, 'N') AS merch_dept_ind,
                    department_dim_hist.subdivision_num,
                    department_dim_hist.subdivision_name,
                    department_dim_hist.division_num,
                    department_dim_hist.division_name,
                    COALESCE(
                        RANGE_INTERSECT(
                            hier.eff_period, 
                            RANGE(department_dim_hist.eff_begin_tmstp_utc, department_dim_hist.eff_end_tmstp_utc)
                        ), 
                        RANGE(department_dim_hist.eff_begin_tmstp_utc, department_dim_hist.eff_end_tmstp_utc)
                    ) AS eff_period,
                    department_dim_hist.eff_begin_tmstp_utc,
                    department_dim_hist.eff_end_tmstp_utc
                FROM 
                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS product_style_dim_hist
                LEFT JOIN 
                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist 
                ON 
                    product_style_dim_hist.dept_num = department_dim_hist.dept_num
                LEFT JOIN (
                    SELECT 
                        subclass_num AS sbclass_num,
                        subclass_short_name AS sbclass_desc,
                        class_num,
                        class_short_name AS class_desc,
                        dept_num,
                        dept_name AS dept_desc,
                        CASE
                            WHEN ROW_NUMBER() OVER (
                                PARTITION BY subclass_num, class_num, dept_num 
                                ORDER BY eff_begin_tmstp
                            ) = 1
                                THEN RANGE(TIMESTAMP '2000-01-01 00:00:00.000000+00:00', eff_end_tmstp_utc)
                            ELSE RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc)
                        END AS eff_period
                    FROM 
                        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_class_subclass_dim_hist 
                ) AS hier 
                ON 
                    hier.sbclass_num = product_style_dim_hist.sbclass_num 
                    AND hier.class_num = product_style_dim_hist.class_num 
                    AND hier.dept_num = product_style_dim_hist.dept_num
                    AND RANGE_OVERLAPS(hier.eff_period, RANGE(product_style_dim_hist.eff_begin_tmstp_utc, product_style_dim_hist.eff_end_tmstp_utc))
                    AND RANGE_OVERLAPS(hier.eff_period, RANGE(department_dim_hist.eff_begin_tmstp_utc, department_dim_hist.eff_end_tmstp_utc))
            )
        ) AS ordered_data
    ) AS grouped_data
    GROUP BY 
        epm_style_num,
        channel_country,
        style_desc,
        style_group_num,
        style_group_desc,
        vendor_label_name,
        sbclass_num,
        sbclass_name,
        class_num,
        class_name,
        dept_num,
        dept_name,
        merch_dept_ind,
        subdivision_num,
        subdivision_name,
        division_num,
        division_name, 
        range_group
    ORDER BY  
        epm_style_num,
        channel_country,
        style_desc,
        style_group_num,
        style_group_desc,
        vendor_label_name,
        sbclass_num,
        sbclass_name,
        class_num,
        class_name,
        dept_num,
        dept_name,
        merch_dept_ind,
        subdivision_num,
        subdivision_name,
        division_num,
        division_name, 
        eff_begin_tmstp
),

SRC_2 AS (
    SELECT 
        rms_sku_num,
        channel_country,
        rms_style_num,
        epm_style_num,
        supp_part_num,
        prmy_supp_num,
        manufacturer_num,
        sku_type_code,
        smart_sample_ind,
        gwp_ind,
        MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp_utc,
        MAX(eff_end_tmstp_utc) AS eff_end_tmstp_utc
    FROM (
        -- Inner normalize
        SELECT 
            *,
            SUM(discontinuity_flag) OVER (
                PARTITION BY rms_sku_num,
                channel_country,
                rms_style_num,
                epm_style_num,
                supp_part_num,
                prmy_supp_num,
                manufacturer_num,
                sku_type_code,
                smart_sample_ind,
                gwp_ind 
                ORDER BY eff_begin_tmstp_utc 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS range_group
        FROM (
            SELECT 
                *,
                CASE 
                    WHEN LAG(eff_end_tmstp_utc) OVER (
                        PARTITION BY rms_sku_num,
                        channel_country,
                        rms_style_num,
                        epm_style_num,
                        supp_part_num,
                        prmy_supp_num,
                        manufacturer_num,
                        sku_type_code,
                        smart_sample_ind,
                        gwp_ind 
                        ORDER BY eff_begin_tmstp_utc
                    ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                    THEN 0
                    ELSE 1
                END AS discontinuity_flag
            FROM (
                SELECT DISTINCT 
                    rms_sku_num,
                    channel_country,
                    rms_style_num,
                    epm_style_num,
                    supp_part_num,
                    prmy_supp_num,
                    manufacturer_num,
                    sku_type_code,
                    smart_sample_ind,
                    gwp_ind,
                    RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc) AS eff_period,
                    eff_begin_tmstp_utc,
                    eff_end_tmstp_utc
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
            )
        ) AS ordered_data
    ) AS grouped_data
    GROUP BY 
        rms_sku_num,
        channel_country,
        rms_style_num,
        epm_style_num,
        supp_part_num,
        prmy_supp_num,
        manufacturer_num,
        sku_type_code,
        smart_sample_ind,
        gwp_ind, 
        range_group
    ORDER BY  
        rms_sku_num,
        channel_country,
        rms_style_num,
        epm_style_num,
        supp_part_num,
        prmy_supp_num,
        manufacturer_num,
        sku_type_code,
        smart_sample_ind,
        gwp_ind,
        eff_begin_tmstp_utc
)

SELECT
    rms_sku_num,
    channel_country,
    sbclass_num,
    sbclass_name,
    class_num,
    class_name,
    dept_num,
    dept_name,
    merch_dept_ind,
    subdivision_num,
    subdivision_name,
    division_num,
    division_name,
    rms_style_num,
    style_desc,
    style_group_num,
    style_group_desc,
    vpn_num,
    supplier_num,
    supplier_name,
    payto_vendor_num,
    payto_vendor_name,
    vendor_label_code,
    vendor_label_name,
    vendor_label_status,
    vendor_brand_code,
    vendor_brand_name,
    sku_type_code,
    smart_sample_ind,
    gwp_ind,
    country_count,
    npg_ind,
    eff_begin_tmstp,
    eff_end_tmstp,
    `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_begin_tmstp as string)) as eff_begin_tmstp_tz,
    `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_end_tmstp as string)) as eff_end_tmstp_tz
FROM (
    SELECT 
        sku0.rms_sku_num,
        sku0.channel_country,
        sty.sbclass_num,
        sty.sbclass_name,
        sty.class_num,
        sty.class_name,
        sty.dept_num,
        sty.dept_name,
        sty.merch_dept_ind,
        sty.subdivision_num,
        sty.subdivision_name,
        sty.division_num,
        sty.division_name,
        sku0.rms_style_num,
        sty.style_desc,
        sty.style_group_num,
        sty.style_group_desc,
        sku0.supp_part_num AS vpn_num,
        sku0.prmy_supp_num AS supplier_num,
        vndr.order_from_vendor_name AS supplier_name,
        CASE
            WHEN LOWER(supp.npg_flag) = 'y' AND LOWER(vrel.payto_vendor_num) <> ''
                THEN vrel.payto_vendor_num
            WHEN LOWER(supp.npg_flag) = 'y' AND LOWER(mfgr.vendor_num) <> ''
                THEN mfgr.vendor_num
            ELSE vndr.payto_vendor_num
        END AS payto_vendor_num,
        CASE
            WHEN LOWER(supp.npg_flag) = 'y' AND LOWER(vrel.payto_vendor_num) <> ''
                THEN vrel.payto_vendor_name
            WHEN LOWER(supp.npg_flag) = 'y' AND LOWER(mfgr.vendor_num) <> ''
                THEN mfgr.vendor_name
            ELSE vndr.payto_vendor_name
        END AS payto_vendor_name,
        lbl.vendor_label_code,
        sty.vendor_label_name,
        lbl.vendor_label_status,
        lbl.vendor_brand_code,
        lbl.vendor_brand_name,
        sku0.sku_type_code,
        sku0.smart_sample_ind,
        sku0.gwp_ind,
        CASE
            WHEN LOWER(MIN(sku0.channel_country) OVER (PARTITION BY sku0.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) <>
                 LOWER(MAX(sku0.channel_country) OVER (PARTITION BY sku0.rms_sku_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
                THEN 2
            ELSE 1
        END AS country_count,
        COALESCE(supp.npg_flag, 'N') AS npg_ind,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY rms_sku_num, sku0.channel_country ORDER BY range_start(sku0.eff_period) ASC) > 1
                THEN range_start(sku0.eff_period)
            ELSE CAST('2000-01-01 00:00:00.000000+00:00' AS TIMESTAMP)
        END AS eff_begin_tmstp,      
        RANGE_END(sku0.eff_period) AS eff_end_tmstp,
        RANGE_INTERSECT(sty.eff_period, RANGE(sku0.eff_begin_tmstp_utc, sku0.eff_end_tmstp_utc)) AS eff_period
    FROM (
        SELECT *,RANGE( eff_begin_tmstp_utc, eff_end_tmstp_utc ) AS eff_period FROM SRC_2
    ) AS sku0
    INNER JOIN (
        SELECT *,RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period FROM SRC
    ) AS sty ON sku0.epm_style_num = sty.epm_style_num 
              AND LOWER(sty.channel_country) = LOWER(sku0.channel_country)
              AND RANGE_OVERLAPS(sty.eff_period, RANGE(sku0.eff_begin_tmstp_utc, sku0.eff_end_tmstp_utc))
    LEFT JOIN (
        SELECT *
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_payto_relationship_dim
        WHERE LOWER(order_from_vendor_num) <> ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY order_from_vendor_num ORDER BY event_seq_num DESC) = 1
    ) AS vndr ON LOWER(vndr.order_from_vendor_num) = LOWER(sku0.prmy_supp_num)
    LEFT JOIN (
        SELECT *
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_label_dim
        QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_label_name ORDER BY dw_sys_load_tmstp DESC, vendor_label_code DESC) = 1
    ) AS lbl ON LOWER(lbl.vendor_label_name) = LOWER(sty.vendor_label_name)
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_dim AS supp ON LOWER(supp.vendor_num) = LOWER(sku0.prmy_supp_num)
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_dim AS mfgr ON LOWER(mfgr.vendor_num) = LOWER(COALESCE(sku0.manufacturer_num, '@' || sku0.rms_sku_num))
    LEFT JOIN (
        SELECT *
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_payto_relationship_dim
        WHERE LOWER(order_from_vendor_num) <> ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY order_from_vendor_num ORDER BY event_seq_num DESC) = 1
    ) AS vrel ON LOWER(vrel.order_from_vendor_num) = LOWER(COALESCE(sku0.manufacturer_num, '@' || sku0.rms_sku_num))
);


-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 5') AS `A12180`;
-- END IF;

COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 6') AS `A12180`;
-- END IF;

BEGIN TRANSACTION;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 7') AS `A12180`;
-- END IF;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('DEMAND_PRODUCT_DETAIL_DIM',  'PRD_NAP_DIM',  'TECH_Merch_NAP_DEMAND_KPI_PRODUCT_LOAD',  'demand_product_detail_dim_load',  2,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME),  'NAP_PRODUCT_DEMAND_DETAIL');

COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;

-- IF _ERROR_CODE <> 0 THEN
--     SELECT ERROR('RC = 8') AS `A12180`;
-- END IF;

--COLLECT STATS ON PRD_NAP_DIM.DEMAND_PRODUCT_DETAIL_DIM;
-- END;