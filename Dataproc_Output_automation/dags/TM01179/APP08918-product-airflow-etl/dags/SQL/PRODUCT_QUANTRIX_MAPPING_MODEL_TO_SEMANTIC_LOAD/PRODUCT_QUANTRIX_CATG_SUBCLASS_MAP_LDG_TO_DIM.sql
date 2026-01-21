
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.catg_subclass_map_dim_vtw (dept_num
, class_num
, sbclass_num
, category
, op_model
, fp_model
, category_planner_1
, category_planner_2
, category_group
, seasonal_designation
, rack_merch_zone
, is_activewear
, channel_category_roles_1
, channel_category_roles_2
, bargainista_dept_map
, eff_begin_tmstp
,eff_begin_tmstp_tz
, eff_end_tmstp
,eff_end_tmstp_tz
, delete_flag)


--with cte 
WITH SRC1 AS (
    SELECT 
        dept_num,
        class_num,
        sbclass_num,
        category,
        op_model,
        fp_model,
        category_planner_1,
        category_planner_2,
        category_group,
        seasonal_designation,
        rack_merch_zone,
        is_activewear,
        channel_category_roles_1,
        channel_category_roles_2,
        bargainista_dept_map,
        delete_flag,
        eff_begin_tmstp,
        eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            dept_num,
            class_num,
            sbclass_num,
            category,
            op_model,
            fp_model,
            category_planner_1,
            category_planner_2,
            category_group,
            seasonal_designation,
            rack_merch_zone,
            is_activewear,
            channel_category_roles_1,
            channel_category_roles_2,
            bargainista_dept_map,
            delete_flag,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp,
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY 
                        dept_num,
                        class_num,
                        sbclass_num,
                        category,
                        op_model,
                        fp_model,
                        category_planner_1,
                        category_planner_2,
                        category_group,
                        seasonal_designation,
                        rack_merch_zone,
                        is_activewear,
                        channel_category_roles_1,
                        channel_category_roles_2,
                        bargainista_dept_map,
                        delete_flag 
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY 
                                dept_num,
                                class_num,
                                sbclass_num,
                                category,
                                op_model,
                                fp_model,
                                category_planner_1,
                                category_planner_2,
                                category_group,
                                seasonal_designation,
                                rack_merch_zone,
                                is_activewear,
                                channel_category_roles_1,
                                channel_category_roles_2,
                                bargainista_dept_map,
                                delete_flag 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT DISTINCT 
                        SRC3.*,
                        RANGE(eff_begin_tmstp, SRC3.eff_end_tmstp) AS eff_period
                        --eff_begin_tmstp_tz,
                        --jwn_udf.udf_time_zone(jwn_udf.iso8601_tmstp(CAST(SRC3.eff_end_tmstp AS STRING))) AS eff_end_tmstp_tz 
                    FROM (
                        SELECT 
                            SRC1.*, 
                            COALESCE(max_eff_end_tmstp, TIMESTAMP '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp  
                        FROM (
                            SELECT DISTINCT 
                                dept_num,
                                class_num,
                                sbclass_num,
                                category,
                                op_model,
                                fp_model,
                                category_planner_1,
                                category_planner_2,
                                category_group,
                                seasonal_designation,
                                rack_merch_zone,
                                is_activewear,
                                channel_category_roles_1,
                                channel_category_roles_2,
                                bargainista_dept_map,
                                delete_flag,
                                CASE 
                                    WHEN delete_flag <> 'Y' THEN
                                        MAX(eff_begin_tmstp) OVER (
                                            PARTITION BY dept_num, class_num, sbclass_num
                                            ORDER BY eff_begin_tmstp 
                                            ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                        )
                                    ELSE 
                                        TIMESTAMP(current_datetime('PST8PDT')) 
                                END AS max_eff_end_tmstp,
                                eff_begin_tmstp
                                --eff_begin_tmstp_tz
                            FROM (
                                SELECT 
                                    CAST(dept_num AS INTEGER) AS dept_num,
                                    class_num,
                                    sbclass_num,
                                    category,
                                    op_model,
                                    fp_model,
                                    category_planner_1,
                                    category_planner_2,
                                    category_group,
                                    seasonal_designation,
                                    rack_merch_zone,
                                    is_activewear,
                                    channel_category_roles_1,
                                    channel_category_roles_2,
                                    bargainista_dept_map,
                                    'N' AS delete_flag,
                                    TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(current_datetime('PST8PDT')), 'America/Los_Angeles')) AS eff_begin_tmstp
                                   -- jwn_udf.default_tz_pst() AS eff_begin_tmstp_tz
                                FROM 
                                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.catg_subclass_map_ldg
                                UNION ALL
                                SELECT DISTINCT 
                                    s0.dept_num,
                                    s0.class_num,
                                    s0.sbclass_num,
                                    s0.category,
                                    s0.op_model,
                                    s0.fp_model,
                                    s0.category_planner_1,
                                    s0.category_planner_2,
                                    s0.category_group,
                                    s0.seasonal_designation,
                                    s0.rack_merch_zone,
                                    s0.is_activewear,
                                    s0.channel_category_roles_1,
                                    s0.channel_category_roles_2,
                                    s0.bargainista_dept_map,
                                    'Y' AS delete_flag,
                                    s0.eff_begin_tmstp
                                    -- S0.eff_begin_tmstp_tz
                                FROM 
                                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.catg_subclass_map_dim AS s0
                                LEFT JOIN 
                                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.catg_subclass_map_ldg AS s1 
                                ON 
                                    s0.dept_num = CAST(s1.dept_num AS FLOAT64) 
                                    AND LOWER(s0.class_num) = LOWER(s1.class_num) 
                                    AND LOWER(s0.sbclass_num) = LOWER(s1.sbclass_num)
                                WHERE 
                                    s1.dept_num IS NULL
                                    AND CAST(s0.eff_end_tmstp AS DATETIME) > CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME)
                            ) AS SRC_2
                            QUALIFY 
                                eff_begin_tmstp < COALESCE(
                                    CASE
                                        WHEN LOWER(delete_flag) <> LOWER('Y') THEN 
                                            MAX(eff_begin_tmstp) OVER (
                                                PARTITION BY dept_num, class_num, sbclass_num 
                                                ORDER BY eff_begin_tmstp 
                                                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                            )
                                        ELSE 
                                            CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP)
                                    END,CAST(CAST('0000-01-01 00:00:00' AS DATETIME) AS TIMESTAMP)
                                )
                        ) AS SRC1
                    ) AS SRC3)
                ) AS ordered_data
            ) AS grouped_data
        GROUP BY 
            dept_num,
                                class_num,
                                sbclass_num,
                                category,
                                op_model,
                                fp_model,
                                category_planner_1,
                                category_planner_2,
                                category_group,
                                seasonal_designation,
                                rack_merch_zone,
                                is_activewear,
                                channel_category_roles_1,
                                channel_category_roles_2,
                                bargainista_dept_map,
                                delete_flag ,
            range_group
        ORDER BY  
            dept_num,
                                class_num,
                                sbclass_num,
                                category,
                                op_model,
                                fp_model,
                                category_planner_1,
                                category_planner_2,
                                category_group,
                                seasonal_designation,
                                rack_merch_zone,
                                is_activewear,
                                channel_category_roles_1,
                                channel_category_roles_2,
                                bargainista_dept_map,
                                delete_flag ,
            eff_begin_tmstp
    )
)


SELECT 
    NRML.dept_num,
    NRML.class_num,
    NRML.sbclass_num,
    NRML.category,
    NRML.op_model,
    NRML.fp_model,
    NRML.category_planner_1,
    NRML.category_planner_2,
    NRML.category_group,
    NRML.seasonal_designation,
    NRML.rack_merch_zone,
    NRML.is_activewear,
    NRML.channel_category_roles_1,
    NRML.channel_category_roles_2,
    NRML.bargainista_dept_map,
     eff_begin,
    `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_begin as string)) as eff_begin_tmstp_tz,
    eff_end,
    `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_end as string)) as eff_end_tmstp_tz,
    NRML.delete_flag 
FROM (
    -- Second normalize
    SELECT
        dept_num,
            class_num,
            sbclass_num,
            category,
            op_model,
            fp_model,
            category_planner_1,
            category_planner_2,
            category_group,
            seasonal_designation,
            rack_merch_zone,
            is_activewear,
            channel_category_roles_1,
            channel_category_roles_2,
            bargainista_dept_map,
            delete_flag,
        MIN(RANGE_START(eff_period)) AS eff_begin,
        MAX(RANGE_END(eff_period)) AS eff_end
    FROM (
        -- Inner normalize
        SELECT 
            dept_num,
            class_num,
            sbclass_num,
            category,
            op_model,
            fp_model,
            category_planner_1,
            category_planner_2,
            category_group,
            seasonal_designation,
            rack_merch_zone,
            is_activewear,
            channel_category_roles_1,
            channel_category_roles_2,
            bargainista_dept_map,
            delete_flag,
            eff_period,
            range_group
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY 
                        dept_num,
                        class_num,
                        sbclass_num,
                        category,
                        op_model,
                        fp_model,
                        category_planner_1,
                        category_planner_2,
                        category_group,
                        seasonal_designation,
                        rack_merch_zone,
                        is_activewear,
                        channel_category_roles_1,
                        channel_category_roles_2,
                        bargainista_dept_map,
                        delete_flag 
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                -- NONSEQUENCED VALIDTIME
                SELECT 
                    dept_num,
                    class_num,
                    sbclass_num,
                    category,
                    op_model,
                    fp_model,
                    category_planner_1,
                    category_planner_2,
                    category_group,
                    seasonal_designation,
                    rack_merch_zone,
                    is_activewear,
                    channel_category_roles_1,
                    channel_category_roles_2,
                    bargainista_dept_map,
                    delete_flag,
                    eff_begin_tmstp,
                    eff_end_tmstp,
                    eff_period,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY 
                                dept_num,
                                class_num,
                                sbclass_num,
                                category,
                                op_model,
                                fp_model,
                                category_planner_1,
                                category_planner_2,
                                category_group,
                                seasonal_designation,
                                rack_merch_zone,
                                is_activewear,
                                channel_category_roles_1,
                                channel_category_roles_2,
                                bargainista_dept_map,
                                delete_flag 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT 
                        SRC.dept_num,
                        SRC.class_num,
                        SRC.sbclass_num,
                        SRC.category,
                        SRC.op_model,
                        SRC.fp_model,
                        SRC.category_planner_1,
                        SRC.category_planner_2,
                        SRC.category_group,
                        SRC.seasonal_designation,
                        SRC.rack_merch_zone,
                        SRC.is_activewear,
                        SRC.channel_category_roles_1,
                        SRC.channel_category_roles_2,
                        SRC.bargainista_dept_map,
                        SRC.delete_flag,
                        COALESCE(
                            RANGE_INTERSECT(
                                SRC.eff_period,
                                RANGE(CAST(TGT.eff_begin_tmstp AS TIMESTAMP), CAST(TGT.eff_end_tmstp AS TIMESTAMP))
                            ), 
                            SRC.eff_period
                        ) AS eff_period,
                        TGT.eff_begin_tmstp,
                        TGT.eff_end_tmstp
                    FROM (
                        SELECT *,
                            RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period 
                        FROM SRC1
                    ) AS SRC
                    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.catg_subclass_map_dim AS tgt 
                    ON SRC.dept_num = tgt.dept_num 
                    AND LOWER(SRC.class_num) = LOWER(tgt.class_num) 
                    AND LOWER(SRC.sbclass_num) = LOWER(tgt.sbclass_num)
                    WHERE 
                        tgt.dept_num IS NULL
                        OR LOWER(SRC.category) <> LOWER(tgt.category)
                        OR (tgt.category IS NULL AND SRC.category IS NOT NULL) 
                        OR (SRC.category IS NULL AND tgt.category IS NOT NULL)
                        OR (LOWER(SRC.op_model) <> LOWER(tgt.op_model) OR tgt.op_model IS NULL AND SRC.op_model IS NOT NULL OR (SRC.op_model IS NULL AND tgt.op_model IS NOT NULL))
                        OR (LOWER(SRC.fp_model) <> LOWER(tgt.fp_model) OR tgt.fp_model IS NULL AND SRC.fp_model IS NOT NULL OR (SRC.fp_model IS NULL AND tgt.fp_model IS NOT NULL))
                        OR (LOWER(SRC.category_planner_1) <> LOWER(tgt.category_planner_1) OR (tgt.category_planner_1 IS NULL AND SRC.category_planner_1 IS NOT NULL OR (SRC.category_planner_1 IS NULL AND tgt.category_planner_1 IS NOT NULL)))
                        OR (LOWER(SRC.channel_category_roles_2) <> LOWER(tgt.channel_category_roles_2) OR (tgt.channel_category_roles_2 IS NULL AND SRC.channel_category_roles_2 IS NOT NULL OR SRC.channel_category_roles_2 IS NULL AND tgt.channel_category_roles_2 IS NOT NULL))
                        OR (LOWER(SRC.seasonal_designation) <> LOWER(tgt.seasonal_designation) OR tgt.seasonal_designation IS NULL AND SRC.seasonal_designation IS NOT NULL OR (SRC.seasonal_designation IS NULL AND tgt.seasonal_designation IS NOT NULL))
                        OR (LOWER(SRC.rack_merch_zone) <> LOWER(tgt.rack_merch_zone) OR (tgt.rack_merch_zone IS NULL AND SRC.rack_merch_zone IS NOT NULL OR SRC.rack_merch_zone IS NULL AND tgt.rack_merch_zone IS NOT NULL))
                        OR (LOWER(SRC.is_activewear) <> LOWER(tgt.is_activewear) OR (tgt.is_activewear IS NULL AND SRC.is_activewear IS NOT NULL OR SRC.is_activewear IS NULL AND tgt.is_activewear IS NOT NULL))
                        OR (LOWER(SRC.channel_category_roles_1) <> LOWER(tgt.channel_category_roles_1) OR tgt.channel_category_roles_1 IS NULL AND SRC.channel_category_roles_1 IS NOT NULL OR (SRC.channel_category_roles_1 IS NULL AND tgt.channel_category_roles_1 IS NOT NULL))
                        OR (LOWER(SRC.bargainista_dept_map) <> LOWER(tgt.bargainista_dept_map) OR tgt.bargainista_dept_map IS NULL AND SRC.bargainista_dept_map IS NOT NULL OR (SRC.bargainista_dept_map IS NULL AND tgt.bargainista_dept_map IS NOT NULL))
                        OR (LOWER(SRC.delete_flag) = LOWER('Y') OR (LOWER(SRC.category_planner_2) <> LOWER(tgt.category_planner_2) OR (tgt.category_planner_2 IS NULL AND SRC.category_planner_2 IS NOT NULL OR SRC.category_planner_2 IS NULL AND tgt.category_planner_2 IS NOT NULL)))
                )
            ) AS ordered_data
        ) AS grouped_data)
        GROUP BY 
            dept_num,
            class_num,
            sbclass_num,
            category,
            op_model,
            fp_model,
            category_planner_1,
            category_planner_2,
            category_group,
            seasonal_designation,
            rack_merch_zone,
            is_activewear,
            channel_category_roles_1,
            channel_category_roles_2,
            bargainista_dept_map,
            delete_flag,
            range_group
        ORDER BY  
            dept_num,
            class_num,
            sbclass_num,
            category,
            op_model,
            fp_model,
            category_planner_1,
            category_planner_2,
            category_group,
            seasonal_designation,
            rack_merch_zone,
            is_activewear,
            channel_category_roles_1,
            channel_category_roles_2,
            bargainista_dept_map,
            delete_flag
    ) AS NRML
QUALIFY (ROW_NUMBER() OVER (PARTITION BY dept_num, class_num, sbclass_num) = 1);
