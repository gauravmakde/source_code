TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.supp_dept_map_dim_vtw;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.supp_dept_map_dim_vtw
( dept_num
, supplier_num
, supplier_group
, banner
, buy_planner
, preferred_partner_desc
, areas_of_responsibility
, is_npg
, diversity_group
, nord_to_rack_transfer_rate
, eff_begin_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp
, eff_end_tmstp_tz
, delete_flag
)

--with cte 

WITH SRC1 AS (
    SELECT 
        dept_num, 
        supplier_num, 
        supplier_group,
        banner,
        buy_planner,
        preferred_partner_desc,
        areas_of_responsibility,
        is_npg,
        diversity_group,
        nord_to_rack_transfer_rate,
        delete_flag,
        eff_begin_tmstp,
        eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            dept_num, supplier_num, supplier_group,banner,buy_planner,preferred_partner_desc,areas_of_responsibility,is_npg,diversity_group,
            nord_to_rack_transfer_rate,delete_flag,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp, 
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY dept_num, supplier_num, supplier_group,banner,buy_planner,preferred_partner_desc,areas_of_responsibility,is_npg,diversity_group,
                            nord_to_rack_transfer_rate,delete_flag
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY dept_num, supplier_num, supplier_group,banner,buy_planner,preferred_partner_desc,areas_of_responsibility,is_npg,diversity_group,
                            nord_to_rack_transfer_rate,delete_flag
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT DISTINCT 
                        dept_num,
                        supplier_num,
                        supplier_group,
                        banner,
                        buy_planner,
                        preferred_partner_desc,
                        areas_of_responsibility,
                        is_npg,
                        diversity_group,
                        nord_to_rack_transfer_rate,  
                        delete_flag,
                        eff_begin_tmstp,
                        eff_end_tmstp
                    FROM (
                        SELECT SRC_2.*,
                            CASE 
                                WHEN delete_flag <> 'Y' THEN
                                    MAX(eff_begin_tmstp) OVER (
                                        PARTITION BY dept_num, 
                                                     supplier_num, 
                                                     banner 
                                        ORDER BY eff_begin_tmstp 
                                        ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                    )
                                ELSE 
                                    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) 
                            END AS max_eff_end_tmstp,
                            COALESCE(
                                CASE 
                                    WHEN delete_flag <> 'Y' THEN
                                        MAX(eff_begin_tmstp) OVER (
                                            PARTITION BY dept_num, 
                                                         supplier_num, 
                                                         banner 
                                            ORDER BY eff_begin_tmstp 
                                            ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                        )
                                    ELSE 
                                        TIMESTAMP(CURRENT_DATETIME('PST8PDT')) 
                                END,
                                TIMESTAMP '9999-12-31 23:59:59.999999+00:00'
                            ) AS eff_end_tmstp
                        FROM (
                            SELECT
                                dept_num,
                                supplier_num,
                                supplier_group,
                                banner,
                                buy_planner,
                                preferred_partner_desc,
                                areas_of_responsibility,
                                (CASE 
                                    WHEN (TRIM(is_npg) = '1' AND TRIM(is_npg) IS NOT NULL) 
                                    THEN 'Y' 
                                    ELSE 'N' 
                                END) AS is_npg,
                                diversity_group,
                                nord_to_rack_transfer_rate,
                                'N' AS delete_flag,
                                TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(CURRENT_DATETIME('PST8PDT')), 'America/Los_Angeles')) AS eff_begin_tmstp
                                --jwn_udf.udf_time_zone(jwn_udf.iso8601_tmstp(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING))) AS eff_begin_tmstp_tz
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_ldg
                            UNION ALL
                            SELECT
                                S0.dept_num,
                                S0.supplier_num,
                                S0.supplier_group,
                                S0.banner,
                                S0.buy_planner,
                                S0.preferred_partner_desc,
                                S0.areas_of_responsibility,
                                S0.is_npg,
                                S0.diversity_group,
                                S0.nord_to_rack_transfer_rate,
                                'Y' AS delete_flag,
                                S0.eff_begin_tmstp,
                                S0.eff_begin_tmstp_tz
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.supp_dept_map_dim S0
                            LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_ldg S1
                                ON S0.dept_num = S1.dept_num
                                AND S0.supplier_num = S1.supplier_num
                                AND S0.banner = S1.banner
                            WHERE S1.dept_num IS NULL
                              AND S0.eff_end_tmstp > TIMESTAMP(CURRENT_DATETIME('PST8PDT'))
                        ) SRC_2
                        QUALIFY SRC_2.eff_begin_tmstp < eff_end_tmstp
                    ) SRC_1
                )
            ) AS ordered_data
        ) AS grouped_data
        GROUP BY 
        dept_num, 
        supplier_num, 
        supplier_group,
        banner,
        buy_planner,
        preferred_partner_desc,
        areas_of_responsibility,
        is_npg,
        diversity_group,
        nord_to_rack_transfer_rate,
        delete_flag,
        range_group
        ORDER BY  
        dept_num, 
        supplier_num, 
        supplier_group,
        banner,
        buy_planner,
        preferred_partner_desc,
        areas_of_responsibility,
        is_npg,
        diversity_group,
        nord_to_rack_transfer_rate,
        delete_flag,
        eff_begin_tmstp
    )
)
--cte end

    -- Second normalize
SELECT  
        dept_num,
        supplier_num,
        supplier_group,
        banner,
        buy_planner,
        preferred_partner_desc,
        areas_of_responsibility,
        is_npg,
        diversity_group,
        nord_to_rack_transfer_rate,
        eff_begin_tmstp,
        `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_begin_tmstp as string)) as eff_begin_tmstp_tz,
        eff_end_tmstp,
        `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_end_tmstp as string)) as eff_end_tmstp_tz,
        delete_flag
    FROM (
        -- Inner normalize
        SELECT 
            dept_num,
            supplier_num,
            supplier_group,
            banner,
            buy_planner,
            preferred_partner_desc,
            areas_of_responsibility,
            is_npg,
            diversity_group,
            nord_to_rack_transfer_rate,
            delete_flag,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp,
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY dept_num, 
                                 supplier_num, 
                                 supplier_group,
                                 banner,
                                 buy_planner,
                                 preferred_partner_desc,
                                 areas_of_responsibility,
                                 is_npg,
                                 diversity_group,
                                 nord_to_rack_transfer_rate,
                                 delete_flag 
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY dept_num, 
                                         supplier_num, 
                                         supplier_group,
                                         banner,
                                         buy_planner,
                                         preferred_partner_desc,
                                         areas_of_responsibility,
                                         is_npg,
                                         diversity_group,
                                         nord_to_rack_transfer_rate,
                                         delete_flag 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT DISTINCT
                        SRC.dept_num,
                        SRC.supplier_num,
                        SRC.supplier_group,
                        SRC.banner,
                        SRC.buy_planner,
                        SRC.preferred_partner_desc,
                        SRC.areas_of_responsibility,
                        SRC.is_npg,
                        SRC.diversity_group,
                        SRC.nord_to_rack_transfer_rate,
                        COALESCE(
                            RANGE_INTERSECT(SRC.eff_period, RANGE(CAST(TGT.eff_begin_tmstp AS TIMESTAMP), CAST(TGT.eff_end_tmstp AS TIMESTAMP))), 
                            SRC.eff_period 
                        ) AS eff_period,
                        delete_flag,
                        SRC.eff_begin_tmstp,
                        SRC.eff_end_tmstp
                    FROM (
                        SELECT *,
                            RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period 
                        FROM SRC1
                    ) SRC
                    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.supp_dept_map_dim TGT
                    ON SRC.dept_num = TGT.dept_num
                    AND SRC.supplier_num = TGT.supplier_num
                    AND LOWER(SRC.supplier_group) = LOWER(TGT.supplier_group)
                    AND LOWER(SRC.banner) = LOWER(TGT.banner)
                    AND RANGE_OVERLAPS(SRC.eff_period, RANGE(CAST(TGT.eff_begin_tmstp AS TIMESTAMP), CAST(TGT.eff_end_tmstp AS TIMESTAMP)))
                    WHERE (
                        TGT.dept_num IS NULL OR (
                            (LOWER(SRC.buy_planner) <> LOWER(TGT.buy_planner) 
                             OR (SRC.buy_planner IS NOT NULL AND TGT.buy_planner IS NULL) 
                             OR (SRC.buy_planner IS NULL AND TGT.buy_planner IS NOT NULL))
                            OR (LOWER(SRC.supplier_group) <> LOWER(TGT.supplier_group) 
                             OR (SRC.supplier_group IS NOT NULL AND TGT.supplier_group IS NULL) 
                             OR (SRC.supplier_group IS NULL AND TGT.supplier_group IS NOT NULL))
                            OR (LOWER(SRC.preferred_partner_desc) <> LOWER(TGT.preferred_partner_desc) 
                             OR (SRC.preferred_partner_desc IS NOT NULL AND TGT.preferred_partner_desc IS NULL) 
                             OR (SRC.preferred_partner_desc IS NULL AND TGT.preferred_partner_desc IS NOT NULL))
                            OR (LOWER(SRC.areas_of_responsibility) <> LOWER(TGT.areas_of_responsibility) 
                             OR (SRC.areas_of_responsibility IS NOT NULL AND TGT.areas_of_responsibility IS NULL) 
                             OR (SRC.areas_of_responsibility IS NULL AND TGT.areas_of_responsibility IS NOT NULL))
                            OR (LOWER(SRC.is_npg) <> LOWER(TGT.is_npg) 
                             OR (SRC.is_npg IS NOT NULL AND TGT.is_npg IS NULL) 
                             OR (SRC.is_npg IS NULL AND TGT.is_npg IS NOT NULL))
                            OR (LOWER(SRC.diversity_group) <> LOWER(TGT.diversity_group) 
                             OR (SRC.diversity_group IS NOT NULL AND TGT.diversity_group IS NULL) 
                             OR (SRC.diversity_group IS NULL AND TGT.diversity_group IS NOT NULL))
                            OR (SRC.nord_to_rack_transfer_rate <> TGT.nord_to_rack_transfer_rate 
                             OR (SRC.nord_to_rack_transfer_rate IS NOT NULL AND TGT.nord_to_rack_transfer_rate IS NULL) 
                             OR (SRC.nord_to_rack_transfer_rate IS NULL AND TGT.nord_to_rack_transfer_rate IS NOT NULL))
                            OR LOWER(SRC.delete_flag) = 'y'
                        )
                    ) 
                ) AS ordered_data)
            ) AS grouped_data
        GROUP BY 
            dept_num, 
            supplier_num, 
            supplier_group,
            banner,
            buy_planner,
            preferred_partner_desc,
            areas_of_responsibility,
            is_npg,
            diversity_group,
            nord_to_rack_transfer_rate,
            delete_flag, 
            range_group
        ORDER BY  
            dept_num, 
            supplier_num, 
            supplier_group,
            banner,
            buy_planner,
            preferred_partner_desc,
            areas_of_responsibility,
            is_npg,
            diversity_group,
            nord_to_rack_transfer_rate,
            delete_flag
    );



BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.supp_dept_map_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.supp_dept_map_dim_vtw AS src
 WHERE LOWER(dept_num) = LOWER(tgt.dept_num)
  AND LOWER(supplier_num) = LOWER(tgt.supplier_num)
  AND LOWER(banner) = LOWER(tgt.banner)
  AND LOWER(delete_flag) = LOWER('Y')
  AND CAST(tgt.eff_end_tmstp AS DATETIME) > CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME));
  
  
  DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.supp_dept_map_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.supp_dept_map_dim_vtw AS src
 WHERE LOWER(dept_num) = LOWER(tgt.dept_num)
  AND LOWER(supplier_num) = LOWER(tgt.supplier_num)
  AND LOWER(banner) = LOWER(tgt.banner)
  AND LOWER(delete_flag) = LOWER('N'));
  
  
  INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.supp_dept_map_dim (dept_num, supplier_num, supplier_group, banner, buy_planner,
 preferred_partner_desc, areas_of_responsibility, is_npg, diversity_group, nord_to_rack_transfer_rate, eff_begin_tmstp,eff_begin_tmstp_tz,
 eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT dept_num,
  supplier_num,
  supplier_group,
  banner,
  buy_planner,
  preferred_partner_desc,
  areas_of_responsibility,
  is_npg,
  diversity_group,
  nord_to_rack_transfer_rate,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT dept_num,
    supplier_num,
    supplier_group,
    banner,
    buy_planner,
    preferred_partner_desc,
    areas_of_responsibility,
    is_npg,
    diversity_group,
    nord_to_rack_transfer_rate,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.supp_dept_map_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.supp_dept_map_dim
   WHERE dept_num = t.dept_num
    AND supplier_num = t.supplier_num
    AND banner = t.banner)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY dept_num, supplier_num, banner)) = 1);
 
 COMMIT Transaction;