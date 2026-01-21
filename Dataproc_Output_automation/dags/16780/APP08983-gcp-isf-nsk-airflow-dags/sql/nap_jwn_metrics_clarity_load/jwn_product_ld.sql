
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_product_ld.sql
-- Author                  : Oleksandr Chaichenko
-- Description             : updates for product related columns at a sku-location-day level
-- Data Source             : PRD_NAP_BASE_VWS.PRODUCT_SKU_DIM
-- ETL Run Frequency       : daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-01  Oleksandr Chaichenko    FA-9355: ETL & ISF Updates - Add Cost Fields to JWN* Inventory table
--*************************************************************************************************************************************


CREATE TEMPORARY TABLE delta_wrk
  AS
    SELECT
        jwn_inventory_sku_loc_day_fact.rms_sku_num,
        jwn_inventory_sku_loc_day_fact.location_num,
        jwn_inventory_sku_loc_day_fact.reporting_date
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact
      WHERE jwn_inventory_sku_loc_day_fact.dw_batch_id = (
        SELECT
            -- today delta
            batch_id
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE upper(rtrim(subject_area_nm, ' ')) = 'NAP_ASCP_CLARITY_LOAD'
      )
       AND jwn_inventory_sku_loc_day_fact.dw_sys_load_tmstp >= (
        SELECT
            batch_start_tmstp
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE upper(rtrim(subject_area_nm, ' ')) = 'NAP_ASCP_CLARITY_LOAD'
      )
       OR jwn_inventory_sku_loc_day_fact.dw_sys_load_tmstp >= (
        SELECT
            --  or day-2 delta with empty div num (evening Product DAG missed records)
            DATE(batch_start_tmstp - INTERVAL 2 DAY)
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE upper(rtrim(subject_area_nm, ' ')) = 'NAP_ASCP_CLARITY_LOAD'
      )
       AND jwn_inventory_sku_loc_day_fact.division_num IS NULL
      GROUP BY 1, 2, 3
;

CREATE TEMPORARY TABLE IF NOT EXISTS delta_wrk_rms_sku_num AS
SELECT rms_sku_num
FROM delta_wrk
GROUP BY rms_sku_num;



CREATE TEMPORARY TABLE delta_wrk_product
  PARTITION BY DATE(eff_begin_tmstp)
  AS
--cte for normalization
WITH SRC1 AS 
(
  SELECT 
    rms_sku_num, channel_country, sku_desc, color_num, color_desc, nord_display_color, return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num, eff_begin_tmstp_utc, eff_end_tmstp_utc
  FROM (
    SELECT 
      rms_sku_num, channel_country, sku_desc, color_num, color_desc, nord_display_color, return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num,
      MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp_utc, MAX(eff_end_tmstp_utc) AS eff_end_tmstp_utc
    FROM (
      SELECT *,
        SUM(discontinuity_flag) OVER (
          PARTITION BY rms_sku_num, channel_country, sku_desc, color_num, color_desc, nord_display_color, return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num
          ORDER BY eff_begin_tmstp_utc 
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS range_group
      FROM (
        SELECT *,
          CASE 
            WHEN LAG(eff_end_tmstp_utc) OVER (
              PARTITION BY rms_sku_num, channel_country, sku_desc, color_num, color_desc, nord_display_color, return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num
              ORDER BY eff_begin_tmstp_utc
            ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
            THEN 0
            ELSE 1
          END AS discontinuity_flag
        FROM (
          SELECT
            hist_0.rms_sku_num, channel_country,
            CASE
              WHEN RTRIM(hist_0.rms_sku_num, ' ') IN ('87562285', '87562287', '87562286', '87562284', '87562288', '87562289')
              THEN REGEXP_REPLACE(SUBSTR(sku_desc, 1, 200), r'\.\d\d\d\d\d\:', r'\:')
              ELSE SUBSTR(sku_desc, 1, 200)
            END AS sku_desc,
            color_num, color_desc, nord_display_color, RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc) AS eff_period,
            return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num, eff_begin_tmstp_utc, eff_end_tmstp_utc
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS hist_0
          INNER JOIN delta_wrk_rms_sku_num AS wrk ON hist_0.rms_sku_num = wrk.rms_sku_num
          WHERE eff_end_tmstp > DATE '2022-10-29'
        ) AS initial_data
      ) AS processed_data
    ) AS ordered_data
    GROUP BY rms_sku_num, channel_country, sku_desc, color_num, color_desc, nord_display_color, return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num, range_group
  ) AS grouped_data
  ORDER BY rms_sku_num, channel_country, sku_desc, color_num, color_desc, nord_display_color, return_disposition_code, selling_status_desc, selling_channel_eligibility_list, prmy_supp_num, eff_begin_tmstp_utc
)
    SELECT
        hist.*,
        min(hist.sbclass_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_sbclass_num,
        min(hist.class_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_class_num,
        min(hist.dept_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_dept_num,
        min(hist.subdivision_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_subdivision_num,
        min(hist.division_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_division_num,
        CASE
          WHEN hist.sbclass_num <> min(hist.sbclass_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
           OR hist.class_num <> min(hist.class_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
           OR hist.dept_num <> min(hist.dept_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
           OR hist.subdivision_num <> min(hist.subdivision_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
           OR hist.division_num <> min(hist.division_num) OVER (PARTITION BY hist.rms_sku_num, hist.channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) THEN 'Y'
          ELSE 'N'
        END AS reclass_ind
      FROM
        (
          --second normalize
         -- (
      SELECT rms_sku_num,channel_country,country_count,vpn_num,sku_desc,color_num,color_desc,nord_display_color,rms_style_num,style_desc,style_group_num,style_group_desc,sbclass_num,sbclass_name,class_num,class_name,dept_num,dept_name,subdivision_num,
        subdivision_name,division_num,division_name,gwp_ind,smart_sample_ind,vendor_brand_name,supplier_num,supplier_name,payto_vendor_num,payto_vendor_name,npg_ind,return_disposition_code,selling_status_desc,selling_channel_eligibility_list,prmy_supp_num,
    MIN(RANGE_START(eff_period)) AS eff_begin_tmstp,
    MAX(RANGE_END(eff_period)) AS eff_end_tmstp
FROM (
    SELECT *,
        SUM(discontinuity_flag) OVER (
            PARTITION BY rms_sku_num,channel_country,country_count,vpn_num,sku_desc,color_num,color_desc,nord_display_color,rms_style_num,style_desc,style_group_num,style_group_desc,sbclass_num,sbclass_name,class_num,class_name,dept_num,dept_name,subdivision_num,
        subdivision_name,division_num,division_name,gwp_ind,smart_sample_ind,vendor_brand_name,supplier_num,supplier_name,payto_vendor_num,payto_vendor_name,npg_ind,return_disposition_code,selling_status_desc,selling_channel_eligibility_list,prmy_supp_num
            ORDER BY eff_begin_tmstp_utc 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS range_group
    FROM (
        SELECT DISTINCT *,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (
                    PARTITION BY rms_sku_num,channel_country,country_count,vpn_num,sku_desc,color_num,color_desc,nord_display_color,rms_style_num,style_desc,style_group_num,style_group_desc,sbclass_num,sbclass_name,class_num,class_name,dept_num,dept_name,subdivision_num,subdivision_name,division_num,division_name,gwp_ind,smart_sample_ind,vendor_brand_name,supplier_num,supplier_name,payto_vendor_num,payto_vendor_name,npg_ind,return_disposition_code,selling_status_desc,selling_channel_eligibility_list,prmy_supp_num
                    ORDER BY eff_begin_tmstp_utc
                ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
        FROM 
          (
             SELECT
              sku0.rms_sku_num,
              sku0.channel_country,
              sku2.country_count,
              sku2.vpn_num,
              sku0.sku_desc,
              sku0.color_num,
              sku0.color_desc,
              sku0.nord_display_color,
              sku2.rms_style_num,
              sku2.style_desc,
              sku2.style_group_num,
              sku2.style_group_desc,
              sku2.sbclass_num,
              sku2.sbclass_name,
              sku2.class_num,
              sku2.class_name,
              sku2.dept_num,
              sku2.dept_name,
              sku2.subdivision_num,
              sku2.subdivision_name,
              sku2.division_num,
              sku2.division_name,
              sku2.gwp_ind,
              sku2.smart_sample_ind,
              sku2.vendor_brand_name,
              sku2.supplier_num,
              sku2.supplier_name,
              sku2.payto_vendor_num,
              sku2.payto_vendor_name,
              sku2.npg_ind,
             SAFE.RANGE_INTERSECT(sku0.eff_period,sku2.eff_period) as eff_period,
              --CAST(bqutil.fn.cw_period_intersection(CAST(sku0.eff_period as STRUCT<lower TIMESTAMP, upper TIMESTAMP>), CAST(sku2.eff_period as STRUCT<lower TIMESTAMP, upper TIMESTAMP>)) as STRUCT<lower DATE, upper DATE>) AS eff_period,
              -- Product Insights columns
              sku0.return_disposition_code,
              sku0.selling_status_desc,
              sku0.selling_channel_eligibility_list,
              sku0.prmy_supp_num,
              sku0.eff_begin_tmstp_utc,
              sku0.eff_end_tmstp_utc
            FROM
              (
              select *,RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc) AS eff_period--timestamp
               from SRC1
              ) AS sku0
              INNER JOIN -- Inventory history only needs these rows
              (
                SELECT
                    hist_0.rms_sku_num,
                    channel_country,
                    country_count,
                    vpn_num,
                    rms_style_num,
                    -- Similar masking as described above to collapse unnecessary changes
                    style_desc AS style_desc,
                    style_group_num,
                    CASE
                      WHEN rtrim(hist_0.rms_sku_num, ' ') IN(
                        '87562285', '87562287', '87562286', '87562284', '87562288', '87562289'
                      ) THEN regexp_replace(style_group_desc, r'\.\d\d\d\d\d$', '')
                      ELSE substr(style_desc, 1, 100)
                    END AS style_group_desc,sbclass_num, sbclass_name, class_num,class_name,dept_num,
                    dept_name, subdivision_num,subdivision_name,
                    division_num,
                    division_name,
                    gwp_ind,
                    smart_sample_ind,
                    vendor_brand_name,
                    supplier_num,
                    supplier_name,
                    payto_vendor_num,
                    payto_vendor_name,
                    npg_ind,
                    RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc) AS eff_period
                  FROM
                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.demand_product_detail_dim_vw AS hist_0
                    INNER JOIN delta_wrk_rms_sku_num AS wrk ON hist_0.rms_sku_num = wrk.rms_sku_num
                    CROSS JOIN UNNEST(ARRAY[
                      CASE
                        WHEN rtrim(hist_0.rms_sku_num, ' ') IN(
                          '87562285', '87562287', '87562286', '87562284', '87562288', '87562289'
                        ) THEN regexp_replace(substr(style_desc, 1, 100), r'\.\d\d\d\d\d$', '')
                        ELSE substr(style_desc, 1, 100)
                      END
                    ]) AS style_desc
                  WHERE eff_end_tmstp > DATE '2022-10-29'
              ) AS sku2 ON sku2.rms_sku_num = sku0.rms_sku_num
               AND sku2.channel_country = sku0.channel_country
               AND RANGE_OVERLAPS(sku2.eff_period,sku0.eff_period)
               --greatest(sku2.eff_period.lower, sku0.eff_period.lower) < least(sku2.eff_period.upper, sku0.eff_period.upper)
          )
         --ordered 
        ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,channel_country,country_count,vpn_num,sku_desc,color_num,color_desc,nord_display_color,rms_style_num,style_desc,style_group_num,style_group_desc,sbclass_num,sbclass_name,class_num,class_name,dept_num,dept_name,subdivision_num,subdivision_name,division_num,division_name,gwp_ind,smart_sample_ind,vendor_brand_name,supplier_num,supplier_name,payto_vendor_num,payto_vendor_name,npg_ind,return_disposition_code,selling_status_desc,selling_channel_eligibility_list,prmy_supp_num,
    range_group
ORDER BY  
    rms_sku_num,channel_country,country_count,vpn_num,sku_desc,color_num,color_desc,nord_display_color,rms_style_num,style_desc,style_group_num,style_group_desc,sbclass_num,sbclass_name,class_num,class_name,dept_num,dept_name,subdivision_num,subdivision_name,division_num,division_name,gwp_ind,smart_sample_ind,vendor_brand_name,supplier_num,supplier_name,payto_vendor_num,payto_vendor_name,npg_ind,return_disposition_code,selling_status_desc,selling_channel_eligibility_list,prmy_supp_num)
        AS hist
;

TRUNCATE TABLE delta_wrk_rms_sku_num;
/*main merge*/
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt USING (
  SELECT
      inv.rms_sku_num,
      inv.location_num,
      inv.reporting_date,
      sku.vpn_num AS vendor_product_num,
      sku.sku_desc,
      sku.color_num,
      sku.color_desc,
      sku.nord_display_color,
      sku.rms_style_num,
      sku.style_desc,
      sku.style_group_num,
      sku.style_group_desc,
      sku.sbclass_num,
      sku.sbclass_name,
      sku.class_num,
      sku.class_name,
      sku.dept_num,
      sku.dept_name,
      sku.subdivision_num,
      sku.subdivision_name,
      sku.division_num,
      sku.division_name,
      sku.npg_ind,
      sku.reclass_ind,
      sku.gwp_ind,
      sku.smart_sample_ind,
      sku.vendor_brand_name,
      sku.supplier_num,
      sku.supplier_name,
      sku.payto_vendor_num,
      sku.payto_vendor_name,
      sku.return_disposition_code,
      sku.selling_status_desc,
      sku.selling_channel_eligibility_list,
      sku.prmy_supp_num,
      (
        SELECT
            batch_id
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE upper(rtrim(subject_area_nm, ' ')) = 'NAP_ASCP_CLARITY_LOAD'
      ) AS dw_batch_id,
      (
        SELECT
            curr_batch_date
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE upper(rtrim(subject_area_nm, ' ')) = 'NAP_ASCP_CLARITY_LOAD'
      ) AS dw_batch_date
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact AS inv
      INNER JOIN delta_wrk AS wrk ON inv.rms_sku_num = wrk.rms_sku_num
       AND inv.location_num = wrk.location_num
       AND inv.reporting_date = wrk.reporting_date
      LEFT OUTER JOIN delta_wrk_product AS sku ON sku.rms_sku_num = inv.rms_sku_num
       AND (sku.channel_country = inv.store_country_code
       OR sku.country_count = 1)
       AND CAST(/* expression of unknown or erroneous type */ inv.reporting_date as TIMESTAMP) < sku.eff_end_tmstp
       AND CAST(/* expression of unknown or erroneous type */ inv.reporting_date as TIMESTAMP) >= sku.eff_begin_tmstp
) AS src
ON src.location_num = tgt.location_num
 AND src.rms_sku_num = tgt.rms_sku_num
 AND src.reporting_date = tgt.reporting_date
   WHEN MATCHED THEN UPDATE SET
    vendor_product_num = src.vendor_product_num, 
    sku_desc = src.sku_desc, 
    color_num = src.color_num, 
    color_desc = src.color_desc, 
    nord_display_color = src.nord_display_color, 
    rms_style_num = src.rms_style_num, 
    style_desc = src.style_desc, 
    style_group_num = src.style_group_num, 
    style_group_desc = src.style_group_desc, 
    sbclass_num = src.sbclass_num, 
    sbclass_name = src.sbclass_name, 
    class_num = src.class_num, 
    class_name = src.class_name, 
    dept_num = src.dept_num, 
    dept_name = src.dept_name, 
    subdivision_num = src.subdivision_num, 
    subdivision_name = src.subdivision_name, 
    division_num = src.division_num, 
    division_name = src.division_name, 
    npg_ind = src.npg_ind, 
    reclass_ind = src.reclass_ind, 
    gwp_ind = src.gwp_ind, 
    smart_sample_ind = src.smart_sample_ind, 
    vendor_brand_name = src.vendor_brand_name, 
    supplier_num = src.supplier_num, 
    supplier_name = src.supplier_name, 
    payto_vendor_num = src.payto_vendor_num, 
    payto_vendor_name = src.payto_vendor_name, 
    return_disposition_code = src.return_disposition_code, 
    selling_status_desc = src.selling_status_desc, 
    selling_channel_eligibility_list = src.selling_channel_eligibility_list, 
    prmy_supp_num = src.prmy_supp_num, 
    dw_batch_id = src.dw_batch_id, 
    dw_batch_date = src.dw_batch_date, 
    dw_sys_updt_tmstp = timestamp(current_datetime('PST8PDT')),
     dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
;



