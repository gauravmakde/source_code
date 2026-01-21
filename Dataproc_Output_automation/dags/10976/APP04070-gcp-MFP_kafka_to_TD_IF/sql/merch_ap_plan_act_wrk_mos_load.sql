---Task_Name=t0_5_ap_plan_act_wrk_mos_load;'

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country, month_num, fiscal_year_num, actual_mos_total_cost_amount, dw_batch_date, dw_sys_load_tmstp)
  SELECT
      max(coalesce(g01.supplier_group, '-1')) AS supplier_group,
      s01.department_num AS dept_num,
      coalesce(b01.banner_id, -1) AS banner_num,
      max(coalesce(b01.channel_country, 'NA')) AS channel_country,
      s01.month_num,
      s01.year_num AS fiscal_year_num,
      sum(s01.mos_total_cost) AS actual_mos_total_cost_amount,
      current_date('PST8PDT'),
      CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT') ) AS DATETIME)
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sp_mos_adjusted_sku_store_week_agg_fact_vw AS s01
      LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01 ON s01.channel_num = b01.channel_num
      LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01 ON s01.supplier_num = g01.supplier_num
       AND cast(s01.department_num  as string)= g01.dept_num
       AND b01.banner_code = g01.banner
       AND  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT') ) AS timestamp) >= eff_begin_tmstp_utc
       AND  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT') ) AS timestamp) < eff_end_tmstp_utc
    WHERE s01.week_num >= (
      SELECT
          min(week_idnt)
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
        WHERE s01.year_num = (
          SELECT
              current_fiscal_year_num - 2
            FROM
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
            WHERE LOWER(interface_code)  = LOWER('MERCH_NAP_SPE_DLY')
        )
    )
     AND s01.week_num <= (
      SELECT
          last_completed_fiscal_week_num
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')
    )
    GROUP BY upper(coalesce(g01.supplier_group, '-1')), 2, 3, upper(coalesce(b01.channel_country, 'NA')), 5, 6
;
