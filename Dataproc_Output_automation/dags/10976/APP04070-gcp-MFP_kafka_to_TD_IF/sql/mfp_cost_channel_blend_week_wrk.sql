/* SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_cost_channel_blend_week_fact_load;
Task_Name=channel_blend_data_load_channel_week_wrk_load;'
FOR SESSION VOLATILE;*/

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.mfp_cost_plan_actual_channel_wrk;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.mfp_cost_plan_actual_channel_wrk 
(dept_num, 
 week_num, 
 channel_num, 
 fulfill_type_num,
 cp_demand_flash_retail_amt, 
 cp_demand_persistent_retail_amt, 
 cp_demand_total_qty, 
 cp_demand_total_retail_amt,
 cp_gross_margin_retail_amt, 
 cp_gross_sales_retail_amt, 
 cp_gross_sales_qty, 
 cp_net_sales_retail_amt, 
 cp_net_sales_qty,
 cp_net_sales_cost_amt, 
 cp_net_sales_trunk_club_retail_amt, 
 cp_net_sales_trunk_club_qty, 
 cp_returns_retail_amt,
 cp_returns_qty, 
 cp_shrink_cost_amt, 
 cp_shrink_qty, 
 op_demand_flash_retail_amt, 
 op_demand_persistent_retail_amt,
 op_demand_total_qty, 
 op_demand_total_retail_amt, 
 op_gross_margin_retail_amt, 
 op_gross_sales_retail_amt,
 op_gross_sales_qty, 
 op_net_sales_retail_amt, 
 op_net_sales_qty, 
 op_net_sales_cost_amt,
 op_net_sales_trunk_club_retail_amt, 
 op_net_sales_trunk_club_qty, 
 op_returns_retail_amt, 
 op_returns_qty,
 op_shrink_cost_amt, 
 op_shrink_qty, 
 sp_demand_flash_retail_amt, 
 sp_demand_persistent_retail_amt, 
 sp_demand_total_qty,
 sp_demand_total_retail_amt, 
 sp_gross_margin_retail_amt, 
 sp_gross_sales_retail_amt, 
 sp_gross_sales_qty,
 sp_net_sales_retail_amt, 
 sp_net_sales_qty, 
 sp_net_sales_cost_amt, 
 sp_net_sales_trunk_club_retail_amt,
 sp_net_sales_trunk_club_qty, 
 sp_returns_retail_amt, 
 sp_returns_qty, 
 sp_shrink_cost_amt, 
 sp_shrink_qty,
 ty_demand_flash_retail_amt, 
 ty_demand_persistent_retail_amt, 
 ty_demand_total_qty, 
 ty_demand_total_retail_amt,
 ty_gross_margin_retail_amt, 
 ty_gross_sales_retail_amt, 
 ty_gross_sales_qty, 
 ty_net_sales_retail_amt, 
 ty_net_sales_qty,
 ty_net_sales_cost_amt, 
 ty_net_sales_trunk_club_retail_amt, 
 ty_net_sales_trunk_club_qty, 
 ty_returns_retail_amt,
 ty_returns_qty, 
 ty_shrink_cost_amt, 
 ty_shrink_qty, 
 ly_demand_flash_retail_amt, 
 ly_demand_persistent_retail_amt,
 ly_demand_total_qty, 
 ly_demand_total_retail_amt, 
 ly_gross_margin_retail_amt, 
 ly_gross_sales_retail_amt,
 ly_gross_sales_qty, 
 ly_net_sales_retail_amt, 
 ly_net_sales_qty, 
 ly_net_sales_cost_amt,
 ly_net_sales_trunk_club_retail_amt, 
 ly_net_sales_trunk_club_qty, 
 ly_returns_retail_amt, 
 ly_returns_qty,
 ly_shrink_cost_amt, 
 ly_shrink_qty, 
 lly_demand_flash_retail_amt, 
 lly_demand_persistent_retail_amt, 
 lly_demand_total_qty, 
 lly_demand_total_retail_amt, 
 lly_gross_margin_retail_amt, 
 lly_gross_sales_retail_amt, 
 lly_gross_sales_qty,
 lly_net_sales_retail_amt, 
 lly_net_sales_qty, 
 lly_net_sales_cost_amt, 
 lly_net_sales_trunk_club_retail_amt, 
 lly_net_sales_trunk_club_qty, 
 lly_returns_retail_amt, 
 lly_returns_qty, 
 lly_shrink_cost_amt, 
 lly_shrink_qty,
 dw_batch_date, 
 dw_sys_load_tmstp)
(SELECT a.dept_num,
  a.week_num,
  a.channel_num,
  a.fulfill_type_num,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_demand_flash_retail_amt
                      ELSE a.cp_demand_flash_retail_amt
                END) AS NUMERIC), 4) AS cp_demand_flash_retail_amt,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_demand_persistent_retail_amt
                      ELSE a.cp_demand_persistent_retail_amt
                      END) AS NUMERIC), 4) AS cp_demand_persistent_retail_amt,
  CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                THEN CAST(a.ty_demand_total_qty AS BIGNUMERIC)
                ELSE a.cp_demand_total_qty
                END) AS NUMERIC) AS cp_demand_total_qty,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_demand_total_retail_amt
                      ELSE a.cp_demand_total_retail_amt
                      END) AS NUMERIC), 4) AS cp_demand_total_retail_amt,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_gross_margin_retail_amt
                      ELSE a.cp_gross_margin_retail_amt
                      END) AS NUMERIC), 4) AS cp_gross_margin_retail_amt,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_gross_sales_retail_amt
                      ELSE a.cp_gross_sales_retail_amt
                      END) AS NUMERIC), 4) AS cp_gross_sales_retail_amt,
  CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                THEN CAST(a.ty_gross_sales_qty AS BIGNUMERIC)
                ELSE a.cp_gross_sales_qty
                END) AS NUMERIC) AS cp_gross_sales_qty,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_net_sales_retail_amt
                      ELSE a.cp_net_sales_retail_amt
                      END) AS NUMERIC), 4) AS cp_net_sales_retail_amt,
  CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                THEN CAST(a.ty_net_sales_qty AS BIGNUMERIC)
                ELSE a.cp_net_sales_qty
                END) AS NUMERIC) AS cp_net_sales_qty,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_net_sales_cost_amt
                      ELSE a.cp_net_sales_cost_amt
                      END) AS NUMERIC), 4) AS cp_net_sales_cost_amt,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_net_sales_trunk_club_retail_amt
                      ELSE a.cp_net_sales_trunk_club_retail_amt
                      END) AS NUMERIC), 4) AS cp_net_sales_trunk_club_retail_amt,
  CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                THEN CAST(a.ty_net_sales_trunk_club_qty AS BIGNUMERIC)
                ELSE a.cp_net_sales_trunk_club_qty
                END) AS NUMERIC) AS cp_net_sales_trunk_club_qty,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_returns_retail_amt
                      ELSE a.cp_returns_retail_amt
                      END) AS NUMERIC), 4) AS cp_returns_retail_amt,
  CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                THEN CAST(a.ty_returns_qty AS BIGNUMERIC)
                ELSE a.cp_returns_qty
                END) AS NUMERIC) AS cp_returns_qty,
  ROUND(CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
                      THEN a.ty_shrink_cost_amt
                      ELSE a.cp_shrink_cost_amt
                      END) AS NUMERIC), 4) AS cp_shrink_cost_amt,
  CAST(SUM(CASE WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_shrink_qty AS BIGNUMERIC)
     ELSE a.cp_shrink_qty
     END) AS NUMERIC) AS cp_shrink_qty,
  ROUND(CAST(SUM(a.op_demand_flash_retail_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.op_demand_persistent_retail_amt) AS NUMERIC), 4),
  SUM(a.op_demand_total_qty),
  ROUND(CAST(SUM(a.op_demand_total_retail_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.op_gross_margin_retail_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.op_gross_sales_retail_amt) AS NUMERIC), 4),
  SUM(a.op_gross_sales_qty),
  ROUND(CAST(SUM(a.op_net_sales_retail_amt) AS NUMERIC), 4),
  SUM(a.op_net_sales_qty),
  ROUND(CAST(SUM(a.op_net_sales_cost_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.op_net_sales_trunk_club_retail_amt) AS NUMERIC), 4),
  SUM(a.op_net_sales_trunk_club_qty),
  ROUND(CAST(SUM(a.op_returns_retail_amt) AS NUMERIC), 4),
  SUM(a.op_returns_qty),
  ROUND(CAST(SUM(a.op_shrink_cost_amt) AS NUMERIC), 4),
  SUM(a.op_shrink_qty),
  ROUND(CAST(SUM(a.sp_demand_flash_retail_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.sp_demand_persistent_retail_amt) AS NUMERIC), 4),
  SUM(a.sp_demand_total_qty),
  ROUND(CAST(SUM(a.sp_demand_total_retail_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.sp_gross_margin_retail_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.sp_gross_sales_retail_amt) AS NUMERIC), 4),
  SUM(a.sp_gross_sales_qty),
  ROUND(CAST(SUM(a.sp_net_sales_retail_amt) AS NUMERIC), 4),
  SUM(a.sp_net_sales_qty),
  ROUND(CAST(SUM(a.sp_net_sales_cost_amt) AS NUMERIC), 4),
  ROUND(CAST(SUM(a.sp_net_sales_trunk_club_retail_amt) AS NUMERIC), 4),
  SUM(a.sp_net_sales_trunk_club_qty),
  ROUND(CAST(SUM(a.sp_returns_retail_amt) AS NUMERIC), 4),
  SUM(a.sp_returns_qty),
  ROUND(CAST(SUM(a.sp_shrink_cost_amt) AS NUMERIC), 4),
  SUM(a.sp_shrink_qty),
  SUM(a.ty_demand_flash_retail_amt),
  SUM(a.ty_demand_persistent_retail_amt),
  SUM(a.ty_demand_total_qty),
  SUM(a.ty_demand_total_retail_amt),
  SUM(a.ty_gross_margin_retail_amt),
  SUM(a.ty_gross_sales_retail_amt),
  SUM(a.ty_gross_sales_qty),
  SUM(a.ty_net_sales_retail_amt),
  SUM(a.ty_net_sales_qty),
  SUM(a.ty_net_sales_cost_amt),
  SUM(a.ty_net_sales_trunk_club_retail_amt),
  SUM(a.ty_net_sales_trunk_club_qty),
  SUM(a.ty_returns_retail_amt),
  SUM(a.ty_returns_qty),
  SUM(a.ty_shrink_cost_amt),
  SUM(a.ty_shrink_qty),
  SUM(a.ly_demand_flash_retail_amt),
  SUM(a.ly_demand_persistent_retail_amt),
  SUM(a.ly_demand_total_qty),
  SUM(a.ly_demand_total_retail_amt),
  SUM(a.ly_gross_margin_retail_amt),
  SUM(a.ly_gross_sales_retail_amt),
  SUM(a.ly_gross_sales_qty),
  SUM(a.ly_net_sales_retail_amt),
  SUM(a.ly_net_sales_qty),
  SUM(a.ly_net_sales_cost_amt),
  SUM(a.ly_net_sales_trunk_club_retail_amt),
  SUM(a.ly_net_sales_trunk_club_qty),
  SUM(a.ly_returns_retail_amt),
  SUM(a.ly_returns_qty),
  SUM(a.ly_shrink_cost_amt),
  SUM(a.ly_shrink_qty),
  SUM(a.lly_demand_flash_retail_amt),
  SUM(a.lly_demand_persistent_retail_amt),
  SUM(a.lly_demand_total_qty),
  SUM(a.lly_demand_total_retail_amt),
  SUM(a.lly_gross_margin_retail_amt),
  SUM(a.lly_gross_sales_retail_amt),
  SUM(a.lly_gross_sales_qty),
  SUM(a.lly_net_sales_retail_amt),
  SUM(a.lly_net_sales_qty),
  SUM(a.lly_net_sales_cost_amt),
  SUM(a.lly_net_sales_trunk_club_retail_amt),
  SUM(a.lly_net_sales_trunk_club_qty),
  SUM(a.lly_returns_retail_amt),
  SUM(a.lly_returns_qty),
  SUM(a.lly_shrink_cost_amt),
  SUM(a.lly_shrink_qty),
  MAX(a.dw_batch_dt) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM (SELECT mfcpf.department_number AS dept_num,
        mfcpf.week_id AS week_num,
        CAST(trunc(cast(mfcpf.channel_id as float64)) AS INTEGER) AS channel_num,
        aimd.fulfill_type_num,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.flash_demand_retail_amount
                 ELSE 0
            END) AS cp_demand_flash_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.persistent_demand_retail_amount
                 ELSE 0
            END) AS cp_demand_persistent_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.total_demand_units
                 ELSE 0
            END) AS cp_demand_total_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.total_demand_dollar_amount
                 ELSE 0
            END) AS cp_demand_total_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.gross_margin_retail_amount
                 ELSE 0
            END) AS cp_gross_margin_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.gross_sales_dollar_amount
                 ELSE 0
            END) AS cp_gross_sales_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.gross_sales_units
                 ELSE 0
            END) AS cp_gross_sales_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.net_sales_retail_amount
                 ELSE 0
           END) AS cp_net_sales_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.net_sales_units
                 ELSE 0
           END) AS cp_net_sales_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.net_sales_dollar_amount
                 ELSE 0
           END) AS cp_net_sales_cost_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.net_sales_trunk_club_dollar_amount
                 ELSE 0
           END) AS cp_net_sales_trunk_club_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.net_sales_trunk_club_units
                 ELSE 0
           END) AS cp_net_sales_trunk_club_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.returns_dollar_amount
                 ELSE 0
            END) AS cp_returns_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.returns_units
                 ELSE 0
            END) AS cp_returns_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.shrink_dollar_amount
                 ELSE 0
            END) AS cp_shrink_cost_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('CURRENT_PLAN')
                 THEN mfcpf.shrink_units
                 ELSE 0
            END) AS cp_shrink_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.flash_demand_retail_amount
                 ELSE 0
            END) AS op_demand_flash_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.persistent_demand_retail_amount
                 ELSE 0
            END) AS op_demand_persistent_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.total_demand_units
                 ELSE 0
            END) AS op_demand_total_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.total_demand_dollar_amount
                 ELSE 0
            END) AS op_demand_total_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.gross_margin_retail_amount
                 ELSE 0
            END) AS op_gross_margin_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.gross_sales_dollar_amount
                 ELSE 0
            END) AS op_gross_sales_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.gross_sales_units
                 ELSE 0
            END) AS op_gross_sales_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.net_sales_retail_amount
                 ELSE 0
            END) AS op_net_sales_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.net_sales_units
                 ELSE 0
            END) AS op_net_sales_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.net_sales_dollar_amount
                 ELSE 0
            END) AS op_net_sales_cost_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.net_sales_trunk_club_dollar_amount
                 ELSE 0
            END) AS op_net_sales_trunk_club_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.net_sales_trunk_club_units
                 ELSE 0
            END) AS op_net_sales_trunk_club_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.returns_dollar_amount
                 ELSE 0
            END) AS op_returns_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.returns_units
                 ELSE 0
            END) AS op_returns_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.shrink_dollar_amount
                 ELSE 0
            END) AS op_shrink_cost_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
                 THEN mfcpf.shrink_units
                 ELSE 0
            END) AS op_shrink_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.flash_demand_retail_amount
                 ELSE 0
            END) AS sp_demand_flash_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.persistent_demand_retail_amount
                 ELSE 0
            END) AS sp_demand_persistent_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.total_demand_units
                 ELSE 0
            END) AS sp_demand_total_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.total_demand_dollar_amount
                 ELSE 0
            END) AS sp_demand_total_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.gross_margin_retail_amount
                 ELSE 0
            END) AS sp_gross_margin_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.gross_sales_dollar_amount
                 ELSE 0
            END) AS sp_gross_sales_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.gross_sales_units
                 ELSE 0
            END) AS sp_gross_sales_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.net_sales_retail_amount
                 ELSE 0
            END) AS sp_net_sales_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.net_sales_units
                 ELSE 0
            END) AS sp_net_sales_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.net_sales_dollar_amount
                 ELSE 0
            END) AS sp_net_sales_cost_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.net_sales_trunk_club_dollar_amount
                 ELSE 0
            END) AS sp_net_sales_trunk_club_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.net_sales_trunk_club_units
                 ELSE 0
            END) AS sp_net_sales_trunk_club_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.returns_dollar_amount
                 ELSE 0
            END) AS sp_returns_retail_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.returns_units
                 ELSE 0
            END) AS sp_returns_qty,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.shrink_dollar_amount
                 ELSE 0
            END) AS sp_shrink_cost_amt,
        SUM(CASE WHEN LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN')
                 THEN mfcpf.shrink_units
                 ELSE 0
            END) AS sp_shrink_qty,
        SUM(0) AS ty_demand_flash_retail_amt,
        SUM(0) AS ty_demand_persistent_retail_amt,
        SUM(0) AS ty_demand_total_qty,
        SUM(0) AS ty_demand_total_retail_amt,
        SUM(0) AS ty_gross_margin_retail_amt,
        SUM(0) AS ty_gross_sales_retail_amt,
        SUM(0) AS ty_gross_sales_qty,
        SUM(0) AS ty_net_sales_retail_amt,
        SUM(0) AS ty_net_sales_qty,
        SUM(0) AS ty_net_sales_cost_amt,
        SUM(0) AS ty_net_sales_trunk_club_retail_amt,
        SUM(0) AS ty_net_sales_trunk_club_qty,
        SUM(0) AS ty_returns_retail_amt,
        SUM(0) AS ty_returns_qty,
        SUM(0) AS ty_shrink_cost_amt,
        SUM(0) AS ty_shrink_qty,
        SUM(0) AS ly_demand_flash_retail_amt,
        SUM(0) AS ly_demand_persistent_retail_amt,
        SUM(0) AS ly_demand_total_qty,
        SUM(0) AS ly_demand_total_retail_amt,
        SUM(0) AS ly_gross_margin_retail_amt,
        SUM(0) AS ly_gross_sales_retail_amt,
        SUM(0) AS ly_gross_sales_qty,
        SUM(0) AS ly_net_sales_retail_amt,
        SUM(0) AS ly_net_sales_qty,
        SUM(0) AS ly_net_sales_cost_amt,
        SUM(0) AS ly_net_sales_trunk_club_retail_amt,
        SUM(0) AS ly_net_sales_trunk_club_qty,
        SUM(0) AS ly_returns_retail_amt,
        SUM(0) AS ly_returns_qty,
        SUM(0) AS ly_shrink_cost_amt,
        SUM(0) AS ly_shrink_qty,
        SUM(0) AS lly_demand_flash_retail_amt,
        SUM(0) AS lly_demand_persistent_retail_amt,
        SUM(0) AS lly_demand_total_qty,
        SUM(0) AS lly_demand_total_retail_amt,
        SUM(0) AS lly_gross_margin_retail_amt,
        SUM(0) AS lly_gross_sales_retail_amt,
        SUM(0) AS lly_gross_sales_qty,
        SUM(0) AS lly_net_sales_retail_amt,
        SUM(0) AS lly_net_sales_qty,
        SUM(0) AS lly_net_sales_cost_amt,
        SUM(0) AS lly_net_sales_trunk_club_retail_amt,
        SUM(0) AS lly_net_sales_trunk_club_qty,
        SUM(0) AS lly_returns_retail_amt,
        SUM(0) AS lly_returns_qty,
        SUM(0) AS lly_shrink_cost_amt,
        SUM(0) AS lly_shrink_qty,
        mtbv.dw_batch_dt
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_channel_plan_fct AS mfcpf
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.alternate_inventory_model_dim AS aimd 
       ON LOWER(mfcpf.alternate_inventory_model) = LOWER(aimd.fulfill_type_desc)
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv 
       ON LOWER(mtbv.interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')
       GROUP BY dept_num,
       week_num,
       channel_num,
       aimd.fulfill_type_num,
       mtbv.dw_batch_dt
UNION ALL
SELECT COALESCE(ddcwv.dept_num, srif.department_num, sif.department_num) AS dept_num,
COALESCE(ddcwv.ord_week_num, srif.week_num, sif.week_num) AS week_num,
COALESCE(ddcwv.channel_num, srif.channel_num, sif.channel_num) AS channel_num,
COALESCE(ddcwv.fulfill_id, srif.fulfilment_num, sif.fulfilment_num) AS fulfill_type_num,
0 AS cp_demand_flash_retail_amt,
0 AS cp_demand_persistent_retail_amt,
0 AS cp_demand_total_qty,
0 AS cp_demand_total_retail_amt,
0 AS cp_gross_margin_retail_amt,
0 AS cp_gross_sales_retail_amt,
0 AS cp_gross_sales_qty,
0 AS cp_net_sales_retail_amt,
0 AS cp_net_sales_qty,
0 AS cp_net_sales_cost_amt,
0 AS cp_net_sales_trunk_club_retail_amt,
0 AS cp_net_sales_trunk_club_qty,
0 AS cp_returns_retail_amt,
0 AS cp_returns_qty,
0 AS cp_shrink_cost_amt,
0 AS cp_shrink_qty,
0 AS op_demand_flash_retail_amt,
0 AS op_demand_persistent_retail_amt,
0 AS op_demand_total_qty,
0 AS op_demand_total_retail_amt,
0 AS op_gross_margin_retail_amt,
0 AS op_gross_sales_retail_amt,
0 AS op_gross_sales_qty,
0 AS op_net_sales_retail_amt,
0 AS op_net_sales_qty,
0 AS op_net_sales_cost_amt,
0 AS op_net_sales_trunk_club_retail_amt,
0 AS op_net_sales_trunk_club_qty,
0 AS op_returns_retail_amt,
0 AS op_returns_qty,
0 AS op_shrink_cost_amt,
0 AS op_shrink_qty,
0 AS sp_demand_flash_retail_amt,
0 AS sp_demand_persistent_retail_amt,
0 AS sp_demand_total_qty,
0 AS sp_demand_total_retail_amt,
0 AS sp_gross_margin_retail_amt,
0 AS sp_gross_sales_retail_amt,
0 AS sp_gross_sales_qty,
0 AS sp_net_sales_retail_amt,
0 AS sp_net_sales_qty,
0 AS sp_net_sales_cost_amt,
0 AS sp_net_sales_trunk_club_retail_amt,
0 AS sp_net_sales_trunk_club_qty,
0 AS sp_returns_retail_amt,
0 AS sp_returns_qty,
0 AS sp_shrink_cost_amt,
0 AS sp_shrink_qty,
ROUND(CAST(COALESCE(ddcwv.dmnd_flash_retl, FORMAT('%4d', 0)) AS NUMERIC), 4) AS ty_demand_flash_retail_amt,
ROUND(CAST(COALESCE(ddcwv.dmnd_persistent_retl, FORMAT('%4d', 0)) AS NUMERIC), 4) AS
ty_demand_persistent_retail_amt,
CAST(COALESCE(ddcwv.dmnd_flash_units + ddcwv.dmnd_persistent_units, 0) AS BIGINT) AS ty_demand_total_qty,
ROUND(CAST(COALESCE(CAST(ddcwv.dmnd_flash_retl AS FLOAT64) + CAST(ddcwv.dmnd_persistent_retl AS FLOAT64), 0) AS NUMERIC), 4) AS ty_demand_total_retail_amt,
COALESCE(srif.gross_margin_retail, 0) AS ty_gross_margin_retail_amt,
CAST(COALESCE(srif.net_sales_retail + srif.returns_retail, 0) AS NUMERIC) AS ty_gross_sales_retail_amt,
COALESCE(srif.net_sales_units + srif.returns_units, 0) AS ty_gross_sales_qty,
COALESCE(srif.net_sales_retail, 0) AS ty_net_sales_retail_amt,
COALESCE(srif.net_sales_units, 0) AS ty_net_sales_qty,
COALESCE(srif.net_sales_cost, 0) AS ty_net_sales_cost_amt,
0 AS ty_net_sales_trunk_club_retail_amt,
0 AS ty_net_sales_trunk_club_qty,
COALESCE(srif.returns_retail, 0) AS ty_returns_retail_amt,
COALESCE(srif.returns_units, 0) AS ty_returns_qty,
COALESCE(sif.shrink_cost, 0) AS ty_shrink_cost_amt,
COALESCE(sif.shrink_units, 0) AS ty_shrink_qty,
0 AS ly_demand_flash_retail_amt,
0 AS ly_demand_persistent_retail_amt,
0 AS ly_demand_total_qty,
0 AS ly_demand_total_retail_amt,
0 AS ly_gross_margin_retail_amt,
0 AS ly_gross_sales_retail_amt,
0 AS ly_gross_sales_qty,
0 AS ly_net_sales_retail_amt,
0 AS ly_net_sales_qty,
0 AS ly_net_sales_cost_amt,
0 AS ly_net_sales_trunk_club_retail_amt,
0 AS ly_net_sales_trunk_club_qty,
0 AS ly_returns_retail_amt,
0 AS ly_returns_qty,
0 AS ly_shrink_cost_amt,
0 AS ly_shrink_qty,
0 AS lly_demand_flash_retail_amt,
0 AS lly_demand_persistent_retail_amt,
0 AS lly_demand_total_qty,
0 AS lly_demand_total_retail_amt,
0 AS lly_gross_margin_retail_amt,
0 AS lly_gross_sales_retail_amt,
0 AS lly_gross_sales_qty,
0 AS lly_net_sales_retail_amt,
0 AS lly_net_sales_qty,
0 AS lly_net_sales_cost_amt,
0 AS lly_net_sales_trunk_club_retail_amt,
0 AS lly_net_sales_trunk_club_qty,
0 AS lly_returns_retail_amt,
0 AS lly_returns_qty,
0 AS lly_shrink_cost_amt,
0 AS lly_shrink_qty,
mtbv0.dw_batch_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_demand_dept_chnl_week_agg_vw AS ddcwv
FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.sales_returns_insight_fact AS srif 
ON ddcwv.ord_week_num = srif.week_num 
AND ddcwv.dept_num =srif.department_num 
AND ddcwv.channel_num = srif.channel_num 
AND ddcwv.fulfill_id = srif.fulfilment_num
FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.shrink_insight_fact AS sif 
ON ddcwv.ord_week_num = sif.week_num 
AND ddcwv.dept_num = sif.department_num
AND ddcwv.channel_num = sif.channel_num
AND ddcwv.fulfill_id = sif.fulfilment_num
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv0 
ON LOWER(mtbv0.interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')
UNION ALL
SELECT COALESCE(ddcwv0.dept_num, srif0.department_num, sif0.department_num) AS dept_num,
wcd.week_num,
COALESCE(ddcwv0.channel_num, srif0.channel_num, sif0.channel_num) AS channel_num,
COALESCE(ddcwv0.fulfill_id, srif0.fulfilment_num, sif0.fulfilment_num) AS fulfill_type_num,
0 AS cp_demand_flash_retail_amt,
0 AS cp_demand_persistent_retail_amt,
0 AS cp_demand_total_qty,
0 AS cp_demand_total_retail_amt,
0 AS cp_gross_margin_retail_amt,
0 AS cp_gross_sales_retail_amt,
0 AS cp_gross_sales_qty,
0 AS cp_net_sales_retail_amt,
0 AS cp_net_sales_qty,
0 AS cp_net_sales_cost_amt,
0 AS cp_net_sales_trunk_club_retail_amt,
0 AS cp_net_sales_trunk_club_qty,
0 AS cp_returns_retail_amt,
0 AS cp_returns_qty,
0 AS cp_shrink_cost_amt,
0 AS cp_shrink_qty,
0 AS op_demand_flash_retail_amt,
0 AS op_demand_persistent_retail_amt,
0 AS op_demand_total_qty,
0 AS op_demand_total_retail_amt,
0 AS op_gross_margin_retail_amt,
0 AS op_gross_sales_retail_amt,
0 AS op_gross_sales_qty,
0 AS op_net_sales_retail_amt,
0 AS op_net_sales_qty,
0 AS op_net_sales_cost_amt,
0 AS op_net_sales_trunk_club_retail_amt,
0 AS op_net_sales_trunk_club_qty,
0 AS op_returns_retail_amt,
0 AS op_returns_qty,
0 AS op_shrink_cost_amt,
0 AS op_shrink_qty,
0 AS sp_demand_flash_retail_amt,
0 AS sp_demand_persistent_retail_amt,
0 AS sp_demand_total_qty,
0 AS sp_demand_total_retail_amt,
0 AS sp_gross_margin_retail_amt,
0 AS sp_gross_sales_retail_amt,
0 AS sp_gross_sales_qty,
0 AS sp_net_sales_retail_amt,
0 AS sp_net_sales_qty,
0 AS sp_net_sales_cost_amt,
0 AS sp_net_sales_trunk_club_retail_amt,
0 AS sp_net_sales_trunk_club_qty,
0 AS sp_returns_retail_amt,
0 AS sp_returns_qty,
0 AS sp_shrink_cost_amt,
0 AS sp_shrink_qty,
0 AS ty_demand_flash_retail_amt,
0 AS ty_demand_persistent_retail_amt,
0 AS ty_demand_total_qty,
0 AS ty_demand_total_retail_amt,
0 AS ty_gross_margin_retail_amt,
0 AS ty_gross_sales_retail_amt,
0 AS ty_gross_sales_qty,
0 AS ty_net_sales_retail_amt,
0 AS ty_net_sales_qty,
0 AS ty_net_sales_cost_amt,
0 AS ty_net_sales_trunk_club_retail_amt,
0 AS ty_net_sales_trunk_club_qty,
0 AS ty_returns_retail_amt,
0 AS ty_returns_qty,
0 AS ty_shrink_cost_amt,
0 AS ty_shrink_qty,
ROUND(CAST(COALESCE(ddcwv0.dmnd_flash_retl, FORMAT('%4d', 0)) AS NUMERIC), 4) AS ly_demand_flash_retail_amt,
ROUND(CAST(COALESCE(ddcwv0.dmnd_persistent_retl, FORMAT('%4d', 0)) AS NUMERIC), 4) AS ly_demand_persistent_retail_amt,
CAST(COALESCE(ddcwv0.dmnd_flash_units + ddcwv0.dmnd_persistent_units, 0) AS BIGINT) AS ly_demand_total_qty,
ROUND(CAST(COALESCE(CAST(ddcwv0.dmnd_flash_retl AS FLOAT64) + CAST(ddcwv0.dmnd_persistent_retl AS FLOAT64), 0) AS NUMERIC), 4) AS ly_demand_total_retail_amt,
COALESCE(srif0.gross_margin_retail, 0) AS ly_gross_margin_retail_amt,
CAST(COALESCE(srif0.net_sales_retail + srif0.returns_retail, 0) AS NUMERIC) AS ly_gross_sales_retail_amt,COALESCE(srif0.net_sales_units + srif0.returns_units, 0) AS ly_gross_sales_qty,
COALESCE(srif0.net_sales_retail, 0) AS ly_net_sales_retail_amt,
COALESCE(srif0.net_sales_units, 0) AS ly_net_sales_qty,
COALESCE(srif0.net_sales_cost, 0) AS ly_net_sales_cost_amt,
0 AS ly_net_sales_trunk_club_retail_amt,
0 AS ly_net_sales_trunk_club_qty,
COALESCE(srif0.returns_retail, 0) AS ly_returns_retail_amt,
COALESCE(srif0.returns_units, 0) AS ly_returns_qty,
COALESCE(sif0.shrink_cost, 0) AS ly_shrink_cost_amt,
COALESCE(sif0.shrink_units, 0) AS ly_shrink_qty,
0 AS lly_demand_flash_retail_amt,
0 AS lly_demand_persistent_retail_amt,
0 AS lly_demand_total_qty,
0 AS lly_demand_total_retail_amt,
0 AS lly_gross_margin_retail_amt,
0 AS lly_gross_sales_retail_amt,
0 AS lly_gross_sales_qty,
0 AS lly_net_sales_retail_amt,
0 AS lly_net_sales_qty,
0 AS lly_net_sales_cost_amt,
0 AS lly_net_sales_trunk_club_retail_amt,
0 AS lly_net_sales_trunk_club_qty,
0 AS lly_returns_retail_amt,
0 AS lly_returns_qty,
0 AS lly_shrink_cost_amt,
0 AS lly_shrink_qty,
mtbv1.dw_batch_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_demand_dept_chnl_week_agg_vw AS ddcwv0
FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.sales_returns_insight_fact AS srif0 
ON ddcwv0.ord_week_num = srif0.week_num 
AND ddcwv0.dept_num = srif0.department_num 
AND ddcwv0.channel_num = srif0.channel_num 
AND ddcwv0.fulfill_id = srif0.fulfilment_num
FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.shrink_insight_fact AS sif0 
ON ddcwv0.ord_week_num = sif0.week_num AND ddcwv0.dept_num = sif0.department_num 
AND ddcwv0.channel_num = sif0.channel_num 
AND ddcwv0.fulfill_id = sif0.fulfilment_num
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd 
ON wcd.ly_week_num = COALESCE(ddcwv0.ord_week_num, srif0.week_num,sif0.week_num) 
AND wcd.ly_week_num <> 444444
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv1 
ON LOWER(mtbv1.interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')
UNION ALL
    SELECT iifbv.dept_num,
     wcd0.week_num,
     iifbv.channel_num,
     iifbv.fulfill_type_num,
     0 AS cp_demand_flash_retail_amt,
     0 AS cp_demand_persistent_retail_amt,
     0 AS cp_demand_total_qty,
     0 AS cp_demand_total_retail_amt,
     0 AS cp_gross_margin_retail_amt,
     0 AS cp_gross_sales_retail_amt,
     0 AS cp_gross_sales_qty,
     0 AS cp_net_sales_retail_amt,
     0 AS cp_net_sales_qty,
     0 AS cp_net_sales_cost_amt,
     0 AS cp_net_sales_trunk_club_retail_amt,
     0 AS cp_net_sales_trunk_club_qty,
     0 AS cp_returns_retail_amt,
     0 AS cp_returns_qty,
     0 AS cp_shrink_cost_amt,
     0 AS cp_shrink_qty,
     0 AS op_demand_flash_retail_amt,
     0 AS op_demand_persistent_retail_amt,
     0 AS op_demand_total_qty,
     0 AS op_demand_total_retail_amt,
     0 AS op_gross_margin_retail_amt,
     0 AS op_gross_sales_retail_amt,
     0 AS op_gross_sales_qty,
     0 AS op_net_sales_retail_amt,
     0 AS op_net_sales_qty,
     0 AS op_net_sales_cost_amt,
     0 AS op_net_sales_trunk_club_retail_amt,
     0 AS op_net_sales_trunk_club_qty,
     0 AS op_returns_retail_amt,
     0 AS op_returns_qty,
     0 AS op_shrink_cost_amt,
     0 AS op_shrink_qty,
     0 AS sp_demand_flash_retail_amt,
     0 AS sp_demand_persistent_retail_amt,
     0 AS sp_demand_total_qty,
     0 AS sp_demand_total_retail_amt,
     0 AS sp_gross_margin_retail_amt,
     0 AS sp_gross_sales_retail_amt,
     0 AS sp_gross_sales_qty,
     0 AS sp_net_sales_retail_amt,
     0 AS sp_net_sales_qty,
     0 AS sp_net_sales_cost_amt,
     0 AS sp_net_sales_trunk_club_retail_amt,
     0 AS sp_net_sales_trunk_club_qty,
     0 AS sp_returns_retail_amt,
     0 AS sp_returns_qty,
     0 AS sp_shrink_cost_amt,
     0 AS sp_shrink_qty,
     0 AS ty_demand_flash_retail_amt,
     0 AS ty_demand_persistent_retail_amt,
     0 AS ty_demand_total_qty,
     0 AS ty_demand_total_retail_amt,
     0 AS ty_gross_margin_retail_amt,
     0 AS ty_gross_sales_retail_amt,
     0 AS ty_gross_sales_qty,
     0 AS ty_net_sales_retail_amt,
     0 AS ty_net_sales_qty,
     0 AS ty_net_sales_cost_amt,
     0 AS ty_net_sales_trunk_club_retail_amt,
     0 AS ty_net_sales_trunk_club_qty,
     0 AS ty_returns_retail_amt,
     0 AS ty_returns_qty,
     0 AS ty_shrink_cost_amt,
     0 AS ty_shrink_qty,
     ROUND(CAST(iifbv.cp_demand_flash_retail_amt AS NUMERIC), 4) AS ly_demand_flash_retail_amt,
     ROUND(CAST(iifbv.cp_demand_persistent_retail_amt AS NUMERIC), 4) AS ly_demand_persistent_retail_amt,
     CAST(iifbv.cp_demand_total_qty AS BIGINT) AS ly_demand_total_qty,
     ROUND(CAST(iifbv.cp_demand_total_retail_amt AS NUMERIC), 4) AS ly_demand_total_retail_amt,
     ROUND(CAST(iifbv.cp_gross_margin_retail_amt AS NUMERIC), 4) AS ly_gross_margin_retail_amt,
     ROUND(CAST(iifbv.cp_gross_sales_retail_amt AS NUMERIC), 4) AS ly_gross_sales_retail_amt,
     CAST(iifbv.cp_gross_sales_qty AS BIGINT) AS ly_gross_sales_qty,
     ROUND(CAST(iifbv.cp_net_sales_retail_amt AS NUMERIC), 4) AS ly_net_sales_retail_amt,
     CAST(iifbv.cp_net_sales_qty AS BIGINT) AS ly_net_sales_qty,
     ROUND(CAST(iifbv.cp_net_sales_cost_amt AS NUMERIC), 4) AS ly_net_sales_cost_amt,
     ROUND(CAST(iifbv.cp_net_sales_trunk_club_retail_amt AS NUMERIC), 4) AS ly_net_sales_trunk_club_retail_amt,
     CAST(iifbv.cp_net_sales_trunk_club_qty AS BIGINT) AS ly_net_sales_trunk_club_qty,
     ROUND(CAST(iifbv.cp_returns_retail_amt AS NUMERIC), 4) AS ly_returns_retail_amt,
     CAST(iifbv.cp_returns_qty AS BIGINT) AS ly_returns_qty,
     ROUND(CAST(iifbv.cp_shrink_cost_amt AS NUMERIC), 4) AS ly_shrink_cost_amt,
     CAST(iifbv.cp_shrink_qty AS BIGINT) AS ly_shrink_qty,
     0 AS lly_demand_flash_retail_amt,
     0 AS lly_demand_persistent_retail_amt,
     0 AS lly_demand_total_qty,
     0 AS lly_demand_total_retail_amt,
     0 AS lly_gross_margin_retail_amt,
     0 AS lly_gross_sales_retail_amt,
     0 AS lly_gross_sales_qty,
     0 AS lly_net_sales_retail_amt,
     0 AS lly_net_sales_qty,
     0 AS lly_net_sales_cost_amt,
     0 AS lly_net_sales_trunk_club_retail_amt,
     0 AS lly_net_sales_trunk_club_qty,
     0 AS lly_returns_retail_amt,
     0 AS lly_returns_qty,
     0 AS lly_shrink_cost_amt,
     0 AS lly_shrink_qty,
     iifbv.dw_batch_dt
    FROM (SELECT mfcpf0.department_number AS dept_num,
          mfcpf0.week_id AS week_num,
         CAST(trunc(cast(mfcpf0.channel_id as float64)) AS INTEGER) AS channel_num,
         aimd0.fulfill_type_num,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.flash_demand_retail_amount
              ELSE 0
         END AS cp_demand_flash_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.persistent_demand_retail_amount
              ELSE 0
         END AS cp_demand_persistent_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.total_demand_units
              ELSE 0
         END AS cp_demand_total_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.total_demand_dollar_amount
              ELSE 0
         END AS cp_demand_total_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.gross_margin_retail_amount
              ELSE 0
         END AS cp_gross_margin_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.gross_sales_dollar_amount
              ELSE 0
         END AS cp_gross_sales_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.gross_sales_units
              ELSE 0
         END AS cp_gross_sales_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.net_sales_retail_amount
              ELSE 0
         END AS cp_net_sales_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.net_sales_units
              ELSE 0
         END AS cp_net_sales_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.net_sales_dollar_amount
              ELSE 0
         END AS cp_net_sales_cost_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.net_sales_trunk_club_dollar_amount
              ELSE 0
         END AS cp_net_sales_trunk_club_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.net_sales_trunk_club_units
              ELSE 0
         END AS cp_net_sales_trunk_club_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.returns_dollar_amount
              ELSE 0
         END AS cp_returns_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.returns_units
              ELSE 0
         END AS cp_returns_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.shrink_dollar_amount
              ELSE 0
         END AS cp_shrink_cost_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
              THEN mfcpf0.shrink_units
              ELSE 0
         END AS cp_shrink_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.flash_demand_retail_amount
              ELSE 0
         END AS op_demand_flash_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.persistent_demand_retail_amount
              ELSE 0
         END AS op_demand_persistent_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.total_demand_units
              ELSE 0
         END AS op_demand_total_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.total_demand_dollar_amount
              ELSE 0
         END AS op_demand_total_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.gross_margin_retail_amount
              ELSE 0
         END AS op_gross_margin_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.gross_sales_dollar_amount
              ELSE 0
         END AS op_gross_sales_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.gross_sales_units
              ELSE 0
         END AS op_gross_sales_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.net_sales_retail_amount
              ELSE 0
         END AS op_net_sales_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.net_sales_units
              ELSE 0
         END AS op_net_sales_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.net_sales_dollar_amount
              ELSE 0
         END AS op_net_sales_cost_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.net_sales_trunk_club_dollar_amount
              ELSE 0
         END AS op_net_sales_trunk_club_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.net_sales_trunk_club_units
              ELSE 0
         END AS op_net_sales_trunk_club_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.returns_dollar_amount
              ELSE 0
         END AS op_returns_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.returns_units
              ELSE 0
         END AS op_returns_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.shrink_dollar_amount
              ELSE 0
         END AS op_shrink_cost_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
              THEN mfcpf0.shrink_units
              ELSE 0
         END AS op_shrink_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.flash_demand_retail_amount
              ELSE 0
         END AS sp_demand_flash_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.persistent_demand_retail_amount
              ELSE 0
         END AS sp_demand_persistent_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.total_demand_units
              ELSE 0
         END AS sp_demand_total_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.total_demand_dollar_amount
              ELSE 0
         END AS sp_demand_total_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.gross_margin_retail_amount
              ELSE 0
         END AS sp_gross_margin_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.gross_sales_dollar_amount
              ELSE 0
         END AS sp_gross_sales_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.gross_sales_units
              ELSE 0
         END AS sp_gross_sales_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.net_sales_retail_amount
              ELSE 0
         END AS sp_net_sales_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.net_sales_units
              ELSE 0
         END AS sp_net_sales_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.net_sales_dollar_amount
              ELSE 0
         END AS sp_net_sales_cost_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.net_sales_trunk_club_dollar_amount
              ELSE 0
         END AS sp_net_sales_trunk_club_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.net_sales_trunk_club_units
              ELSE 0
         END AS sp_net_sales_trunk_club_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.returns_dollar_amount
              ELSE 0
         END AS sp_returns_retail_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.returns_units
              ELSE 0
         END AS sp_returns_qty,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.shrink_dollar_amount
              ELSE 0
         END AS sp_shrink_cost_amt,
         CASE WHEN LOWER(mfcpf0.financial_plan_version) = LOWER('SNAP_PLAN')
              THEN mfcpf0.shrink_units
              ELSE 0
         END AS sp_shrink_qty,
         0 AS ty_demand_flash_retail_amt,
         0 AS ty_demand_persistent_retail_amt,
         0 AS ty_demand_total_qty,
         0 AS ty_demand_total_retail_amt,
         0 AS ty_gross_margin_retail_amt,
         0 AS ty_gross_sales_retail_amt,
         0 AS ty_gross_sales_qty,
         0 AS ty_net_sales_retail_amt,
         0 AS ty_net_sales_qty,
         0 AS ty_net_sales_cost_amt,
         0 AS ty_net_sales_trunk_club_retail_amt,
         0 AS ty_net_sales_trunk_club_qty,
         0 AS ty_returns_retail_amt,
         0 AS ty_returns_qty,
         0 AS ty_shrink_cost_amt,
         0 AS ty_shrink_qty,
         0 AS ly_demand_flash_retail_amt,
         0 AS ly_demand_persistent_retail_amt,
         0 AS ly_demand_total_qty,
         0 AS ly_demand_total_retail_amt,
         0 AS ly_gross_margin_retail_amt,
         0 AS ly_gross_sales_retail_amt,
         0 AS ly_gross_sales_qty,
         0 AS ly_net_sales_retail_amt,
         0 AS ly_net_sales_qty,
         0 AS ly_net_sales_cost_amt,
         0 AS ly_net_sales_trunk_club_retail_amt,
         0 AS ly_net_sales_trunk_club_qty,
         0 AS ly_returns_retail_amt,
         0 AS ly_returns_qty,
         0 AS ly_shrink_cost_amt,
         0 AS ly_shrink_qty,
         0 AS lly_demand_flash_retail_amt,
         0 AS lly_demand_persistent_retail_amt,
         0 AS lly_demand_total_qty,
         0 AS lly_demand_total_retail_amt,
         0 AS lly_gross_margin_retail_amt,
         0 AS lly_gross_sales_retail_amt,
         0 AS lly_gross_sales_qty,
         0 AS lly_net_sales_retail_amt,
         0 AS lly_net_sales_qty,
         0 AS lly_net_sales_cost_amt,
         0 AS lly_net_sales_trunk_club_retail_amt,
         0 AS lly_net_sales_trunk_club_qty,
         0 AS lly_returns_retail_amt,
         0 AS lly_returns_qty,
         0 AS lly_shrink_cost_amt,
         0 AS lly_shrink_qty,
         mtbv2.dw_batch_dt
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_channel_plan_fct AS mfcpf0
         INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.alternate_inventory_model_dim AS aimd0 
         ON LOWER(mfcpf0.alternate_inventory_model) = LOWER(aimd0.fulfill_type_desc)
         INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv2 
         ON LOWER(mtbv2.interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')) AS iifbv
         INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd0 
         ON wcd0.ly_week_num <> 444444 
         AND iifbv.week_num = wcd0.ly_week_num
         WHERE wcd0.LY_WEEK_NUM  > (SELECT CASE WHEN DAY_NUM_OF_FISCAL_WEEK = 7
                                                THEN l01.CURRENT_FISCAL_WEEK_NUM
                                                ELSE l01.LAST_COMPLETED_FISCAL_WEEK_NUM
                                           END
                                   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_VWS.MERCH_TRAN_LAST_COMPLETED_MONTH_VW l01 
                                   JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.DAY_CAL_454_DIM d01
                                   ON l01.DW_BATCH_DT = d01.DAY_DATE
                                   WHERE LOWER(l01.INTERFACE_CODE) = LOWER('MFP_CHNL_BLEND_WKLY'))
UNION ALL
SELECT COALESCE(ddcwv1.dept_num, srif1.department_num, sif1.department_num) AS dept_num,
wcd1.week_num,
COALESCE(ddcwv1.channel_num, srif1.channel_num, sif1.channel_num) AS channel_num,
COALESCE(ddcwv1.fulfill_id, srif1.fulfilment_num, sif1.fulfilment_num) AS fulfill_type_num,
0 AS cp_demand_flash_retail_amt,
0 AS cp_demand_persistent_retail_amt,
0 AS cp_demand_total_qty,
0 AS cp_demand_total_retail_amt,
0 AS cp_gross_margin_retail_amt,
0 AS cp_gross_sales_retail_amt,
0 AS cp_gross_sales_qty,
0 AS cp_net_sales_retail_amt,
0 AS cp_net_sales_qty,
0 AS cp_net_sales_cost_amt,
0 AS cp_net_sales_trunk_club_retail_amt,
0 AS cp_net_sales_trunk_club_qty,
0 AS cp_returns_retail_amt,
0 AS cp_returns_qty,
0 AS cp_shrink_cost_amt,
0 AS cp_shrink_qty,
0 AS op_demand_flash_retail_amt,
0 AS op_demand_persistent_retail_amt,
0 AS op_demand_total_qty,
0 AS op_demand_total_retail_amt,
0 AS op_gross_margin_retail_amt,
0 AS op_gross_sales_retail_amt,
0 AS op_gross_sales_qty,
0 AS op_net_sales_retail_amt,
0 AS op_net_sales_qty,
0 AS op_net_sales_cost_amt,
0 AS op_net_sales_trunk_club_retail_amt,
0 AS op_net_sales_trunk_club_qty,
0 AS op_returns_retail_amt,
0 AS op_returns_qty,
0 AS op_shrink_cost_amt,
0 AS op_shrink_qty,
0 AS sp_demand_flash_retail_amt,
0 AS sp_demand_persistent_retail_amt,
0 AS sp_demand_total_qty,
0 AS sp_demand_total_retail_amt,
0 AS sp_gross_margin_retail_amt,
0 AS sp_gross_sales_retail_amt,
0 AS sp_gross_sales_qty,
0 AS sp_net_sales_retail_amt,
0 AS sp_net_sales_qty,
0 AS sp_net_sales_cost_amt,
0 AS sp_net_sales_trunk_club_retail_amt,
0 AS sp_net_sales_trunk_club_qty,
0 AS sp_returns_retail_amt,
0 AS sp_returns_qty,
0 AS sp_shrink_cost_amt,
0 AS sp_shrink_qty,
0 AS ty_demand_flash_retail_amt,
0 AS ty_demand_persistent_retail_amt,
0 AS ty_demand_total_qty,
0 AS ty_demand_total_retail_amt,
0 AS ty_gross_margin_retail_amt,
0 AS ty_gross_sales_retail_amt,
0 AS ty_gross_sales_qty,
0 AS ty_net_sales_retail_amt,
0 AS ty_net_sales_qty,
0 AS ty_net_sales_cost_amt,
0 AS ty_net_sales_trunk_club_retail_amt,
0 AS ty_net_sales_trunk_club_qty,
0 AS ty_returns_retail_amt,
0 AS ty_returns_qty,
0 AS ty_shrink_cost_amt,
0 AS ty_shrink_qty,
0 AS ly_demand_flash_retail_amt,
0 AS ly_demand_persistent_retail_amt,
0 AS ly_demand_total_qty,
0 AS ly_demand_total_retail_amt,
0 AS ly_gross_margin_retail_amt,
0 AS ly_gross_sales_retail_amt,
0 AS ly_gross_sales_qty,
0 AS ly_net_sales_retail_amt,
0 AS ly_net_sales_qty,
0 AS ly_net_sales_cost_amt,
0 AS ly_net_sales_trunk_club_retail_amt,
0 AS ly_net_sales_trunk_club_qty,
0 AS ly_returns_retail_amt,
0 AS ly_returns_qty,
0 AS ly_shrink_cost_amt,
0 AS ly_shrink_qty,
ROUND(CAST(COALESCE(ddcwv1.dmnd_flash_retl, FORMAT('%4d', 0)) AS NUMERIC), 4) AS lly_demand_flash_retail_amt,
ROUND(CAST(COALESCE(ddcwv1.dmnd_persistent_retl, FORMAT('%4d', 0)) AS NUMERIC), 4) AS lly_demand_persistent_retail_amt,
CAST(COALESCE(ddcwv1.dmnd_flash_units + ddcwv1.dmnd_persistent_units, 0) AS BIGINT) AS lly_demand_total_qty,
ROUND(CAST(COALESCE(CAST(ddcwv1.dmnd_flash_retl AS FLOAT64) + CAST(ddcwv1.dmnd_persistent_retl AS FLOAT64), 0) AS NUMERIC), 4) AS lly_demand_total_retail_amt,
COALESCE(srif1.gross_margin_retail, 0) AS lly_gross_margin_retail_amt,
CAST(COALESCE(srif1.net_sales_retail + srif1.returns_retail, 0) AS NUMERIC) AS lly_gross_sales_retail_amt,
COALESCE(srif1.net_sales_units + srif1.returns_units, 0) AS lly_gross_sales_qty,
COALESCE(srif1.net_sales_retail, 0) AS lly_net_sales_retail_amt,
COALESCE(srif1.net_sales_units, 0) AS lly_net_sales_qty,
COALESCE(srif1.net_sales_cost, 0) AS lly_net_sales_cost_amt,
0 AS lly_net_sales_trunk_club_retail_amt,
0 AS lly_net_sales_trunk_club_qty,
COALESCE(srif1.returns_retail, 0) AS lly_returns_retail_amt,
COALESCE(srif1.returns_units, 0) AS lly_returns_qty,
COALESCE(sif1.shrink_cost, 0) AS lly_shrink_cost_amt,
COALESCE(sif1.shrink_units, 0) AS lly_shrink_qty,
mtbv3.dw_batch_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_demand_dept_chnl_week_agg_vw AS ddcwv1
FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.sales_returns_insight_fact AS srif1 
ON ddcwv1.ord_week_num = srif1.week_num 
AND ddcwv1.dept_num = srif1.department_num 
AND ddcwv1.channel_num = srif1.channel_num 
AND ddcwv1.fulfill_id = srif1.fulfilment_num       
FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.shrink_insight_fact AS sif1 
ON ddcwv1.ord_week_num = sif1.week_num 
AND ddcwv1.dept_num = sif1.department_num 
AND ddcwv1.channel_num = sif1.channel_num 
AND ddcwv1.fulfill_id = sif1.fulfilment_num
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd1 
ON wcd1.lly_week_num = COALESCE(ddcwv1.ord_week_num, srif1.week_num, sif1.week_num) 
AND wcd1.lly_week_num <> 444444
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv3 
ON LOWER(mtbv3.interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')) AS a
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c 
ON a.week_num = c.week_idnt
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l01 
ON LOWER(l01.interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')
GROUP BY a.dept_num,
 a.week_num,
 a.channel_num,
 a.fulfill_type_num,
 dw_sys_load_tmstp);