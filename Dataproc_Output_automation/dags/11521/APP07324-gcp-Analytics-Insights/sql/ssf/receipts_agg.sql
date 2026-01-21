
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.ssf_t2_schema}}.receipts_agg ;


INSERT INTO `{{params.gcp_project_id}}`.{{params.ssf_t2_schema}}.receipts_agg
with pct_wk as (
SELECT 
 cccpw.week_idnt,
 dc.month_num,
 cccpw.chnl_idnt ,
 SUM(cccpw.rcpt_need_u) OVER (PARTITION BY cccpw.week_idnt, cccpw.chnl_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
  UNBOUNDED FOLLOWING) AS rcpt_u_wk,
 SUM(cccpw.rcpt_need_u) OVER (PARTITION BY dc.month_num, cccpw.chnl_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
  UNBOUNDED FOLLOWING) AS rcpt_u_mth,
   (SUM(cccpw.rcpt_need_u) OVER (PARTITION BY cccpw.week_idnt, cccpw.chnl_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING)) / (SUM(cccpw.rcpt_need_u) OVER (PARTITION BY dc.month_num, cccpw.chnl_idnt RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS pct_rcpt_wk
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_channel_cost_plans_weekly AS cccpw
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc ON cccpw.week_idnt = dc.week_num
WHERE cccpw.chnl_idnt IN (110, 210)
 AND cccpw.week_idnt >= (SELECT week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND cccpw.week_idnt <= (SELECT week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
    WHERE day_date = CURRENT_DATE('PST8PDT')) + 100
)
, plan as (
SELECT 
 loc_idnt,
 mth_idnt,
 chnl_idnt,
 SUM(rcpt_plan) AS plan_units_month
FROM `{{params.gcp_project_id}}`.t2dl_das_location_planning.loc_plan_prd_vw
WHERE mth_idnt >= (SELECT month_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND mth_idnt <= (SELECT month_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
    WHERE day_date = CURRENT_DATE('PST8PDT')) + 100
 AND loc_idnt IN (SELECT store_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
   WHERE LOWER(store_type_code) IN (LOWER('FL'), LOWER('RK')))
 AND (loc_idnt < 57 OR loc_idnt > 57 AND loc_idnt < 800)
GROUP BY 
 loc_idnt,
 mth_idnt,
 chnl_idnt
)
SELECT store_num,
 week_num AS wk_idnt,
 SUM(receipts_total_units_ty) AS receipts_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_agg_fact
WHERE week_num BETWEEN 202101 AND ((SELECT week_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
     WHERE day_date = CURRENT_DATE('PST8PDT')) - 1)
GROUP BY 
 store_num,
 wk_idnt
    UNION ALL
select
loc_idnt,
week_idnt as wk_idnt,
CAST(ROUND(SUM(plan_units_month * pct_rcpt_wk), 0) AS NUMERIC) AS receipts_units
FROM plan
FULL OUTER JOIN pct_wk on pct_wk.month_num = plan.mth_idnt 
and pct_wk.chnl_idnt = plan.chnl_idnt
WHERE week_idnt >= (select week_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal where day_date = CURRENT_DATE('PST8PDT'))
GROUP BY
loc_idnt,
week_idnt
;

