CREATE TEMPORARY TABLE IF NOT EXISTS selling_recommendation_decision_fact_complete_proposals
CLUSTER BY scenario_id
AS
SELECT srdf.scenario_id,
 COUNT(srdf.scenario_id) AS recommendation_count,
 ssf.proposals_count
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.selling_recommendation_decision_fact AS srdf
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.selling_scenario_fact AS ssf ON LOWER(srdf.scenario_id) = LOWER(ssf.selling_scenario_id)
WHERE LOWER(srdf.selling_recommendation_decision_state) = LOWER('PROPOSED')
 AND LOWER(ssf.selling_scenario_state) = LOWER('PRE_SELECTED')
GROUP BY srdf.scenario_id,
 ssf.proposals_count
HAVING recommendation_count = ssf.proposals_count;



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact (rms_style_num, color_num, channel_country, channel_brand,
 selling_channel, effective_begin_tmstp,effective_begin_tmstp_tz, bucket_type, is_remark, div_num, div_desc, grp_num, grp_desc, dept_num,
 dept_desc, clearance_price_amt, inventory_units, inventory_cost, inventory_dollars, avg_regular_price_amt)
(SELECT DISTINCT cmscv.rms_style_num,
  cmscv.color_num,
  cmscv.channel_country,
  cmscv.channel_brand,
  cmsqeowf.selling_channel,
  cast(cmscv.last_clearance_markdown_date as timestamp) AS effective_begin_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(cast(effective_begin_tmstp as string))) as  effective_begin_tmstp_tz,
  'ACTIVE' AS bucket_type,
  'N' AS is_remark,
  cmhd.div_num,
  cmhd.div_desc,
  cmhd.grp_num,
  cmhd.grp_desc,
  cmhd.dept_num,
  cmhd.dept_desc,
  cmf.clearance_price_amt,
  cmsqeowf.inventory_units,
  cmsqeowf.inventory_cost,
  cmsqeowf.inventory_dollars,
  CAST(cmsqeowf.inventory_dollars / cmsqeowf.inventory_units AS NUMERIC) AS avg_regular_price_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_style_color_vw AS cmscv
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_fact AS cmf ON LOWER(cmscv.rms_style_num) = LOWER(cmf.rms_style_num)
       AND LOWER(COALESCE(LTRIM(cmscv.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmf.color_num, '0'), 'NA')) AND
       LOWER(cmscv.channel_country) = LOWER(cmf.channel_country) AND LOWER(cmscv.channel_brand) = LOWER(cmf.channel_brand
       ) AND cmscv.last_clearance_markdown_date = cmf.effective_begin_tmstp AND LOWER(cmf.clearance_markdown_state) =
    LOWER('DECLARED')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_hierarchy_dim AS cmhd ON LOWER(cmscv.rms_style_num) = LOWER(cmhd.rms_style_num
      ) AND LOWER(COALESCE(LTRIM(cmscv.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmhd.color_num, '0'), 'NA')) AND
    LOWER(cmscv.channel_country) = LOWER(cmhd.channel_country)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_fact AS cmsqeowf ON LOWER(cmscv.rms_style_num
       ) = LOWER(cmsqeowf.rms_style_num) AND LOWER(COALESCE(LTRIM(cmscv.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmsqeowf
         .color_num, '0'), 'NA')) AND LOWER(cmscv.channel_country) = LOWER(cmsqeowf.channel_country) AND LOWER(cmscv.channel_brand
     ) = LOWER(cmsqeowf.channel_brand));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact (rms_style_num, color_num, channel_country, channel_brand,
 selling_channel, effective_begin_tmstp, effective_begin_tmstp_tz, bucket_type, is_remark, div_num, div_desc, grp_num, grp_desc, dept_num,
 dept_desc, clearance_price_amt, inventory_units, inventory_cost, inventory_dollars, avg_regular_price_amt)
(SELECT cmf.rms_style_num,
  cmf.color_num,
  cmf.channel_country,
  cmf.channel_brand,
  cmsqeowf.selling_channel,
  cast(cmf.effective_begin_tmstp as timestamp),
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(cast(effective_begin_tmstp as string))) as  effective_begin_tmstp_tz,
  'FUTURE' AS bucket_type,
  'N' AS is_remark,
  cmhd.div_num,
  cmhd.div_desc,
  cmhd.grp_num,
  cmhd.grp_desc,
  cmhd.dept_num,
  cmhd.dept_desc,
  MIN(cmf.clearance_price_amt) AS clearance_price_amt,
  cmsqeowf.inventory_units,
  cmsqeowf.inventory_cost,
  cmsqeowf.inventory_dollars,
  CAST(cmsqeowf.inventory_dollars / cmsqeowf.inventory_units AS NUMERIC) AS avg_regular_price_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_fact AS cmf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_hierarchy_dim AS cmhd ON LOWER(cmf.rms_style_num) = LOWER(cmhd.rms_style_num
      ) AND LOWER(COALESCE(LTRIM(cmf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmhd.color_num, '0'), 'NA')) AND
    LOWER(cmf.channel_country) = LOWER(cmhd.channel_country)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_fact AS cmsqeowf ON LOWER(cmf.rms_style_num) =
      LOWER(cmsqeowf.rms_style_num) AND LOWER(COALESCE(LTRIM(cmf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmsqeowf
         .color_num, '0'), 'NA')) AND LOWER(cmf.channel_country) = LOWER(cmsqeowf.channel_country) AND LOWER(cmf.channel_brand
     ) = LOWER(cmsqeowf.channel_brand)
 WHERE CAST(cmf.effective_begin_tmstp AS DATE) BETWEEN DATE_ADD(CURRENT_DATE("PST8PDT"), INTERVAL 1 DAY) AND (DATE_ADD(DATE_TRUNC(DATE_ADD(CURRENT_DATE("PST8PDT")
       ,INTERVAL 1 WEEK), WEEK(SATURDAY)), INTERVAL 35 DAY))
  AND LOWER(cmf.clearance_markdown_state) = LOWER('DECLARED')
 GROUP BY cmf.rms_style_num,
  cmf.color_num,
  cmf.channel_country,
  cmf.channel_brand,
  cmsqeowf.selling_channel,
  cmf.effective_begin_tmstp,
  bucket_type,
  is_remark,
  cmhd.div_num,
  cmhd.div_desc,
  cmhd.grp_num,
  cmhd.grp_desc,
  cmhd.dept_num,
  cmhd.dept_desc,
  cmsqeowf.inventory_units,
  cmsqeowf.inventory_cost,
  cmsqeowf.inventory_dollars,
  avg_regular_price_amt);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact (rms_style_num, color_num, channel_country, channel_brand,
 selling_channel, effective_begin_tmstp, effective_begin_tmstp_tz, bucket_type, is_remark, div_num, div_desc, grp_num, grp_desc, dept_num,
 dept_desc, clearance_price_amt, inventory_units, inventory_cost, inventory_dollars, avg_regular_price_amt)
(SELECT srdf.rms_style_num,
  srdf.color_num,
  srdf.channel_country,
  srdf.channel_brand,
  cmsqeowf.selling_channel,
  cast(COALESCE(srdf.overridden_start_tmstp, srdf.recommended_start_tmstp) as timestamp) AS effective_begin_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(cast(cast(COALESCE(srdf.overridden_start_tmstp, srdf.recommended_start_tmstp) as timestamp) as string))) as  effective_begin_tmstp_tz,
  'PRE_SELECTED' AS bucket_type,
  'N' AS is_remark,
  cmhd.div_num,
  cmhd.div_desc,
  cmhd.grp_num,
  cmhd.grp_desc,
  cmhd.dept_num,
  cmhd.dept_desc,
  COALESCE(srdf.overridden_price_amt, srdf.recommended_price_amt) AS clearance_price_amt,
  cmsqeowf.inventory_units,
  cmsqeowf.inventory_cost,
  cmsqeowf.inventory_dollars,
  CAST(cmsqeowf.inventory_dollars / cmsqeowf.inventory_units AS NUMERIC) AS avg_regular_price_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.selling_recommendation_decision_fact AS srdf
  INNER JOIN selling_recommendation_decision_fact_complete_proposals AS srdfcp ON LOWER(srdf.scenario_id) = LOWER(srdfcp
    .scenario_id)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_fact AS cmsqeowf ON LOWER(srdf.rms_style_num)
      = LOWER(cmsqeowf.rms_style_num) AND LOWER(COALESCE(LTRIM(srdf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmsqeowf
         .color_num, '0'), 'NA')) AND LOWER(srdf.channel_country) = LOWER(cmsqeowf.channel_country) AND LOWER(srdf.channel_brand
     ) = LOWER(cmsqeowf.channel_brand)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_hierarchy_dim AS cmhd ON LOWER(srdf.rms_style_num) = LOWER(cmhd.rms_style_num
      ) AND LOWER(COALESCE(LTRIM(srdf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmhd.color_num, '0'), 'NA')) AND
    LOWER(srdf.channel_country) = LOWER(cmhd.channel_country)
 WHERE LOWER(srdf.selling_recommendation_decision_state) = LOWER('PROPOSED'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact (rms_style_num, color_num, channel_country, channel_brand,
 selling_channel, effective_begin_tmstp,effective_begin_tmstp_tz, bucket_type, is_remark, div_num, div_desc, grp_num, grp_desc, dept_num,
 dept_desc, clearance_price_amt, inventory_units, inventory_cost, inventory_dollars, avg_regular_price_amt)
(SELECT cmsf.rms_style_num,
  cmsf.color_num,
  cmsf.channel_country,
  cmsf.channel_brand,
  cmsqeowf.selling_channel,
  cast(cmsf.effective_start_tmstp as timestamp) AS effective_begin_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(cast(cast(cmsf.effective_start_tmstp as timestamp) as string))) as  effective_begin_tmstp_tz,
  cmsf.clearance_markdown_submission_state AS bucket_type,
  'N' AS is_remark,
  cmhd.div_num,
  cmhd.div_desc,
  cmhd.grp_num,
  cmhd.grp_desc,
  cmhd.dept_num,
  cmhd.dept_desc,
  cmsf.price_amt AS clearance_price_amt,
  cmsqeowf.inventory_units,
  cmsqeowf.inventory_cost,
  cmsqeowf.inventory_dollars,
  CAST(cmsqeowf.inventory_dollars / cmsqeowf.inventory_units AS NUMERIC) AS avg_regular_price_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_submission_fact AS cmsf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_fact AS cmsqeowf ON LOWER(cmsf.rms_style_num)
      = LOWER(cmsqeowf.rms_style_num) AND LOWER(COALESCE(LTRIM(cmsf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmsqeowf
         .color_num, '0'), 'NA')) AND LOWER(cmsf.channel_country) = LOWER(cmsqeowf.channel_country) AND LOWER(cmsf.channel_brand
     ) = LOWER(cmsqeowf.channel_brand)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_hierarchy_dim AS cmhd ON LOWER(cmsf.rms_style_num) = LOWER(cmhd.rms_style_num
      ) AND LOWER(COALESCE(LTRIM(cmsf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmhd.color_num, '0'), 'NA')) AND
    LOWER(cmsf.channel_country) = LOWER(cmhd.channel_country)
 WHERE LOWER(cmsf.clearance_markdown_submission_state) = LOWER('PRE_SELECTED'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact (rms_style_num, color_num, channel_country, channel_brand,
 selling_channel, bucket_type, div_num, div_desc, grp_num, grp_desc, dept_num, dept_desc, inventory_units,
 inventory_cost, inventory_dollars, avg_regular_price_amt)
(SELECT cmsqeowf.rms_style_num,
  cmsqeowf.color_num,
  cmsqeowf.channel_country,
  cmsqeowf.channel_brand,
  cmsqeowf.selling_channel,
  'UNACTIONED' AS bucket_type,
  cmhd.div_num,
  cmhd.div_desc,
  cmhd.grp_num,
  cmhd.grp_desc,
  cmhd.dept_num,
  cmhd.dept_desc,
  cmsqeowf.inventory_units,
  cmsqeowf.inventory_cost,
  cmsqeowf.inventory_dollars,
  CAST(cmsqeowf.inventory_dollars / cmsqeowf.inventory_units AS NUMERIC) AS avg_regular_price_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_fact AS cmsqeowf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_hierarchy_dim AS cmhd ON LOWER(cmsqeowf.rms_style_num) = LOWER(cmhd.rms_style_num
      ) AND LOWER(COALESCE(LTRIM(cmsqeowf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmhd.color_num, '0'), 'NA'))
   AND LOWER(cmsqeowf.channel_country) = LOWER(cmhd.channel_country)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_buckets_fact AS cmb ON LOWER(cmsqeowf.rms_style_num) = LOWER(cmb.rms_style_num
       ) AND LOWER(COALESCE(LTRIM(cmsqeowf.color_num, '0'), 'NA')) = LOWER(COALESCE(LTRIM(cmb.color_num, '0'), 'NA'))
    AND LOWER(cmsqeowf.channel_country) = LOWER(cmb.channel_country) AND LOWER(cmsqeowf.channel_brand) = LOWER(cmb.channel_brand
     )
  INNER JOIN (SELECT DISTINCT dept_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_buckets_fact) AS clearance_departments ON cmhd.dept_num =
   clearance_departments.dept_num
 WHERE cmb.rms_style_num IS NULL);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact SET
 clearance_markdown_stage = 'LAST_CHANCE',
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME("PST8PDT")) AS DATETIME)
WHERE clearance_price_amt = 0.01;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact SET
 clearance_markdown_stage = 'RACKING',
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME("PST8PDT")) AS DATETIME)
WHERE LOWER(SUBSTR(SUBSTR(FORMAT('%.2f', clearance_price_amt), 1, 15), 3 * -1)) = LOWER('.97') AND LOWER(channel_brand)
  = LOWER('NORDSTROM');


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact AS clearance_markdown_buckets_fact0 SET
 is_remark = 'Y',
 clearance_markdown_stage = 'REMARK',
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME("PST8PDT")) AS DATETIME) FROM (SELECT rms_style_num,
   color_num,
   channel_country,
   channel_brand,
   selling_channel,
   MIN(effective_begin_tmstp) AS first_mark_effective_begin_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_buckets_fact
  GROUP BY rms_style_num,
   color_num,
   channel_country,
   channel_brand,
   selling_channel) AS bucket_src
WHERE LOWER(COALESCE(clearance_markdown_buckets_fact0.rms_style_num, 'NA')) = LOWER(COALESCE(bucket_src.rms_style_num,
         'NA')) AND LOWER(COALESCE(clearance_markdown_buckets_fact0.color_num, 'NA')) = LOWER(COALESCE(bucket_src.color_num
         , 'NA')) AND LOWER(clearance_markdown_buckets_fact0.channel_country) = LOWER(bucket_src.channel_country) AND
     LOWER(clearance_markdown_buckets_fact0.channel_brand) = LOWER(bucket_src.channel_brand) AND LOWER(clearance_markdown_buckets_fact0
     .selling_channel) = LOWER(bucket_src.selling_channel) AND LOWER(clearance_markdown_buckets_fact0.bucket_type) =
   LOWER('UNACTIONED') AND clearance_markdown_buckets_fact0.effective_begin_tmstp > cast(bucket_src.first_mark_effective_begin_tmstp as timestamp);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_buckets_fact SET
 clearance_markdown_stage = 'FIRST_MARK',
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME("PST8PDT")) AS DATETIME)
WHERE LOWER(is_remark) = LOWER('N') AND clearance_markdown_stage IS NULL;