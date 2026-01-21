BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;



BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.clearance_markdown_hierarchy_hist_dim (rms_style_num, color_num, channel_country, div_num,
 div_desc, grp_num, grp_desc, dept_num, dept_desc)
(SELECT DISTINCT rms_style_num,
  color_num,
  channel_country,
  div_num,
  div_desc,
  grp_num,
  grp_desc,
  dept_num,
  dept_desc
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_vw);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;

END;
