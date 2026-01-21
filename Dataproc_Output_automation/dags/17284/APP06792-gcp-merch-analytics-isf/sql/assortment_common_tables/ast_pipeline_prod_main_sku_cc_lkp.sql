/*
Purpose:        Inserts data in table `sku_cc_lkp` for Sku CC Lookup
Variable(s):    {t2dl_das_assortment_dim. T2DL_DAS_ASSORTMENT_DIM (prod) or T3DL_ACE_ASSORTMENT
                {{params.env_suffix}} '' or '_dev' table suffix for prod testing
Author(s):      Sara Riker & Christine Buckler
*/





TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_cc_lkp{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_cc_lkp{{params.env_suffix}}

(SELECT rms_sku_num AS sku_idnt,
  channel_country,
  TRIM(COALESCE(FORMAT('%11d', dept_num), 'UNKNOWN') || '_' || COALESCE(prmy_supp_num, 'UNKNOWN') || '_' || COALESCE(supp_part_num
       , 'UNKNOWN') || '_' || COALESCE(color_num, 'UNKNOWN')) AS customer_choice,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),


 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw);


	 