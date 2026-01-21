SELECT DISTINCT
dept_num AS DEPT_IDNT,
coalesce(OReplace(style_desc, Chr(10), ''),'')  AS RMS_STYLE_DESC,
rms_sku_num AS RMS_SKU_IDNT, 
coalesce(color_num,'') AS COLR_IDNT,
coalesce(OReplace(supp_color, Chr(10), ''),'') AS SUPP_COLOR, 
coalesce(size_1_num,'') AS SIZE_1_IDNT,
coalesce(OReplace(size_1_desc, Chr(10), ''),'') AS SIZE_1_DESC,
coalesce(size_2_num,'') AS SIZE_2_IDNT,
coalesce(OReplace(size_2_desc, Chr(10), ''),'') AS SIZE_2_DESC,
rms_style_num AS RMS_STYLE_IDNT
FROM {db_env}_NAP_BASE_VWS.MERCH_PRODUCT_SKU_DIM_AS_IS_VW
where dept_idnt is not null
and channel_country='US';