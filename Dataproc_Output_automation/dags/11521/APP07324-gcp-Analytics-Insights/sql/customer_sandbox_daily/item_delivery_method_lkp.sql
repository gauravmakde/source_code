

DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
---Task_Name=item_delivery_method_lkp_build;'*/


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.item_delivery_method_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.item_delivery_method_lkp
(SELECT item_delivery_method_desc,
  ROW_NUMBER() OVER (ORDER BY item_delivery_method_desc) AS item_delivery_method_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(item_delivery_method, 'NA') AS item_delivery_method_desc
   FROM `{{params.gcp_project_id}}`.T2DL_DAS_STRATEGY.cco_line_items AS cli) AS a);

