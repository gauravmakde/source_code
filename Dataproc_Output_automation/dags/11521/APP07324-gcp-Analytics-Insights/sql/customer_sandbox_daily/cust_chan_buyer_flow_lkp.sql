


DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
---Task_Name=cust_chan_buyer_flow_lkp_build;'*/


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_chan_buyer_flow_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_chan_buyer_flow_lkp
(SELECT cust_chan_buyer_flow_desc,
  ROW_NUMBER() OVER (ORDER BY cust_chan_buyer_flow_desc) AS cust_chan_buyer_flow_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(cust_chan_buyer_flow, 'NA') AS cust_chan_buyer_flow_desc
   FROM `{{params.gcp_project_id}}`.T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes AS cli) AS a);
