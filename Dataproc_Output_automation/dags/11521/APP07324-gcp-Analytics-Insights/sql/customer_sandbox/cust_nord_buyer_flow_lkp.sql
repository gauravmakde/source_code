BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=cust_chan_buyer_flow_lkp_11521_ACE_ENG;
---     Task_Name=cust_nord_buyer_flow_lkp;'*/
---     FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_nord_buyer_flow_lkp;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_nord_buyer_flow_lkp
(SELECT DISTINCT cust_chan_buyer_flow_desc AS cust_nord_buyer_flow_desc,
  cust_chan_buyer_flow_num AS cust_nord_buyer_flow_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_chan_buyer_flow_lkp);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (cust_nord_buyer_flow_num), COLUMN (cust_nord_buyer_flow_desc) ON t2dl_das_strategy.cust_nord_buyer_flow_lkp;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
