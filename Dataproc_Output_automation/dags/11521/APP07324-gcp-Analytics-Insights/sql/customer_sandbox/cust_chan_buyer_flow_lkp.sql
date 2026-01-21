-- CREATE OR REPLACE PROCEDURE t2dl_das_strategy.input_bteq_17()
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_chan_buyer_flow_lkp;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_chan_buyer_flow_lkp
(SELECT cust_chan_buyer_flow_desc,
  ROW_NUMBER() OVER (ORDER BY cust_chan_buyer_flow_desc) AS cust_chan_buyer_flow_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME(('PST8PDT'))) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(cust_chan_buyer_flow, 'NA') AS cust_chan_buyer_flow_desc
   FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_cust_chan_yr_attributes AS cli) AS a);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (cust_chan_buyer_flow_num), COLUMN (cust_chan_buyer_flow_desc) ON {{params.str_t2_schema}}.cust_chan_buyer_flow_lkp;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
