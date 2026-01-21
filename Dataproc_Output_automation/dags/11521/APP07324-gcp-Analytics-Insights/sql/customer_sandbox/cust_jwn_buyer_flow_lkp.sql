-- SET QUERY_BAND = 'App_ID=APP08737;
--      DAG_ID=cust_chan_buyer_flow_lkp_11521_ACE_ENG;
--      Task_Name=cust_jwn_buyer_flow_lkp;'
--      FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_jwn_buyer_flow_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 05/22/2023

Notes:
-- Customer JWN Buyerflow Look-up table for microstrategy customer sandbox
-- Rebuilding a copy of cust_jwn_buyer_flow_lkp table as Microstrategy requires seperate look-up for each prompt
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_jwn_buyer_flow_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_jwn_buyer_flow_lkp
(SELECT DISTINCT cust_chan_buyer_flow_desc AS cust_jwn_buyer_flow_desc,
  cust_chan_buyer_flow_num AS cust_jwn_buyer_flow_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_chan_buyer_flow_lkp);

 

-- SET QUERY_BAND = NONE FOR SESSION;