SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_nms_market_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer NMS Market Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.cust_nms_market_lkp
;

INSERT INTO {str_t2_schema}.cust_nms_market_lkp
SELECT DISTINCT cust_NMS_market_desc
     , ROW_NUMBER() OVER (ORDER BY cust_NMS_market_desc ASC) AS cust_NMS_market_num
     , CASE WHEN UPPER(cust_NMS_market_desc) IN ('CHICAGO','DETROIT','MINNEAPOLIS') THEN 'MIDWEST'
            WHEN UPPER(cust_NMS_market_desc) IN ('NEW_YORK','WASHINGTON','BOSTON','PHILADELPHIA') THEN 'NORTHEAST'
            WHEN UPPER(cust_NMS_market_desc) IN ('SEATTLE','PORTLAND','BOSTON','SAN_FRANCISCO') THEN 'NORTHWEST'
            WHEN UPPER(cust_NMS_market_desc) IN ('LOS_ANGELES','SAN_DIEGO') THEN 'SCAL'
            WHEN UPPER(cust_NMS_market_desc) IN ('ATLANTA','MIAMI') THEN 'SOUTHEAST'
            WHEN UPPER(cust_NMS_market_desc) IN ('AUSTIN','HOUSTON','DENVER','DALLAS') THEN 'SOUTHWEST'
            ELSE 'NON-NMS REGION' END AS cust_nms_region_desc
     , CASE WHEN cust_nms_region_desc = 'MIDWEST' THEN 1
            WHEN cust_nms_region_desc = 'NORTHEAST' THEN 2
            WHEN cust_nms_region_desc = 'NORTHWEST' THEN 3
            WHEN cust_nms_region_desc = 'SCAL' THEN 4
            WHEN cust_nms_region_desc = 'SOUTHEAST' THEN 5
            WHEN cust_nms_region_desc = 'SOUTHWEST' THEN 6
            ELSE 7 END AS cust_nms_region_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT local_market AS cust_NMS_market_desc
      FROM PRD_NAP_USR_VWS.LOCAL_MARKET_POSTAL_DIM) a
;


SET QUERY_BAND = NONE FOR SESSION;
