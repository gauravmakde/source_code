SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_nms_market_lkp_11521_ACE_ENG;
     Task_Name=cust_nms_market_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_nms_market_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Notes:
-- Customer NMS Market Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_nms_market_lkp
;

INSERT INTO {str_t2_schema}.cust_nms_market_lkp
    SELECT DISTINCT cust_NMS_market_desc
		,ROW_NUMBER() OVER (ORDER BY cust_NMS_market_desc  ASC ) AS cust_NMS_market_num
		, CASE WHEN UPPER(cust_NMS_market_desc) in ('CHICAGO','DETROIT','MINNEAPOLIS') THEN 'MIDWEST'
		       WHEN UPPER(cust_NMS_market_desc) in ('NEW_YORK','WASHINGTON','BOSTON','PHILADELPHIA') THEN 'NORTHEAST'
		       WHEN UPPER(cust_NMS_market_desc) in ('SEATTLE','PORTLAND','BOSTON','SAN_FRANCISCO') THEN 'NORTHWEST'
		       WHEN UPPER(cust_NMS_market_desc) in ('LOS_ANGELES','SAN_DIEGO') THEN 'SCAL'
		       WHEN UPPER(cust_NMS_market_desc) in ('ATLANTA','MIAMI') THEN 'SOUTHEAST'
		       WHEN UPPER(cust_NMS_market_desc) in ('AUSTIN','HOUSTON','DENVER','DALLAS') THEN 'SOUTHWEST'
		       ELSE 'NON-NMS REGION' END AS cust_nms_region_desc
		 , CASE WHEN cust_nms_region_desc='MIDWEST' THEN 1
		       WHEN cust_nms_region_desc = 'NORTHEAST' THEN 2
		       WHEN cust_nms_region_desc='NORTHWEST' THEN 3
		       WHEN cust_nms_region_desc=  'SCAL' THEN 4
		       WHEN cust_nms_region_desc ='SOUTHEAST' THEN 5
		       WHEN cust_nms_region_desc= 'SOUTHWEST' THEN 6
		       ELSE 7 END AS cust_nms_region_num
		       
            , CURRENT_TIMESTAMP as dw_sys_load_tmstp

	FROM (SELECT DISTINCT local_market as cust_NMS_market_desc FROM PRD_NAP_USR_VWS.LOCAL_MARKET_POSTAL_DIM)a  ;

SET QUERY_BAND = NONE FOR SESSION;