SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=item_delivery_method_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Item Delivery Type Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM T2DL_DAS_STRATEGY.item_delivery_method_lkp
;

INSERT INTO T2DL_DAS_STRATEGY.item_delivery_method_lkp
SELECT DISTINCT item_delivery_method_desc
     , ROW_NUMBER() OVER (ORDER BY item_delivery_method_desc ASC) AS item_delivery_method_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(item_delivery_method, 'NA') AS item_delivery_method_desc
      FROM T2DL_DAS_STRATEGY.cco_line_items cli) a
;


SET QUERY_BAND = NONE FOR SESSION;
