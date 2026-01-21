SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=item_delivery_method_lkp_11521_ACE_ENG;
     Task_Name=item_delivery_method_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.item_delivery_method_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/07/2023

Notes:
-- Item Delivery Type Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.item_delivery_method_lkp
;

INSERT INTO {str_t2_schema}.item_delivery_method_lkp
    SELECT DISTINCT item_delivery_method_desc
                   ,ROW_NUMBER() OVER (ORDER BY item_delivery_method_desc  ASC ) AS item_delivery_method_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(item_delivery_method, 'NA') as item_delivery_method_desc  
            from T2DL_DAS_STRATEGY.cco_line_items cli)a ;


SET QUERY_BAND = NONE FOR SESSION;