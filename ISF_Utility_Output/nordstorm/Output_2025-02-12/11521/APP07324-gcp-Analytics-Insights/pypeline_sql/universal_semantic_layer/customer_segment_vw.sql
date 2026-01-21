SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=customer_segment_vw_11521_ACE_ENG;
     Task_Name=customer_segment_vw;'
     FOR SESSION VOLATILE;

/*


T2/View Name: t2dl_das_usl.customer_segment_vw
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 10th 2023

Note:
-- View to directly get customer predicted segment based on how and where customers shop and their attitudes toward style / shopping 

*/

REPLACE VIEW {usl_t2_schema}.customer_segment_vw
AS
LOCK ROW FOR ACCESS
select cus.acp_id
	,cus.predicted_ct_segment customer_segment
from T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment cus
join {usl_t2_schema}.acp_driver_dim ac on ac.acp_id = cus.acp_id
;

SET QUERY_BAND = NONE FOR SESSION;