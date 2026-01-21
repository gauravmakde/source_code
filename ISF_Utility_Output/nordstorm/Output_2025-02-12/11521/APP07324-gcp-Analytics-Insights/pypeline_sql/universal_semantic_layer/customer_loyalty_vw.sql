SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=customer_loyalty_vw_11521_ACE_ENG;
     Task_Name=customer_loyalty_vw;'
     FOR SESSION VOLATILE;

/*


T2/View Name: t2dl_das_usl.customer_loyalty_vw
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 10th 2023

Note:
-- View to directly get customer loyalty related info (level, whether member or not, whether card member or not)

*/

REPLACE VIEW {usl_t2_schema}.customer_loyalty_vw
AS
LOCK ROW FOR ACCESS
select cus.acp_id 
      ,loyalty_level customer_loyalty_level
      ,is_loyalty_member customer_loyalty_ismember
      ,is_cardmember customer_loyalty_iscardmember
from T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_LOYALTY cus
join {usl_t2_schema}.acp_driver_dim ac on ac.acp_id = cus.acp_id
;

SET QUERY_BAND = NONE FOR SESSION;