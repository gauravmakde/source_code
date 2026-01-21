/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=customer_affinity_vw_11521_ACE_ENG;
     Task_Name=customer_affinity_vw;'
     FOR SESSION VOLATILE;


/*


T2/View Name:t2dl_das_usl.customer_affinity_vw
Team/Owner:Customer Analytics
Date Created/Modified:2023-06-27

Note:
-- View to directly get customer-level affinity persona / division / cluster view

*/

REPLACE VIEW {usl_t2_schema}.customer_affinity_vw
AS
LOCK ROW FOR ACCESS

        select cus.acp_id acp_id
           ,case when affinity_womens > affinity_mens and affinity_womens > affinity_kids then 'womens'
                 when affinity_mens  > affinity_womens and affinity_mens > affinity_kids then 'mens'
                 when affinity_kids > affinity_mens and affinity_kids > affinity_womens  then 'kids'
                 else 'other'
                 end customer_affinity_persona

           ,case  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_apparel then 'apparel'
                  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_accessories then 'accessories'
                  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_active then 'active'
                  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_beauty then 'beauty'
                  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_designer then 'designer'
                  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_home then 'home'
                  when greatest(affinity_accessories,affinity_active ,affinity_apparel ,affinity_beauty,affinity_designer ,affinity_home ,affinity_shoes ) = affinity_shoes then 'shoes'
                  else 'other'
                  end customer_affinity_division

          ,concat(customer_affinity_persona,' ',customer_affinity_division) customer_affinity_cluster

          ,case when customer_affinity_cluster IN ('mens apparel','mens designer','mens active') then 'mens apparel'
                when customer_affinity_cluster IN ('womens apparel','womens designer','womens active','other designer','other active','other apparel') then 'womens apparel'
                when customer_affinity_cluster IN ('kids apparel','kids accessories','kids designer','kids active','kids beauty') then 'kids apparel and accessories'
                when customer_affinity_cluster IN ('mens home','womens home','kids home') then 'other home'
                when customer_affinity_cluster IN ('mens beauty') then 'mens accessories'
                when customer_affinity_cluster IN ('womens beauty','other beauty') then 'womens accessories'
                when customer_affinity_cluster IN ('other shoes') then 'womens shoes'
                else customer_affinity_cluster
                end smart_capacity_id

          ,affinity_accessories customer_affinity_accessories
          ,affinity_active customer_affinity_active
          ,affinity_apparel customer_affinity_apparel
          ,affinity_beauty customer_affinity_beauty
          ,affinity_designer customer_affinity_designer
          ,affinity_home customer_affinity_home
          ,affinity_shoes customer_affinity_shoes
          ,affinity_kids customer_affinity_kids
          ,affinity_mens customer_affinity_mens
          ,affinity_womens customer_affinity_womens
          
        from T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_MERCH cus
        join {usl_t2_schema}.acp_driver_dim ac on ac.acp_id = cus.acp_id
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

