SET QUERY_BAND = 'App_ID=APP09442;
DAG_ID=nmn_head_brands_11521_ACE_ENG;
Task_Name=nmn_head_brands;' 
FOR SESSION VOLATILE;


DELETE
FROM  {nmn_t2_schema}.nmn_head_brands
;
INSERT INTO {nmn_t2_schema}.nmn_head_brands
SELECT
     head_brands,
     brand_id
FROM  {nmn_t2_schema}.nmn_head_brands_ldg
WHERE 1=1
; 

SET QUERY_BAND = NONE FOR SESSION;

