SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=affiliate_publisher_mapping_11521_ACE_ENG;
     Task_Name=affiliate_publisher_mapping;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_FUNNEL_IO.affiliate_publisher_mapping
Owner: Analytics Engineering
Modified: 2023-09-13

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {funnel_io_t2_schema}.affiliate_publisher_mapping
;

INSERT INTO {funnel_io_t2_schema}.affiliate_publisher_mapping
SELECT publisher_id,
      encrypted_id,
      publisher_name,
      publisher_group,
      publisher_subgroup,
      CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {funnel_io_t2_schema}.affiliate_publisher_mapping_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
