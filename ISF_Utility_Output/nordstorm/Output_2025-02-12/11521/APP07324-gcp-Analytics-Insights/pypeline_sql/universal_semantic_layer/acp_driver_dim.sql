SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=acp_driver_dim_11521_ACE_ENG;
     Task_Name=acp_driver_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_usl.acp_driver_dim
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 9th 2023

Note:
-- Purpose of the table: Acp ID driver table for all customer focused dimension tables for the Universal Semantic Layer - Acp IDs since FY 2021
-- Update Cadence: Daily

*/
create multiset volatile table acp_driver as (
select distinct hdr.acp_id
from prd_nap_usr_vws.retail_tran_detail_fact_vw hdr
where hdr.business_day_date >= '2021-01-31'
) with data primary index(acp_id) on commit preserve rows;

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM {usl_t2_schema}.acp_driver_dim;

INSERT INTO {usl_t2_schema}.acp_driver_dim
SELECT acp_id
      ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM acp_driver
;

COLLECT STATISTICS COLUMN (acp_id) -- column name used for primary index
on {usl_t2_schema}.acp_driver_dim;
SET QUERY_BAND = NONE FOR SESSION;