SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=customer_acquisition_dim_11521_ACE_ENG;
     Task_Name=customer_acquisition_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_usl.customer_acquisition_dim
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 10th 2023

Note:
-- Purpose of the table: Dim table to directly get acquisition related info (date, year, month, box) of the customers
-- Update Cadence: Daily

*/


/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM {usl_t2_schema}.customer_acquisition_dim;

INSERT INTO {usl_t2_schema}.customer_acquisition_dim
SELECT cus.acp_id acp_id
      ,acquired_date customer_acquired_date
      ,year(acquired_date) customer_acquired_year
      ,month(acquired_date) customer_acquired_month
      ,acquired_box customer_acquired_box
      ,((CURRENT_DATE() -  acquired_date)/365) customer_life_years
      ,((CURRENT_DATE() -  acquired_date)/30) customer_life_months
      ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_JOURNEY cus
join {usl_t2_schema}.acp_driver_dim ac on ac.acp_id = cus.acp_id
;

COLLECT STATISTICS COLUMN (acp_id) -- column name used for primary index
                   ,COLUMN (customer_acquired_date)
on {usl_t2_schema}.customer_acquisition_dim;

SET QUERY_BAND = NONE FOR SESSION;