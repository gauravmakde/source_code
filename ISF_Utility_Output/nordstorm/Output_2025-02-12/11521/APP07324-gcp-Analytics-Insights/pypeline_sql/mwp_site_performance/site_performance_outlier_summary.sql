SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_outlier_summary_11521_ACE_ENG;
     Task_Name=site_performance_outlier_summary;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_DIGENG.site_performance_outlier_summary
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

Note:
-- Calculate 100 day rolling average, transfer data from T2_landing to T2_final table.
-- Update Cadence - Daily

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from {mwp_t2_schema}.site_performance_outlier_summary
where eventtime between {start_date} and {end_date}
;

INSERT INTO {mwp_t2_schema}.site_performance_outlier_summary
SELECT 
  eventtime 
  , navigationtype   
  , pagetype 
  , experience 
  , brand_country 
  , avg_page_interactive
  , CURRENT_TIMESTAMP as dw_sys_load_tmstp
  FROM {mwp_t2_schema}.site_performance_outlier_summary_ldg
  WHERE eventtime between {start_date} and {end_date}  
;  

collect statistics column (eventtime)
on {mwp_t2_schema}.site_performance_outlier_summary
;

-- drop staging table
drop table {mwp_t2_schema}.site_performance_outlier_summary_ldg
;


SET QUERY_BAND = NONE FOR SESSION;