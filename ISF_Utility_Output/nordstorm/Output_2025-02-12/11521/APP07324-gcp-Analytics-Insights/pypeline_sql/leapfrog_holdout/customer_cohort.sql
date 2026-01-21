/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=customer_cohort_11521_ACE_ENG; 
     Task_Name=customer_cohort;'
     FOR SESSION VOLATILE;

 
/*
T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/
 
 

/*
Temp table notes here if applicable
*/


/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    {deg_t2_schema}.customer_cohort
WHERE   execution_qtr_end_dt>= {end_date} and execution_qtr_start_dt <={end_date}
;


INSERT INTO {deg_t2_schema}.customer_cohort
SELECT  acp_id, 
defining_year_ending_qtr, 
defining_year_start_dt, 
defining_year_end_dt, 
execution_qtr, 
execution_qtr_start_dt, 
execution_qtr_end_dt, 
acquired_ind, 
nonrestaurant_trips, 
engagement_cohort
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
        ,execution_qtr as execution_qtr_partition
FROM	T2DL_DAS_AEC.audience_engagement_cohorts 
WHERE   execution_qtr_end_dt>= {end_date} and execution_qtr_start_dt <={end_date}
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (acp_id, defining_year_ending_qtr,acquired_ind,nonrestaurant_trips,engagement_cohort), -- column names used for primary index
                    COLUMN (execution_qtr_partition)  -- column names used for partition
on {deg_t2_schema}.customer_cohort;


/*  
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
   