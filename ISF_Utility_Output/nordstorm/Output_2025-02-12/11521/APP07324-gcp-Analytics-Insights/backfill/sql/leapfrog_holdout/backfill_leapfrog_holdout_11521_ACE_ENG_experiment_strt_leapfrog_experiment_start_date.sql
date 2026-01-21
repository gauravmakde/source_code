/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_holdout_11521_ACE_ENG;
     Task_Name=leapfrog_experiment_start_date;'
     FOR SESSION VOLATILE;


/*


T2/Table Name: leapfrog_experiment_start_date
Team/Owner: Jin Liu
Date Created/Modified: 2023-08-30

Note:
-- What is the the purpose of the table : to get at what date, what experiments started 
-- What is the update cadence/lookback window: daily 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
 
*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on T2DL_DAS_DIGENG.leapfrog_experiment_start_date_ldg
;

delete
from    T2DL_DAS_DIGENG.leapfrog_experiment_start_date
;

insert into T2DL_DAS_DIGENG.leapfrog_experiment_start_date
select  
activity_date_partition	,
channel,
experience,
week_start_day_date	,
month_start_day_date	,
quarter_start_day_date	,
daily_experimentname	,
wtd_experimentname	,
mtd_experimentname	,
qtd_experimentname				
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from    T2DL_DAS_DIGENG.leapfrog_experiment_start_date_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on T2DL_DAS_DIGENG.leapfrog_experiment_start_date
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;