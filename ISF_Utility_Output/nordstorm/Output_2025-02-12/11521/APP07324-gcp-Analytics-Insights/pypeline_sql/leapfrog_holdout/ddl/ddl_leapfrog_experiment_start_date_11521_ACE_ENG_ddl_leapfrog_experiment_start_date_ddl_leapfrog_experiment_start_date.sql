/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_leapfrog_experiment_start_date_11521_ACE_ENG;
     Task_Name=ddl_leapfrog_experiment_start_date;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'leapfrog_experiment_start_date', OUT_RETURN_MSG);


create multiset table T2DL_DAS_DIGENG.leapfrog_experiment_start_date
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
   activity_date_partition		date	,
channel varchar(150) character set unicode not casespecific,
experience varchar(150) character set unicode not casespecific,
week_start_day_date		date	,
month_start_day_date		date	,
quarter_start_day_date		date	,
daily_experimentname		varchar(300) character set unicode not casespecific,
wtd_experimentname		varchar(300)	character set unicode not casespecific,
mtd_experimentname		varchar(300)	character set unicode not casespecific,
qtd_experimentname		varchar(300)	character set unicode not casespecific
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(activity_date_partition)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

-- Table Comment (STANDARD)
COMMENT ON  T2DL_DAS_DIGENG.leapfrog_experiment_start_date IS 'The table that help find when experiment started';


/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;