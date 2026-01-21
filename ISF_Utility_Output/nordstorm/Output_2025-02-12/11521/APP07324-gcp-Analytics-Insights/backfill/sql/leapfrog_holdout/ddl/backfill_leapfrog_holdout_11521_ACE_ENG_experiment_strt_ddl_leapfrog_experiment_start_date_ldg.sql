/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_leapfrog_experiment_start_date_11521_ACE_ENG;
     Task_Name=ddl_leapfrog_experiment_start_date_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_DIGENG', 'leapfrog_experiment_start_date_ldg', OUT_RETURN_MSG);

create multiset table T2DL_DAS_DIGENG.leapfrog_experiment_start_date_ldg
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
    )
primary index(activity_date_partition)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;