/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_sprints_11521_ACE_ENG;
     Task_Name=nap_migration_sprints;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (databasename), -- column names used for primary index
                    COLUMN (tablename)  -- column names used for partition
on {techex_t2_schema}.nap_migration_sprints
;

delete
from    {techex_t2_schema}.nap_migration_sprints
;

insert into {techex_t2_schema}.nap_migration_sprints
select  distinct lower(databasename) as databasename
        , lower(tablename) as tablename
        , sprint
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.nap_migration_sprints_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (databasename), -- column names used for primary index
                    COLUMN (tablename)  -- column names used for partition
on {techex_t2_schema}.nap_migration_sprints
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

