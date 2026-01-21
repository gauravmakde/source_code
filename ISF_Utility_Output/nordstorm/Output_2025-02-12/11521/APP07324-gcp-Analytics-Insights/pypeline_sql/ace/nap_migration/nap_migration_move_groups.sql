/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_move_groups_11521_ACE_ENG;
     Task_Name=nap_migration_move_groups;'
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
on {techex_t2_schema}.nap_migration_move_groups
;

delete
from    {techex_t2_schema}.nap_migration_move_groups
;

insert into {techex_t2_schema}.nap_migration_move_groups
select  distinct lower(databasename) as databasename
        , lower(tablename) as tablename
        , move_group
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.nap_migration_move_groups_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (databasename), -- column names used for primary index
                    COLUMN (tablename)  -- column names used for partition
on {techex_t2_schema}.nap_migration_move_groups
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

