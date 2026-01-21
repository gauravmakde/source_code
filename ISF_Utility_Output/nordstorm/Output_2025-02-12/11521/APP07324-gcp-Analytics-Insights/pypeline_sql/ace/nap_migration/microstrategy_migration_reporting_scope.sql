/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_microstrategy_migration_reporting_scope_11521_ACE_ENG;
     Task_Name=microstrategy_migration_reporting_scope;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (schema_name, object_name),
                    COLUMN (schema_name) ,
                    COLUMN (object_name)
on {techex_t2_schema}.microstrategy_migration_reporting_scope
;

delete
from    {techex_t2_schema}.microstrategy_migration_reporting_scope
;

insert into {techex_t2_schema}.microstrategy_migration_reporting_scope
select  project 
        , schema_object_name
        , schema_name
        , object_name
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.microstrategy_migration_reporting_scope_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (schema_name, object_name),
                    COLUMN (schema_name), -- column names used for primary index
                    COLUMN (object_name)  -- column names used for partition
on {techex_t2_schema}.microstrategy_migration_reporting_scope
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


