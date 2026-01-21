/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_migration_reporting_objects_11521_ACE_ENG;
     Task_Name=migration_reporting_objects;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (schema_name, table_name),
                    COLUMN (schema_name) ,
                    COLUMN (table_name)
on {techex_t2_schema}.migration_reporting_objects_ldg
;

delete
from    {techex_t2_schema}.migration_reporting_objects
;

insert into {techex_t2_schema}.migration_reporting_objects
select  site_name
        , workbook_name
        , schema_object_name
        , schema_name
        , table_name
        , bi_platform
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.migration_reporting_objects_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (schema_name, table_name),
                    COLUMN (schema_name), -- column names used for primary index
                    COLUMN (table_name)  -- column names used for partition
on {techex_t2_schema}.migration_reporting_objects
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
