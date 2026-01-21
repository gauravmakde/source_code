/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_terameta_objects_11521_ACE_ENG;
     Task_Name=terameta_objects;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (object_database,object_name),
                    COLUMN (object_database),
                    COLUMN (object_name)
on {techex_t2_schema}.terameta_objects_ldg;

delete
from    {techex_t2_schema}.terameta_objects
;

insert into {techex_t2_schema}.terameta_objects
select    object_database,
          object_name,
          object_database_name,
          object_kind,
          object_bus_desc,
          business_process,
          terameta_appid,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.terameta_objects_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (object_database,object_name),
                    COLUMN (object_database),
                    COLUMN (object_name)
on {techex_t2_schema}.terameta_objects;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
