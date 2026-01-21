/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_teradata_big_drop_list_11521_ACE_ENG;
     Task_Name=teradata_big_drop_list;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename) 
on {techex_t2_schema}.teradata_big_drop_list_ldg
;

delete
from    {techex_t2_schema}.teradata_big_drop_list
;

insert into {techex_t2_schema}.teradata_big_drop_list
select    databasename,
          tablename, 
          object_type,
          update_date,
          comments,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.teradata_big_drop_list_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename) 
on {techex_t2_schema}.teradata_big_drop_list
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
